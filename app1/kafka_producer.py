"""
kafka_producer.py
─────────────────
Simule une sonde spatiale qui envoie les données planétaires
vers un topic Kafka.

Comportement :
  - Lit planets.csv
  - Maintient un registre local des planètes déjà envoyées
  - N'envoie un message que si la planète est nouvelle
    (première réception = découverte)
  - Envoie les données planet par planet avec un délai configurable

Usage :
  python kafka_producer.py [--delay 2] [--topic planets]
"""

import json
import time
import argparse
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

# ─── CONFIG ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:29092"   # Depuis l'intérieur du réseau Docker
TOPIC_NAME      = "planets"
CSV_PATH        = "planets.csv"
REGISTRY_PATH   = "known_planets.json"   # Planètes déjà connues


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def load_registry() -> set:
    """Charge le registre des planètes déjà connues."""
    try:
        with open(REGISTRY_PATH, "r") as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()


def save_registry(registry: set) -> None:
    """Sauvegarde le registre mis à jour."""
    with open(REGISTRY_PATH, "w") as f:
        json.dump(list(registry), f)


def create_producer() -> KafkaProducer:
    """Crée le producteur Kafka avec retry."""
    retries = 10
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            print(f"✅ Connecté à Kafka ({KAFKA_BOOTSTRAP})")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Kafka pas encore prêt ({attempt+1}/{retries}), retry dans 5s...")
            time.sleep(5)
    raise RuntimeError("Impossible de se connecter à Kafka après plusieurs tentatives.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main(delay: float, topic: str) -> None:
    df = pd.read_csv(CSV_PATH)
    registry = load_registry()
    producer = create_producer()

    print(f"\n🚀 Démarrage de la transmission vers le topic '{topic}'")
    print(f"   Planètes déjà connues : {len(registry)}\n")

    new_discoveries = 0

    for _, row in df.iterrows():
        planet_name = row["name"]
        planet_data = row.to_dict()

        # Ajout d'un timestamp de réception (simulation temps réel)
        planet_data["received_at"] = datetime.utcnow().isoformat()

        # ── Vérification : planète déjà connue ? ────────────────────────────
        if planet_name in registry:
            print(f"   ⏭️  {planet_name} déjà enregistrée, message ignoré.")
            continue

        # ── Nouvelle planète → on l'envoie ──────────────────────────────────
        future = producer.send(
            topic=topic,
            key=planet_name,
            value=planet_data,
        )

        try:
            metadata = future.get(timeout=10)
            registry.add(planet_name)
            new_discoveries += 1
            print(
                f"   📡 [{metadata.offset:>4}] Planète '{planet_name}' transmise "
                f"(partition {metadata.partition}, label={row['label']})"
            )
        except Exception as e:
            print(f"   ❌ Erreur lors de l'envoi de '{planet_name}': {e}")

        time.sleep(delay)

    producer.flush()
    save_registry(registry)

    print(f"\n✅ Transmission terminée.")
    print(f"   Nouvelles découvertes envoyées : {new_discoveries}")
    print(f"   Total planètes connues        : {len(registry)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Producer – Sonde Spatiale")
    parser.add_argument("--delay", type=float, default=2.0, help="Délai entre chaque message (secondes)")
    parser.add_argument("--topic", type=str, default=TOPIC_NAME, help="Topic Kafka cible")
    args = parser.parse_args()
    main(delay=args.delay, topic=args.topic)