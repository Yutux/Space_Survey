"""
kafka_producer.py  (v5 — Physique cohérente)
─────────────────────────────────────────────
Simule des capteurs planétaires avec des mesures physiquement cohérentes.

Cohérence assurée :
  - Radiations calculées depuis luminosité étoile + distance + champ magnétique
  - Température varie autour de la température d'équilibre réelle
  - stellar_flux envoyé pour scoring en temps réel
  - Noms de colonnes alignés avec batch_ml.py et generate_systems.py

Usage :
  python kafka_producer.py [--delay 1.0] [--source local|nasa|both]
"""

import json
import time
import math
import random
import argparse
import requests
import pandas as pd
from io import StringIO
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC_SENSORS   = "planet_sensors"

NASA_API_URL = (
    "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    "?query=select+hostname,pl_name,st_spectype,st_lum,st_age,"
    "pl_orbsmax,pl_rade,pl_bmasse,pl_eqt"
    "+from+ps"
    "+where+default_flag=1"
    "+and+pl_eqt+is+not+null"
    "+and+pl_orbsmax+is+not+null"
    "+and+pl_bmasse+is+not+null"
    "+and+pl_rade+is+not+null"
    "+order+by+hostname"
    "&format=csv"
)

# ─── PROPRIÉTÉS STELLAIRES ───────────────────────────────────────────────────
# Utilisées pour calculer les radiations physiquement correctes
STAR_PROPERTIES = {
    "Naine rouge":    {"luminosity": 0.01,  "xuv_factor": 10.0,  "tidal_lock_au": 0.3},
    "Naine orange":   {"luminosity": 0.3,   "xuv_factor": 2.0,   "tidal_lock_au": 0.15},
    "Naine jaune":    {"luminosity": 1.0,   "xuv_factor": 1.0,   "tidal_lock_au": 0.05},
    "Naine blanche":  {"luminosity": 0.001, "xuv_factor": 50.0,  "tidal_lock_au": 0.5},
    "Geante bleue":   {"luminosity": 50000, "xuv_factor": 100000.0, "tidal_lock_au": 0.0},
    "Geante rouge":   {"luminosity": 1000,  "xuv_factor": 5.0,   "tidal_lock_au": 0.0},
    "Sous-geante":    {"luminosity": 3.0,   "xuv_factor": 1.5,   "tidal_lock_au": 0.05},
    "Unknown":        {"luminosity": 1.0,   "xuv_factor": 1.0,   "tidal_lock_au": 0.05},
}

# ─── PROFILS CAPTEURS PAR TYPE DE PLANÈTE ────────────────────────────────────
# Uniquement les capteurs qui NE dépendent PAS de l'étoile
# (radiation calculée séparément via physique stellaire)
PLANET_PROFILES = {
    "Tellurique": {
        "wind_speed":   (20,  80),
        "pressure":     (900, 1100),
        "vegetation":   (0.0, 0.6),
        "humidity":     (10,  80),
        "seismic":      (0,   4),
        "magnetic_ut":  (20,  60),
    },
    "Ocean": {
        "wind_speed":   (30,  120),
        "pressure":     (1000, 1100),
        "vegetation":   (0.4, 0.9),
        "humidity":     (80,  100),
        "seismic":      (0,   3),
        "magnetic_ut":  (25,  55),
    },
    "Super-Terre": {
        "wind_speed":   (50,  200),
        "pressure":     (1100, 2000),
        "vegetation":   (0.0, 0.4),
        "humidity":     (20,  70),
        "seismic":      (1,   6),
        "magnetic_ut":  (30,  90),
    },
    "Gazeuse": {
        "wind_speed":   (500, 2000),
        "pressure":     (10000, 100000),
        "vegetation":   (0.0, 0.0),
        "humidity":     (0,   5),
        "seismic":      (0,   2),
        "magnetic_ut":  (100, 1000),
    },
    "Geante gazeuse": {
        "wind_speed":   (800, 3000),
        "pressure":     (50000, 500000),
        "vegetation":   (0.0, 0.0),
        "humidity":     (0,   2),
        "seismic":      (0,   1),
        "magnetic_ut":  (500, 5000),
    },
    "Naine glacee": {
        "wind_speed":   (0,   30),
        "pressure":     (0.1, 10),
        "vegetation":   (0.0, 0.0),
        "humidity":     (0,   5),
        "seismic":      (0,   2),
        "magnetic_ut":  (1,   15),
    },
    "Lave": {
        "wind_speed":   (100, 500),
        "pressure":     (50,  500),
        "vegetation":   (0.0, 0.0),
        "humidity":     (0,   2),
        "seismic":      (5,   10),
        "magnetic_ut":  (5,   30),
    },
    "Unknown": {
        "wind_speed":   (0,   100),
        "pressure":     (100, 1000),
        "vegetation":   (0.0, 0.2),
        "humidity":     (0,  50),
        "seismic":      (0,   5),
        "magnetic_ut":  (5,   50),
    },
}


def rand_in(min_val: float, max_val: float, noise: float = 0.1) -> float:
    base  = random.uniform(min_val, max_val)
    sigma = (max_val - min_val) * noise
    return round(max(min_val * 0.5, base + random.gauss(0, sigma)), 3)


def compute_stellar_flux(luminosity: float, distance_au: float) -> float:
    """Flux stellaire relatif (1.0 = Terre autour du Soleil)."""
    if distance_au <= 0:
        return 99999.0
    return round(luminosity / (distance_au ** 2), 6)


def compute_surface_radiation(
    luminosity: float, distance_au: float,
    xuv_factor: float, magnetic_field: bool,
    atmosphere_n2: float
) -> float:
    """
    Radiation de surface en mSv/h — physiquement cohérente.
    - Flux stellaire XUV atténué par champ magnétique et atmosphère
    - Calibré : Terre ≈ 0.00011 mSv/h
    """
    flux    = luminosity / max(distance_au ** 2, 1e-6)
    raw_rad = flux * xuv_factor * 0.0001

    if magnetic_field:
        raw_rad *= 0.1   # champ magnétique → -90%

    # Atmosphère azotée dense → protection supplémentaire
    atm_factor = max(0.01, atmosphere_n2 / 78.0)
    raw_rad /= atm_factor

    # Bruit capteur ±10%
    noise = random.gauss(0, raw_rad * 0.1)
    return round(max(0.00001, raw_rad + noise), 6)


def is_tidally_locked(distance_au: float, tidal_lock_au: float) -> bool:
    return distance_au <= tidal_lock_au


def simulate_sensor(planet: dict, tick: int) -> dict:
    """
    Génère une lecture de capteur physiquement cohérente.
    Les radiations dépendent de l'étoile + distance + magnétisme.
    La température varie autour de la température d'équilibre calculée.
    """
    ptype      = planet.get("planet_type", "Unknown")
    profile    = PLANET_PROFILES.get(ptype, PLANET_PROFILES["Unknown"])
    star_type  = planet.get("star_type", "Unknown")
    star_props = STAR_PROPERTIES.get(star_type, STAR_PROPERTIES["Unknown"])

    distance       = planet.get("distance_au", 1.0)
    magnetic_field = planet.get("magnetic_field", False)
    atm_n2         = planet.get("atmosphere_n2", 78.0)

    # ── Température : cycle jour/nuit autour de la base physique ─────────────
    base_temp   = planet.get("avg_temp_celsius", 20.0)
    day_cycle   = math.sin(tick * 0.1) * 5      # ±5°C cycle jour/nuit
    temp_noise  = random.gauss(0, 2)             # bruit capteur ±2°C
    temperature = round(base_temp + day_cycle + temp_noise, 2)

    # ── Gravité : légère variation ────────────────────────────────────────────
    gravity = round(planet.get("gravity_g", 1.0) + random.gauss(0, 0.005), 4)

    # ── Flux stellaire et radiations (physique réelle) ────────────────────────
    s_flux   = compute_stellar_flux(star_props["luminosity"], distance)
    surf_rad = compute_surface_radiation(
        star_props["luminosity"], distance,
        star_props["xuv_factor"], magnetic_field, atm_n2
    )

    # ── Verrouillage marée ───────────────────────────────────────────────────
    tidal = is_tidally_locked(distance, star_props["tidal_lock_au"])

    return {
        # ── Identité ──────────────────────────────────────────────────────────
        "planet_name":      planet["name"],
        "system_name":      planet["system_name"],
        "star_type":        star_type,
        "planet_type":      ptype,

        # ── Données orbitales ─────────────────────────────────────────────────
        "distance_au":      distance,
        "mass_earth":       planet.get("mass_earth", 1.0),
        "radius_km":        planet.get("radius_km", 6371),

        # ── Capteurs température et gravité ───────────────────────────────────
        "temperature_c":    temperature,        # pour le score temps réel
        "avg_temp_celsius": base_temp,          # température de référence
        "gravity_g":        gravity,

        # ── Flux et radiations (physique stellaire) ───────────────────────────
        "stellar_flux":              s_flux,
        "surface_radiation_msv_h":   surf_rad,
        "radiation_msv_h":           surf_rad,  # alias pour le score streaming

        # ── Capteurs atmosphériques ───────────────────────────────────────────
        "wind_speed_kmh":   rand_in(*profile["wind_speed"]),
        "pressure_hpa":     rand_in(*profile["pressure"]),
        "vegetation_index": round(rand_in(*profile["vegetation"]), 3),
        "humidity_pct":     rand_in(*profile["humidity"]),
        "seismic_activity": round(rand_in(*profile["seismic"]), 2),
        "magnetic_field_ut":rand_in(*profile["magnetic_ut"]),

        # ── Composition atmosphérique (valeurs de base + bruit) ───────────────
        "atmosphere_o2":    round(max(0.0, planet.get("atmosphere_o2", 0) + random.gauss(0, 0.3)), 2),
        "atmosphere_co2":   round(max(0.0, planet.get("atmosphere_co2", 0) + random.gauss(0, 0.2)), 2),
        "atmosphere_n2":    round(max(0.0, atm_n2 + random.gauss(0, 0.5)), 2),
        "o2_pct":           round(max(0.0, planet.get("atmosphere_o2", 0) + random.gauss(0, 0.3)), 2),
        "co2_ppm":          round(max(0.0, planet.get("atmosphere_co2", 0) * 10000 + random.gauss(0, 100)), 1),

        # ── Conditions de surface ─────────────────────────────────────────────
        "has_water_int":          int(planet.get("has_water", False)),
        "magnetic_field_int":     int(magnetic_field),
        "in_hz_int":              int(planet.get("in_habitable_zone", False)),
        "tidally_locked_int":     int(tidal),
        "retains_atmosphere_int": int(planet.get("retains_atmosphere", True)),
        "habitable_candidate_int":int(planet.get("habitable_candidate", False)),

        # ── Métadonnées ───────────────────────────────────────────────────────
        "tick":      tick,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source":    planet.get("source", "local"),
    }


# ─── CHARGEMENT DES PLANÈTES ─────────────────────────────────────────────────

def load_local_planets(path: str = "planets.csv") -> list:
    try:
        df = pd.read_csv(path)
        planets = []
        for _, row in df.iterrows():
            planets.append({
                "name":                  row["name"],
                "system_name":           row["system_name"],
                "star_type":             row["star_type"],
                "planet_type":           row["planet_type"],
                "distance_au":           float(row["distance_au"]),
                "mass_earth":            float(row["mass_earth"]),
                "radius_km":             float(row["radius_km"]),
                "gravity_g":             float(row["gravity_g"]),
                "avg_temp_celsius":      float(row["avg_temp_celsius"]),
                "atmosphere_o2":         float(row.get("atmosphere_o2", 0)),
                "atmosphere_co2":        float(row.get("atmosphere_co2", 0)),
                "atmosphere_n2":         float(row.get("atmosphere_n2", 0)),
                "has_water":             bool(row.get("has_water", False)),
                "magnetic_field":        bool(row.get("magnetic_field", False)),
                "in_habitable_zone":     bool(row.get("in_habitable_zone", False)),
                "retains_atmosphere":    bool(row.get("retains_atmosphere", True)),
                "habitable_candidate":   bool(row.get("habitable_candidate", False)),
                "source":                "local",
            })
        print(f"   📁 {len(planets)} planètes fictives chargées depuis {path}")
        return planets
    except FileNotFoundError:
        print(f"   ⚠️  {path} introuvable.")
        return []


def load_nasa_planets(limit: int = 30) -> list:
    try:
        print("   🌐 Requête NASA Exoplanet Archive...")
        resp = requests.get(NASA_API_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text)).head(limit)
        planets = []
        for _, row in df.iterrows():
            temp_k   = float(row["pl_eqt"]) if pd.notna(row["pl_eqt"]) else 300
            temp_c   = round(temp_k - 273.15, 1)
            radius_c = float(row["pl_rade"]) if pd.notna(row["pl_rade"]) else 1.0
            mass     = float(row["pl_bmasse"]) if pd.notna(row["pl_bmasse"]) else 1.0

            if radius_c < 1.5 and mass < 2:
                ptype = "Tellurique"
            elif radius_c < 2.5 and mass < 10:
                ptype = "Super-Terre"
            elif radius_c < 6:
                ptype = "Gazeuse"
            else:
                ptype = "Geante gazeuse"

            dist = float(row["pl_orbsmax"]) if pd.notna(row["pl_orbsmax"]) else 1.0

            planets.append({
                "name":               str(row["pl_name"]),
                "system_name":        str(row["hostname"]),
                "star_type":          "Unknown",
                "planet_type":        ptype,
                "distance_au":        dist,
                "mass_earth":         mass,
                "radius_km":          round(radius_c * 6371, 1),
                "gravity_g":          round(mass / (radius_c ** 2), 3),
                "avg_temp_celsius":   temp_c,
                "atmosphere_o2":      0.0,
                "atmosphere_co2":     0.0,
                "atmosphere_n2":      0.0,
                "has_water":          False,
                "magnetic_field":     False,
                "in_habitable_zone":  False,
                "retains_atmosphere": True,
                "habitable_candidate":False,
                "source":             "nasa",
            })
        print(f"   🛸 {len(planets)} exoplanètes NASA chargées")
        return planets
    except Exception as e:
        print(f"   ❌ Erreur NASA API : {e}")
        return []


# ─── KAFKA ───────────────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            print(f"✅ Connecté à Kafka ({KAFKA_BOOTSTRAP})\n")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Kafka pas prêt ({attempt+1}/10), retry dans 5s...")
            time.sleep(5)
    raise RuntimeError("Impossible de se connecter à Kafka.")


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main(delay: float, source: str, rounds: int):
    print("🚀 Space Sensor Stream v5 — Démarrage\n")
    print("📦 Chargement des planètes...")

    planets = []
    if source in ("local", "both"):
        planets += load_local_planets()
    if source in ("nasa", "both"):
        planets += load_nasa_planets(limit=30)

    if not planets:
        print("❌ Aucune planète chargée.")
        return

    print(f"\n🌍 {len(planets)} planètes en surveillance\n")

    producer = create_producer()
    tick = 0

    print(f"📡 Envoi vers topic '{TOPIC_SENSORS}' (Ctrl+C pour stopper)\n")
    header = f"{'Planète':<28} {'Étoile':<16} {'Type':<15} {'Temp':>8} {'Flux':>8} {'Rad(mSv/h)':>12} {'O2':>6}"
    print(header)
    print("─" * len(header))

    try:
        while rounds == -1 or tick < rounds:
            for planet in planets:
                reading = simulate_sensor(planet, tick)

                producer.send(
                    TOPIC_SENSORS,
                    key=reading["planet_name"],
                    value=reading
                )

                icon = "🛸" if reading["source"] == "nasa" else "🌍"
                print(
                    f"{icon} {reading['planet_name']:<26} "
                    f"{reading['star_type']:<16} "
                    f"{reading['planet_type']:<15} "
                    f"{reading['temperature_c']:>7.1f}°C "
                    f"{reading['stellar_flux']:>8.4f} "
                    f"{reading['surface_radiation_msv_h']:>11.5f} "
                    f"{reading['atmosphere_o2']:>5.1f}%"
                )

                time.sleep(delay)

            tick += 1
            producer.flush()
            print(f"\n   ── Cycle {tick} ({len(planets)} lectures) ──\n")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\n⛔ Arrêt.")
    finally:
        producer.flush()
        producer.close()
        print(f"✅ {tick * len(planets)} lectures envoyées.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay",  type=float, default=0.5)
    parser.add_argument("--source", type=str,   default="both",
                        choices=["local", "nasa", "both"])
    parser.add_argument("--rounds", type=int,   default=-1)
    args = parser.parse_args()
    main(args.delay, args.source, args.rounds)