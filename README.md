# 🚀 Space Pipeline — Big Data ML & Streaming + IA

Pipeline de traitement de données planétaires combinant **NASA API**, **Kafka**,
**Spark Streaming**, **ML**, **HDFS**, **MinIO** et **MongoDB**.

---

## 📐 Architecture complète

```
NASA Exoplanet Archive (API gratuite)
        │
        │  HTTP depuis votre PC
        ▼
kafka_producer.py  (votre PC)
        │
        │  1. Récupère les vraies exoplanètes NASA + planètes locales
        │  2. Simule des capteurs physiquement cohérents
        │     (température, flux stellaire, radiations, O2, vent...)
        │  3. Envoie chaque lecture toutes les 2s → Kafka
        │
        ▼
Kafka (Docker)  ── topic: planet_sensors
        │
        ▼
spark_streaming.py  (Docker)
        │
        │  Toutes les 20 secondes (batch) :
        │  ├─ Score d'habitabilité (UDF physique)
        │  ├─ Alertes (radiation, séisme, température...)
        │  ├─ CSV brut       → MinIO (space-streaming/raw/)
        │  ├─ Appel Claude AI → analyse astrophysique
        │  ├─ CSV traité     → MinIO (space-streaming/processed/)
        │  ├─ Documents      → MongoDB (space_pipeline.planet_readings)
        │  └─ Parquet        → HDFS
        │
        ├─────────────────┬──────────────────┬────────────────
        ▼                 ▼                  ▼                ▼
    Console          MinIO CSV           MongoDB           HDFS
    (terminal)    raw/ + processed/   (localhost:8081)  (localhost:9870)
                  (localhost:9001)

generate_systems.py ──► planets.csv ──► HDFS ──► batch_ml.py (Random Forest)
```

---

## 🧰 Prérequis

| Outil          | Version minimale | Vérification             |
|----------------|-----------------|--------------------------|
| Docker Desktop | 24+             | `docker --version`       |
| Docker Compose | v2+             | `docker compose version` |
| Python         | 3.10+           | `python --version`       |

Dépendances Python locales :

```powershell
pip install pandas kafka-python requests
```

---

## 📁 Structure du projet

```
.
├── docker-compose.yml        ← mis à jour (MongoDB + Mongo Express)
├── hadoop.env
└── app/
    ├── generate_systems.py   # Génération planètes fictives
    ├── kafka_producer.py     # Producteur capteurs (NASA + simulation)
    ├── spark_streaming.py    # Streaming + IA + MinIO CSV + MongoDB
    └── batch_ml.py           # Pipeline ML batch (Random Forest)
```

> ⚠️ **Windows PowerShell** : toutes les commandes `docker exec` sur **une seule ligne**.

---
---

## 🚀 Lancement — Étape par étape

### Étape 1 — Démarrer l'infrastructure

```powershell
docker compose up -d
```

Vérifier que tout est démarré (attendre 45-60 secondes) :

```powershell
docker compose ps
```

Tous les services doivent afficher `running` ou `Up`.

---

### Étape 2 — Installer les dépendances Python dans Spark

```powershell
docker exec -it spark-master pip install requests pymongo minio
```

---

### Étape 3 — Générer les données planétaires locales

```powershell
cd "D:\space planer\app"
python generate_systems.py
```

Produit `planets.csv` et `star_systems.csv`.

---

### Étape 4 — Charger `planets.csv` dans HDFS

```powershell
docker exec -it namenode hdfs dfs -mkdir -p /users/space/input
docker cp "D:\space planer\app\planets.csv" namenode:/tmp/planets.csv
docker exec -it namenode hdfs dfs -put /tmp/planets.csv /users/space/input/
```

---

### Étape 5 — Lancer le Spark Streaming (Terminal 1)

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/app/spark_streaming.py
```

Attendez de voir :

```
✅ Streaming v5 lancé !
   📦 MinIO brut      : space-streaming/raw/
   📦 MinIO traité    : space-streaming/processed/
   🍃 MongoDB         : space_pipeline.planet_readings
   ⏱️  Batch interval  : 20 secondes
```

---

### Étape 6 — Lancer le producteur Kafka (Terminal 2)

```powershell
cd "D:\space planer\app"

# Mode recommandé : NASA + locales, délai 2s entre chaque planète
python kafka_producer.py --source both --delay 2.0

# Uniquement locales (pas besoin d'internet)
python kafka_producer.py --source local --delay 2.0

# Uniquement NASA
python kafka_producer.py --source nasa --delay 2.0

# Cycles limités (ex: 5 cycles puis arrêt)
python kafka_producer.py --source both --rounds 5 --delay 2.0
```

Toutes les **20 secondes**, Spark déclenche un batch qui :
- Sauvegarde le CSV brut dans MinIO
- Appelle AI pour analyse
- Sauvegarde le CSV traité dans MinIO
- Insère dans MongoDB

---

### Étape 7 — Lancer le pipeline ML batch (optionnel, Terminal 3)

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master local[2] --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/app/batch_ml.py
```

---

## 🖥️ Interfaces Web

| Service              | URL                    | Identifiants                  |
|----------------------|------------------------|-------------------------------|
| **MinIO Console**    | http://localhost:9001  | `minioadmin` / `minioadmin`   |
| **Mongo Express**    | http://localhost:8081  | aucun (accès libre)           |
| **HDFS NameNode**    | http://localhost:9870  | —                             |
| **Spark Master**     | http://localhost:8080  | —                             |

---

## 📦 Structure MinIO (bucket: space-streaming)

```
space-streaming/
├── raw/
│   ├── epoch_0001_20260423_142500.csv   ← données brutes capteurs
│   ├── epoch_0002_20260423_142520.csv
│   └── ...
└── processed/
    ├── epoch_0001_20260423_142510.csv   ← données + score + analyse IA
    ├── epoch_0002_20260423_142530.csv
    └── ...
```

**Différence entre raw et processed :**

| Champ | raw | processed |
|---|---|---|
| Données capteurs | ✅ | ✅ |
| habitability_score | ❌ | ✅ |
| habitability_class | ❌ | ✅ |
| alerts | ❌ | ✅ |
| ai_analysis | ❌ | ✅ |

---

## 🍃 MongoDB — Structure des documents

Chaque document inséré dans `space_pipeline.planet_readings` contient :

```json
{
  "planet_name": "Aeloria-I",
  "system_name": "Aeloria Prime",
  "star_type": "Naine jaune",
  "planet_type": "Tellurique",
  "temperature_c": 14.8,
  "stellar_flux": 1.1080,
  "surface_radiation_msv_h": 0.000128,
  "atmosphere_o2": 19.8,
  "habitability_score": 83.0,
  "habitability_class": "habitable",
  "alerts": "RAS",
  "ai_analysis": "Aeloria-I présente des conditions remarquablement proches...",
  "batch_id": 3,
  "processed_at": "2026-04-23T14:25:10Z",
  "source": "local"
}
```

Pour consulter via Mongo Express : **http://localhost:8081**
→ Database: `space_pipeline` → Collection: `planet_readings`

---

## 🟢 Score d'habitabilité

| Score   | Classe       | Signification                       |
|---------|--------------|-------------------------------------|
| ≥ 60    | habitable    | Conditions favorables à la vie      |
| 30–59   | inconnue     | Conditions limites                  |
| < 30    | non_habitable| Conditions incompatibles            |

Critères : température, flux stellaire, O₂, CO₂, N₂, eau, champ magnétique,
radiations, gravité, végétation, humidité, activité sismique, vent,
verrouillage marée, rétention atmosphérique.

---

## 🛑 Arrêter

```powershell
# Arrêter le producteur
Ctrl+C

# Arrêter le streaming
Ctrl+C

# Arrêter tous les containers
docker compose down

# Arrêter ET supprimer toutes les données (HDFS + MinIO + MongoDB)
docker compose down -v
```

---

## 🐛 Erreurs fréquentes et solutions

---

### ❌ `ModuleNotFoundError: No module named 'pandas'`

```powershell
pip install pandas kafka-python requests
```

---

### ❌ `No such file or directory: 'planets.csv'`

```powershell
cd "D:\space planer\app"
python generate_systems.py
python kafka_producer.py
```

---

### ❌ `Kafka pas prêt, retry dans 5s...`

```powershell
docker compose ps
docker logs kafka
docker compose restart kafka
# Attendre 15s puis relancer le producteur
```

---

### ❌ `Class org.apache.hadoop.fs.s3a.S3AFileSystem not found`

Toujours inclure `--packages` dans la commande spark-submit :

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/app/batch_ml.py
```

---

### ❌ `OCI runtime exec failed: exec: "\\"`

Écrire la commande sur **une seule ligne** dans PowerShell. Ne jamais utiliser `\`.

---

### ❌ `Erreur MongoDB : ServerSelectionTimeoutError`

```powershell
# Vérifier que MongoDB est démarré
docker compose ps mongodb

# Relancer si nécessaire
docker compose restart mongodb

# Vérifier les logs
docker logs mongodb
```

---

### ❌ `Erreur MinIO CSV : ...`

```powershell
# Vérifier que MinIO est accessible
docker compose ps minio

# Vérifier que le bucket existe
docker exec -it minio-init mc ls local/
```

---

### ❌ Le streaming ne reçoit rien (batch vide en boucle)

Le producteur n'est pas lancé. Ouvrir un second terminal :

```powershell
cd "D:\space planer\app"
python kafka_producer.py --source both --delay 2.0
```

---

### ❌ `WARN KafkaDataConsumer: not running in UninterruptibleThread`

Warning ignorable — comportement normal en mode `local[2]`.

---

### ❌ `WARN MetricsConfig: Cannot locate configuration`

Warning ignorable — fichier de configuration optionnel absent.
