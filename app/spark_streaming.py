"""
spark_streaming.py  (v4 — Physique cohérente)
───────────────────────────────────────────────
Score d'habitabilité basé sur des contraintes astrophysiques réelles.

Corrections v4.1 :
  - Mapping labels ML depuis le modèle (ordre réel, pas hardcodé)
  - avg_temp_celsius utilisé pour le score (cohérent avec l'entraînement)
  - temperature_c utilisé uniquement pour les alertes temps réel
  - Colonnes vectorielles ML filtrées avant écriture MongoDB
  - lit(None).cast(StringType()) pour ml_predicted_label si modèle absent

Lancement :
  docker exec -it spark-master /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/app/spark_streaming.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, when, udf, round as spark_round, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, DoubleType
)
from pyspark.ml import PipelineModel
from pymongo import MongoClient
from datetime import datetime

KAFKA_BOOTSTRAP  = "kafka:29092"
KAFKA_TOPIC      = "planet_sensors"
HDFS_OUTPUT      = "hdfs://namenode:9000/users/space/streaming_output"
MODEL_PATH       = "hdfs://namenode:9000/users/space/models/planet_classifier_v4"
MINIO_ENDPOINT   = "http://minio:9090"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_RAW_DATA   = "s3a://space-raw/readings"
MINIO_OUTPUT     = "s3a://space-streaming/results"
MINIO_MODELS     = "s3a://space-models/planet_classifier_v4"

MONGODB_HOST                 = "mongodb"
MONGODB_PORT                 = 27017
MONGODB_USER                 = "mongoadmin"
MONGODB_PASS                 = "mongoadmin"
MONGODB_DB                   = "space_pipeline"
MONGODB_COLLECTION_PROCESSED = "processed_readings"

CHECKPOINT_CONSOLE = "hdfs://namenode:9000/users/space/checkpoints/streaming_v4_console"
CHECKPOINT_HDFS    = "hdfs://namenode:9000/users/space/checkpoints/streaming_v4_hdfs"
CHECKPOINT_MINIO   = "hdfs://namenode:9000/users/space/checkpoints/streaming_v4_minio"
CHECKPOINT_RAW     = "hdfs://namenode:9000/users/space/checkpoints/streaming_v4_raw"
CHECKPOINT_MONGODB = "hdfs://namenode:9000/users/space/checkpoints/streaming_v4_mongodb"

# ─── SCHÉMA KAFKA ─────────────────────────────────────────────────────────────
sensor_schema = StructType([
    StructField("planet_name",              StringType(),  True),
    StructField("system_name",              StringType(),  True),
    StructField("star_type",                StringType(),  True),
    StructField("planet_type",              StringType(),  True),
    StructField("distance_au",              DoubleType(),  True),
    StructField("mass_earth",               DoubleType(),  True),
    StructField("radius_km",                DoubleType(),  True),

    # Capteurs physiques
    StructField("temperature_c",            DoubleType(),  True),
    StructField("avg_temp_celsius",         DoubleType(),  True),
    StructField("gravity_g",                DoubleType(),  True),
    StructField("stellar_flux",             DoubleType(),  True),
    StructField("surface_radiation_msv_h",  DoubleType(),  True),
    StructField("radiation_msv_h",          DoubleType(),  True),

    # Capteurs atmosphériques
    StructField("wind_speed_kmh",           DoubleType(),  True),
    StructField("pressure_hpa",             DoubleType(),  True),
    StructField("vegetation_index",         DoubleType(),  True),
    StructField("humidity_pct",             DoubleType(),  True),
    StructField("seismic_activity",         DoubleType(),  True),
    StructField("magnetic_field_ut",        DoubleType(),  True),

    # Composition atmosphérique
    StructField("atmosphere_o2",            DoubleType(),  True),
    StructField("atmosphere_co2",           DoubleType(),  True),
    StructField("atmosphere_n2",            DoubleType(),  True),
    StructField("o2_pct",                   DoubleType(),  True),
    StructField("co2_ppm",                  DoubleType(),  True),

    # Conditions de surface (int)
    StructField("has_water_int",            IntegerType(), True),
    StructField("magnetic_field_int",       IntegerType(), True),
    StructField("in_hz_int",                IntegerType(), True),
    StructField("tidally_locked_int",       IntegerType(), True),
    StructField("retains_atmosphere_int",   IntegerType(), True),
    StructField("habitable_candidate_int",  IntegerType(), True),

    StructField("tick",                     IntegerType(), True),
    StructField("timestamp",                StringType(),  True),
    StructField("source",                   StringType(),  True),
])


# ─── UDF : SCORE D'HABITABILITÉ ──────────────────────────────────────────────
# Utilise avg_temp_celsius (température de référence physique) pour être
# cohérent avec les données d'entraînement du modèle ML.
# temperature_c (avec bruit capteur) est réservé aux alertes temps réel.
@udf(returnType=FloatType())
def compute_habitability_score(
    planet_type, stellar_flux, surface_radiation_msv_h,
    avg_temp_celsius, gravity_g, atmosphere_o2, atmosphere_co2, atmosphere_n2,
    has_water_int, magnetic_field_int, in_hz_int,
    tidally_locked_int, retains_atmosphere_int, habitable_candidate_int,
    wind_speed_kmh, seismic_activity, vegetation_index, humidity_pct
):
    # ── ÉLIMINATOIRES ABSOLUS ─────────────────────────────────────────────────
    if habitable_candidate_int == 0:
        return 0.0

    if stellar_flux is not None and stellar_flux > 10.0:
        return 0.0

    if surface_radiation_msv_h is not None and surface_radiation_msv_h > 5.0:
        return 0.0

    if avg_temp_celsius is not None and (avg_temp_celsius < -80 or avg_temp_celsius > 80):
        return 0.0

    if retains_atmosphere_int == 0:
        return 0.0

    if tidally_locked_int == 1 and magnetic_field_int == 0:
        return 0.0

    # ── SCORE ─────────────────────────────────────────────────────────────────
    score = 0.0

    # Zone habitable (max 20 pts)
    if in_hz_int == 1:
        score += 20

    # Température (max 20 pts)
    if avg_temp_celsius is not None:
        if -10 <= avg_temp_celsius <= 50:   score += 20
        elif -30 <= avg_temp_celsius <= 65: score += 10
        elif -60 <= avg_temp_celsius <= 80: score += 3

    # Flux stellaire (max 15 pts)
    if stellar_flux is not None:
        if 0.25 <= stellar_flux <= 1.5:  score += 15
        elif 0.1 <= stellar_flux <= 3.0: score += 7
        elif stellar_flux <= 8.0:        score += 2

    # O2 (max 15 pts)
    if atmosphere_o2 is not None:
        if 18 <= atmosphere_o2 <= 25:    score += 15
        elif 12 <= atmosphere_o2 <= 30:  score += 8
        elif atmosphere_o2 >= 5:         score += 3

    # Eau (max 10 pts)
    if has_water_int == 1:
        score += 10

    # Champ magnétique (max 10 pts)
    if magnetic_field_int == 1:
        score += 10
    elif tidally_locked_int == 1:
        score -= 10

    # Radiations (max 10 pts)
    if surface_radiation_msv_h is not None:
        if surface_radiation_msv_h <= 0.1:   score += 10
        elif surface_radiation_msv_h <= 0.5: score += 6
        elif surface_radiation_msv_h <= 2.0: score += 2
        elif surface_radiation_msv_h <= 5.0: score -= 5

    # Atmosphère N2 dense (max 5 pts)
    if atmosphere_n2 is not None:
        if atmosphere_n2 >= 70:   score += 5
        elif atmosphere_n2 >= 40: score += 2

    # CO2 (malus effet de serre)
    if atmosphere_co2 is not None:
        if atmosphere_co2 > 10:   score -= 15
        elif atmosphere_co2 > 5:  score -= 8
        elif atmosphere_co2 > 2:  score -= 3

    # Gravité
    if gravity_g is not None:
        if 0.7 <= gravity_g <= 1.5:   score += 3
        elif 0.4 <= gravity_g <= 2.5: score += 1
        elif gravity_g < 0.4:         score -= 10

    # Végétation (max 5 pts)
    if vegetation_index is not None:
        score += min(5, vegetation_index * 5)

    # Humidité (max 5 pts)
    if humidity_pct is not None:
        if 20 <= humidity_pct <= 80: score += 5
        elif humidity_pct >= 10:     score += 2

    # Type de planète (bonus)
    if planet_type == "Ocean":         score += 5
    elif planet_type == "Tellurique":  score += 3
    elif planet_type == "Super-Terre": score += 1

    # Activité sismique (malus)
    if seismic_activity is not None:
        if seismic_activity >= 8:    score -= 15
        elif seismic_activity >= 6:  score -= 8
        elif seismic_activity >= 4:  score -= 3

    # Vent extrême (malus)
    if wind_speed_kmh is not None:
        if wind_speed_kmh > 500:   score -= 15
        elif wind_speed_kmh > 200: score -= 5

    return max(0.0, min(100.0, float(score)))


# ─── UDF : ALERTES ────────────────────────────────────────────────────────────
@udf(returnType=StringType())
def compute_alerts(
    temperature_c, surface_radiation_msv_h, seismic_activity,
    wind_speed_kmh, atmosphere_o2, stellar_flux, tidally_locked_int
):
    alerts = []
    if temperature_c is not None:
        if temperature_c > 80:    alerts.append("🔥 TEMPERATURE CRITIQUE")
        elif temperature_c < -80: alerts.append("🧊 FROID EXTREME")
    if surface_radiation_msv_h is not None:
        if surface_radiation_msv_h > 5.0:  alerts.append("☢️  RADIATION LETALE")
        elif surface_radiation_msv_h > 2.0: alerts.append("⚠️  RADIATION ELEVEE")
    if seismic_activity is not None and seismic_activity >= 7:
        alerts.append("🌋 SEISME MAJEUR")
    if wind_speed_kmh is not None and wind_speed_kmh > 400:
        alerts.append("🌪️  TEMPETE EXTREME")
    if atmosphere_o2 is not None and atmosphere_o2 < 5:
        alerts.append("😮 O2 INSUFFISANT")
    if stellar_flux is not None and stellar_flux > 10:
        alerts.append("☀️  FLUX STELLAIRE FATAL")
    if tidally_locked_int == 1:
        alerts.append("🔒 VERROUILLAGE MAREE")
    return " | ".join(alerts) if alerts else "✅ RAS"


# ─── SESSION SPARK ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SpaceSensorStreaming_v4")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint",                 MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",               MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",               MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",        "true")
    .config("spark.hadoop.fs.s3a.impl",                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("🚀 SparkSession initialisée — Space Sensor Streaming v4")


# ─── FONCTION : ÉCRITURE VERS MONGODB ────────────────────────────────────────
# Colonnes vectorielles Spark exclues — PyMongo ne sait pas les sérialiser
VECTOR_COLS = {
    "star_type_idx", "planet_type_idx", "label_index",
    "star_type_vec", "planet_type_vec",
    "features_raw", "features",
    "rawPrediction", "probability", "prediction"
}

def write_to_mongodb(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    cols_to_keep = [c for c in batch_df.columns if c not in VECTOR_COLS]
    clean_df = batch_df.select(cols_to_keep)

    try:
        client = MongoClient(
            f"mongodb://{MONGODB_USER}:{MONGODB_PASS}@{MONGODB_HOST}:{MONGODB_PORT}/",
            serverSelectionTimeoutMS=5000
        )
        db         = client[MONGODB_DB]
        collection = db[MONGODB_COLLECTION_PROCESSED]

        records   = clean_df.collect()
        documents = []
        for row in records:
            doc = row.asDict()
            doc["_inserted_at"] = datetime.utcnow()
            doc["_batch_id"]    = batch_id
            documents.append(doc)

        if documents:
            result = collection.insert_many(documents)
            print(f"   ✅ MongoDB : {len(result.inserted_ids)} documents insérés (batch {batch_id})")

        client.close()
    except Exception as e:
        print(f"   ⚠️  Erreur MongoDB batch {batch_id} : {e}")


# ─── CHARGEMENT DU MODÈLE ML ─────────────────────────────────────────────────
print("📦 Chargement du modèle ML entraîné...")
model  = None
labels = None

try:
    model = PipelineModel.load(MODEL_PATH)
    print(f"✅ Modèle chargé depuis HDFS : {MODEL_PATH}")
except Exception as e:
    try:
        model = PipelineModel.load(MINIO_MODELS)
        print(f"✅ Modèle chargé depuis MinIO : {MINIO_MODELS}")
    except Exception as e2:
        print(f"⚠️  Impossible de charger le modèle : {e2}")
        print("   Utilisation du score basé sur les règles uniquement.")

# Récupérer l'ordre RÉEL des labels depuis le StringIndexer (stage 2)
# Évite le mapping hardcodé qui peut être incorrect selon l'ordre d'entraînement
if model is not None:
    labels = model.stages[2].labels
    print(f"🏷️  Labels du modèle (ordre réel) : {labels}")


# ─── LECTURE KAFKA ────────────────────────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = raw_stream.select(
    from_json(col("value").cast("string"), sensor_schema).alias("d")
).select("d.*")


# ─── ENRICHISSEMENT ───────────────────────────────────────────────────────────
enriched = (
    parsed
    .withColumn("habitability_score", spark_round(
        compute_habitability_score(
            col("planet_type"),
            col("stellar_flux"),
            col("surface_radiation_msv_h"),
            col("avg_temp_celsius"),        # ← référence physique (cohérent ML)
            col("gravity_g"),
            col("atmosphere_o2"),
            col("atmosphere_co2"),
            col("atmosphere_n2"),
            col("has_water_int"),
            col("magnetic_field_int"),
            col("in_hz_int"),
            col("tidally_locked_int"),
            col("retains_atmosphere_int"),
            col("habitable_candidate_int"),
            col("wind_speed_kmh"),
            col("seismic_activity"),
            col("vegetation_index"),
            col("humidity_pct"),
        ), 1
    ))
    .withColumn("habitability_class",
        when(col("habitability_score") >= 60, "🟢 habitable")
        .when(col("habitability_score") >= 30, "🟡 inconnue")
        .otherwise("🔴 non_habitable")
    )
    .withColumn("alerts", compute_alerts(
        col("temperature_c"),               # ← capteur temps réel pour alertes
        col("surface_radiation_msv_h"),
        col("seismic_activity"),
        col("wind_speed_kmh"),
        col("atmosphere_o2"),
        col("stellar_flux"),
        col("tidally_locked_int"),
    ))
    .withColumn("processed_at", current_timestamp())
)


# ─── PRÉDICTIONS DU MODÈLE ML ────────────────────────────────────────────────
if model is not None and labels is not None:
    print("🤖 Application du modèle ML aux données de streaming...")

    predictions = model.transform(enriched)

    # Mapping depuis l'ordre RÉEL des labels — pas hardcodé
    labels_list = labels

    @udf(returnType=StringType())
    def map_prediction_to_label(prediction):
        if prediction is None:
            return "unknown"
        idx = int(prediction)
        if idx < len(labels_list):
            return labels_list[idx]
        return "unknown"

    enriched_final = predictions.withColumn(
        "ml_predicted_label",
        map_prediction_to_label(col("prediction"))
    )
    print("✅ Prédictions du modèle ajoutées au flux")

else:
    # Modèle absent → colonne vide typée (StringType obligatoire pour Parquet)
    enriched_final = enriched.withColumn(
        "ml_predicted_label",
        lit(None).cast(StringType())
    )


# ─── SORTIE CONSOLE ───────────────────────────────────────────────────────────
console_query = (
    enriched_final.select(
        "planet_name", "star_type", "planet_type",
        "stellar_flux", "surface_radiation_msv_h",
        "avg_temp_celsius", "temperature_c",
        "atmosphere_o2", "in_hz_int", "tidally_locked_int",
        "habitability_score", "habitability_class",
        col("ml_predicted_label").alias("ml_prediction"),
        "alerts"
    )
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("numRows", 30)
    .option("checkpointLocation", CHECKPOINT_CONSOLE)
    .start()
)

# ─── SORTIE DONNÉES BRUTES → MINIO ───────────────────────────────────────────
raw_query = (
    parsed.select(
        "planet_name", "system_name", "star_type", "planet_type",
        "distance_au", "mass_earth", "radius_km",
        "temperature_c", "avg_temp_celsius", "gravity_g",
        "stellar_flux", "surface_radiation_msv_h", "radiation_msv_h",
        "wind_speed_kmh", "pressure_hpa", "vegetation_index", "humidity_pct",
        "seismic_activity", "magnetic_field_ut",
        "atmosphere_o2", "atmosphere_co2", "atmosphere_n2",
        "o2_pct", "co2_ppm",
        "has_water_int", "magnetic_field_int", "in_hz_int",
        "tidally_locked_int", "retains_atmosphere_int", "habitable_candidate_int",
        "tick", "timestamp", "source"
    )
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", MINIO_RAW_DATA)
    .option("checkpointLocation", CHECKPOINT_RAW)
    .start()
)
print(f"📦 Données brutes → MinIO : {MINIO_RAW_DATA}")

# ─── SORTIE DONNÉES TRAITÉES → HDFS ──────────────────────────────────────────
hdfs_query = (
    enriched_final.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", HDFS_OUTPUT)
    .option("checkpointLocation", CHECKPOINT_HDFS)
    .start()
)
print(f"💾 Données traitées → HDFS : {HDFS_OUTPUT}")

# ─── SORTIE DONNÉES TRAITÉES → MINIO ─────────────────────────────────────────
minio_query = (
    enriched_final.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", MINIO_OUTPUT)
    .option("checkpointLocation", CHECKPOINT_MINIO)
    .start()
)
print(f"📦 Données traitées → MinIO : {MINIO_OUTPUT}")

# ─── SORTIE DONNÉES TRAITÉES → MONGODB ───────────────────────────────────────
mongodb_query = (
    enriched_final.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_MONGODB)
    .foreachBatch(write_to_mongodb)
    .start()
)
print(f"🗄️  Données traitées → MongoDB : {MONGODB_DB}.{MONGODB_COLLECTION_PROCESSED}")

print("✅ Streaming capteurs planétaires v4 lancé !")
print("\n📊 Architecture de sauvegarde :")
print(f"   📡 Données brutes        → MinIO : {MINIO_RAW_DATA}")
print(f"   ✨ Données traitées      → HDFS  : {HDFS_OUTPUT}")
print(f"   📦 Données traitées      → MinIO : {MINIO_OUTPUT}")
print(f"   🗄️  Données traitées     → MongoDB : {MONGODB_COLLECTION_PROCESSED}")
print(f"   📨 Topic Kafka           : {KAFKA_TOPIC}")

if model is not None:
    print(f"   🤖 Modèle ML            : ACTIF — labels {labels}")
else:
    print(f"   🤖 Modèle ML            : INACTIF (scoring par règles uniquement)")

print("\n   Score habitabilité (règles physiques) :")
print("   🟢 ≥ 60  →  habitable")
print("   🟡 ≥ 30  →  inconnue")
print("   🔴 < 30  →  non_habitable")
print("\n   Accès web :")
print("   💻 MinIO         : http://localhost:9001 (minioadmin / minioadmin)")
print("   💻 Mongo Express : http://localhost:8081")
print("   💻 Spark Master  : http://localhost:8080\n")

minio_query.awaitTermination()