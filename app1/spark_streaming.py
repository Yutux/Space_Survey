"""
spark_streaming.py
──────────────────
Consomme le topic Kafka 'planets' en temps réel avec Spark Structured
Streaming, applique des transformations et écrit les résultats dans HDFS.

Pipeline :
  Kafka topic 'planets'
    → parse JSON
    → déduplication (planètes déjà vues)
    → calcul du score d'habitabilité
    → classification en temps réel (règles + ML score)
    → écriture dans HDFS (format Parquet)
    → affichage console

Lancement (depuis le container spark-master) :
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --master spark://spark-master:7077 \
    /opt/bitnami/spark/app/spark_streaming.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, udf,
    current_timestamp, when, expr
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType, BooleanType, TimestampType
)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC     = "planets"
HDFS_OUTPUT     = "hdfs://namenode:9000/users/space/streaming_output"
CHECKPOINT_DIR  = "hdfs://namenode:9000/users/space/checkpoints/streaming"


# ─── SCHEMA DES MESSAGES KAFKA ───────────────────────────────────────────────
planet_schema = StructType([
    StructField("name",               StringType(),  True),
    StructField("radius_km",          FloatType(),   True),
    StructField("distance_au",        FloatType(),   True),
    StructField("mass_earth",         FloatType(),   True),
    StructField("gravity_g",          FloatType(),   True),
    StructField("avg_temp_celsius",   FloatType(),   True),
    StructField("atmosphere_o2",      FloatType(),   True),
    StructField("atmosphere_co2",     FloatType(),   True),
    StructField("atmosphere_n2",      FloatType(),   True),
    StructField("has_water",          BooleanType(), True),
    StructField("magnetic_field",     BooleanType(), True),
    StructField("moons",              IntegerType(), True),
    StructField("label",              StringType(),  True),
    StructField("received_at",        StringType(),  True),
])


# ─── UDF : score d'habitabilité (0 → 100) ───────────────────────────────────
@udf(returnType=FloatType())
def compute_habitability_score(
    avg_temp, atmosphere_o2, has_water, gravity_g,
    magnetic_field, distance_au, atmosphere_co2
):
    """
    Score simple basé sur des critères biologiques connus :
      - Température entre -10 et 40°C          → +30 pts
      - O2 entre 15% et 30%                    → +25 pts
      - Présence d'eau liquide                 → +20 pts
      - Gravité entre 0.5 et 1.5 g             → +10 pts
      - Champ magnétique (protection radiation) → +10 pts
      - Distance au soleil entre 0.8 et 1.8 UA → +5 pts
    Pénalités :
      - CO2 > 10%                              → -15 pts
    """
    score = 0.0
    if avg_temp is not None and -10 <= avg_temp <= 40:
        score += 30
    if atmosphere_o2 is not None and 15 <= atmosphere_o2 <= 30:
        score += 25
    if has_water:
        score += 20
    if gravity_g is not None and 0.5 <= gravity_g <= 1.5:
        score += 10
    if magnetic_field:
        score += 10
    if distance_au is not None and 0.8 <= distance_au <= 1.8:
        score += 5
    if atmosphere_co2 is not None and atmosphere_co2 > 10:
        score -= 15
    return max(0.0, float(score))


# ─── SPARK SESSION ───────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SpacePipelineStreaming")
    .master("spark://spark-master:7077")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("🚀 SparkSession initialisée — lecture du flux Kafka...")


# ─── LECTURE DU FLUX KAFKA ───────────────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Décodage de la valeur JSON
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), planet_schema).alias("data")
).select("data.*")


# ─── TRANSFORMATIONS ─────────────────────────────────────────────────────────

# 1. Ajout du score d'habitabilité
enriched = parsed_stream.withColumn(
    "habitability_score",
    compute_habitability_score(
        col("avg_temp_celsius"),
        col("atmosphere_o2"),
        col("has_water"),
        col("gravity_g"),
        col("magnetic_field"),
        col("distance_au"),
        col("atmosphere_co2"),
    )
)

# 2. Classification automatique basée sur le score
enriched = enriched.withColumn(
    "predicted_class",
    when(col("habitability_score") >= 60, "habitable")
    .when(col("habitability_score") >= 30, "inconnue")
    .otherwise("non_habitable")
)

# 3. Timestamp de traitement Spark
enriched = enriched.withColumn("processed_at", current_timestamp())


# ─── ÉCRITURE CONSOLE (debug) ────────────────────────────────────────────────
console_query = (
    enriched
    .select("name", "avg_temp_celsius", "atmosphere_o2",
            "has_water", "habitability_score", "predicted_class", "label")
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)


# ─── ÉCRITURE HDFS (Parquet) ──────────────────────────────────────────────────
hdfs_query = (
    enriched
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", HDFS_OUTPUT)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

print("✅ Streaming lancé — en attente de messages Kafka...")
hdfs_query.awaitTermination()