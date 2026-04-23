"""
batch_ml.py  (v4 — Contraintes astrophysiques réelles)
────────────────────────────────────────────────────────
Pipeline ML avec features physiquement cohérentes avec le streaming.

Features utilisées :
  - stellar_flux            : flux stellaire (luminosité / distance²)
  - surface_radiation_msv_h : radiations de surface calculées
  - avg_temp_celsius        : température d'équilibre
  - gravity_g               : gravité (rétention atmosphérique)
  - atmosphere_o2/co2/n2    : composition atmosphérique
  - has_water_int           : présence d'eau liquide
  - magnetic_field_int      : champ magnétique (protection)
  - in_hz_int               : dans la zone habitable
  - tidally_locked_int      : verrouillage marée
  - retains_atmosphere_int  : rétention atmosphérique possible
  - star_type_vec           : type d'étoile (encodé)
  - planet_type_vec         : type de planète (encodé)

Lancement :
  docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master local[2] \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/app/batch_ml.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, StandardScaler, OneHotEncoder
)
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

HDFS_INPUT  = "hdfs://namenode:9000/users/space/input/planets.csv"
HDFS_OUTPUT = "hdfs://namenode:9000/users/space/batch_output"
MODEL_PATH  = "hdfs://namenode:9000/users/space/models/planet_classifier_v4"

MINIO_ENDPOINT   = "http://minio:9090"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_OUTPUT     = "s3a://space-batch/results"
MINIO_MODELS     = "s3a://space-models/planet_classifier_v4"

spark = (
    SparkSession.builder
    .appName("SpacePipelineBatchML_v4")
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

# ─── CHARGEMENT ──────────────────────────────────────────────────────────────
print("📊 Chargement des données depuis HDFS...")
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(HDFS_INPUT)
)
print(f"   {df.count()} planètes dans {df.select('system_name').distinct().count()} systèmes\n")

print("Distribution des labels :")
df.groupBy("label").count().orderBy("count", ascending=False).show()

print("Distribution par type d'étoile :")
df.groupBy("star_type", "label").count().orderBy("star_type").show()

print("Distribution par type de planète :")
df.groupBy("planet_type", "label").count().orderBy("planet_type").show()

# ─── PIPELINE ────────────────────────────────────────────────────────────────
star_type_indexer   = StringIndexer(inputCol="star_type",   outputCol="star_type_idx",   handleInvalid="keep")
planet_type_indexer = StringIndexer(inputCol="planet_type", outputCol="planet_type_idx", handleInvalid="keep")
label_indexer       = StringIndexer(inputCol="label",       outputCol="label_index",      handleInvalid="keep")
star_type_encoder   = OneHotEncoder(inputCol="star_type_idx",   outputCol="star_type_vec")
planet_type_encoder = OneHotEncoder(inputCol="planet_type_idx", outputCol="planet_type_vec")

# Features alignées avec kafka_producer.py
feature_cols = [
    # ── Type étoile / planète (encodés) ──────────────────────────
    "star_type_vec",
    "planet_type_vec",

    # ── Physique orbitale et stellaire ───────────────────────────
    "distance_au",          # distance à l'étoile
    "stellar_flux",         # flux stellaire relatif (clé habitabilité)

    # ── Physique planétaire ───────────────────────────────────────
    "radius_km",
    "mass_earth",
    "gravity_g",            # rétention atmosphérique
    "avg_temp_celsius",     # température d'équilibre

    # ── Radiations ───────────────────────────────────────────────
    "surface_radiation_msv_h",  # radiations de surface calculées

    # ── Atmosphère ───────────────────────────────────────────────
    "atmosphere_o2",
    "atmosphere_co2",
    "atmosphere_n2",

    # ── Conditions de surface ────────────────────────────────────
    "has_water_int",
    "magnetic_field_int",

    # ── Zone habitable et contraintes physiques ──────────────────
    "in_hz_int",
    "tidally_locked_int",
    "retains_atmosphere_int",
    #"habitable_candidate_int",
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="keep")
scaler    = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label_index",
    seed=42
)

pipeline = Pipeline(stages=[
    star_type_indexer, planet_type_indexer, label_indexer,
    star_type_encoder, planet_type_encoder,
    assembler, scaler, rf,
])

# ─── CROSS-VALIDATION ────────────────────────────────────────────────────────
paramGrid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees,            [50, 100, 200])
    .addGrid(rf.maxDepth,            [4, 6, 8])
    .addGrid(rf.minInstancesPerNode, [1, 2])
    .build()
)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label_index",
    predictionCol="prediction",
    metricName="accuracy"
)

crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=5,
    seed=42
)

df_clean = df.filter(col("label").isNotNull())
train_df, test_df = df_clean.randomSplit([0.85, 0.15], seed=42)

# ─── ENTRAÎNEMENT ────────────────────────────────────────────────────────────
print("🤖 Entraînement Random Forest avec CrossValidator (5 folds × 18 combinaisons)...")
print("   ⏳ Cela peut prendre quelques minutes...\n")

cv_model   = crossval.fit(train_df)
best_model = cv_model.bestModel
labels     = best_model.stages[2].labels  # LabelIndexer = stage 2

def add_predicted_label(df):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    label_map = {float(i): l for i, l in enumerate(labels)}
    convert   = udf(lambda x: label_map.get(float(x), "unknown"), StringType())
    return df.withColumn("predicted_label", convert(col("prediction")))

best_rf = next(s for s in best_model.stages if isinstance(s, RandomForestClassificationModel))

print(f"✅ Meilleurs hyperparamètres :")
print(f"   numTrees             = {best_rf.getNumTrees}")
print(f"   maxDepth             = {best_rf.getMaxDepth()}")
print(f"   minInstancesPerNode  = {best_rf.getMinInstancesPerNode()}\n")

# ─── IMPORTANCE DES FEATURES ─────────────────────────────────────────────────
print("🔍 Importance des features (top 10) :")
importances = best_rf.featureImportances
# Les features encodées OHE sont en tête, on affiche les numériques
numeric_features = [f for f in feature_cols if "vec" not in f]
# Index des features numériques dans le vecteur final (après OHE)
# On affiche l'importance brute du RF
for i, imp in enumerate(importances):
    if imp > 0.01:
        print(f"   feature[{i:2d}]  importance = {imp:.4f}")
print()

# ─── ÉVALUATION ──────────────────────────────────────────────────────────────
predictions = add_predicted_label(best_model.transform(test_df))

for metric in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
    evaluator.setMetricName(metric)
    score = evaluator.evaluate(predictions)
    print(f"   {metric:20} : {score:.2%}")
print()

predictions.select(
    "name", "star_type", "planet_type",
    "stellar_flux", "surface_radiation_msv_h",
    "avg_temp_celsius", "in_habitable_zone",
    "label", "predicted_label"
).show(truncate=False)

# ─── PRÉDICTIONS SUR TOUT LE DATASET ─────────────────────────────────────────
all_preds = add_predicted_label(best_model.transform(df_clean))
result = all_preds.select(
    "system_name", "name", "star_type", "planet_type",
    "distance_au", "avg_temp_celsius", "stellar_flux",
    "surface_radiation_msv_h", "in_habitable_zone",
    "atmosphere_o2", "has_water_int", "gravity_g",
    "tidally_locked", "retains_atmosphere",
    "label", "predicted_label"
)
result.orderBy("system_name", "distance_au").show(100, truncate=False)

print("📊 Synthèse par système stellaire :")
all_preds.groupBy("system_name", "star_type").agg(
    count("*").alias("nb_planetes"),
    count(when(col("predicted_label") == "habitable",     1)).alias("habitables"),
    count(when(col("predicted_label") == "inconnue",      1)).alias("inconnues"),
    count(when(col("predicted_label") == "non_habitable", 1)).alias("non_habitables"),
).orderBy("system_name").show(truncate=False)

# ─── SAUVEGARDE ──────────────────────────────────────────────────────────────
print(f"💾 Sauvegarde HDFS : {HDFS_OUTPUT}")
result.write.mode("overwrite").parquet(HDFS_OUTPUT)
best_model.write().overwrite().save(MODEL_PATH)

print(f"📦 Sauvegarde MinIO : {MINIO_OUTPUT}  → http://localhost:9001")
result.write.mode("overwrite").parquet(MINIO_OUTPUT)
best_model.write().overwrite().save(MINIO_MODELS)

print("\n✅ Pipeline batch v4 terminé !")
spark.stop()