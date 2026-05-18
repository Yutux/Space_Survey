"""
batch_ml.py
───────────
Traitement batch avec Spark MLlib :
  1. Charge planets.csv depuis HDFS
  2. Entraîne un modèle Random Forest pour la classification
     (habitable / non_habitable / inconnue)
  3. Évalue le modèle
  4. Applique le modèle sur toutes les planètes
  5. Sauvegarde les résultats enrichis dans HDFS

Lancement :
  spark-submit \
    --master spark://spark-master:7077 \
    /opt/bitnami/spark/app/batch_ml.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, IndexToString, StandardScaler
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# ─── CONFIG ──────────────────────────────────────────────────────────────────
HDFS_INPUT     = "hdfs://namenode:9000/users/space/input/planets.csv"
HDFS_OUTPUT    = "hdfs://namenode:9000/users/space/batch_output"
MODEL_PATH     = "hdfs://namenode:9000/users/space/models/planet_classifier"


# ─── SPARK SESSION ───────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SpacePipelineBatchML")
    .master("local[2]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("📊 Chargement des données planétaires depuis HDFS...")


# ─── CHARGEMENT DES DONNÉES ──────────────────────────────────────────────────
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(HDFS_INPUT)
)

df.printSchema()
print(f"   {df.count()} planètes chargées.\n")


# ─── PRÉPARATION DES FEATURES ────────────────────────────────────────────────
# Features numériques pour le modèle
feature_cols = [
    "radius_km",
    "distance_au",
    "mass_earth",
    "gravity_g",
    "avg_temp_celsius",
    "atmosphere_o2",
    "atmosphere_co2",
    "atmosphere_n2",
    "has_water_int",
    "magnetic_field_int",
    "moons",
]

# Conversion booléens si nécessaire (au cas où ils sont stockés en string)
df = df.withColumn(
    "has_water_int",
    when(col("has_water").cast("string").isin("true", "True", "1"), 1).otherwise(0)
)
df = df.withColumn(
    "magnetic_field_int",
    when(col("magnetic_field").cast("string").isin("true", "True", "1"), 1).otherwise(0)
)

# On retire les lignes sans label (ne devrait pas arriver avec notre dataset)
df_clean = df.filter(col("label").isNotNull())

print("Distribution des labels :")
df_clean.groupBy("label").count().show()


# ─── PIPELINE ML ─────────────────────────────────────────────────────────────

# 1. Encodage de la cible (String → Index)
label_indexer = StringIndexer(
    inputCol="label",
    outputCol="label_index",
    handleInvalid="keep"
)

# 2. Assemblage des features en un vecteur
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw",
    handleInvalid="keep"
)

# 3. Normalisation
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

# 4. Modèle : Random Forest
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label_index",
    numTrees=50,
    maxDepth=5,
    seed=42,
)

# 5. Décodage de la prédiction (Index → String)
label_converter = IndexToString(
    inputCol="prediction",
    outputCol="predicted_label",
    labels=[]   # sera rempli par le modèle entraîné
)

pipeline = Pipeline(stages=[label_indexer, assembler, scaler, rf, label_converter])


# ─── ENTRAÎNEMENT ────────────────────────────────────────────────────────────
# Split train/test (80/20) — avec si peu de planètes, sert surtout de démo
train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)

print("🤖 Entraînement du modèle Random Forest...")
model = pipeline.fit(train_df)

# Mise à jour du label converter avec les labels réels
label_converter.setLabels(
    model.stages[0].labels
)


# ─── ÉVALUATION ──────────────────────────────────────────────────────────────
predictions = model.transform(test_df)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label_index",
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print(f"\n📈 Accuracy sur le jeu de test : {accuracy:.2%}\n")

# Rapport détaillé
print("Résultats sur le jeu de test :")
predictions.select("name", "label", "predicted_label", "probability") \
           .show(truncate=False)


# ─── PRÉDICTION SUR TOUTES LES PLANÈTES ─────────────────────────────────────
print("🔭 Application du modèle sur l'ensemble des planètes...")
all_predictions = model.transform(df_clean)

result = all_predictions.select(
    "name",
    "radius_km",
    "distance_au",
    "avg_temp_celsius",
    "atmosphere_o2",
    "has_water_int",
    "gravity_g",
    "label",
    "predicted_label",
)

result.show(truncate=False)


# ─── ANALYSE : CAPACITÉ TOTALE PAR CATÉGORIE ─────────────────────────────────
print("📊 Synthèse par catégorie prédite :")
all_predictions.groupBy("predicted_label").agg(
    {"radius_km": "avg", "avg_temp_celsius": "avg", "atmosphere_o2": "avg"}
).show()


# ─── SAUVEGARDE DANS HDFS ────────────────────────────────────────────────────
print(f"💾 Sauvegarde des résultats dans HDFS : {HDFS_OUTPUT}")
result.write.mode("overwrite").parquet(HDFS_OUTPUT)

# Sauvegarde du modèle
print(f"💾 Sauvegarde du modèle dans : {MODEL_PATH}")
model.write().overwrite().save(MODEL_PATH)

print("\n✅ Pipeline batch terminé avec succès !")
spark.stop()