import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC_NAME = "player_metrics"

print(f"DEBUG: Démarrage Spark. Connexion Kafka à : {KAFKA_BOOTSTRAP}")

# Initialisation Spark
spark = SparkSession.builder \
    .appName("CAN2025_RealTime_Analytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 1. DÉFINITION DU SCHÉMA (Correspond au JSON du Producer V2) ---
# Note la structure imbriquée pour "position"
schema = StructType([
    StructField("player_id", IntegerType()),
    StructField("timestamp", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("acceleration_ms2", DoubleType()),
    StructField("total_distance_m", DoubleType()),
    StructField("sprint_count", IntegerType()),
    StructField("heart_rate_bpm", IntegerType()),
    StructField("fatigue_index", DoubleType()),
    StructField("position", StructType([
        StructField("x", IntegerType()),
        StructField("y", IntegerType())
    ])),
    StructField("impact_g", DoubleType())
])

# --- 2. LECTURE DU STREAM (INPUT) ---
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# --- 3. TRANSFORMATION (PARSING) ---
# On convertit les bytes Kafka en String, puis en JSON structuré
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# --- 4. ANALYSE & ALERTING (LOGIQUE MÉTIER) ---
# On ajoute une colonne "Alert" basée sur des seuils critiques
analyzed_stream = parsed_stream.withColumn(
    "ALERT_TYPE",
    when(col("impact_g") > 8.0, "🔴 CHOC VIOLENT")
    .when(col("heart_rate_bpm") > 190, "🟠 ARYTHMIE / SURCHAUFFE")
    .when(col("fatigue_index") > 90, "🟡 RISQUE BLESSURE (FATIGUE)")
    .when(col("acceleration_ms2") < -4.0, "🔵 FREINAGE BRUTAL") # Possible chute
    .otherwise("🟢 NOMINAL")
)

# On sélectionne les colonnes les plus intéressantes pour l'affichage console
display_stream = analyzed_stream.select(
    col("player_id"),
    col("speed_kmh"),
    col("heart_rate_bpm"),
    col("fatigue_index"),
    col("impact_g"),
    col("ALERT_TYPE")
)

# --- 5. ÉCRITURE (OUTPUT) ---
# Pour l'instant, on affiche juste dans la console Docker pour valider
query = display_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
