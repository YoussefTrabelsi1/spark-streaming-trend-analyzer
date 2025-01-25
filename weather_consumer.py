from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WeatherConsumer") \
    .master("local[*]") \
    .getOrCreate()

# Kafka topic and broker configuration
KAFKA_TOPIC = 'weather-topic'
KAFKA_BROKER = 'localhost:9092'

# Define schema for incoming weather data
schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", StringType()) \
    .add("humidity", StringType()) \
    .add("weather_desc", StringType()) \
    .add("wind_speed_kmph", StringType()) \
    .add("visibility_km", StringType()) \
    .add("feels_like_C", StringType()) \
    .add("observation_time", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse Kafka messages
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Display the processed data in the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
