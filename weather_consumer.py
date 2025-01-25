from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, lit, when
from pyspark.sql.types import StructType, StringType, FloatType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WeatherConsumerWithTransformations") \
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

# Convert relevant columns to numeric types
transformed_df = parsed_df \
    .withColumn("temperature_C", col("temperature").cast(FloatType())) \
    .withColumn("humidity", col("humidity").cast(FloatType())) \
    .withColumn("wind_speed_kmph", col("wind_speed_kmph").cast(FloatType())) \
    .withColumn("visibility_km", col("visibility_km").cast(FloatType()))

# Filter cities with humidity > 80%
filtered_df = transformed_df.filter(col("humidity") > 80)

# Add a derived column for temperature in Fahrenheit
enriched_df = filtered_df.withColumn(
    "temperature_F",
    (col("temperature_C") * 9 / 5) + 32
)

# Aggregate average temperature across cities
aggregated_df = enriched_df.groupBy().agg(
    avg("temperature_C").alias("average_temperature_C"),
    avg("humidity").alias("average_humidity")
)

# Display filtered and enriched data in the console
query1 = enriched_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Display aggregated metrics in the console
query2 = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query1.awaitTermination()
query2.awaitTermination()
