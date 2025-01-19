from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import from_json, col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .master("local[*]") \
    .config("spark.jars", "spark_jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,spark_jars/kafka-clients-3.5.0.jar") \
    .getOrCreate()


# Kafka topic and broker configuration
KAFKA_TOPIC = "test-topic"
KAFKA_BROKER = "localhost:9092"

# Define schema for incoming data
schema = StructType() \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("age", IntegerType())

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the Kafka messages
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write the parsed data to the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the streaming to finish
query.awaitTermination()
