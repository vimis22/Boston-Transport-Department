import os
from pyspark import SparkConf

# This is here where the ETL-Component is connected to Kafka.
# The ETL-Analysis listens on 3 topics and in this context the 3 datasets are defined in the KAFKA_TOPIC.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL", "http://schema-registry.bigdata.svc.cluster.local:8081"
)

# Spark Connect Configuration (for Kubernetes deployment)
# Set USE_SPARK_CONNECT=true to connect to remote Spark cluster instead of local SparkSession
USE_SPARK_CONNECT = os.getenv("USE_SPARK_CONNECT", "false").lower() == "true"
SPARK_CONNECT_URL = os.getenv("SPARK_CONNECT_URL", "sc://spark-connect-server:15002")

KAFKA_TOPICS = {
    "bike": "bike-trips",
    "taxi": "taxi-trips",
    "weather": "weather-data",
}

# Schema subjects for each topic (Confluent Schema Registry naming convention)
SCHEMA_SUBJECTS = {
    "bike": "bike-data-value",
    "taxi": "taxi-data-value",
    "weather": "weather-data-value",
    "accidents": "accident-data-value",
}

# This is where Spark Structured Streaming runs and saves the state here.
# If the system crashes, then we are able to start again.
SPARK_APP_NAME = "ETL Data Analysis"
# The Processed Data is saved in the /data/processed folder.
OUTPUT_BASE_PATH = os.getenv("OUTPUT_BASE_PATH", "/data/processed_simple")
CHECKPOINT_BASE_PATH = os.getenv(
    "CHECKPOINT_BASE_PATH", "/tmp/spark_checkpoints_simple"
)
BATCH_INTERVAL = "10 seconds"

# Analytics Configuration
ANALYTICS_OUTPUT_PATH = os.getenv("ANALYTICS_OUTPUT_PATH", "/data/analytics")
ANALYTICS_CHECKPOINT_PATH = os.getenv(
    "ANALYTICS_CHECKPOINT_PATH", "/tmp/spark_checkpoints_analytics"
)

# Windowing Configuration for Analytics
WINDOW_DURATION_SHORT = "5 minutes"  # For real-time metrics
WINDOW_DURATION_MEDIUM = "15 minutes"  # For correlations
WINDOW_DURATION_LONG = "1 hour"  # For safety analysis
SLIDE_DURATION = "5 minutes"  # Sliding window interval

# Watermark Configuration (following src/etl pattern)
WATERMARK_DURATION = os.getenv(
    "WATERMARK_DURATION", "10 minutes"
)  # Late data tolerance

# Enable/Disable Analytics Streams
ENABLE_WEATHER_TRANSPORT_CORRELATION = (
    os.getenv("ENABLE_WEATHER_TRANSPORT_CORRELATION", "true").lower() == "true"
)
ENABLE_WEATHER_SAFETY_ANALYSIS = (
    os.getenv("ENABLE_WEATHER_SAFETY_ANALYSIS", "true").lower() == "true"
)
ENABLE_SURGE_WEATHER_CORRELATION = (
    os.getenv("ENABLE_SURGE_WEATHER_CORRELATION", "true").lower() == "true"
)
ENABLE_TRANSPORT_USAGE_SUMMARY = (
    os.getenv("ENABLE_TRANSPORT_USAGE_SUMMARY", "true").lower() == "true"
)

# NEW: Enable/Disable Enhanced Academic Analytics
ENABLE_PEARSON_CORRELATIONS = (
    os.getenv("ENABLE_PEARSON_CORRELATIONS", "true").lower() == "true"
)
ENABLE_BINNED_AGGREGATIONS = (
    os.getenv("ENABLE_BINNED_AGGREGATIONS", "true").lower() == "true"
)
ENABLE_PRECIPITATION_ANALYSIS = (
    os.getenv("ENABLE_PRECIPITATION_ANALYSIS", "true").lower() == "true"
)
ENABLE_TEMPORAL_CORRELATIONS = (
    os.getenv("ENABLE_TEMPORAL_CORRELATIONS", "true").lower() == "true"
)
ENABLE_MULTI_VARIABLE_SUMMARY = (
    os.getenv("ENABLE_MULTI_VARIABLE_SUMMARY", "true").lower() == "true"
)

# NEW: Enable/Disable Accident-Weather Correlation Analytics
ENABLE_ACCIDENT_WEATHER_CORRELATION = (
    os.getenv("ENABLE_ACCIDENT_WEATHER_CORRELATION", "true").lower() == "true"
)

# Spark Configuration in order to forward it to the SparkSession
spark_config = (
    SparkConf()
    .setAppName(SPARK_APP_NAME)
    .set("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE_PATH)
    .set("spark.sql.streaming.schemaInference", "true")
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
    # Kafka specific configs, so that we recieve data from Kafka Streaming.
    .set(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0",
    )
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
)
