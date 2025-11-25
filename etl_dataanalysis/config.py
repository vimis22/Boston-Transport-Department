import os
from pyspark import SparkConf

#This is here where the ETL-Component is connected to Kafka.
#The ETL-Analysis listens on 3 topics and in this context the 3 datasets are defined in the KAFKA_TOPIC.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = {
    "bike": "bike-trips",
    "taxi": "taxi-trips",
    "weather": "weather-data",
}

#This is where Spark Structured Streaming runs and saves the state here.
#If the system crashes, then we are able to start again.
SPARK_APP_NAME = "ETL Data Analysis"
#The Processed Data is saved in the /data/processed folder.
OUTPUT_BASE_PATH = os.getenv("OUTPUT_BASE_PATH", "/data/processed_simple")
CHECKPOINT_BASE_PATH = os.getenv("CHECKPOINT_BASE_PATH", "/tmp/spark_checkpoints_simple")
BATCH_INTERVAL = "10 seconds"

# Spark Configuration in order to forward it to the SparkSession
spark_config = (
    SparkConf()
    .setAppName(SPARK_APP_NAME)
    .set("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE_PATH)
    .set("spark.sql.streaming.schemaInference", "true")
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
    # Kafka specific configs, so that we recieve data from Kafka Streaming.
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
)

