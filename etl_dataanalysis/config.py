KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPICS = {
    "bike": "bike-trips",
    "taxi": "taxi-trips",
    "weather": "weather-data",
}

SPARK_APP_NAME = "ETL Data Analysis"
OUTPUT_BASE_PATH = "/data/processed_simple"
CHECKPOINT_BASE_PATH = "/tmp/spark_checkpoints_simple"
BACH_INTERVAL = "10 seconds"

