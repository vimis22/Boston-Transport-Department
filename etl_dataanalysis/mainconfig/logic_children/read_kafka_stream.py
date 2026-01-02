from pyspark.sql import SparkSession, DataFrame
from ..parent import config


def read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    """
    Read streaming data from Kafka topic.

    Args:
        spark: SparkSession instance
        topic: Kafka topic name to subscribe to

    Returns:
        DataFrame: Streaming DataFrame from Kafka
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
