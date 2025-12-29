import logging
import struct
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro
from data_analysis import config

logger = logging.getLogger(__name__)


# Beskriv kort, hvad metoden gør i to sætninger.
def write_to_kafka_with_avro(
    df: DataFrame, topic: str, schema: str, schema_id: int, query_name: str
):
    """
    Write DataFrame to Kafka with proper Avro encoding (Confluent Wire Format).

    Args:
        df: DataFrame to write
        topic: Kafka topic name
        schema: Avro schema JSON string
        schema_id: Schema ID from Schema Registry
        query_name: Name for the streaming query

    Returns:
        StreamingQuery object
    """
    # Create Confluent Wire Format header (Magic Byte + Schema ID)
    # Magic Byte (0) + Schema ID (4 bytes, big-endian)
    header = bytearray([0]) + struct.pack(">I", schema_id)

    # Encode DataFrame as Avro with Confluent header
    payload = df.select(
        F.concat(F.lit(header), to_avro(F.struct("*"), schema)).alias("value")
    )

    checkpoint_path = f"{config.CHECKPOINT_BASE_PATH}/kafka_output/{query_name}"

    logger.info(
        f"Writing to Kafka topic '{topic}' with Avro encoding (schema ID: {schema_id})"
    )
    return (
        payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime=config.BATCH_INTERVAL)
        .queryName(query_name)
        .start()
    )
