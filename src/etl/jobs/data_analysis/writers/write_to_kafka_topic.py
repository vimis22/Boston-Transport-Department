"""
Kafka Writer med Avro Serialization - Open/Closed Principle (OCP)
"""

import struct
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, concat, struct as make_struct
from pyspark.sql.avro.functions import to_avro


class KafkaAvroWriter:
    """Kafka writer med Confluent Wire Format support."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        checkpoint_location: str,
        trigger_interval: str = "10 seconds"
    ):
        self.bootstrap_servers = bootstrap_servers
        self.checkpoint_location = checkpoint_location
        self.trigger_interval = trigger_interval
    
    
    def write_stream_to_kafka(
        self,
        df: DataFrame,
        topic_name: str,
        schema_string: str,
        schema_id: int,
        output_mode: str = "append"
    ):
        """Skriv DataFrame til Kafka topic med Avro serialization."""
        
        # Create Confluent Wire Format header
        magic_byte = bytearray([0])
        schema_id_bytes = struct.pack(">I", schema_id)
        header = bytes(magic_byte + schema_id_bytes)
        
        # Serialize med Avro
        avro_payload = to_avro(
            make_struct([col(c) for c in df.columns]),
            schema_string
        )
        
        # Combine header + payload
        kafka_value = concat(lit(header), avro_payload)
        kafka_df = df.select(kafka_value.alias("value"))
        
        # Write stream
        query = (
            kafka_df
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("topic", topic_name)
            .option("checkpointLocation", f"{self.checkpoint_location}/{topic_name}")
            .outputMode(output_mode)
            .trigger(processingTime=self.trigger_interval)
            .start()
        )
        
        return query


def create_kafka_writer(config: dict):
    """Factory function til at oprette KafkaAvroWriter."""
    return KafkaAvroWriter(
        bootstrap_servers=config.get("KAFKA_BOOTSTRAP_SERVERS"),
        checkpoint_location=config.get("CHECKPOINT_BASE_PATH"),
        trigger_interval=config.get("TRIGGER_INTERVAL", "10 seconds")
    )
