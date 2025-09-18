"""Writers package for ETL output destinations."""
from .write_to_kafka_topic import KafkaAvroWriter, create_kafka_writer

__all__ = ["KafkaAvroWriter", "create_kafka_writer"]
