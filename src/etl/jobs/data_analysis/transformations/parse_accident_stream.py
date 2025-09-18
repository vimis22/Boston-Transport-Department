from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, hour, date_format
from .decode_avro_payload import decode_avro_payload


# Parser accident streaming data fra Kafka med mode_type (bike/mv/ped) og location info.
# Dekoder Avro payload og ekstraherer flat structure og konverterer dispatch timestamps til partitions.
def parse_accident_stream(df: DataFrame, schema: str) -> DataFrame:
    """
    Parse accident stream from Kafka with Avro encoding.

    Args:
        df: Raw Kafka DataFrame with binary value column
        schema: Avro schema string from Schema Registry
    """
    # Decode Avro payload (skipping 5-byte Confluent header)
    decoded_df = df.select(
        decode_avro_payload("value", schema).alias("accident"),
        col("timestamp").alias("kafka_timestamp"),
    )

    # Extract fields from decoded Avro structure
    result_df = decoded_df.select(
        col("accident.dispatch_ts").alias("dispatch_ts"),
        col("accident.mode_type").alias("mode_type"),
        col("accident.location_type").alias("location_type"),
        col("accident.street").alias("street"),
        col("accident.xstreet1").alias("xstreet1"),
        col("accident.xstreet2").alias("xstreet2"),
        col("accident.x_cord").cast("double").alias("x_cord"),
        col("accident.y_cord").cast("double").alias("y_cord"),
        col("accident.lat").cast("double").alias("lat"),
        col("accident.long").cast("double").alias("long"),
        col("kafka_timestamp"),
    )

    # Convert dispatch_ts to timestamp and extract date parts for partitioning
    final_df = (
        result_df.withColumn(
            "dispatch_timestamp",
            to_timestamp(col("dispatch_ts"), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("year", year(col("dispatch_timestamp")))
        .withColumn("month", month(col("dispatch_timestamp")))
        .withColumn("date", date_format(col("dispatch_timestamp"), "yyyy-MM-dd"))
        .withColumn("hour", hour(col("dispatch_timestamp")))
    )

    # Add watermark for late data handling (following src/etl pattern)
    final_df = final_df.withWatermark("dispatch_timestamp", "10 minutes")

    return final_df
