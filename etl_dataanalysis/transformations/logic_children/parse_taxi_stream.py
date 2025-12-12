from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    hour,
    date_format
)
from .decode_avro_payload import decode_avro_payload


# Parser taxi streaming data fra Kafka med embedded weather snapshots (camelCase fields).
# Dekoder Avro payload og ekstraherer flat structure og konverterer datetime til timestamp med partitions.
def parse_taxi_stream(df: DataFrame, schema: str) -> DataFrame:
    """
    Parse taxi stream from Kafka with Avro encoding.

    Args:
        df: Raw Kafka DataFrame with binary value column
        schema: Avro schema string from Schema Registry
    """
    # Decode Avro payload (skipping 5-byte Confluent header)
    decoded_df = df.select(
        decode_avro_payload("value", schema).alias("taxi"),
        col("timestamp").alias("kafka_timestamp")
    )

    # Extract fields from decoded Avro structure
    result_df = decoded_df.select(
        col("taxi.id").alias("trip_id"),
        col("taxi.datetime").alias("datetime"),
        col("taxi.source").alias("pickup_location"),
        col("taxi.destination").alias("dropoff_location"),
        col("taxi.cab_type").alias("cab_type"),
        col("taxi.product_id").alias("product_id"),
        col("taxi.name").alias("product_name"),
        col("taxi.price").cast("double").alias("price"),
        col("taxi.distance").cast("double").alias("distance"),
        col("taxi.surge_multiplier").cast("double").alias("surge_multiplier"),
        col("taxi.latitude").cast("double").alias("latitude"),
        col("taxi.longitude").cast("double").alias("longitude"),
        col("taxi.temperature").cast("double").alias("temperature"),
        col("taxi.apparentTemperature").cast("double").alias("apparent_temperature"),
        col("taxi.short_summary").alias("weather_summary"),
        col("taxi.precipIntensity").cast("double").alias("precip_intensity"),
        col("taxi.humidity").cast("double").alias("humidity"),
        col("taxi.windSpeed").cast("double").alias("wind_speed"),
        col("kafka_timestamp")
    )

    # Convert datetime to timestamp and extract date parts for partitioning
    final_df = result_df.withColumn(
        "datetime_ts", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "year", year(col("datetime_ts"))
    ).withColumn(
        "month", month(col("datetime_ts"))
    ).withColumn(
        "date", date_format(col("datetime_ts"), "yyyy-MM-dd")
    ).withColumn(
        "hour", hour(col("datetime_ts"))
    )

    return final_df
