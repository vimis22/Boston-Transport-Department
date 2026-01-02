"""
Bike-Weather Data Aggregation ETL Job

Joins bike trip data with weather observations to find the nearest weather reading
for each bike trip, then outputs the enriched data to Kafka.
"""

import os
import struct
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro, to_avro
import requests

# =============================================================================
# Configuration (Environment Variables with Defaults)
# =============================================================================

SPARK_CONNECT_URL = os.getenv("SPARK_CONNECT_URL", "sc://spark-connect-server:15002")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-broker:9092")

# Topic configuration
BIKE_TOPIC = os.getenv("BIKE_TOPIC", "bike-data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather-data")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "bike-weather-aggregate")

# Schema subjects
BIKE_SCHEMA_SUBJECT = os.getenv("BIKE_SCHEMA_SUBJECT", "bike-data-value")
WEATHER_SCHEMA_SUBJECT = os.getenv("WEATHER_SCHEMA_SUBJECT", "weather-data-value")
OUTPUT_SCHEMA_SUBJECT = os.getenv("OUTPUT_SCHEMA_SUBJECT", "bike-weather-aggregate-value")

# Processing configuration
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/bike-weather-aggregate-checkpoint")
WATERMARK_DURATION = os.getenv("WATERMARK_DURATION", "10 minutes")
JOIN_INTERVAL = os.getenv("JOIN_INTERVAL", "1 hour")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "1 second")


# =============================================================================
# Helper Functions
# =============================================================================

def get_latest_schema(subject: str) -> Tuple[str, int]:
    """Fetch the latest Avro schema from Schema Registry."""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    return data["schema"], data["id"]


def decode_avro_payload(col_name: str, schema: str) -> F.Column:
    """Decode Avro payload, skipping the 5-byte Confluent header."""
    return from_avro(F.expr(f"substring({col_name}, 6, length({col_name})-5)"), schema)


def create_bike_stream(spark: SparkSession, schema: str) -> DataFrame:
    """Create a streaming DataFrame for bike trip data."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", BIKE_TOPIC)
        .option("startingOffsets", "latest")
        .load()
        .select(decode_avro_payload("value", schema).alias("bike"))
        .select(
            F.col("bike.start_station_latitude"),
            F.col("bike.start_station_longitude"),
            F.col("bike.end_station_latitude"),
            F.col("bike.end_station_longitude"),
            F.col("bike.tripduration"),
            F.to_timestamp(F.col("bike.starttime")).alias("starttime"),
            F.to_timestamp(F.col("bike.stoptime")).alias("stoptime"),
        )
        .withColumn("join_key", F.lit(1))
        .alias("bikes")
        .withWatermark("starttime", WATERMARK_DURATION)
    )


def create_weather_stream(spark: SparkSession, schema: str) -> DataFrame:
    """Create a streaming DataFrame for weather observations."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", WEATHER_TOPIC)
        .option("startingOffsets", "latest")
        .load()
        .select(decode_avro_payload("value", schema).alias("weather"))
        .select(
            F.col("weather.dry_bulb_temperature_celsius"),
            F.col("weather.precip_daily_mm"),
            F.to_timestamp(F.col("weather.observation_date")).alias("observation_date"),
        )
        .withColumn("join_key", F.lit(1))
        .alias("weather")
        .withWatermark("observation_date", WATERMARK_DURATION)
    )


def join_bike_weather(bikes: DataFrame, weather: DataFrame) -> DataFrame:
    """
    Join bike trips with weather observations using an interval join.
    Finds the nearest weather observation within the configured time window.
    """
    joined = bikes.join(
        weather,
        on=(
            (F.col("bikes.join_key") == F.col("weather.join_key"))
            & (F.col("weather.observation_date") >= F.col("bikes.starttime") - F.expr(f"interval {JOIN_INTERVAL}"))
            & (F.col("weather.observation_date") <= F.col("bikes.starttime") + F.expr(f"interval {JOIN_INTERVAL}"))
        ),
        how="inner",
    ).withColumn(
        "time_diff",
        F.abs(F.col("bikes.starttime").cast("long") - F.col("weather.observation_date").cast("long")),
    )

    # Aggregate to find the nearest weather observation per bike trip
    return (
        joined.groupBy(
            "bikes.tripduration",
            "bikes.starttime",
            "bikes.stoptime",
            "bikes.start_station_latitude",
            "bikes.start_station_longitude",
            "bikes.end_station_latitude",
            "bikes.end_station_longitude",
        )
        .agg(
            F.min_by(
                F.struct("weather.observation_date", "weather.dry_bulb_temperature_celsius", "weather.precip_daily_mm"),
                F.col("time_diff"),
            ).alias("nearest_weather")
        )
        .select(
            F.col("starttime").cast("string"),
            F.col("stoptime").cast("string"),
            F.col("nearest_weather.observation_date").cast("string").alias("weather_ts"),
            F.col("nearest_weather.dry_bulb_temperature_celsius").alias("temp_c"),
            F.col("nearest_weather.precip_daily_mm").alias("precip_daily_mm"),
            F.col("tripduration"),
            F.col("start_station_latitude"),
            F.col("start_station_longitude"),
            F.col("end_station_latitude"),
            F.col("end_station_longitude"),
        )
    )


def write_to_kafka(df: DataFrame, schema: str, schema_id: int) -> None:
    """Write enriched data to Kafka output topic."""
    # Create Confluent Wire Format header (Magic Byte + Schema ID)
    # Magic Byte (0) + Schema ID (4 bytes, big-endian)
    header = bytearray([0]) + struct.pack(">I", schema_id)

    payload = df.select(
        F.concat(
            F.lit(header),
            to_avro(F.struct("*"), schema)
        ).alias("value")
    )

    (
        payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .option("failOnDataLoss", "false")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("bike-weather-data-aggregation")
        .start()
    )


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Run the bike-weather data aggregation streaming job."""
    print("Starting bike-weather data aggregation job...")
    print(f"  Spark Connect: {SPARK_CONNECT_URL}")
    print(f"  Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"  Output Topic: {OUTPUT_TOPIC}")

    # Initialize Spark session
    spark = (
        SparkSession.builder
        .remote(SPARK_CONNECT_URL)
        .appName("bike-weather-data-aggregation")
        .getOrCreate()
    )
    print(f"Connected to Spark {spark.version}")

    # Fetch Avro schemas
    print("Fetching schemas from registry...")
    bike_schema, _ = get_latest_schema(BIKE_SCHEMA_SUBJECT)
    weather_schema, _ = get_latest_schema(WEATHER_SCHEMA_SUBJECT)
    output_schema, output_schema_id = get_latest_schema(OUTPUT_SCHEMA_SUBJECT)

    # Create streaming DataFrames
    bikes = create_bike_stream(spark, bike_schema)
    weather = create_weather_stream(spark, weather_schema)

    # Join and enrich data
    enriched = join_bike_weather(bikes, weather)

    # Write to output
    write_to_kafka(enriched, output_schema, output_schema_id)

    print("Streaming query started successfully!")


if __name__ == "__main__":
    main()
