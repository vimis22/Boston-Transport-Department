"""
Bike-Weather Distance Calculation ETL Job

Reads aggregated bike-weather data, calculates trip distance (using Haversine formula)
and average speed, then outputs the enriched data to Kafka.
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
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "bike-weather-aggregate")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "bike-weather-distance")

# Schema subjects
INPUT_SCHEMA_SUBJECT = os.getenv("INPUT_SCHEMA_SUBJECT", "bike-weather-aggregate-value")
OUTPUT_SCHEMA_SUBJECT = os.getenv("OUTPUT_SCHEMA_SUBJECT", "bike-weather-distance-value")

# Processing configuration
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/bike-weather-distance-checkpoint")
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


def calculate_distance_and_speed(df: DataFrame) -> DataFrame:
    """
    Calculate distance (meters) and average speed (m/s) for bike trips.
    Uses Haversine formula for distance.
    """
    # Earth radius in meters
    R = 6371000

    # Convert coordinates to radians
    lat1 = F.radians(F.col("start_station_latitude"))
    lon1 = F.radians(F.col("start_station_longitude"))
    lat2 = F.radians(F.col("end_station_latitude"))
    lon2 = F.radians(F.col("end_station_longitude"))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = (F.sin(dlat / 2) ** 2) + F.cos(lat1) * F.cos(lat2) * (F.sin(dlon / 2) ** 2)
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    distance = R * c

    return df.withColumn("distance_meters", distance).withColumn(
        "average_speed_kmh",
        F.when(F.col("tripduration") > 0, (distance / F.col("tripduration")) * 3.6)
        .otherwise(0.0).alias("average_speed_kmh")
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
        .queryName("bike-weather-distance")
        .start()
    )


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Run the bike-weather distance calculation streaming job."""
    print("Starting bike-weather distance calculation job...")
    print(f"  Spark Connect: {SPARK_CONNECT_URL}")
    print(f"  Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"  Input Topic: {INPUT_TOPIC}")
    print(f"  Output Topic: {OUTPUT_TOPIC}")

    # Initialize Spark session
    spark = (
        SparkSession.builder
        .remote(SPARK_CONNECT_URL)
        .appName("bike-weather-distance")
        .getOrCreate()
    )
    print(f"Connected to Spark {spark.version}")

    # Fetch Avro schemas
    print("Fetching schemas from registry...")
    input_schema, _ = get_latest_schema(INPUT_SCHEMA_SUBJECT)
    output_schema, output_schema_id = get_latest_schema(OUTPUT_SCHEMA_SUBJECT)

    # Read from Kafka
    print(f"Reading from {INPUT_TOPIC}...")
    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Decode and Process
    decoded = stream_df.select(decode_avro_payload("value", input_schema).alias("data")).select("data.*")
    
    # Calculate distance and speed
    calculated = calculate_distance_and_speed(decoded)
    
    # Select final fields matching output schema
    final_df = calculated.select(
        "starttime",
        "stoptime",
        "weather_ts",
        "temp_c",
        "precip_daily_mm",
        "tripduration",
        "distance_meters",
        "average_speed_kmh"
    )

    # Write to output
    write_to_kafka(final_df, output_schema, output_schema_id)

    print("Streaming query started successfully!")


if __name__ == "__main__":
    main()

