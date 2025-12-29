"""
Descriptive Statistics ETL Job - Mulighed 2 Fase 1

Denne ETL læser fra bike-weather-distance topic og beregner:
- Overall descriptive statistics
- Statistics grouped by weather condition
- Statistics grouped by temperature buckets
- Statistics grouped by precipitation level

Output sendes til separate Kafka topics som Kafka Connect kan synce til Hive.
"""

import os
import sys
import struct
import logging
from typing import Tuple

# Add jobs directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, length, when
from pyspark.sql.avro.functions import to_avro, from_avro
import data_analysis.config as config
from data_analysis.utils.get_latest_schema import get_latest_schema
from data_analysis.utils.read_kafka_stream import read_kafka_stream
from data_analysis.transformations import (
    calculate_overall_statistics,
    calculate_statistics_by_weather_condition,
    calculate_statistics_by_temperature_bucket,
    calculate_statistics_by_precipitation_level,
)
from data_analysis.writers.write_to_kafka_topic import create_kafka_writer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark Connect session."""
    spark = (
        SparkSession.builder
        .remote(config.SPARK_CONNECT_URL)
        .appName("statistics-etl")
        .getOrCreate()
    )
    logger.info(f"✓ Connected to Spark {spark.version}")
    return spark


def parse_bike_weather_distance_stream(raw_df, schema_string):
    """Parse bike-weather-distance stream from Kafka."""
    decoded_df = raw_df.select(
        from_avro(
            col("value").substr(lit(6), length(col("value")) - 5),
            schema_string
        ).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    result_df = decoded_df.select("data.*", "kafka_timestamp")
    return result_df


def write_statistics_to_kafka(df, topic_name, schema_string, schema_id, config_dict):
    """
    Write statistics DataFrame to Kafka with Avro serialization.
    Uses Confluent Wire Format (magic byte + schema ID + Avro payload).

    For aggregation streams, we use "complete" output mode.
    """
    # Create Confluent Wire Format header
    magic_byte = bytearray([0])
    schema_id_bytes = struct.pack(">I", schema_id)
    header = bytes(magic_byte + schema_id_bytes)

    # Serialize to Avro
    from pyspark.sql.functions import struct as make_struct
    avro_payload = to_avro(
        make_struct([col(c) for c in df.columns]),
        schema_string
    )

    # Combine header + payload
    kafka_value = concat(lit(header), avro_payload)
    kafka_df = df.select(kafka_value.alias("value"))

    # Write stream directly (not using writer since we already have serialized data)
    query = (
        kafka_df
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config_dict["KAFKA_BOOTSTRAP_SERVERS"])
        .option("topic", topic_name)
        .option("checkpointLocation", f"{config_dict['CHECKPOINT_BASE_PATH']}/{topic_name}")
        .outputMode("complete")  # Complete mode for aggregations!
        .trigger(processingTime=config_dict["TRIGGER_INTERVAL"])
        .start()
    )

    return query


def main():
    """Main ETL flow for descriptive statistics calculation."""

    logger.info("=== Starting Descriptive Statistics ETL ===")

    # ========================================
    # STEP 1: INITIALIZE SPARK
    # ========================================
    spark = create_spark_session()

    # ========================================
    # STEP 2: FETCH AVRO SCHEMAS
    # ========================================
    logger.info("\nStep 1: Fetching Avro schemas...")

    # Input schema (bike-weather-distance)
    input_schema, _ = get_latest_schema("bike-weather-distance-value")
    logger.info("✓ Input schema fetched")

    # Output schemas
    overall_schema, overall_schema_id = get_latest_schema("weather-transport-statistics-overall-value")
    logger.info(f"✓ Overall statistics schema fetched (ID: {overall_schema_id})")

    by_condition_schema, by_condition_schema_id = get_latest_schema("weather-transport-statistics-by-condition-value")
    logger.info(f"✓ By-condition statistics schema fetched (ID: {by_condition_schema_id})")

    by_temp_schema, by_temp_schema_id = get_latest_schema("weather-transport-statistics-by-temperature-value")
    logger.info(f"✓ By-temperature statistics schema fetched (ID: {by_temp_schema_id})")

    by_precip_schema, by_precip_schema_id = get_latest_schema("weather-transport-statistics-by-precipitation-value")
    logger.info(f"✓ By-precipitation statistics schema fetched (ID: {by_precip_schema_id})")

    # ========================================
    # STEP 3: READ INPUT STREAM
    # ========================================
    logger.info("\nStep 2: Reading bike-weather-distance stream...")

    raw_stream = read_kafka_stream(
        spark,
        config.KAFKA_TOPICS.get("bike_weather_distance", "bike-weather-distance")
    )

    # Parse Avro data
    input_df = parse_bike_weather_distance_stream(raw_stream, input_schema)
    logger.info("✓ Input stream parsed")

    # Add derived columns needed for grouping
    input_df = input_df.withColumn(
        "weather_condition",
        when((col("temp_c") >= 10) & (col("temp_c") <= 25) & (col("precip_daily_mm") == 0), lit("good"))
        .when((col("temp_c") < 5) | (col("temp_c") > 30) | (col("precip_daily_mm") > 5), lit("poor"))
        .otherwise(lit("fair"))
    ).withColumn(
        "temp_bucket",
        when(col("temp_c") < 0, lit("Below 0°C"))
        .when((col("temp_c") >= 0) & (col("temp_c") < 10), lit("0-10°C"))
        .when((col("temp_c") >= 10) & (col("temp_c") < 20), lit("10-20°C"))
        .when((col("temp_c") >= 20) & (col("temp_c") < 30), lit("20-30°C"))
        .otherwise(lit("Above 30°C"))
    ).withColumn(
        "precip_level",
        when(col("precip_daily_mm") == 0, lit("None"))
        .when(col("precip_daily_mm") < 1, lit("Light (<1mm)"))
        .when((col("precip_daily_mm") >= 1) & (col("precip_daily_mm") < 5), lit("Moderate (1-5mm)"))
        .otherwise(lit("Heavy (>5mm)"))
    )
    logger.info("✓ Derived columns added (weather_condition, temp_bucket, precip_level)")

    # ========================================
    # STEP 4: CALCULATE STATISTICS
    # ========================================
    logger.info("\nStep 3: Calculating descriptive statistics...")

    # Overall statistics
    overall_stats = calculate_overall_statistics(input_df)
    logger.info("✓ Overall statistics calculated")

    # Statistics by weather condition
    stats_by_condition = calculate_statistics_by_weather_condition(input_df)
    logger.info("✓ Statistics by weather condition calculated")

    # Statistics by temperature bucket
    stats_by_temp = calculate_statistics_by_temperature_bucket(input_df)
    logger.info("✓ Statistics by temperature bucket calculated")

    # Statistics by precipitation level
    stats_by_precip = calculate_statistics_by_precipitation_level(input_df)
    logger.info("✓ Statistics by precipitation level calculated")

    # ========================================
    # STEP 5: WRITE TO KAFKA TOPICS
    # ========================================
    logger.info("\nStep 4: Writing statistics to Kafka topics...")

    config_dict = {
        "KAFKA_BOOTSTRAP_SERVERS": config.KAFKA_BOOTSTRAP_SERVERS,
        "CHECKPOINT_BASE_PATH": config.CHECKPOINT_BASE_PATH + "_stats",
        "TRIGGER_INTERVAL": "30 seconds"  # Update stats every 30 seconds
    }

    # Write overall statistics
    query1 = write_statistics_to_kafka(
        overall_stats,
        "weather-transport-statistics-overall",
        overall_schema,
        overall_schema_id,
        config_dict
    )
    logger.info("✓ Overall statistics stream started")

    # Write by-condition statistics
    query2 = write_statistics_to_kafka(
        stats_by_condition,
        "weather-transport-statistics-by-condition",
        by_condition_schema,
        by_condition_schema_id,
        config_dict
    )
    logger.info("✓ By-condition statistics stream started")

    # Write by-temperature statistics
    query3 = write_statistics_to_kafka(
        stats_by_temp,
        "weather-transport-statistics-by-temperature",
        by_temp_schema,
        by_temp_schema_id,
        config_dict
    )
    logger.info("✓ By-temperature statistics stream started")

    # Write by-precipitation statistics
    query4 = write_statistics_to_kafka(
        stats_by_precip,
        "weather-transport-statistics-by-precipitation",
        by_precip_schema,
        by_precip_schema_id,
        config_dict
    )
    logger.info("✓ By-precipitation statistics stream started")

    # ========================================
    # DONE - Streaming queries run in Spark Connect
    # ========================================
    logger.info("\n=== Statistics ETL streaming queries started successfully! ===")
    logger.info("Kafka Connect will sync data to Hive automatically...")
    logger.info("Dashboard can query from:")
    logger.info("  - weather_transport_statistics_overall")
    logger.info("  - weather_transport_statistics_by_condition")
    logger.info("  - weather_transport_statistics_by_temperature")
    logger.info("  - weather_transport_statistics_by_precipitation")


if __name__ == "__main__":
    main()
