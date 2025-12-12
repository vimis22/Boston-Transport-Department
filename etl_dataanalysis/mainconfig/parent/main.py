import logging
import requests
import struct
from typing import Tuple
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro

#Here we begin all 3 streams.
from . import config
from .transformations import (
    parse_bike_stream,
    parse_taxi_stream,
    parse_weather_stream,
    parse_accident_stream,
)
from etl_dataanalysis.aggregations.parent.windowed_aggregations import (
    create_combined_transport_weather_window,
    aggregate_accident_data_by_window,
    create_weather_binned_aggregations
)
from .analytics import (
    calculate_weather_transport_correlation,
    calculate_weather_safety_risk,
    calculate_surge_weather_correlation,
    generate_transport_usage_summary,
    # NEW: Enhanced analytics for academic correlation analysis
    calculate_pearson_correlations,
    calculate_binned_weather_aggregations,
    calculate_precipitation_impact_analysis,
    calculate_temporal_segmented_correlations,
    calculate_multi_variable_correlation_summary,
    # NEW: Accident-weather correlation for safety analysis
    calculate_accident_weather_correlation
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Helper function to fetch Avro schema from Schema Registry
def get_latest_schema(subject: str) -> Tuple[str, int]:
    """
    Fetch the latest Avro schema from Schema Registry.

    Args:
        subject: Schema subject name (e.g., "bike-trips-value")

    Returns:
        Tuple of (schema_json_string, schema_id)
    """
    url = f"{config.SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    logger.info(f"Fetching schema from: {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    return data["schema"], data["id"]


# Beskriv kort, hvad metoden gør i to sætninger.
def create_spark_session() -> SparkSession:
    """
    Create Spark session - supports both local and Spark Connect modes.

    If USE_SPARK_CONNECT=true, connects to remote Spark cluster (Kubernetes).
    Otherwise, creates local SparkSession.
    """
    if config.USE_SPARK_CONNECT:
        logger.info(f"Using Spark Connect: {config.SPARK_CONNECT_URL}")
        return (
            SparkSession.builder
            .remote(config.SPARK_CONNECT_URL)
            .appName(config.SPARK_APP_NAME)
            .getOrCreate()
        )
    else:
        logger.info("Using local SparkSession")
        return (
            SparkSession.builder.config(conf=config.spark_config).getOrCreate()
        )

# Beskriv kort, hvad metoden gør i to sætninger.
def read_kafka_stream(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

# Beskriv kort, hvad metoden gør i to sætninger.
def write_parquest_stream(df, subfolder: str):
    output_path = f"{config.OUTPUT_BASE_PATH}/{subfolder}"
    checkpoint_path = f"{config.CHECKPOINT_BASE_PATH}/{subfolder}"

    logger.info(f"Writing to {output_path}")
    return (
        df.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("year", "month", "date", "hour")
        .outputMode("append")
        .trigger(processingTime=config.BATCH_INTERVAL)
        .start()
    )

# Beskriv kort, hvad metoden gør i to sætninger.
def write_analytics_stream(df, subfolder: str, output_mode: str = "append"):
    """
    Write analytics results to a separate analytics folder.

    Args:
        df: DataFrame to write
        subfolder: Subfolder name under analytics path
        output_mode: Spark output mode ("append", "update", or "complete")
    """
    output_path = f"{config.ANALYTICS_OUTPUT_PATH}/{subfolder}"
    checkpoint_path = f"{config.ANALYTICS_CHECKPOINT_PATH}/{subfolder}"

    logger.info(f"Writing analytics to {output_path} (mode: {output_mode})")
    return (
        df.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode(output_mode)
        .trigger(processingTime=config.BATCH_INTERVAL)
        .start()
    )

# Beskriv kort, hvad metoden gør i to sætninger.
def write_to_kafka_with_avro(df, topic: str, schema: str, schema_id: int, query_name: str):
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
        F.concat(
            F.lit(header),
            to_avro(F.struct("*"), schema)
        ).alias("value")
    )

    checkpoint_path = f"{config.CHECKPOINT_BASE_PATH}/kafka_output/{query_name}"

    logger.info(f"Writing to Kafka topic '{topic}' with Avro encoding (schema ID: {schema_id})")
    return (
        payload.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime=config.BATCH_INTERVAL)
        .queryName(query_name)
        .start()
    )

# Beskriv kort, hvad metoden gør i to sætninger.
def main():
    logger.info("=== Starting Boston Transport ETL with Data Analysis ===")
    spark = create_spark_session()

    # Set log level (only available for local SparkSession, not Spark Connect)
    if not config.USE_SPARK_CONNECT:
        spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Spark Version: {spark.version}")
    logger.info(f"Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Schema Registry: {config.SCHEMA_REGISTRY_URL}")
    logger.info(f"Output base: {config.OUTPUT_BASE_PATH}")
    logger.info(f"Analytics output: {config.ANALYTICS_OUTPUT_PATH}")

    # Track all streaming queries
    queries = []

    try:
        # ========================================
        # STEP 0: FETCH AVRO SCHEMAS FROM SCHEMA REGISTRY
        # ========================================
        logger.info("Step 0: Fetching Avro schemas from Schema Registry...")

        bike_schema, _ = get_latest_schema(config.SCHEMA_SUBJECTS["bike"])
        logger.info("✓ Bike schema fetched")

        taxi_schema, _ = get_latest_schema(config.SCHEMA_SUBJECTS["taxi"])
        logger.info("✓ Taxi schema fetched")

        weather_schema, _ = get_latest_schema(config.SCHEMA_SUBJECTS["weather"])
        logger.info("✓ Weather schema fetched")

        accident_schema, _ = get_latest_schema(config.SCHEMA_SUBJECTS["accidents"])
        logger.info("✓ Accident schema fetched")

        logger.info("All schemas fetched successfully!\n")

        # ========================================
        # STEP 1: EXTRACTION & BASIC TRANSFORMATION
        # ========================================
        logger.info("Step 1: Reading and parsing Kafka streams with Avro decoding...")

        bike_raw = read_kafka_stream(spark, config.KAFKA_TOPICS["bike"])
        bike_df = parse_bike_stream(bike_raw, bike_schema)
        bike_query = write_parquest_stream(bike_df, "bike_trips")
        queries.append(bike_query)

        taxi_raw = read_kafka_stream(spark, config.KAFKA_TOPICS["taxi"])
        taxi_df = parse_taxi_stream(taxi_raw, taxi_schema)
        taxi_query = write_parquest_stream(taxi_df, "taxi_trips")
        queries.append(taxi_query)

        weather_raw = read_kafka_stream(spark, config.KAFKA_TOPICS["weather"])
        weather_df = parse_weather_stream(weather_raw, weather_schema)  # Now includes enrichment!
        weather_query = write_parquest_stream(weather_df, "weather_data")
        queries.append(weather_query)

        accident_raw = read_kafka_stream(spark, "accidents")
        accident_df = parse_accident_stream(accident_raw, accident_schema)
        accident_query = write_parquest_stream(accident_df, "accidents")
        queries.append(accident_query)

        logger.info("✓ Basic ETL streams started with Avro decoding")

        # ========================================
        # STEP 2: ANALYTICS & CORRELATIONS
        # ========================================
        logger.info("Step 2: Setting up analytics streams...")

        # Analytics 1: Weather-Transport Correlation
        if config.ENABLE_WEATHER_TRANSPORT_CORRELATION:
            logger.info("  - Creating weather-transport correlation stream...")
            combined_df = create_combined_transport_weather_window(
                bike_df, taxi_df, weather_df,
                window_duration=config.WINDOW_DURATION_MEDIUM
            )
            correlation_df = calculate_weather_transport_correlation(combined_df)
            correlation_query = write_analytics_stream(
                correlation_df,
                "weather_transport_correlation",
                output_mode="append"
            )
            queries.append(correlation_query)
            logger.info("  ✓ Weather-transport correlation stream started")

        # Analytics 2: Weather-Safety Risk Analysis
        if config.ENABLE_WEATHER_SAFETY_ANALYSIS:
            logger.info("  - Creating weather-safety analysis stream...")
            safety_df = calculate_weather_safety_risk(
                accident_df, weather_df,
                window_duration=config.WINDOW_DURATION_LONG
            )
            safety_query = write_analytics_stream(
                safety_df,
                "weather_safety_analysis",
                output_mode="append"
            )
            queries.append(safety_query)
            logger.info("  ✓ Weather-safety analysis stream started")

        # Analytics 3: Surge-Weather Correlation
        if config.ENABLE_SURGE_WEATHER_CORRELATION:
            logger.info("  - Creating surge-weather correlation stream...")
            surge_df = calculate_surge_weather_correlation(taxi_df)
            surge_query = write_analytics_stream(
                surge_df,
                "surge_weather_correlation",
                output_mode="append"
            )
            queries.append(surge_query)
            logger.info("  ✓ Surge-weather correlation stream started")

        # Analytics 4: Transport Usage Summary (for dashboard time series)
        if config.ENABLE_TRANSPORT_USAGE_SUMMARY:
            logger.info("  - Creating transport usage summary stream...")
            summary_df = generate_transport_usage_summary(
                bike_df, taxi_df,
                window_duration=config.WINDOW_DURATION_LONG
            )
            summary_query = write_analytics_stream(
                summary_df,
                "transport_usage_summary",
                output_mode="append"
            )
            queries.append(summary_query)
            logger.info("  ✓ Transport usage summary stream started")

        # ========================================
        # STEP 3: ENHANCED ACADEMIC ANALYTICS
        # ========================================
        logger.info("Step 3: Setting up enhanced academic analytics streams...")

        # Analytics 5: Pearson Correlation Metrics
        if config.ENABLE_PEARSON_CORRELATIONS:
            logger.info("  - Creating Pearson correlation metrics stream...")
            combined_df = create_combined_transport_weather_window(
                bike_df, taxi_df, weather_df,
                window_duration=config.WINDOW_DURATION_MEDIUM
            )
            pearson_df = calculate_pearson_correlations(combined_df)
            pearson_query = write_analytics_stream(
                pearson_df,
                "pearson_correlations",
                output_mode="append"
            )
            queries.append(pearson_query)
            logger.info("  ✓ Pearson correlation stream started")

        # Analytics 6: Binned Weather Aggregations (for scatter plots)
        if config.ENABLE_BINNED_AGGREGATIONS:
            logger.info("  - Creating binned weather aggregations stream...")
            combined_df = create_combined_transport_weather_window(
                bike_df, taxi_df, weather_df,
                window_duration=config.WINDOW_DURATION_MEDIUM
            )
            binned_df = calculate_binned_weather_aggregations(combined_df)
            binned_query = write_analytics_stream(
                binned_df,
                "weather_binned_metrics",
                output_mode="append"
            )
            queries.append(binned_query)
            logger.info("  ✓ Binned aggregations stream started")

        # Analytics 7: Precipitation Impact Analysis
        if config.ENABLE_PRECIPITATION_ANALYSIS:
            logger.info("  - Creating precipitation impact analysis stream...")
            combined_df = create_combined_transport_weather_window(
                bike_df, taxi_df, weather_df,
                window_duration=config.WINDOW_DURATION_MEDIUM
            )
            precip_df = calculate_precipitation_impact_analysis(combined_df)
            precip_query = write_analytics_stream(
                precip_df,
                "precipitation_impact",
                output_mode="append"
            )
            queries.append(precip_query)
            logger.info("  ✓ Precipitation impact stream started")

        # Analytics 8: Temporal Segmented Correlations
        if config.ENABLE_TEMPORAL_CORRELATIONS:
            logger.info("  - Creating temporal segmented correlations stream...")
            combined_df = create_combined_transport_weather_window(
                bike_df, taxi_df, weather_df,
                window_duration=config.WINDOW_DURATION_MEDIUM
            )
            temporal_df = calculate_temporal_segmented_correlations(combined_df)
            temporal_query = write_analytics_stream(
                temporal_df,
                "temporal_correlations",
                output_mode="append"
            )
            queries.append(temporal_query)
            logger.info("  ✓ Temporal correlation stream started")

        # Analytics 9: Multi-Variable Correlation Summary
        if config.ENABLE_MULTI_VARIABLE_SUMMARY:
            logger.info("  - Creating multi-variable correlation summary stream...")
            combined_df = create_combined_transport_weather_window(
                bike_df, taxi_df, weather_df,
                window_duration=config.WINDOW_DURATION_MEDIUM
            )
            multi_var_df = calculate_multi_variable_correlation_summary(combined_df)
            multi_var_query = write_analytics_stream(
                multi_var_df,
                "multi_variable_summary",
                output_mode="append"
            )
            queries.append(multi_var_query)
            logger.info("  ✓ Multi-variable summary stream started")

        # Analytics 10: Accident-Weather Correlation (NEW - Safety Analysis)
        if config.ENABLE_ACCIDENT_WEATHER_CORRELATION:
            logger.info("  - Creating accident-weather correlation stream...")
            combined_df = create_combined_transport_weather_window(
                bike_df, taxi_df, weather_df,
                window_duration=config.WINDOW_DURATION_MEDIUM
            )
            accident_weather_df = calculate_accident_weather_correlation(combined_df, accident_df)
            accident_weather_query = write_analytics_stream(
                accident_weather_df,
                "accident_weather_correlation",
                output_mode="append"
            )
            queries.append(accident_weather_query)
            logger.info("  ✓ Accident-weather correlation stream started")

        logger.info(f"\n=== ALL STREAMS STARTED ({len(queries)} total) ===")
        logger.info("Raw data → /data/processed_simple/")
        logger.info("Analytics → /data/analytics/")
        logger.info("\n📊 ENHANCED ACADEMIC ANALYTICS ENABLED 📊")
        logger.info("  ✓ Pearson correlations")
        logger.info("  ✓ Binned aggregations (graph-ready)")
        logger.info("  ✓ Precipitation impact analysis")
        logger.info("  ✓ Temporal segmentation (rush hour vs leisure)")
        logger.info("  ✓ Multi-variable correlation summary")
        logger.info("\nStreaming queries are running. Press Ctrl+C to stop.\n")

        # Wait for termination
        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("\n=== Stopping ETL (KeyboardInterrupt) ===")
        for query in queries:
            query.stop()
    except Exception:
        logger.exception("Error in ETL")
    finally:
        spark.stop()
        logger.info("=== ETL finished ===")

if __name__ == "__main__":
    main()

