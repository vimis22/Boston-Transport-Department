"""
Simplified Bike-Weather ETL Job - Mulighed 1

Dette ETL job følger SOLID principper og producerer data til bike_weather_distance tabel.
Matcher eksisterende dashboard krav.

Data Flow:
    1. Læs bike_data + weather_data fra Kafka
    2. Join dem sammen baseret på tidspunkt
    3. Beregn distance og speed (Haversine)
    4. Skriv til Kafka topic → Kafka Connect → Hive
    
Simple, forståelige beregninger som kan forsvares til eksamen!
"""

import logging
from data_analysis import config
from data_analysis.utils.create_spark_session import create_spark_session
from pyspark.sql.functions import lit
from data_analysis.utils.get_latest_schema import get_latest_schema
from data_analysis.utils.read_kafka_stream import read_kafka_stream

# Import vores nye SOLID moduler
from data_analysis.transformations import (
    parse_bike_stream,
    parse_weather_stream,
    join_bike_weather_data,
    add_weather_condition_category,
    calculate_trip_distance_and_speed,
    filter_outliers,
    validate_coordinates,
)
from data_analysis.writers import create_kafka_writer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """
    Main ETL function - Simplified og fokuseret.
    
    Følger Single Responsibility Principle:
        - Hent data
        - Transform data
        - Skriv data
    """
    logger.info("=== Starting Simplified Bike-Weather ETL ===")
    logger.info("Målsætning: Producere bike_weather_distance data til dashboard")
    
    # Initialize Spark
    spark = create_spark_session()
    logger.info(f"Spark Version: {spark.version}")
    logger.info(f"Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Schema Registry: {config.SCHEMA_REGISTRY_URL}")
    
    try:
        # ========================================
        # STEP 1: HENT AVRO SCHEMAS
        # ========================================
        logger.info("\nStep 1: Fetching Avro schemas...")
        
        bike_schema, _ = get_latest_schema(config.SCHEMA_SUBJECTS["bike"])
        logger.info("✓ Bike schema fetched")
        
        weather_schema, _ = get_latest_schema(config.SCHEMA_SUBJECTS["weather"])
        logger.info("✓ Weather schema fetched")
        
        # Hent schema for OUTPUT topic (bike-weather-distance)
        output_schema_subject = "bike-weather-distance-value"
        output_schema, output_schema_id = get_latest_schema(output_schema_subject)
        logger.info(f"✓ Output schema fetched (ID: {output_schema_id})")
        
        # ========================================
        # STEP 2: LÆS OG PARSE KAFKA STREAMS
        # ========================================
        logger.info("\nStep 2: Reading Kafka streams...")
        
        # Læs rådata
        bike_raw = read_kafka_stream(spark, config.KAFKA_TOPICS["bike"])
        weather_raw = read_kafka_stream(spark, config.KAFKA_TOPICS["weather"])
        
        # Parse med Avro
        bike_df = parse_bike_stream(bike_raw, bike_schema)
        logger.info("✓ Bike stream parsed")
        
        weather_df = parse_weather_stream(weather_raw, weather_schema)
        logger.info("✓ Weather stream parsed")
        
        # ========================================
        # STEP 3: JOIN BIKE + WEATHER DATA
        # ========================================
        logger.info("\nStep 3: Joining bike and weather data...")
        
        # Join baseret på tidspunkt (30 minutters tolerance)
        bike_weather_df = join_bike_weather_data(
            bike_df,
            weather_df,
            time_window_minutes=30
        )
        logger.info("✓ Bike-weather join completed")
        
        # ========================================
        # STEP 4: BEREGN TRIP METRICS
        # ========================================
        logger.info("\nStep 4: Calculating trip metrics...")
        
        # Beregn distance og speed (Haversine formula)
        enriched_df = calculate_trip_distance_and_speed(bike_weather_df)
        logger.info("✓ Distance and speed calculated")
        
        # Tilføj vejr kategori
        enriched_df = add_weather_condition_category(enriched_df)
        logger.info("✓ Weather categories added")
        
        # Tilføj precipitation hvis tilgængelig
        # (BEMÆRK: Dette kræver at weather_df har precipitation data)
        # enriched_df = add_precipitation_indicator(enriched_df, weather_df)
        
        # Validér koordinater og filtrer outliers
        validated_df = validate_coordinates(enriched_df)
        final_df = filter_outliers(validated_df, max_speed_kmh=50.0)
        logger.info("✓ Data validated and outliers filtered")
        
        # ========================================
        # STEP 5: SKRIV TIL KAFKA TOPIC
        # ========================================
        logger.info("\nStep 5: Writing to Kafka topic 'bike-weather-distance'...")
        
        # Prepare kolonne liste der matcher schema
        output_columns = [
            "trip_id", "starttime", "stoptime", "tripduration",
            "start_station_id", "start_station_name",
            "start_station_latitude", "start_station_longitude",
            "end_station_id", "end_station_name",
            "end_station_latitude", "end_station_longitude",
            "distance_meters", "average_speed_kmh",
            "temp_c", "wind_speed_ms", "visibility_m",
            "weather_condition", "date", "hour"
        ]
        
        # Tilføj manglende kolonner med NULL hvis de ikke findes
        for col_name in output_columns:
            if col_name not in final_df.columns:
                final_df = final_df.withColumn(col_name, lit(None))
        
        # Select kun de kolonner vi skal bruge
        output_df = final_df.select(*output_columns)
        
        # Opret Kafka writer
        kafka_writer = create_kafka_writer({
            "KAFKA_BOOTSTRAP_SERVERS": config.KAFKA_BOOTSTRAP_SERVERS,
            "CHECKPOINT_BASE_PATH": config.CHECKPOINT_BASE_PATH,
            "TRIGGER_INTERVAL": "10 seconds"
        })
        
        # Skriv til Kafka
        query = kafka_writer.write_stream_to_kafka(
            df=output_df,
            topic_name="bike-weather-distance",
            schema_string=output_schema,
            schema_id=output_schema_id,
            output_mode="append"
        )
        
        logger.info("✓ Kafka stream started")
        logger.info(f"✓ Writing to topic: bike-weather-distance")
        logger.info(f"✓ Checkpoint: {config.CHECKPOINT_BASE_PATH}/bike-weather-distance")
        
        # ========================================
        # STEP 6: STREAMING STARTED
        # ========================================
        logger.info("\n=== ETL streaming query started successfully! ===")
        logger.info("Kafka Connect vil automatisk synkronisere til Hive...")
        logger.info("Dashboard kan nu query data fra Hive tabel: bike_weather_distance")
        logger.info("\nKeeping job alive to maintain streaming query...")

        # Keep the job running to maintain streaming query
        import signal
        import time

        def signal_handler(sig, frame):
            logger.info("\n=== Received termination signal, stopping query ===")
            query.stop()
            logger.info("Query stopped successfully")
            raise SystemExit(0)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Wait indefinitely (query runs in Spark Connect server)
        try:
            while True:
                time.sleep(60)
                # Log status every minute
                logger.info(f"Simplified ETL alive - Query active: {query.isActive}")
        except KeyboardInterrupt:
            logger.info("\n=== KeyboardInterrupt received, stopping query ===")
            query.stop()

    except Exception as e:
        logger.error(f"Error in ETL: {e}", exc_info=True)
        raise
    finally:
        logger.info("\n=== ETL shutdown ===")


if __name__ == "__main__":
    main()
