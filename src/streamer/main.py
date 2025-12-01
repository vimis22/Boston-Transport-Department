import logging
import os
import time
import json
from datetime import datetime
from streamers import WeatherStreamer, TaxiStreamer, BikeStreamer
from utils import TimeManager, KafkaProxy, SchemaRegistry
from config import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_datasets_exist() -> bool:
    """Check if all required datasets exist locally"""
    datasets = {
        "weather": config.weather_data_path,
        "taxi": config.taxi_data_path,
        "bike": config.bike_data_path,
    }
    
    all_exist = True
    for dataset_name, filepath in datasets.items():
        if os.path.exists(filepath):
            logger.info(f"Dataset {dataset_name} exists at {filepath}")
        else:
            logger.warning(f"Dataset {dataset_name} not found at {filepath}")
            all_exist = False
    return all_exist


def download_datasets_from_hadoop():
    """
    Download datasets from hadoop cluster.
    TODO: Implement actual download logic when needed.
    For now, just log that datasets should be downloaded.
    """
    logger.info("TODO: Implement dataset download from hadoop cluster")
    logger.info("For now, ensure datasets are available at:")
    logger.info(f"  Weather: {config.weather_data_path}")
    logger.info(f"  Taxi: {config.taxi_data_path}")
    logger.info(f"  Bike: {config.bike_data_path}")


def main():
    """Main streamer loop"""
    logger.info("Starting streamer...")
    logger.info(f"Time Manager URL: {config.time_manager_url}")
    logger.info(f"Kafka REST Proxy URL: {config.kafka_rest_proxy_url}")
    logger.info(f"Schema Registry URL: {config.schema_registry_url}")
    
    # Check if datasets exist
    if not check_datasets_exist():
        logger.warning("Some datasets are missing. Attempting to download from hadoop...")
        download_datasets_from_hadoop()
        
        # Check again after download attempt
        if not check_datasets_exist():
            logger.error("Required datasets are still missing. Exiting.")
            return
    
    # Initialize Schema Registry
    schema_registry = SchemaRegistry(base_url=config.schema_registry_url)
    
    # Fetch Avro schemas from Schema Registry
    try:
        logger.info("Fetching Avro schemas from Schema Registry...")
        weather_subject = f"{config.weather_topic}-value"
        taxi_subject = f"{config.taxi_topic}-value"
        bike_subject = f"{config.bike_topic}-value"
        
        weather_schema_info = schema_registry.get_schema(weather_subject)
        taxi_schema_info = schema_registry.get_schema(taxi_subject)
        bike_schema_info = schema_registry.get_schema(bike_subject)
        
        # Extract schema dictionaries from the response
        # Schema Registry returns schema as a JSON string, so we need to parse it
        weather_schema = json.loads(weather_schema_info["schema"])
        taxi_schema = json.loads(taxi_schema_info["schema"])
        bike_schema = json.loads(bike_schema_info["schema"])
        
        logger.info(f"  Weather schema (ID: {weather_schema_info['id']}, version: {weather_schema_info['version']})")
        logger.info(f"  Taxi schema (ID: {taxi_schema_info['id']}, version: {taxi_schema_info['version']})")
        logger.info(f"  Bike schema (ID: {bike_schema_info['id']}, version: {bike_schema_info['version']})")
        logger.info("Avro schemas fetched successfully")
    except Exception as e:
        logger.error(f"Failed to fetch Avro schemas from Schema Registry: {e}", exc_info=True)
        logger.error("Make sure schemas are uploaded using tools/create-schemas.py")
        return
    
    # Initialize streamers with config paths
    weather_streamer = WeatherStreamer(config.weather_data_path)
    taxi_streamer = TaxiStreamer(config.taxi_data_path)
    bike_streamer = BikeStreamer(config.bike_data_path)
    
    # Initialize time manager
    time_manager = TimeManager(base_url=config.time_manager_url)
    
    # Initialize Kafka proxy
    kafka = KafkaProxy(
        base_url=config.kafka_rest_proxy_url,
        cluster_id=config.kafka_cluster_id,
    )
    
    # Auto-discover cluster ID if not provided
    try:
        if config.kafka_cluster_id:
            logger.info(f"Using provided Kafka cluster ID: {config.kafka_cluster_id}")
        else:
            logger.info("Auto-discovering Kafka cluster ID...")
            cluster_id = kafka.cluster_id  # Trigger discovery
            logger.info(f"Discovered Kafka cluster ID: {cluster_id}")
    except Exception as e:
        logger.error(f"Failed to discover/get Kafka cluster ID: {e}", exc_info=True)
        return
    
    # Ensure topics exist
    try:
        logger.info("Ensuring Kafka topics exist...")
        kafka.ensure_topic(config.weather_topic, partitions=1, replication_factor=1)
        kafka.ensure_topic(config.taxi_topic, partitions=1, replication_factor=1)
        kafka.ensure_topic(config.bike_topic, partitions=1, replication_factor=1)
        logger.info("Kafka topics ready")
    except Exception as e:
        logger.error(f"Failed to ensure Kafka topics exist: {e}", exc_info=True)
        return
    
    # Initialize last_time to None (will be set on first iteration)
    last_time: datetime | None = None
    
    logger.info("Streamer initialized. Starting main loop...")
    logger.info(f"Checking for data every {config.poll_interval_seconds} seconds...")
    logger.info("Press Ctrl+C to stop")
    
    try:
        while True:
            # Get current time and check for run_id changes atomically
            try:
                current_time, run_id_changed = time_manager.get_current_time_and_check_run_id()
            except RuntimeError as e:
                logger.error(f"Failed to get current time: {e}")
                time.sleep(config.poll_interval_seconds)
                continue
            
            # Check if run_id changed (time travel occurred)
            if run_id_changed:
                logger.warning("⚠️  Run ID changed - time travel detected! Resetting query window.")
                last_time = current_time
                continue
            
            # If this is the first iteration, set last_time to current_time
            if last_time is None:
                last_time = current_time
                logger.info(f"Initialized last_time to {last_time.isoformat()}")
                time.sleep(config.poll_interval_seconds)
                continue
            
            # Only query if time has advanced
            if current_time <= last_time:
                logger.debug(f"Time has not advanced. Current: {current_time.isoformat()}, Last: {last_time.isoformat()}")
                time.sleep(config.poll_interval_seconds)
                continue
            
            logger.info(f"Querying data from {last_time.isoformat()} to {current_time.isoformat()}")
            
            # Query and publish each dataset
            try:
                # Weather data
                weather_rows = weather_streamer.get_rows(last_time, current_time)
                if weather_rows:
                    logger.info(f"  Weather: {len(weather_rows)} rows to publish")
                    try:
                        results = kafka.post_records_avro(
                            config.weather_topic,
                            weather_rows,
                            weather_schema,
                        )
                        # Log first result for verification
                        if results:
                            first_result = results[0]
                            logger.info(
                                f"  Weather: Published {len(weather_rows)} rows to {config.weather_topic} "
                                f"(first: partition={first_result.get('partition_id')}, offset={first_result.get('offset')})"
                            )
                    except Exception as e:
                        logger.error(f"  Weather: Failed to publish: {e}", exc_info=True)
                else:
                    logger.debug("  Weather: 0 rows")
                
                # Taxi data
                taxi_rows = taxi_streamer.get_rows(last_time, current_time)
                if taxi_rows:
                    logger.info(f"  Taxi: {len(taxi_rows)} rows to publish")
                    try:
                        results = kafka.post_records_avro(
                            config.taxi_topic,
                            taxi_rows,
                            taxi_schema,
                        )
                        if results:
                            first_result = results[0]
                            logger.info(
                                f"  Taxi: Published {len(taxi_rows)} rows to {config.taxi_topic} "
                                f"(first: partition={first_result.get('partition_id')}, offset={first_result.get('offset')})"
                            )
                    except Exception as e:
                        logger.error(f"  Taxi: Failed to publish: {e}", exc_info=True)
                else:
                    logger.debug("  Taxi: 0 rows")
                
                # Bike data
                bike_rows = bike_streamer.get_rows(last_time, current_time)
                if bike_rows:
                    logger.info(f"  Bike: {len(bike_rows)} rows to publish")
                    try:
                        results = kafka.post_records_avro(
                            config.bike_topic,
                            bike_rows,
                            bike_schema,
                        )
                        if results:
                            first_result = results[0]
                            logger.info(
                                f"  Bike: Published {len(bike_rows)} rows to {config.bike_topic} "
                                f"(first: partition={first_result.get('partition_id')}, offset={first_result.get('offset')})"
                            )
                    except Exception as e:
                        logger.error(f"  Bike: Failed to publish: {e}", exc_info=True)
                else:
                    logger.debug("  Bike: 0 rows")
                
                # Update last_time
                last_time = current_time
                
            except Exception as e:
                logger.error(f"Error querying/publishing datasets: {e}", exc_info=True)
            
            # Sleep before next iteration
            time.sleep(config.poll_interval_seconds)
            
    except KeyboardInterrupt:
        logger.info("Streamer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
