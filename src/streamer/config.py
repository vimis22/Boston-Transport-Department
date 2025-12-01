"""Configuration for the streamer"""

import os
from dataclasses import dataclass


@dataclass
class Config:
    """Streamer configuration"""
    
    # Dataset paths
    datasets_dir: str = "./boston_datasets/bigdata"
    weather_data_path: str = "./boston_datasets/bigdata/weather_data.parquet"
    taxi_data_path: str = "./boston_datasets/bigdata/taxi_data.parquet"
    bike_data_path: str = "./boston_datasets/bigdata/bike_data.parquet"
    
    # Time Manager
    time_manager_url: str = os.getenv("TIME_MANAGER_URL", "http://localhost:8000")
    
    # Kafka
    kafka_rest_proxy_url: str = os.getenv("KAFKA_REST_PROXY_URL", "http://localhost:8082")
    kafka_cluster_id: str | None = os.getenv("KAFKA_CLUSTER_ID", None)
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    
    # Kafka topics
    weather_topic: str = "weather-data"
    taxi_topic: str = "taxi-data"
    bike_topic: str = "bike-data"
    
    # Streaming settings
    poll_interval_seconds: float = 1.0
    
    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables"""
        return cls(
            datasets_dir=os.getenv("DATASETS_DIR", cls.datasets_dir),
            weather_data_path=os.getenv("WEATHER_DATA_PATH", cls.weather_data_path),
            taxi_data_path=os.getenv("TAXI_DATA_PATH", cls.taxi_data_path),
            bike_data_path=os.getenv("BIKE_DATA_PATH", cls.bike_data_path),
            time_manager_url=os.getenv("TIME_MANAGER_URL", cls.time_manager_url),
            kafka_rest_proxy_url=os.getenv("KAFKA_REST_PROXY_URL", cls.kafka_rest_proxy_url),
            kafka_cluster_id=os.getenv("KAFKA_CLUSTER_ID", cls.kafka_cluster_id),
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", cls.schema_registry_url),
            weather_topic=os.getenv("WEATHER_TOPIC", cls.weather_topic),
            taxi_topic=os.getenv("TAXI_TOPIC", cls.taxi_topic),
            bike_topic=os.getenv("BIKE_TOPIC", cls.bike_topic),
            poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", cls.poll_interval_seconds)),
        )


# Global config instance
config = Config.from_env()

