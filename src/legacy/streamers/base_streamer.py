"""
Base Streamer Class
Provides common functionality for all data streamers.
Synchronizes with time-manager and publishes to Kafka.
"""

import csv
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import requests
from kafka import KafkaProducer
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseStreamer:
    """Base class for data streamers"""

    def __init__(
        self,
        name: str,
        data_file: str,
        kafka_topic: str,
        time_column: str,
        time_format: str = "%Y-%m-%d %H:%M:%S"
    ):
        """
        Initialize streamer

        Args:
            name: Streamer name for logging
            data_file: Path to CSV data file
            kafka_topic: Kafka topic to publish to
            time_column: Column name containing timestamp
            time_format: Datetime format string
        """
        self.name = name
        self.data_file = data_file
        self.kafka_topic = kafka_topic
        self.time_column = time_column
        self.time_format = time_format

        # Configuration
        self.time_manager_url = os.getenv('TIME_MANAGER_URL', 'http://localhost:5000')
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.poll_interval = float(os.getenv('POLL_INTERVAL', '1.0'))  # seconds

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )

        # Load data
        self.data: List[Dict] = []
        self.data_index = 0

        logger.info(f"{self.name} initialized - Topic: {self.kafka_topic}")

    def load_data(self):
        """Load CSV data into memory"""
        logger.info(f"Loading data from {self.data_file}")
        try:
            with open(self.data_file, 'r') as f:
                reader = csv.DictReader(f)
                self.data = list(reader)

            # Sort by timestamp
            self.data.sort(key=lambda x: self._parse_timestamp(x[self.time_column]))

            logger.info(f"Loaded {len(self.data)} records")
            if self.data:
                first_time = self._parse_timestamp(self.data[0][self.time_column])
                last_time = self._parse_timestamp(self.data[-1][self.time_column])
                logger.info(f"Time range: {first_time} to {last_time}")

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime"""
        try:
            return datetime.strptime(timestamp_str, self.time_format)
        except ValueError:
            # Try ISO format as fallback
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

    def get_current_sim_time(self) -> Optional[datetime]:
        """Get current simulation time from time-manager"""
        try:
            response = requests.get(f"{self.time_manager_url}/api/v1/clock/time", timeout=5)
            response.raise_for_status()
            time_str = response.json()['current_time']
            return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        except Exception as e:
            logger.error(f"Error getting simulation time: {e}")
            return None

    def get_sim_status(self) -> Optional[Dict]:
        """Get simulation status from time-manager"""
        try:
            response = requests.get(f"{self.time_manager_url}/api/v1/clock/status", timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting simulation status: {e}")
            return None

    def transform_record(self, record: Dict) -> Dict:
        """
        Transform record before publishing (override in subclasses)

        Args:
            record: Raw record from CSV

        Returns:
            Transformed record
        """
        return record

    def get_partition_key(self, record: Dict, timestamp: datetime) -> str:
        """
        Generate partition key for Kafka message

        Args:
            record: Record to publish
            timestamp: Record timestamp

        Returns:
            Partition key (date-hour format: YYYY-MM-DD-HH)
        """
        return timestamp.strftime("%Y-%m-%d-%H")

    def publish_record(self, record: Dict, timestamp: datetime):
        """
        Publish record to Kafka

        Args:
            record: Record to publish
            timestamp: Record timestamp
        """
        try:
            # Transform record
            transformed = self.transform_record(record)

            # Add metadata
            message = {
                'data': transformed,
                'timestamp': timestamp.isoformat(),
                'source': self.name
            }

            # Get partition key
            partition_key = self.get_partition_key(record, timestamp)

            # Publish to Kafka
            future = self.producer.send(
                self.kafka_topic,
                key=partition_key,
                value=message
            )

            # Wait for confirmation (optional, can be async)
            future.get(timeout=10)

            logger.debug(f"Published record: {partition_key} to {self.kafka_topic}")

        except Exception as e:
            logger.error(f"Error publishing record: {e}")

    def stream(self):
        """Main streaming loop"""
        logger.info(f"{self.name} streaming started")

        # Wait for time-manager to be available
        while True:
            status = self.get_sim_status()
            if status:
                logger.info(f"Connected to time-manager: {status}")
                break
            logger.warning("Waiting for time-manager...")
            time.sleep(5)

        # Reset data index
        self.data_index = 0
        last_sim_time = None

        while True:
            try:
                # Get current simulation status
                status = self.get_sim_status()
                if not status:
                    logger.warning("Lost connection to time-manager")
                    time.sleep(self.poll_interval)
                    continue

                sim_state = status['state']
                sim_time = datetime.fromisoformat(status['current_time'].replace('Z', '+00:00'))

                # Only process when simulation is running
                if sim_state != 'running':
                    if last_sim_time is None or sim_state == 'stopped':
                        logger.info(f"Simulation {sim_state}, waiting...")
                    time.sleep(self.poll_interval)
                    last_sim_time = sim_time
                    continue

                # Check for reset (sim_time < last_sim_time)
                if last_sim_time and sim_time < last_sim_time:
                    logger.info("Simulation reset detected, resetting data index")
                    self.data_index = 0

                # Publish all records up to current simulation time
                records_published = 0
                while self.data_index < len(self.data):
                    record = self.data[self.data_index]
                    record_time = self._parse_timestamp(record[self.time_column])

                    if record_time <= sim_time:
                        self.publish_record(record, record_time)
                        self.data_index += 1
                        records_published += 1
                    else:
                        break

                if records_published > 0:
                    logger.info(
                        f"Published {records_published} records. "
                        f"Progress: {self.data_index}/{len(self.data)} "
                        f"({100 * self.data_index / len(self.data):.1f}%)"
                    )

                # Check if we've reached the end
                if self.data_index >= len(self.data):
                    logger.info(f"{self.name} completed all data")
                    # Keep running but just wait
                    time.sleep(self.poll_interval * 10)
                else:
                    # Sleep before next poll
                    time.sleep(self.poll_interval)

                last_sim_time = sim_time

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in streaming loop: {e}")
                time.sleep(self.poll_interval)

        # Cleanup
        self.producer.flush()
        self.producer.close()
        logger.info(f"{self.name} stopped")

    def run(self):
        """Run the streamer"""
        try:
            self.load_data()
            self.stream()
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
