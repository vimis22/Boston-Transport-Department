"""
Transformations module for parsing streaming data from Kafka.

This parent module delegates to logic_children modules following Separation of Concerns.
Each parse function handles a specific data stream type (bike, taxi, weather, accident).
"""

from pyspark.sql import DataFrame

# Import helper function from logic_children
from data_analysis.transformations.decode_avro_payload import decode_avro_payload

# Import parse functions from logic_children
from data_analysis.transformations.parse_bike_stream import parse_bike_stream
from data_analysis.transformations.parse_taxi_stream import parse_taxi_stream
from data_analysis.transformations.parse_weather_stream import parse_weather_stream
from data_analysis.transformations.parse_accident_stream import parse_accident_stream

# Export all functions for backward compatibility
__all__ = [
    "decode_avro_payload",
    "parse_bike_stream",
    "parse_taxi_stream",
    "parse_weather_stream",
    "parse_accident_stream",
]
