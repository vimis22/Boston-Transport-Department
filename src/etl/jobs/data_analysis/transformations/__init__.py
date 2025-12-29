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

# Import NEW simplified transformation functions (SOLID principles)
from data_analysis.transformations.join_bike_weather import (
    join_bike_weather_data,
    add_weather_condition_category,
    add_precipitation_indicator,
)
from data_analysis.transformations.calculate_trip_metrics import (
    calculate_trip_distance_and_speed,
    filter_outliers,
    validate_coordinates,
)

# Import descriptive statistics functions (Mulighed 2 - Fase 1)
from data_analysis.transformations.calculate_descriptive_statistics import (
    calculate_overall_statistics,
    calculate_statistics_by_weather_condition,
    calculate_statistics_by_temperature_bucket,
    calculate_statistics_by_precipitation_level,
)

# Export all functions for backward compatibility
__all__ = [
    "decode_avro_payload",
    "parse_bike_stream",
    "parse_taxi_stream",
    "parse_weather_stream",
    "parse_accident_stream",
    # NEW exports (Mulighed 1)
    "join_bike_weather_data",
    "add_weather_condition_category",
    "add_precipitation_indicator",
    "calculate_trip_distance_and_speed",
    "filter_outliers",
    "validate_coordinates",
    # NEW exports (Mulighed 2 - Descriptive Statistics)
    "calculate_overall_statistics",
    "calculate_statistics_by_weather_condition",
    "calculate_statistics_by_temperature_bucket",
    "calculate_statistics_by_precipitation_level",
]
