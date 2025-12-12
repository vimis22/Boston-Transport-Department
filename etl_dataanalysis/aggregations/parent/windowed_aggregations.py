"""
Windowed aggregations for streaming data analysis.

This module provides time-based windowing operations for:
1. Bike rental statistics
2. Taxi ride analytics
3. Weather condition summaries
4. Accident event tracking
5. Cross-dataset correlations

This parent module delegates to specialized logic_children modules
following Separation of Concerns and SOLID principles.
"""

from pyspark.sql import DataFrame

# Import all aggregation functions from logic_children
from etl_dataanalysis.aggregations.logic_children.aggregate_bike_data_by_window import (
    aggregate_bike_data_by_window
)
from etl_dataanalysis.aggregations.logic_children.aggregate_taxi_data_by_window import (
    aggregate_taxi_data_by_window
)
from etl_dataanalysis.aggregations.logic_children.aggregate_weather_data_by_window import (
    aggregate_weather_data_by_window
)
from etl_dataanalysis.aggregations.logic_children.aggregate_accident_data_by_window import (
    aggregate_accident_data_by_window
)
from etl_dataanalysis.aggregations.logic_children.create_combined_transport_weather_window import (
    create_combined_transport_weather_window
)
from etl_dataanalysis.aggregations.logic_children.create_weather_binned_aggregations import (
    create_weather_binned_aggregations
)

# Re-export all functions to maintain backward compatibility
__all__ = [
    'aggregate_bike_data_by_window',
    'aggregate_taxi_data_by_window',
    'aggregate_weather_data_by_window',
    'aggregate_accident_data_by_window',
    'create_combined_transport_weather_window',
    'create_weather_binned_aggregations'
]
