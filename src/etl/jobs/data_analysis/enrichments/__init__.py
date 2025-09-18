"""
Weather data enrichment and parsing utilities.

This module provides functions to:
1. Parse complex NOAA weather strings (TMP, WND, VIS, etc.)
2. Create weather categories and buckets for analysis
3. Derive meaningful weather conditions from raw observations

This parent module delegates to logic_children modules following Separation of Concerns.
"""

from pyspark.sql import DataFrame

# Import functions and UDFs from enrichments modules
from data_analysis.enrichments.parse_temperature import (
    parse_temperature,
    parse_temperature_udf
)
from data_analysis.enrichments.parse_wind_speed import (
    parse_wind_speed,
    parse_wind_speed_udf
)
from data_analysis.enrichments.parse_visibility import (
    parse_visibility,
    parse_visibility_udf
)
from data_analysis.enrichments.enrich_weather_data import (
    enrich_weather_data
)
from data_analysis.enrichments.add_precipitation_indicator import (
    add_precipitation_indicator
)

# Export all functions and UDFs for backward compatibility
__all__ = [
    'parse_temperature',
    'parse_temperature_udf',
    'parse_wind_speed',
    'parse_wind_speed_udf',
    'parse_visibility',
    'parse_visibility_udf',
    'enrich_weather_data',
    'add_precipitation_indicator'
]
