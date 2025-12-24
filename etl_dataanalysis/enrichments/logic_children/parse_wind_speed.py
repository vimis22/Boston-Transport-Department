"""
Parse NOAA wind observation string.

This module provides functionality to parse NOAA wind speed data.
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


def parse_wind_speed(wnd_string: str) -> float:
    """
    Parse NOAA wind observation string to get wind speed in m/s.

    Format: '160,1,N,0046,1' where 0046 is wind speed in m/s (scaled by 10)
    Returns: Wind speed in m/s (e.g., 4.6)
    """
    if not wnd_string or wnd_string == "":
        return None

    try:
        parts = wnd_string.split(',')
        if len(parts) >= 4:
            wind_speed_part = parts[3]
            # Convert to float and divide by 10
            wind_speed = float(wind_speed_part) / 10.0
            return wind_speed
    except (ValueError, IndexError, AttributeError):
        return None


# Register UDF for Spark
parse_wind_speed_udf = udf(parse_wind_speed, DoubleType())
