"""
Parse NOAA temperature string to Celsius.

This module provides functionality to parse NOAA temperature data.
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


def parse_temperature(tmp_string: str) -> float:
    """
    Parse NOAA temperature string to Celsius.

    Format: '+0056,1' where +0056 is temp in tenths of degree Celsius
    Returns: Temperature in Celsius (e.g., 5.6)
    """
    if not tmp_string or tmp_string == "":
        return None

    try:
        # Extract the numeric part (e.g., '+0056' from '+0056,1')
        temp_part = tmp_string.split(',')[0]
        # Convert to float and divide by 10 (it's in tenths of degree)
        temp_value = float(temp_part) / 10.0
        return temp_value
    except (ValueError, IndexError, AttributeError):
        return None


# Register UDF for Spark
parse_temperature_udf = udf(parse_temperature, DoubleType())
