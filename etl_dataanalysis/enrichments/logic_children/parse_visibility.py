"""
Parse NOAA visibility string.

This module provides functionality to parse NOAA visibility data.
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


def parse_visibility(vis_string: str) -> float:
    """
    Parse NOAA visibility string to get visibility in meters.

    Format: '016000,1,9,9' where 016000 is visibility in meters
    Returns: Visibility in meters (e.g., 16000)
    """
    if not vis_string or vis_string == "":
        return None

    try:
        parts = vis_string.split(',')
        if len(parts) >= 1:
            visibility = float(parts[0])
            return visibility
    except (ValueError, IndexError, AttributeError):
        return None


# Register UDF for Spark
parse_visibility_udf = udf(parse_visibility, DoubleType())
