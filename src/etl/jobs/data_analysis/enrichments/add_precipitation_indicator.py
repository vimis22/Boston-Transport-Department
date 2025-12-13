"""
Add precipitation indicators to weather data.

This module provides functionality to add precipitation indicators based on weather conditions.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def add_precipitation_indicator(df: DataFrame) -> DataFrame:
    """
    Add precipitation indicators based on weather remarks or precipitation data.

    This is a simplified version - in reality, you'd parse the AA1-AA4 fields
    or look at the REM field for precipitation mentions.
    """
    # For now, we'll create a simple placeholder
    # In a real implementation, you'd parse the precipitation fields
    df = df.withColumn(
        "has_precipitation",
        when(col("weather_condition_score") < 40, True).otherwise(False),
    )

    df = df.withColumn(
        "precip_category",
        when(col("has_precipitation") == False, "none")
        .when(col("weather_condition_score") < 20, "heavy")
        .when(col("weather_condition_score") < 30, "moderate")
        .otherwise("light"),
    )

    return df
