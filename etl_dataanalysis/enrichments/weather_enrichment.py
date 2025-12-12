"""
Weather data enrichment and parsing utilities.

This module provides functions to:
1. Parse complex NOAA weather strings (TMP, WND, VIS, etc.)
2. Create weather categories and buckets for analysis
3. Derive meaningful weather conditions from raw observations
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, udf, when, regexp_extract, substring, length, trim
)
from pyspark.sql.types import DoubleType, StringType, IntegerType

# Beskriv kort, hvad metoden gør i to sætninger.
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

# Beskriv kort, hvad metoden gør i to sætninger.
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

# Beskriv kort, hvad metoden gør i to sætninger.
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


# Register UDFs for Spark
parse_temperature_udf = udf(parse_temperature, DoubleType())
parse_wind_speed_udf = udf(parse_wind_speed, DoubleType())
parse_visibility_udf = udf(parse_visibility, DoubleType())

# Beskriv kort, hvad metoden gør i to sætninger.
def enrich_weather_data(df: DataFrame) -> DataFrame:
    """
    Enrich weather DataFrame with parsed values and categories.

    Input: Raw weather DataFrame with TMP, WND, VIS strings
    Output: Enriched DataFrame with:
        - temperature_celsius
        - wind_speed_ms
        - visibility_m
        - temp_bucket (e.g., '0-5C', '5-10C')
        - wind_category (calm/breezy/windy/stormy)
        - visibility_category (poor/moderate/good/excellent)
        - weather_condition_score (0-100, higher = better conditions)
    """

    # Parse raw weather strings
    enriched_df = df.withColumn(
        "temperature_celsius",
        parse_temperature_udf(col("temperature"))
    ).withColumn(
        "wind_speed_ms",
        parse_wind_speed_udf(col("wind"))
    ).withColumn(
        "visibility_m",
        parse_visibility_udf(col("visibility"))
    )

    # Temperature buckets (5-degree intervals)
    enriched_df = enriched_df.withColumn(
        "temp_bucket",
        when(col("temperature_celsius").isNull(), "unknown")
        .when(col("temperature_celsius") < -10, "below_-10C")
        .when(col("temperature_celsius") < -5, "-10_to_-5C")
        .when(col("temperature_celsius") < 0, "-5_to_0C")
        .when(col("temperature_celsius") < 5, "0_to_5C")
        .when(col("temperature_celsius") < 10, "5_to_10C")
        .when(col("temperature_celsius") < 15, "10_to_15C")
        .when(col("temperature_celsius") < 20, "15_to_20C")
        .when(col("temperature_celsius") < 25, "20_to_25C")
        .when(col("temperature_celsius") < 30, "25_to_30C")
        .otherwise("above_30C")
    )

    # Wind categories (based on Beaufort scale)
    enriched_df = enriched_df.withColumn(
        "wind_category",
        when(col("wind_speed_ms").isNull(), "unknown")
        .when(col("wind_speed_ms") < 1.5, "calm")           # 0-1.5 m/s
        .when(col("wind_speed_ms") < 5.5, "light_breeze")  # 1.5-5.5 m/s
        .when(col("wind_speed_ms") < 10.8, "moderate")     # 5.5-10.8 m/s
        .when(col("wind_speed_ms") < 17.2, "fresh")        # 10.8-17.2 m/s
        .otherwise("strong_wind")                           # > 17.2 m/s
    )

    # Visibility categories
    enriched_df = enriched_df.withColumn(
        "visibility_category",
        when(col("visibility_m").isNull(), "unknown")
        .when(col("visibility_m") < 1000, "very_poor")      # < 1km
        .when(col("visibility_m") < 4000, "poor")           # 1-4km
        .when(col("visibility_m") < 10000, "moderate")      # 4-10km
        .when(col("visibility_m") < 20000, "good")          # 10-20km
        .otherwise("excellent")                              # > 20km
    )

    # Weather condition score (0-100, higher = better)
    # Based on: temperature comfort, low wind, good visibility
    enriched_df = enriched_df.withColumn(
        "weather_condition_score",
        when(col("temperature_celsius").isNull(), 50.0)  # Unknown = neutral
        .otherwise(
            # Temperature component (0-40): ideal 15-25C
            when(col("temperature_celsius").between(15, 25), 40.0)
            .when(col("temperature_celsius").between(10, 15), 35.0)
            .when(col("temperature_celsius").between(25, 30), 35.0)
            .when(col("temperature_celsius").between(5, 10), 25.0)
            .when(col("temperature_celsius").between(0, 5), 20.0)
            .otherwise(10.0)
            +
            # Wind component (0-30): prefer calm to light breeze
            when(col("wind_speed_ms") < 1.5, 30.0)
            .when(col("wind_speed_ms") < 5.5, 25.0)
            .when(col("wind_speed_ms") < 10.8, 15.0)
            .otherwise(5.0)
            +
            # Visibility component (0-30): prefer excellent visibility
            when(col("visibility_m") > 20000, 30.0)
            .when(col("visibility_m") > 10000, 25.0)
            .when(col("visibility_m") > 4000, 15.0)
            .otherwise(5.0)
        )
    )

    # Is it "good weather" for outdoor activities?
    enriched_df = enriched_df.withColumn(
        "is_good_weather",
        when(col("weather_condition_score") >= 70, True)
        .otherwise(False)
    )

    # Is it "bad weather" (risky conditions)?
    enriched_df = enriched_df.withColumn(
        "is_bad_weather",
        when(col("weather_condition_score") <= 30, True)
        .otherwise(False)
    )

    return enriched_df

# Beskriv kort, hvad metoden gør i to sætninger.
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
        when(col("weather_condition_score") < 40, True)
        .otherwise(False)
    )

    df = df.withColumn(
        "precip_category",
        when(col("has_precipitation") == False, "none")
        .when(col("weather_condition_score") < 20, "heavy")
        .when(col("weather_condition_score") < 30, "moderate")
        .otherwise("light")
    )

    return df
