"""
Enrich weather DataFrame with parsed values and categories.

This module provides the main enrichment function for weather data.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, expr


def enrich_weather_data(df: DataFrame) -> DataFrame:
    """
    Enrich weather DataFrame with parsed values and categories.

    Input: Raw weather DataFrame with temperature/wind/visibility (can be strings or doubles)
    Output: Enriched DataFrame with:
        - temp_c (alias for temperature_celsius)
        - wind_speed_ms
        - visibility_m
        - temp_bucket (e.g., '0-5C', '5-10C')
        - wind_category (calm/breezy/windy/stormy)
        - visibility_category (poor/moderate/good/excellent)
        - weather_condition_score (0-100, higher = better conditions)
    """

    # Temperature: Check if already parsed (double) or needs parsing (string)
    enriched_df = df.withColumn(
        "temp_c",
        when(col("temperature").cast("string").contains(","),
             # Parse coded string: Format '+0056,1' -> extract '+0056' and divide by 10
             expr("CAST(split(temperature, ',')[0] AS DOUBLE) / 10.0")
        ).otherwise(
             # Already parsed as double
             col("temperature").cast("double")
        )
    )

    # Wind speed: Check if already parsed or needs parsing
    enriched_df = enriched_df.withColumn(
        "wind_speed_ms",
        when(col("wind").cast("string").contains(","),
             # Parse coded string: Format '160,1,N,0046,1' -> extract 4th element (0046) and divide by 10
             expr("CAST(split(wind, ',')[3] AS DOUBLE) / 10.0")
        ).otherwise(
             # If single value, assume already in m/s
             col("wind").cast("double")
        )
    )

    # Visibility: Check if already parsed or needs parsing
    enriched_df = enriched_df.withColumn(
        "visibility_m",
        when(col("visibility").cast("string").contains(","),
             # Parse coded string: Format '016000,1,9,9' -> extract first element
             expr("CAST(split(visibility, ',')[0] AS DOUBLE)")
        ).otherwise(
             # Already parsed as meters
             col("visibility").cast("double")
        )
    )

    # Temperature buckets (5-degree intervals)
    enriched_df = enriched_df.withColumn(
        "temp_bucket",
        when(col("temp_c").isNull(), "unknown")
        .when(col("temp_c") < -10, "below_-10C")
        .when(col("temp_c") < -5, "-10_to_-5C")
        .when(col("temp_c") < 0, "-5_to_0C")
        .when(col("temp_c") < 5, "0_to_5C")
        .when(col("temp_c") < 10, "5_to_10C")
        .when(col("temp_c") < 15, "10_to_15C")
        .when(col("temp_c") < 20, "15_to_20C")
        .when(col("temp_c") < 25, "20_to_25C")
        .when(col("temp_c") < 30, "25_to_30C")
        .otherwise("above_30C"),
    )

    # Wind categories (based on Beaufort scale)
    enriched_df = enriched_df.withColumn(
        "wind_category",
        when(col("wind_speed_ms").isNull(), "unknown")
        .when(col("wind_speed_ms") < 1.5, "calm")  # 0-1.5 m/s
        .when(col("wind_speed_ms") < 5.5, "light_breeze")  # 1.5-5.5 m/s
        .when(col("wind_speed_ms") < 10.8, "moderate")  # 5.5-10.8 m/s
        .when(col("wind_speed_ms") < 17.2, "fresh")  # 10.8-17.2 m/s
        .otherwise("strong_wind"),  # > 17.2 m/s
    )

    # Visibility categories
    enriched_df = enriched_df.withColumn(
        "visibility_category",
        when(col("visibility_m").isNull(), "unknown")
        .when(col("visibility_m") < 1000, "very_poor")  # < 1km
        .when(col("visibility_m") < 4000, "poor")  # 1-4km
        .when(col("visibility_m") < 10000, "moderate")  # 4-10km
        .when(col("visibility_m") < 20000, "good")  # 10-20km
        .otherwise("excellent"),  # > 20km
    )

    # Weather condition score (0-100, higher = better)
    # Based on: temperature comfort, low wind, good visibility
    enriched_df = enriched_df.withColumn(
        "weather_condition_score",
        when(col("temp_c").isNull(), 50.0).otherwise(
            when(col("temp_c").between(15, 25), 40.0)
            .when(col("temp_c").between(10, 15), 35.0)
            .when(col("temp_c").between(25, 30), 35.0)
            .when(col("temp_c").between(5, 10), 25.0)
            .when(col("temp_c").between(0, 5), 20.0)
            .otherwise(10.0)
            +
            when(col("wind_speed_ms") < 1.5, 30.0)
            .when(col("wind_speed_ms") < 5.5, 25.0)
            .when(col("wind_speed_ms") < 10.8, 15.0)
            .otherwise(5.0)
            +
            when(col("visibility_m") > 20000, 30.0)
            .when(col("visibility_m") > 10000, 25.0)
            .when(col("visibility_m") > 4000, 15.0)
            .otherwise(5.0)
        ),
    )

    # Is it "good weather" for outdoor activities?
    enriched_df = enriched_df.withColumn(
        "is_good_weather",
        when(col("weather_condition_score") >= 70, True).otherwise(False),
    )

    # Is it "bad weather" (risky conditions)?
    enriched_df = enriched_df.withColumn(
        "is_bad_weather",
        when(col("weather_condition_score") <= 30, True).otherwise(False),
    )

    return enriched_df
