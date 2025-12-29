"""
Weather data windowed aggregation logic.

This module provides time-based windowing operations for weather condition summaries.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    window,
    count,
    avg,
    min as spark_min,
    max as spark_max,
    when,
    first,
)


# Beskriv kort, hvad metoden gør i to sætninger.
def aggregate_weather_data_by_window(
    df: DataFrame, window_duration: str = "15 minutes", slide_duration: str = None
) -> DataFrame:
    """
    Aggregate weather observation data in time windows.

    Args:
        df: Weather DataFrame with datetime_ts and enriched columns
        window_duration: Window size
        slide_duration: Slide interval (None = tumbling)

    Returns:
        DataFrame with aggregated weather metrics per time window
    """
    # Group by time window
    if slide_duration:
        windowed = df.groupBy(
            window(col("datetime_ts"), window_duration, slide_duration)
        )
    else:
        windowed = df.groupBy(window(col("datetime_ts"), window_duration))

    # Compute aggregations
    result = windowed.agg(
        count("*").alias("observation_count"),
        # Temperature stats
        avg("temp_c").alias("avg_temperature_c"),
        spark_min("temp_c").alias("min_temperature_c"),
        spark_max("temp_c").alias("max_temperature_c"),
        # Wind stats
        avg("wind_speed_ms").alias("avg_wind_speed_ms"),
        spark_max("wind_speed_ms").alias("max_wind_speed_ms"),
        # Visibility stats
        avg("visibility_m").alias("avg_visibility_m"),
        spark_min("visibility_m").alias("min_visibility_m"),
        # Weather quality
        avg("weather_condition_score").alias("avg_weather_score"),
        count(when(col("is_good_weather") == True, 1)).alias("good_weather_obs_count"),
        count(when(col("is_bad_weather") == True, 1)).alias("bad_weather_obs_count"),
        # Most common categories (simplified - take first occurrence)
        first("temp_bucket").alias("dominant_temp_bucket"),
        first("wind_category").alias("dominant_wind_category"),
        first("visibility_category").alias("dominant_visibility_category"),
    )

    # Add derived metrics
    result = (
        result.withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn(
            "temperature_range_c", col("max_temperature_c") - col("min_temperature_c")
        )
        .withColumn(
            "good_weather_ratio",
            when(
                col("observation_count") > 0,
                col("good_weather_obs_count") / col("observation_count"),
            ).otherwise(0.0),
        )
    )

    return result
