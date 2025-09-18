"""
Taxi data windowed aggregation logic.

This module provides time-based windowing operations for taxi ride analytics.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    window,
    count,
    avg,
    sum as spark_sum,
    max as spark_max,
    when,
)


# Beskriv kort, hvad metoden gør i to sætninger.
def aggregate_taxi_data_by_window(
    df: DataFrame, window_duration: str = "15 minutes", slide_duration: str = None
) -> DataFrame:
    """
    Aggregate taxi ride data in time windows.

    Args:
        df: Taxi DataFrame with datetime_ts column
        window_duration: Window size
        slide_duration: Slide interval (None = tumbling)

    Returns:
        DataFrame with aggregated taxi metrics per time window
    """
    # Group by time window and cab type
    if slide_duration:
        windowed = df.groupBy(
            window(col("datetime_ts"), window_duration, slide_duration), "cab_type"
        )
    else:
        windowed = df.groupBy(window(col("datetime_ts"), window_duration), "cab_type")

    # Compute aggregations
    result = windowed.agg(
        count("*").alias("ride_count"),
        avg("price").alias("avg_price"),
        spark_sum("price").alias("total_revenue"),
        avg("distance").alias("avg_distance"),
        avg("surge_multiplier").alias("avg_surge"),
        spark_max("surge_multiplier").alias("max_surge"),
        # Weather at ride time (from embedded weather snapshot)
        avg("temperature").alias("avg_temperature"),
        avg("apparent_temperature").alias("avg_feels_like_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("precip_intensity").alias("avg_precip_intensity"),
    )

    # Add derived metrics
    result = (
        result.withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("revenue_per_ride", col("total_revenue") / col("ride_count"))
        .withColumn(
            "high_surge_indicator", when(col("avg_surge") > 1.5, True).otherwise(False)
        )
    )

    return result
