"""
Accident data windowed aggregation logic.

This module provides time-based windowing operations for accident event tracking.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, avg, when
)


# Beskriv kort, hvad metoden gør i to sætninger.
def aggregate_accident_data_by_window(df: DataFrame, window_duration: str = "15 minutes",
                                       slide_duration: str = None) -> DataFrame:
    """
    Aggregate accident data in time windows.

    Args:
        df: Accident DataFrame with dispatch_timestamp column
        window_duration: Window size (default: 15 minutes to match transport data)
        slide_duration: Slide interval (None = tumbling)

    Returns:
        DataFrame with aggregated accident metrics per time window
    """
    # Add watermark for late data
    df_with_watermark = df.withWatermark("dispatch_timestamp", "10 minutes")

    # Group by time window and mode type
    if slide_duration:
        windowed = df_with_watermark.groupBy(
            window(col("dispatch_timestamp"), window_duration, slide_duration),
            "mode_type"
        )
    else:
        windowed = df_with_watermark.groupBy(
            window(col("dispatch_timestamp"), window_duration),
            "mode_type"
        )

    # Compute aggregations
    result = windowed.agg(
        count("*").alias("accident_count"),
        count(when(col("location_type") == "Intersection", 1)).alias("intersection_accidents"),
        count(when(col("location_type") == "Street", 1)).alias("street_accidents"),
        avg(col("lat")).alias("avg_accident_lat"),
        avg(col("long")).alias("avg_accident_long")
    )

    # Add derived metrics
    result = result.withColumn(
        "window_start", col("window.start")
    ).withColumn(
        "window_end", col("window.end")
    ).withColumn(
        "intersection_accident_ratio",
        when(col("accident_count") > 0,
             col("intersection_accidents") / col("accident_count"))
        .otherwise(0.0)
    )

    return result
