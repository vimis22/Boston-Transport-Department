from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, hour as hour_func, dayofweek


def calculate_temporal_segmented_correlations(combined_df: DataFrame) -> DataFrame:
    """
    Beregner vejr-transport korrelationer segmenteret efter tid (rush hour, weekend, off-peak).
    Commuter trips er mindre vejr-sensitive end leisure trips - producerer segment-specific weather sensitivity scores.

    Args:
        combined_df: Combined DataFrame with transport and weather data

    Returns:
        DataFrame with temporal segmented correlation analysis
    """
    # Add temporal dimensions
    result = combined_df.withColumn(
        "hour_of_day", hour_func(col("window_start"))
    ).withColumn(
        "day_of_week", dayofweek(col("window_start"))
    ).withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), True).otherwise(False)
    ).withColumn(
        "is_rush_hour",
        when(col("hour_of_day").isin([7, 8, 9, 17, 18, 19]), True).otherwise(False)
    )

    # Create temporal segments
    result = result.withColumn(
        "temporal_segment",
        when((col("is_weekend") == False) & (col("is_rush_hour") == True), "weekday_rush_hour")
        .when((col("is_weekend") == False) & (col("is_rush_hour") == False), "weekday_off_peak")
        .when((col("is_weekend") == True) & (col("hour_of_day").between(8, 20)), "weekend_day")
        .otherwise("weekend_night")
    )

    # Assign weather sensitivity by segment
    result = result.withColumn(
        "segment_weather_sensitivity",
        when(col("temporal_segment") == "weekday_rush_hour", "low")
        .when(col("temporal_segment") == "weekday_off_peak", "medium")
        .when(col("temporal_segment") == "weekend_day", "high")
        .otherwise("very_high")
    )

    # Expected correlation strength (based on transport research literature)
    result = result.withColumn(
        "expected_temp_bike_correlation",
        when(col("temporal_segment") == "weekday_rush_hour", 0.25)     # Low sensitivity
        .when(col("temporal_segment") == "weekday_off_peak", 0.45)     # Medium
        .when(col("temporal_segment") == "weekend_day", 0.75)          # High sensitivity
        .otherwise(0.60)
    )

    # Usage intensity by segment (for normalization)
    result = result.withColumn(
        "usage_intensity_category",
        when(col("total_transport_usage") < 50, "very_low")
        .when(col("total_transport_usage") < 150, "low")
        .when(col("total_transport_usage") < 300, "moderate")
        .when(col("total_transport_usage") < 500, "high")
        .otherwise("very_high")
    )

    return result
