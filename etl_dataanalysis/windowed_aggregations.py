"""
Windowed aggregations for streaming data analysis.

This module provides time-based windowing operations for:
1. Bike rental statistics
2. Taxi ride analytics
3. Weather condition summaries
4. Cross-dataset correlations
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, avg, sum as spark_sum, min as spark_min, max as spark_max,
    stddev, collect_list, first, last, expr, lit
)


def aggregate_bike_data_by_window(df: DataFrame, window_duration: str = "15 minutes",
                                   slide_duration: str = None) -> DataFrame:
    """
    Aggregate bike rental data in time windows.

    Args:
        df: Bike DataFrame with start_time_ts column
        window_duration: Window size (e.g., "5 minutes", "15 minutes", "1 hour")
        slide_duration: Slide interval (None = tumbling window, e.g., "5 minutes" = sliding)

    Returns:
        DataFrame with aggregated bike metrics per time window
    """
    # Group by time window
    if slide_duration:
        windowed = df.groupBy(
            window(col("start_time_ts"), window_duration, slide_duration),
            "user_type"
        )
    else:
        windowed = df.groupBy(
            window(col("start_time_ts"), window_duration),
            "user_type"
        )

    # Compute aggregations
    result = windowed.agg(
        count("*").alias("rental_count"),
        avg("duration_seconds").alias("avg_duration_seconds"),
        spark_min("duration_seconds").alias("min_duration_seconds"),
        spark_max("duration_seconds").alias("max_duration_seconds"),
        stddev("duration_seconds").alias("stddev_duration"),

        # Station popularity
        count("start_station_id").alias("total_trips_started"),

        # User demographics
        avg("birth_year").alias("avg_birth_year"),
        count(when(col("gender") == 2, 1)).alias("female_count"),
        count(when(col("gender") == 1, 1)).alias("male_count"),
        count(when(col("gender") == 0, 1)).alias("unknown_gender_count"),
    )

    # Add derived metrics
    result = result.withColumn(
        "window_start", col("window.start")
    ).withColumn(
        "window_end", col("window.end")
    ).withColumn(
        "avg_duration_minutes", col("avg_duration_seconds") / 60.0
    ).withColumn(
        "gender_diversity_ratio",
        when(col("male_count") + col("female_count") > 0,
             col("female_count") / (col("male_count") + col("female_count")))
        .otherwise(None)
    )

    return result


def aggregate_taxi_data_by_window(df: DataFrame, window_duration: str = "15 minutes",
                                   slide_duration: str = None) -> DataFrame:
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
            window(col("datetime_ts"), window_duration, slide_duration),
            "cab_type"
        )
    else:
        windowed = df.groupBy(
            window(col("datetime_ts"), window_duration),
            "cab_type"
        )

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
    result = result.withColumn(
        "window_start", col("window.start")
    ).withColumn(
        "window_end", col("window.end")
    ).withColumn(
        "revenue_per_ride", col("total_revenue") / col("ride_count")
    ).withColumn(
        "high_surge_indicator",
        when(col("avg_surge") > 1.5, True).otherwise(False)
    )

    return result


def aggregate_weather_data_by_window(df: DataFrame, window_duration: str = "15 minutes",
                                      slide_duration: str = None) -> DataFrame:
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
        windowed = df.groupBy(
            window(col("datetime_ts"), window_duration)
        )

    # Compute aggregations
    result = windowed.agg(
        count("*").alias("observation_count"),

        # Temperature stats
        avg("temperature_celsius").alias("avg_temperature_c"),
        spark_min("temperature_celsius").alias("min_temperature_c"),
        spark_max("temperature_celsius").alias("max_temperature_c"),

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
    result = result.withColumn(
        "window_start", col("window.start")
    ).withColumn(
        "window_end", col("window.end")
    ).withColumn(
        "temperature_range_c", col("max_temperature_c") - col("min_temperature_c")
    ).withColumn(
        "good_weather_ratio",
        when(col("observation_count") > 0,
             col("good_weather_obs_count") / col("observation_count"))
        .otherwise(0.0)
    )

    return result


def aggregate_accident_data_by_window(df: DataFrame, window_duration: str = "1 hour",
                                       slide_duration: str = None) -> DataFrame:
    """
    Aggregate accident data in time windows.

    Args:
        df: Accident DataFrame with dispatch_timestamp column
        window_duration: Window size
        slide_duration: Slide interval (None = tumbling)

    Returns:
        DataFrame with aggregated accident metrics per time window
    """
    # Group by time window
    if slide_duration:
        windowed = df.groupBy(
            window(col("dispatch_timestamp"), window_duration, slide_duration),
            "mode_type"
        )
    else:
        windowed = df.groupBy(
            window(col("dispatch_timestamp"), window_duration),
            "mode_type"
        )

    # Compute aggregations
    result = windowed.agg(
        count("*").alias("accident_count"),
        count(when(col("location_type") == "Intersection", 1)).alias("intersection_accidents"),
        count(when(col("location_type") == "Street", 1)).alias("street_accidents"),
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


def create_combined_transport_weather_window(bike_df: DataFrame, taxi_df: DataFrame,
                                               weather_df: DataFrame,
                                               window_duration: str = "15 minutes") -> DataFrame:
    """
    Create a unified time-windowed view combining transport and weather data.

    This enables correlation analysis between weather conditions and transport usage.

    Args:
        bike_df: Bike DataFrame with start_time_ts
        taxi_df: Taxi DataFrame with datetime_ts
        weather_df: Weather DataFrame with datetime_ts
        window_duration: Window size for aggregation

    Returns:
        Combined DataFrame with transport and weather metrics aligned by time window
    """
    # Aggregate each dataset
    bike_agg = aggregate_bike_data_by_window(bike_df, window_duration)
    taxi_agg = aggregate_taxi_data_by_window(taxi_df, window_duration)
    weather_agg = aggregate_weather_data_by_window(weather_df, window_duration)

    # Sum bike rentals across user types
    bike_total = bike_agg.groupBy("window_start", "window_end").agg(
        spark_sum("rental_count").alias("total_bike_rentals"),
        avg("avg_duration_minutes").alias("avg_bike_duration_min")
    )

    # Sum taxi rides across cab types
    taxi_total = taxi_agg.groupBy("window_start", "window_end").agg(
        spark_sum("ride_count").alias("total_taxi_rides"),
        avg("avg_price").alias("avg_taxi_price"),
        avg("avg_surge").alias("avg_taxi_surge")
    )

    # Join all three on time window
    combined = bike_total.join(taxi_total, ["window_start", "window_end"], "full_outer")
    combined = combined.join(weather_agg.select(
        "window_start", "window_end",
        "avg_temperature_c", "avg_wind_speed_ms", "avg_weather_score",
        "good_weather_ratio", "dominant_temp_bucket", "dominant_wind_category"
    ), ["window_start", "window_end"], "left")

    # Fill nulls with 0 for counts
    combined = combined.fillna({
        "total_bike_rentals": 0,
        "total_taxi_rides": 0
    })

    # Add total transport usage
    combined = combined.withColumn(
        "total_transport_usage",
        col("total_bike_rentals") + col("total_taxi_rides")
    )

    # Add transport mode share
    combined = combined.withColumn(
        "bike_share_pct",
        when(col("total_transport_usage") > 0,
             (col("total_bike_rentals") / col("total_transport_usage")) * 100)
        .otherwise(0.0)
    )

    return combined
