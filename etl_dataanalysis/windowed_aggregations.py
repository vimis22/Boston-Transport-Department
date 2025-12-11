"""
Windowed aggregations for streaming data analysis.

This module provides time-based windowing operations for:
1. Bike rental statistics
2. Taxi ride analytics
3. Weather condition summaries
4. Accident event tracking
5. Cross-dataset correlations
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
    from pyspark.sql.functions import when

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


def create_weather_binned_aggregations(combined_df: DataFrame) -> DataFrame:
    """
    Create graph-ready binned aggregations for scatter plots.

    **PURPOSE**: Address Oskar's requirement for correlation graphs.

    Input: Combined transport-weather DataFrame with per-window metrics
    Output: Aggregated statistics grouped by weather bins

    **FOR VISUALIZATION**:
    Dashboard queries this table to create scatter plots:
    - X-axis: temp_bin_center (e.g., 2.5°C, 7.5°C, 12.5°C...)
    - Y-axis: avg_bike_rentals_in_bin

    Example output row:
    {
        temp_bin: "5_to_10C",
        temp_bin_center: 7.5,
        avg_bike_rentals: 120.5,
        avg_taxi_rides: 45.2,
        sample_count: 340
    }

    This enables Oskar to plot: "Temperature vs Bike Rentals"
    showing that bike usage peaks at 15-20°C.
    """
    from pyspark.sql.functions import avg as spark_avg, count, stddev

    # Add temperature bins
    binned_df = combined_df.withColumn(
        "temp_bin",
        when(col("avg_temperature_c").isNull(), "unknown")
        .when(col("avg_temperature_c") < 0, "-5_to_0C")
        .when(col("avg_temperature_c") < 5, "0_to_5C")
        .when(col("avg_temperature_c") < 10, "5_to_10C")
        .when(col("avg_temperature_c") < 15, "10_to_15C")
        .when(col("avg_temperature_c") < 20, "15_to_20C")
        .when(col("avg_temperature_c") < 25, "20_to_25C")
        .otherwise("25_to_30C")
    ).withColumn(
        "wind_bin",
        when(col("avg_wind_speed_ms").isNull(), "unknown")
        .when(col("avg_wind_speed_ms") < 3, "0_to_3ms")
        .when(col("avg_wind_speed_ms") < 6, "3_to_6ms")
        .when(col("avg_wind_speed_ms") < 9, "6_to_9ms")
        .when(col("avg_wind_speed_ms") < 12, "9_to_12ms")
        .otherwise("12+ms")
    ).withColumn(
        "weather_score_bin",
        when(col("avg_weather_score").isNull(), "unknown")
        .when(col("avg_weather_score") < 30, "poor_0-30")
        .when(col("avg_weather_score") < 50, "fair_30-50")
        .when(col("avg_weather_score") < 70, "good_50-70")
        .otherwise("excellent_70+")
    )

    # Aggregate by temperature bins
    temp_binned = binned_df.filter(col("temp_bin") != "unknown").groupBy("temp_bin").agg(
        spark_avg("total_bike_rentals").alias("avg_bike_rentals_in_temp_bin"),
        spark_avg("total_taxi_rides").alias("avg_taxi_rides_in_temp_bin"),
        spark_avg("total_transport_usage").alias("avg_total_usage_in_temp_bin"),
        count("*").alias("sample_count"),
        stddev("total_bike_rentals").alias("stddev_bike_rentals")
    ).withColumn(
        "aggregation_type", lit("by_temperature_bin")
    )

    # Aggregate by wind bins
    wind_binned = binned_df.filter(col("wind_bin") != "unknown").groupBy("wind_bin").agg(
        spark_avg("total_bike_rentals").alias("avg_bike_rentals_in_wind_bin"),
        spark_avg("total_taxi_rides").alias("avg_taxi_rides_in_wind_bin"),
        count("*").alias("sample_count")
    ).withColumn(
        "aggregation_type", lit("by_wind_bin")
    )

    # Aggregate by weather score bins
    weather_binned = binned_df.filter(col("weather_score_bin") != "unknown").groupBy("weather_score_bin").agg(
        spark_avg("total_bike_rentals").alias("avg_bike_rentals_in_score_bin"),
        spark_avg("total_taxi_rides").alias("avg_taxi_rides_in_score_bin"),
        spark_avg("avg_weather_score").alias("avg_score_in_bin"),
        count("*").alias("sample_count")
    ).withColumn(
        "aggregation_type", lit("by_weather_score_bin")
    )

    # Return the temperature-binned results (most important for Oskar's use case)
    return temp_binned
