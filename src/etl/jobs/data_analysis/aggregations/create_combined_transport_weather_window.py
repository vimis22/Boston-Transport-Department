"""
Combined transport-weather windowed aggregation logic.

This module creates a unified time-windowed view combining transport and weather data
for correlation analysis.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, sum as spark_sum, when
from data_analysis.aggregations.aggregate_bike_data_by_window import (
    aggregate_bike_data_by_window,
)
from data_analysis.aggregations.aggregate_taxi_data_by_window import (
    aggregate_taxi_data_by_window,
)
from data_analysis.aggregations.aggregate_weather_data_by_window import (
    aggregate_weather_data_by_window,
)


# Beskriv kort, hvad metoden gør i to sætninger.
def create_combined_transport_weather_window(
    bike_df: DataFrame,
    taxi_df: DataFrame,
    weather_df: DataFrame,
    window_duration: str = "15 minutes",
) -> DataFrame:
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
        avg("avg_duration_minutes").alias("avg_bike_duration_min"),
    )

    # Sum taxi rides across cab types
    taxi_total = taxi_agg.groupBy("window_start", "window_end").agg(
        spark_sum("ride_count").alias("total_taxi_rides"),
        avg("avg_price").alias("avg_taxi_price"),
        avg("avg_surge").alias("avg_taxi_surge"),
    )

    # Join all three on time window
    combined = bike_total.join(taxi_total, ["window_start", "window_end"], "full_outer")
    combined = combined.join(
        weather_agg.select(
            "window_start",
            "window_end",
            "avg_temperature_c",
            "avg_wind_speed_ms",
            "avg_weather_score",
            "good_weather_ratio",
            "dominant_temp_bucket",
            "dominant_wind_category",
        ),
        ["window_start", "window_end"],
        "left",
    )

    # Fill nulls with 0 for counts
    combined = combined.fillna({"total_bike_rentals": 0, "total_taxi_rides": 0})

    # Add total transport usage
    combined = combined.withColumn(
        "total_transport_usage", col("total_bike_rentals") + col("total_taxi_rides")
    )

    # Add transport mode share
    combined = combined.withColumn(
        "bike_share_pct",
        when(
            col("total_transport_usage") > 0,
            (col("total_bike_rentals") / col("total_transport_usage")) * 100,
        ).otherwise(0.0),
    )

    return combined
