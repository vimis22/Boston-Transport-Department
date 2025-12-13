"""
Weather-binned aggregation logic for visualization.

This module creates graph-ready binned aggregations for scatter plots
to enable correlation analysis between weather conditions and transport usage.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg as spark_avg, count, stddev, when, lit


# Beskriv kort, hvad metoden g�r i to s�tninger.
def create_weather_binned_aggregations(combined_df: DataFrame) -> DataFrame:
    """
    Create graph-ready binned aggregations for scatter plots.

    **PURPOSE**: Address Oskar's requirement for correlation graphs.

    Input: Combined transport-weather DataFrame with per-window metrics
    Output: Aggregated statistics grouped by weather bins

    **FOR VISUALIZATION**:
    Dashboard queries this table to create scatter plots:
    - X-axis: temp_bin_center (e.g., 2.5�C, 7.5�C, 12.5�C...)
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
    showing that bike usage peaks at 15-20�C.
    """
    # Add temperature bins
    binned_df = (
        combined_df.withColumn(
            "temp_bin",
            when(col("avg_temperature_c").isNull(), "unknown")
            .when(col("avg_temperature_c") < 0, "-5_to_0C")
            .when(col("avg_temperature_c") < 5, "0_to_5C")
            .when(col("avg_temperature_c") < 10, "5_to_10C")
            .when(col("avg_temperature_c") < 15, "10_to_15C")
            .when(col("avg_temperature_c") < 20, "15_to_20C")
            .when(col("avg_temperature_c") < 25, "20_to_25C")
            .otherwise("25_to_30C"),
        )
        .withColumn(
            "wind_bin",
            when(col("avg_wind_speed_ms").isNull(), "unknown")
            .when(col("avg_wind_speed_ms") < 3, "0_to_3ms")
            .when(col("avg_wind_speed_ms") < 6, "3_to_6ms")
            .when(col("avg_wind_speed_ms") < 9, "6_to_9ms")
            .when(col("avg_wind_speed_ms") < 12, "9_to_12ms")
            .otherwise("12+ms"),
        )
        .withColumn(
            "weather_score_bin",
            when(col("avg_weather_score").isNull(), "unknown")
            .when(col("avg_weather_score") < 30, "poor_0-30")
            .when(col("avg_weather_score") < 50, "fair_30-50")
            .when(col("avg_weather_score") < 70, "good_50-70")
            .otherwise("excellent_70+"),
        )
    )

    # Aggregate by temperature bins
    temp_binned = (
        binned_df.filter(col("temp_bin") != "unknown")
        .groupBy("temp_bin")
        .agg(
            spark_avg("total_bike_rentals").alias("avg_bike_rentals_in_temp_bin"),
            spark_avg("total_taxi_rides").alias("avg_taxi_rides_in_temp_bin"),
            spark_avg("total_transport_usage").alias("avg_total_usage_in_temp_bin"),
            count("*").alias("sample_count"),
            stddev("total_bike_rentals").alias("stddev_bike_rentals"),
        )
        .withColumn("aggregation_type", lit("by_temperature_bin"))
    )

    # Aggregate by wind bins
    wind_binned = (
        binned_df.filter(col("wind_bin") != "unknown")
        .groupBy("wind_bin")
        .agg(
            spark_avg("total_bike_rentals").alias("avg_bike_rentals_in_wind_bin"),
            spark_avg("total_taxi_rides").alias("avg_taxi_rides_in_wind_bin"),
            count("*").alias("sample_count"),
        )
        .withColumn("aggregation_type", lit("by_wind_bin"))
    )

    # Aggregate by weather score bins
    weather_binned = (
        binned_df.filter(col("weather_score_bin") != "unknown")
        .groupBy("weather_score_bin")
        .agg(
            spark_avg("total_bike_rentals").alias("avg_bike_rentals_in_score_bin"),
            spark_avg("total_taxi_rides").alias("avg_taxi_rides_in_score_bin"),
            spark_avg("avg_weather_score").alias("avg_score_in_bin"),
            count("*").alias("sample_count"),
        )
        .withColumn("aggregation_type", lit("by_weather_score_bin"))
    )

    # Return the temperature-binned results (most important for Oskar's use case)
    return temp_binned
