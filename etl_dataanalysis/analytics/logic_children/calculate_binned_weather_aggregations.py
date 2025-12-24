from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def calculate_binned_weather_aggregations(combined_df: DataFrame) -> DataFrame:
    """
    Laver binned aggregations af transport usage per vejr-intervaller (temperature, vind, weather score).
    Producerer graf-klar data med (x,y) koordinater til scatter plots.

    Args:
        combined_df: Combined DataFrame with transport and weather data

    Returns:
        DataFrame with binned weather aggregations for visualization
    """
    # Create more granular temperature bins
    result = combined_df.withColumn(
        "temp_bin_numeric",
        when(col("avg_temperature_c").isNull(), -999)
        .otherwise((col("avg_temperature_c") / 5).cast("int") * 5)  # Round to nearest 5
    ).withColumn(
        "temp_bin_label",
        when(col("avg_temperature_c").isNull(), "unknown")
        .when(col("avg_temperature_c") < -10, "below_-10C")
        .when(col("avg_temperature_c") < -5, "-10_to_-5C")
        .when(col("avg_temperature_c") < 0, "-5_to_0C")
        .when(col("avg_temperature_c") < 5, "0_to_5C")
        .when(col("avg_temperature_c") < 10, "5_to_10C")
        .when(col("avg_temperature_c") < 15, "10_to_15C")
        .when(col("avg_temperature_c") < 20, "15_to_20C")
        .when(col("avg_temperature_c") < 25, "20_to_25C")
        .when(col("avg_temperature_c") < 30, "25_to_30C")
        .otherwise("above_30C")
    )

    # Create wind speed bins
    result = result.withColumn(
        "wind_bin_numeric",
        when(col("avg_wind_speed_ms").isNull(), -999)
        .otherwise((col("avg_wind_speed_ms") / 2).cast("int") * 2)  # Bins of 2 m/s
    ).withColumn(
        "wind_bin_label",
        when(col("avg_wind_speed_ms").isNull(), "unknown")
        .when(col("avg_wind_speed_ms") < 2, "calm_0-2ms")
        .when(col("avg_wind_speed_ms") < 5, "light_2-5ms")
        .when(col("avg_wind_speed_ms") < 8, "moderate_5-8ms")
        .when(col("avg_wind_speed_ms") < 12, "fresh_8-12ms")
        .otherwise("strong_12+ms")
    )

    # Create weather score bins (quintiles)
    result = result.withColumn(
        "weather_score_bin",
        when(col("avg_weather_score").isNull(), "unknown")
        .when(col("avg_weather_score") < 20, "very_poor_0-20")
        .when(col("avg_weather_score") < 40, "poor_20-40")
        .when(col("avg_weather_score") < 60, "moderate_40-60")
        .when(col("avg_weather_score") < 80, "good_60-80")
        .otherwise("excellent_80-100")
    )

    # Add usage per unit weather metric (elasticity visualization)
    result = result.withColumn(
        "bikes_per_degree",
        when((col("avg_temperature_c").isNotNull()) & (col("avg_temperature_c") != 0),
             col("total_bike_rentals") / col("avg_temperature_c"))
        .otherwise(None)
    ).withColumn(
        "bikes_per_weather_score_point",
        when((col("avg_weather_score").isNotNull()) & (col("avg_weather_score") > 0),
             col("total_bike_rentals") / col("avg_weather_score"))
        .otherwise(None)
    )

    return result
