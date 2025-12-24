from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, coalesce


def calculate_multi_variable_correlation_summary(combined_df: DataFrame) -> DataFrame:
    """
    Genererer comprehensive correlation summary på tværs af alle vejr-transport variabler.
    Producerer normalized metrics, polynomial terms, interaction terms og predictive model for bike demand.

    Args:
        combined_df: Combined DataFrame with transport and weather data

    Returns:
        DataFrame with multi-variable correlation summary including normalized metrics,
        polynomial terms, interaction terms, and predictive demand models
    """
    # Compute normalized metrics for better correlation analysis
    result = combined_df.withColumn(
        "bike_usage_normalized",
        when(col("total_bike_rentals") > 0,
             col("total_bike_rentals") / 100.0)  # Normalize to scale 0-N
        .otherwise(0)
    ).withColumn(
        "taxi_usage_normalized",
        when(col("total_taxi_rides") > 0,
             col("total_taxi_rides") / 50.0)     # Different scale for taxis
        .otherwise(0)
    ).withColumn(
        "temperature_normalized",
        when(col("avg_temperature_c").isNotNull(),
             (col("avg_temperature_c") + 10) / 40.0)  # Normalize -10°C to 30°C ’ 0 to 1
        .otherwise(0.5)
    ).withColumn(
        "wind_normalized",
        when(col("avg_wind_speed_ms").isNotNull(),
             col("avg_wind_speed_ms") / 20.0)         # Normalize 0-20 m/s ’ 0 to 1
        .otherwise(0)
    ).withColumn(
        "weather_score_normalized",
        coalesce(col("avg_weather_score") / 100.0, lit(0.5))
    )

    # Compute squared terms for polynomial relationships
    # (Sometimes weather-transport relationships are non-linear)
    result = result.withColumn(
        "temperature_squared",
        when(col("avg_temperature_c").isNotNull(),
             col("avg_temperature_c") * col("avg_temperature_c"))
        .otherwise(None)
    )

    # Interaction terms (captures combined effects)
    result = result.withColumn(
        "temp_wind_interaction",
        when((col("avg_temperature_c").isNotNull()) & (col("avg_wind_speed_ms").isNotNull()),
             col("avg_temperature_c") * col("avg_wind_speed_ms"))
        .otherwise(None)
    )

    # Create categorical correlation indicators
    result = result.withColumn(
        "strong_positive_weather_for_bikes",
        when((col("avg_temperature_c").between(15, 25)) &
             (col("avg_wind_speed_ms") < 8) &
             (col("avg_weather_score") > 70), True)
        .otherwise(False)
    ).withColumn(
        "strong_negative_weather_for_bikes",
        when((col("avg_temperature_c") < 0) |
             (col("avg_wind_speed_ms") > 12) |
             (col("avg_weather_score") < 30), True)
        .otherwise(False)
    )

    # Expected bike rental index (predictive model based on weather)
    result = result.withColumn(
        "predicted_bike_demand_index",
        # Simple linear model: Demand = f(temp, wind, weather_score)
        (col("temperature_normalized") * 40) +        # Temperature contribution
        ((1 - col("wind_normalized")) * 30) +          # Wind penalty (inverted)
        (col("weather_score_normalized") * 30)         # Overall weather contribution
    ).withColumn(
        "actual_bike_demand_index",
        col("bike_usage_normalized") * 100
    ).withColumn(
        "demand_prediction_error",
        col("actual_bike_demand_index") - col("predicted_bike_demand_index")
    )

    return result
