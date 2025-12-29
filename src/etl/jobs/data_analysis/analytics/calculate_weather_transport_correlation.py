from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit


def calculate_weather_transport_correlation(combined_df: DataFrame) -> DataFrame:
    """
    Beregner korrelation mellem vejrforhold og transport-brug (bikes og taxis).
    Producerer elasticitet scores og forventede vs faktiske demand metrics.

    Args:
        combined_df: Combined DataFrame with transport and weather data

    Returns:
        DataFrame with weather-transport correlation metrics
    """
    # Add weather impact indicators
    result = (
        combined_df.withColumn(
            "is_cold", when(col("avg_temperature_c") < 5, 1).otherwise(0)
        )
        .withColumn("is_hot", when(col("avg_temperature_c") > 25, 1).otherwise(0))
        .withColumn(
            "is_comfortable",
            when(col("avg_temperature_c").between(15, 25), 1).otherwise(0),
        )
        .withColumn("is_windy", when(col("avg_wind_speed_ms") > 10, 1).otherwise(0))
        .withColumn(
            "is_poor_weather", when(col("avg_weather_score") < 40, 1).otherwise(0)
        )
    )

    # Calculate transport mode preferences by weather
    result = result.withColumn(
        "bike_preference_score",
        when(
            col("total_transport_usage") > 0,
            (col("total_bike_rentals") / col("total_transport_usage"))
            * col("avg_weather_score"),
        ).otherwise(0),
    ).withColumn(
        "taxi_preference_score",
        when(
            col("total_transport_usage") > 0,
            (col("total_taxi_rides") / col("total_transport_usage"))
            * (100 - col("avg_weather_score")),
        ).otherwise(0),
    )

    # Weather elasticity: how much does demand change with weather?
    # Simplified: compare to "baseline" (comfortable weather)
    result = result.withColumn(
        "bike_weather_elasticity",
        when(col("is_comfortable") == 1, 0.0)  # Baseline
        .when(col("is_cold") == 1, -0.3)  # 30% decrease in cold
        .when(col("is_hot") == 1, -0.2)  # 20% decrease in heat
        .when(col("is_windy") == 1, -0.25)  # 25% decrease in wind
        .otherwise(-0.15),  # 15% decrease other bad weather
    ).withColumn(
        "taxi_weather_elasticity",
        when(col("is_comfortable") == 1, 0.0)  # Baseline
        .when(col("is_poor_weather") == 1, 0.4)  # 40% increase in bad weather
        .otherwise(0.1),  # 10% increase in other conditions
    )

    # Expected vs actual usage (simple model)
    # Baseline assumption: 100 bikes, 50 taxis per 15-min window in good weather
    result = (
        result.withColumn(
            "expected_bike_rentals", lit(100) * (1 + col("bike_weather_elasticity"))
        )
        .withColumn(
            "expected_taxi_rides", lit(50) * (1 + col("taxi_weather_elasticity"))
        )
        .withColumn(
            "bike_demand_deviation",
            col("total_bike_rentals") - col("expected_bike_rentals"),
        )
        .withColumn(
            "taxi_demand_deviation",
            col("total_taxi_rides") - col("expected_taxi_rides"),
        )
    )

    # Weather impact score (-100 to +100)
    # Positive = good for transport, Negative = bad for transport
    result = result.withColumn(
        "weather_transport_impact_score",
        (col("avg_weather_score") - 50) * 2,  # Scale to -100 to +100
    )

    return result
