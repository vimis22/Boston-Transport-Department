from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def calculate_precipitation_impact_analysis(combined_df: DataFrame) -> DataFrame:
    """
    Analyserer specifik impact af precipitation (regn) på transport mode choice (bike vs taxi).
    Isolerer regn som key weather variable og beregner modal substitution og elasticity.

    Args:
        combined_df: Combined DataFrame with transport and weather data

    Returns:
        DataFrame with precipitation impact analysis

    Note:
        Infers precipitation from weather score (simplified in absence of direct precip data).
        In real scenario, you'd use dedicated precipitation sensors.
    """
    # Infer precipitation from weather score (simplified in absence of direct precip data)
    result = combined_df.withColumn(
        "precipitation_indicator",
        when(col("avg_weather_score") < 30, "likely_precipitating")
        .when(col("avg_weather_score") < 50, "possibly_precipitating")
        .otherwise("dry")
    ).withColumn(
        "precipitation_binary",
        when(col("avg_weather_score") < 40, 1).otherwise(0)
    )

    # Calculate mode split under different precipitation conditions
    result = result.withColumn(
        "bike_mode_share_pct",
        when(col("total_transport_usage") > 0,
             (col("total_bike_rentals") / col("total_transport_usage")) * 100)
        .otherwise(0)
    ).withColumn(
        "taxi_mode_share_pct",
        when(col("total_transport_usage") > 0,
             (col("total_taxi_rides") / col("total_transport_usage")) * 100)
        .otherwise(0)
    )

    # Precipitation impact score
    # Measures how much precipitation reduces bike usage
    result = result.withColumn(
        "precip_bike_impact_score",
        when(col("precipitation_binary") == 1,
             -1 * (col("avg_weather_score") - 100) / 2)  # Higher negative impact when worse weather
        .otherwise(0)
    ).withColumn(
        "precip_taxi_boost_score",
        when(col("precipitation_binary") == 1,
             (100 - col("avg_weather_score")) / 2)       # Taxi usage increases
        .otherwise(0)
    )

    # Modal substitution indicator
    result = result.withColumn(
        "weather_driven_mode_substitution",
        when((col("precipitation_binary") == 1) & (col("taxi_mode_share_pct") > 60), True)
        .otherwise(False)
    )

    return result
