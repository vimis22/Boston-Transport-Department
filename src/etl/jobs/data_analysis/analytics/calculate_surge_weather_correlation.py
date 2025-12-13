from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def calculate_surge_weather_correlation(taxi_df: DataFrame) -> DataFrame:
    """
    Analyserer korrelation mellem taxi surge pricing og vejrforhold ved ride-tidspunkt.
    Taxi data har embedded weather snapshots, så vi korrelerer surge_multiplier med weather direkte.

    Args:
        taxi_df: Taxi DataFrame with embedded weather data

    Returns:
        DataFrame with surge-weather correlation metrics
    """
    # Add weather categories based on embedded weather
    result = (
        taxi_df.withColumn(
            "is_raining", when(col("precip_intensity") > 0, 1).otherwise(0)
        )
        .withColumn("is_very_cold", when(col("temperature") < 0, 1).otherwise(0))
        .withColumn("is_very_hot", when(col("temperature") > 30, 1).otherwise(0))
        .withColumn("is_humid", when(col("humidity") > 0.8, 1).otherwise(0))
    )

    # High surge indicator
    result = result.withColumn(
        "is_surge", when(col("surge_multiplier") > 1.0, 1).otherwise(0)
    )

    # Weather-driven surge score
    # Higher score = more likely surge is weather-driven
    result = result.withColumn(
        "weather_driven_surge_score",
        (col("is_raining") * 30)
        + (col("is_very_cold") * 20)
        + (col("is_very_hot") * 15)
        + (col("is_humid") * 10),
    )

    # Classify surge reasons
    result = result.withColumn(
        "surge_reason",
        when((col("is_surge") == 0), "no_surge")
        .when(col("weather_driven_surge_score") > 40, "weather_driven")
        .when(col("weather_driven_surge_score") > 20, "partially_weather")
        .otherwise("demand_driven"),
    )

    return result
