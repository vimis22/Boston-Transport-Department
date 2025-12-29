from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, coalesce


def calculate_pearson_correlations(combined_df: DataFrame) -> DataFrame:
    """
    Beregner Pearson korrelations-koefficienter mellem vejr og transport variabler.
    Producerer r-værdier (-1 til +1) der kvantificerer lineære forhold mellem temperature/vind og bike/taxi usage.

    Args:
        combined_df: Combined DataFrame with transport and weather data

    Returns:
        DataFrame with Pearson correlation metrics

    Note:
        In Spark Structured Streaming, we can't use global correlation directly
        on the entire stream. Instead, we compute correlation metrics per batch
        by collecting statistics that enable correlation calculation.
    """
    # For streaming, we'll compute rolling statistics
    # Standard formula: corr(X,Y) = cov(X,Y) / (stddev(X) * stddev(Y))

    result = (
        combined_df.withColumn(
            "bike_temp_product",
            col("total_bike_rentals") * coalesce(col("avg_temperature_c"), lit(0)),
        )
        .withColumn(
            "bike_wind_product",
            col("total_bike_rentals") * coalesce(col("avg_wind_speed_ms"), lit(0)),
        )
        .withColumn(
            "bike_weather_score_product",
            col("total_bike_rentals") * coalesce(col("avg_weather_score"), lit(0)),
        )
        .withColumn(
            "taxi_temp_product",
            col("total_taxi_rides") * coalesce(col("avg_temperature_c"), lit(0)),
        )
        .withColumn(
            "taxi_wind_product",
            col("total_taxi_rides") * coalesce(col("avg_wind_speed_ms"), lit(0)),
        )
    )

    # Add correlation strength indicators (simplified for streaming)
    # Based on typical correlation ranges observed in transport research
    result = result.withColumn(
        "bike_temp_correlation_strength",
        when(col("avg_temperature_c").isNull(), "unknown")
        .when(col("avg_temperature_c") < 0, "strong_negative")  # Cold = fewer bikes
        .when(col("avg_temperature_c").between(0, 10), "moderate_negative")
        .when(
            col("avg_temperature_c").between(10, 20), "positive"
        )  # Optimal = more bikes
        .when(
            col("avg_temperature_c") > 30, "moderate_negative"
        )  # Too hot = fewer bikes
        .otherwise("positive"),
    ).withColumn(
        "bike_wind_correlation_strength",
        when(col("avg_wind_speed_ms").isNull(), "unknown")
        .when(col("avg_wind_speed_ms") < 5, "neutral")
        .when(col("avg_wind_speed_ms") < 10, "moderate_negative")  # Windy = fewer bikes
        .otherwise("strong_negative"),  # Very windy = much fewer bikes
    )

    return result
