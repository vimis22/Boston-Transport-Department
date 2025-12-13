from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window, count, avg, when


def calculate_weather_safety_risk(
    accident_df: DataFrame, weather_df: DataFrame, window_duration: str = "1 hour"
) -> DataFrame:
    """
    Beregner safety risk baseret på vejrforhold og accident-frekvens per time-window.
    Joiner accidents med weather data og producerer risk scores.

    Args:
        accident_df: Accident DataFrame with dispatch_timestamp
        weather_df: Weather DataFrame with datetime_ts
        window_duration: Time window duration (default: "1 hour")

    Returns:
        DataFrame with safety risk scores and categories
    """
    # Aggregate accidents by time window
    accident_window = (
        accident_df.groupBy(
            window(col("dispatch_timestamp"), window_duration), "mode_type"
        )
        .agg(count("*").alias("accident_count"))
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    # Aggregate weather by same window
    weather_window = (
        weather_df.groupBy(window(col("datetime_ts"), window_duration))
        .agg(
            avg("temperature_celsius").alias("avg_temp"),
            avg("wind_speed_ms").alias("avg_wind"),
            avg("visibility_m").alias("avg_visibility"),
            avg("weather_condition_score").alias("avg_weather_score"),
            count(when(col("is_bad_weather") == True, 1)).alias("bad_weather_obs"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    # Join accidents with weather
    safety_risk = accident_window.join(
        weather_window, ["window_start", "window_end"], "left"
    )

    # Calculate risk scores
    safety_risk = safety_risk.withColumn(
        "base_accident_risk",
        when(col("accident_count").isNull(), 0).otherwise(col("accident_count")),
    )

    # Weather risk multipliers
    safety_risk = safety_risk.withColumn(
        "weather_risk_multiplier",
        when(col("avg_weather_score").isNull(), 1.0)
        .when(col("avg_weather_score") < 20, 3.0)  # Very bad weather = 3x risk
        .when(col("avg_weather_score") < 40, 2.0)  # Bad weather = 2x risk
        .when(col("avg_weather_score") < 60, 1.5)  # Poor weather = 1.5x risk
        .otherwise(1.0),  # Good weather = baseline
    )

    # Combined safety risk index (0-10 scale)
    safety_risk = safety_risk.withColumn(
        "safety_risk_index",
        (col("base_accident_risk") * col("weather_risk_multiplier")).cast("double"),
    )

    # Normalize to 0-10 scale (assuming max 20 accidents per hour in worst case)
    safety_risk = safety_risk.withColumn(
        "safety_risk_index_normalized",
        when(col("safety_risk_index") > 10, 10.0).otherwise(col("safety_risk_index")),
    )

    # Risk categories
    safety_risk = safety_risk.withColumn(
        "risk_category",
        when(col("safety_risk_index_normalized") < 2, "low")
        .when(col("safety_risk_index_normalized") < 5, "moderate")
        .when(col("safety_risk_index_normalized") < 8, "high")
        .otherwise("very_high"),
    )

    # Add mode-specific insights
    safety_risk = safety_risk.withColumn(
        "high_risk_mode",
        when(col("mode_type") == "bike", "cycling")
        .when(col("mode_type") == "ped", "pedestrian")
        .when(col("mode_type") == "mv", "vehicle")
        .otherwise("unknown"),
    )

    return safety_risk
