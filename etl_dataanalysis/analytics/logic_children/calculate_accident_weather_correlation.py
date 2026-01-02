from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, avg, when, lit,
    hour as hour_func
)


def calculate_accident_weather_correlation(combined_df: DataFrame, accident_df: DataFrame) -> DataFrame:
    """
    Analyserer korrelation mellem vejrforhold og accident occurrence (ulykker) per transport mode.
    Beregner weather risk scores, accident rates per 1000 trips, og safety alert triggers for Boston Transport Department.

    Args:
        combined_df: Combined DataFrame with transport and weather data
        accident_df: Accident DataFrame with dispatch_timestamp

    Returns:
        DataFrame with accident-weather correlation analysis including risk scores,
        accident rates, and safety alert indicators
    """
    # Aggregate accidents by 15-minute windows
    accident_agg = accident_df.withWatermark("dispatch_timestamp", "10 minutes") \
        .groupBy(
            window(col("dispatch_timestamp"), "15 minutes"),
            "mode_type"
        ).agg(
            count("*").alias("accident_count"),
            count(when(col("location_type") == "Intersection", 1)).alias("intersection_accidents"),
            avg(col("lat")).alias("avg_accident_lat"),
            avg(col("long")).alias("avg_accident_long")
        ).withColumn(
            "window_start", col("window.start")
        ).withColumn(
            "window_end", col("window.end")
        )

    # Join accidents with weather data
    result = accident_agg.join(
        combined_df.select(
            "window_start", "window_end",
            "avg_temperature_c", "avg_wind_speed_ms", "avg_weather_score",
            "total_bike_rentals", "total_taxi_rides", "total_transport_usage"
        ),
        ["window_start", "window_end"],
        "left"
    )

    # Infer precipitation from weather score
    result = result.withColumn(
        "has_precipitation",
        when(col("avg_weather_score") < 40, True).otherwise(False)
    ).withColumn(
        "is_freezing",
        when(col("avg_temperature_c") < 0, True).otherwise(False)
    ).withColumn(
        "is_high_wind",
        when(col("avg_wind_speed_ms") > 11, True).otherwise(False)  # > 40 km/h
    )

    # Calculate accident rate per 1000 transport trips
    result = result.withColumn(
        "accident_rate_per_1000_trips",
        when(col("total_transport_usage") > 0,
             (col("accident_count") / col("total_transport_usage")) * 1000)
        .otherwise(None)
    )

    # Weather risk scoring for accidents (0-100 scale)
    result = result.withColumn(
        "weather_accident_risk_score",
        lit(50) +  # Base risk
        when(col("has_precipitation"), 20).otherwise(0) +      # +20 if raining
        when(col("is_freezing"), 25).otherwise(0) +           # +25 if freezing
        when(col("is_high_wind"), 15).otherwise(0) +          # +15 if high wind
        when(col("avg_weather_score") < 30, 15).otherwise(0)  # +15 if very poor visibility
    )

    # Categorize risk
    result = result.withColumn(
        "weather_accident_risk_category",
        when(col("weather_accident_risk_score") < 60, "low")
        .when(col("weather_accident_risk_score") < 75, "medium")
        .when(col("weather_accident_risk_score") < 90, "high")
        .otherwise("critical")
    )

    # Mode-specific risk factors
    result = result.withColumn(
        "mode_weather_vulnerability",
        when((col("mode_type") == "bike") & col("has_precipitation"), "high_vulnerability")
        .when((col("mode_type") == "bike") & col("is_high_wind"), "high_vulnerability")
        .when((col("mode_type") == "mv") & col("is_freezing"), "high_vulnerability")
        .when((col("mode_type") == "ped") & col("is_high_wind"), "medium_vulnerability")
        .otherwise("low_vulnerability")
    )

    # Add temporal context
    result = result.withColumn(
        "hour_of_day", hour_func(col("window_start"))
    ).withColumn(
        "is_rush_hour",
        when(col("hour_of_day").isin([7, 8, 9, 17, 18, 19]), True).otherwise(False)
    )

    # Calculate expected vs actual accident rate
    # (Predictive model: accidents increase with bad weather)
    result = result.withColumn(
        "expected_accident_multiplier",
        lit(1.0) +
        when(col("has_precipitation"), 0.7).otherwise(0.0) +      # 70% increase in rain
        when(col("is_freezing"), 1.2).otherwise(0.0) +            # 120% increase when freezing
        when(col("is_high_wind"), 0.5).otherwise(0.0)             # 50% increase in high wind
    ).withColumn(
        "baseline_accident_rate",
        lit(2.0)  # Baseline: ~2 accidents per 1000 trips in good weather
    ).withColumn(
        "expected_accidents_in_weather",
        col("baseline_accident_rate") * col("expected_accident_multiplier") *
        (col("total_transport_usage") / 1000.0)
    ).withColumn(
        "accident_deviation",
        col("accident_count") - col("expected_accidents_in_weather")
    )

    # Boston Transport Department alert threshold
    result = result.withColumn(
        "should_send_safety_alert",
        when((col("weather_accident_risk_score") >= 85) |
             (col("accident_rate_per_1000_trips") > 10), True)
        .otherwise(False)
    )

    return result
