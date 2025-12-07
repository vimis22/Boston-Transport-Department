"""
Core analytics computations for weather-transport-safety correlations.

This module implements domain-specific analytics including:
1. Weather-transport correlation metrics
2. Weather-safety risk analysis
3. Transport elasticity calculations
4. Predictive risk scores
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, avg, count, sum as spark_sum, stddev, corr, lit, expr, coalesce
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def calculate_weather_transport_correlation(combined_df: DataFrame) -> DataFrame:
    """
    Calculate correlation metrics between weather conditions and transport usage.

    Input: Combined DataFrame from windowed aggregations with:
        - total_bike_rentals, total_taxi_rides
        - avg_temperature_c, avg_wind_speed_ms, avg_weather_score
        - good_weather_ratio

    Output: DataFrame with correlation metrics and insights
    """
    # Add weather impact indicators
    result = combined_df.withColumn(
        "is_cold", when(col("avg_temperature_c") < 5, 1).otherwise(0)
    ).withColumn(
        "is_hot", when(col("avg_temperature_c") > 25, 1).otherwise(0)
    ).withColumn(
        "is_comfortable", when(col("avg_temperature_c").between(15, 25), 1).otherwise(0)
    ).withColumn(
        "is_windy", when(col("avg_wind_speed_ms") > 10, 1).otherwise(0)
    ).withColumn(
        "is_poor_weather", when(col("avg_weather_score") < 40, 1).otherwise(0)
    )

    # Calculate transport mode preferences by weather
    result = result.withColumn(
        "bike_preference_score",
        when(col("total_transport_usage") > 0,
             (col("total_bike_rentals") / col("total_transport_usage")) * col("avg_weather_score"))
        .otherwise(0)
    ).withColumn(
        "taxi_preference_score",
        when(col("total_transport_usage") > 0,
             (col("total_taxi_rides") / col("total_transport_usage")) * (100 - col("avg_weather_score")))
        .otherwise(0)
    )

    # Weather elasticity: how much does demand change with weather?
    # Simplified: compare to "baseline" (comfortable weather)
    result = result.withColumn(
        "bike_weather_elasticity",
        when(col("is_comfortable") == 1, 0.0)  # Baseline
        .when(col("is_cold") == 1, -0.3)        # 30% decrease in cold
        .when(col("is_hot") == 1, -0.2)         # 20% decrease in heat
        .when(col("is_windy") == 1, -0.25)      # 25% decrease in wind
        .otherwise(-0.15)                        # 15% decrease other bad weather
    ).withColumn(
        "taxi_weather_elasticity",
        when(col("is_comfortable") == 1, 0.0)  # Baseline
        .when(col("is_poor_weather") == 1, 0.4) # 40% increase in bad weather
        .otherwise(0.1)                          # 10% increase in other conditions
    )

    # Expected vs actual usage (simple model)
    # Baseline assumption: 100 bikes, 50 taxis per 15-min window in good weather
    result = result.withColumn(
        "expected_bike_rentals",
        lit(100) * (1 + col("bike_weather_elasticity"))
    ).withColumn(
        "expected_taxi_rides",
        lit(50) * (1 + col("taxi_weather_elasticity"))
    ).withColumn(
        "bike_demand_deviation",
        col("total_bike_rentals") - col("expected_bike_rentals")
    ).withColumn(
        "taxi_demand_deviation",
        col("total_taxi_rides") - col("expected_taxi_rides")
    )

    # Weather impact score (-100 to +100)
    # Positive = good for transport, Negative = bad for transport
    result = result.withColumn(
        "weather_transport_impact_score",
        (col("avg_weather_score") - 50) * 2  # Scale to -100 to +100
    )

    return result


def calculate_weather_safety_risk(accident_df: DataFrame, weather_df: DataFrame,
                                    window_duration: str = "1 hour") -> DataFrame:
    """
    Calculate safety risk metrics based on weather conditions and accident frequency.

    Args:
        accident_df: Accident DataFrame with dispatch_timestamp
        weather_df: Weather DataFrame with datetime_ts and enriched weather columns
        window_duration: Time window for aggregation

    Returns:
        DataFrame with safety risk scores and weather-accident correlations
    """
    from pyspark.sql.functions import window

    # Aggregate accidents by time window
    accident_window = accident_df.groupBy(
        window(col("dispatch_timestamp"), window_duration),
        "mode_type"
    ).agg(
        count("*").alias("accident_count")
    ).withColumn(
        "window_start", col("window.start")
    ).withColumn(
        "window_end", col("window.end")
    ).drop("window")

    # Aggregate weather by same window
    weather_window = weather_df.groupBy(
        window(col("datetime_ts"), window_duration)
    ).agg(
        avg("temperature_celsius").alias("avg_temp"),
        avg("wind_speed_ms").alias("avg_wind"),
        avg("visibility_m").alias("avg_visibility"),
        avg("weather_condition_score").alias("avg_weather_score"),
        count(when(col("is_bad_weather") == True, 1)).alias("bad_weather_obs")
    ).withColumn(
        "window_start", col("window.start")
    ).withColumn(
        "window_end", col("window.end")
    ).drop("window")

    # Join accidents with weather
    safety_risk = accident_window.join(
        weather_window,
        ["window_start", "window_end"],
        "left"
    )

    # Calculate risk scores
    safety_risk = safety_risk.withColumn(
        "base_accident_risk",
        when(col("accident_count").isNull(), 0)
        .otherwise(col("accident_count"))
    )

    # Weather risk multipliers
    safety_risk = safety_risk.withColumn(
        "weather_risk_multiplier",
        when(col("avg_weather_score").isNull(), 1.0)
        .when(col("avg_weather_score") < 20, 3.0)   # Very bad weather = 3x risk
        .when(col("avg_weather_score") < 40, 2.0)   # Bad weather = 2x risk
        .when(col("avg_weather_score") < 60, 1.5)   # Poor weather = 1.5x risk
        .otherwise(1.0)                              # Good weather = baseline
    )

    # Combined safety risk index (0-10 scale)
    safety_risk = safety_risk.withColumn(
        "safety_risk_index",
        (col("base_accident_risk") * col("weather_risk_multiplier"))
        .cast("double")
    )

    # Normalize to 0-10 scale (assuming max 20 accidents per hour in worst case)
    safety_risk = safety_risk.withColumn(
        "safety_risk_index_normalized",
        when(col("safety_risk_index") > 10, 10.0)
        .otherwise(col("safety_risk_index"))
    )

    # Risk categories
    safety_risk = safety_risk.withColumn(
        "risk_category",
        when(col("safety_risk_index_normalized") < 2, "low")
        .when(col("safety_risk_index_normalized") < 5, "moderate")
        .when(col("safety_risk_index_normalized") < 8, "high")
        .otherwise("very_high")
    )

    # Add mode-specific insights
    safety_risk = safety_risk.withColumn(
        "high_risk_mode",
        when(col("mode_type") == "bike", "cycling")
        .when(col("mode_type") == "ped", "pedestrian")
        .when(col("mode_type") == "mv", "vehicle")
        .otherwise("unknown")
    )

    return safety_risk


def calculate_surge_weather_correlation(taxi_df: DataFrame) -> DataFrame:
    """
    Analyze correlation between taxi surge pricing and weather conditions.

    The taxi DataFrame already has embedded weather snapshots, so we can
    directly correlate surge multiplier with weather at ride time.

    Args:
        taxi_df: Taxi DataFrame with surge_multiplier and weather_snapshot fields

    Returns:
        DataFrame with surge-weather correlation metrics
    """
    # Add weather categories based on embedded weather
    result = taxi_df.withColumn(
        "is_raining",
        when(col("precip_intensity") > 0, 1).otherwise(0)
    ).withColumn(
        "is_very_cold",
        when(col("temperature") < 0, 1).otherwise(0)
    ).withColumn(
        "is_very_hot",
        when(col("temperature") > 30, 1).otherwise(0)
    ).withColumn(
        "is_humid",
        when(col("humidity") > 0.8, 1).otherwise(0)
    )

    # High surge indicator
    result = result.withColumn(
        "is_surge",
        when(col("surge_multiplier") > 1.0, 1).otherwise(0)
    )

    # Weather-driven surge score
    # Higher score = more likely surge is weather-driven
    result = result.withColumn(
        "weather_driven_surge_score",
        (col("is_raining") * 30) +
        (col("is_very_cold") * 20) +
        (col("is_very_hot") * 15) +
        (col("is_humid") * 10)
    )

    # Classify surge reasons
    result = result.withColumn(
        "surge_reason",
        when((col("is_surge") == 0), "no_surge")
        .when(col("weather_driven_surge_score") > 40, "weather_driven")
        .when(col("weather_driven_surge_score") > 20, "partially_weather")
        .otherwise("demand_driven")
    )

    return result


def generate_transport_usage_summary(bike_df: DataFrame, taxi_df: DataFrame,
                                       window_duration: str = "1 hour") -> DataFrame:
    """
    Generate hourly summary statistics for transport usage.

    Useful for dashboard time-series visualizations.

    Args:
        bike_df: Bike rental DataFrame
        taxi_df: Taxi ride DataFrame
        window_duration: Aggregation window

    Returns:
        Combined transport usage summary by time window
    """
    from pyspark.sql.functions import window, hour as hour_func, dayofweek

    # Bike hourly aggregation
    bike_hourly = bike_df.groupBy(
        window(col("start_time_ts"), window_duration)
    ).agg(
        count("*").alias("bike_count"),
        avg("duration_seconds").alias("avg_bike_duration"),
        count(when(col("user_type") == "Subscriber", 1)).alias("subscriber_count"),
        count(when(col("user_type") == "Customer", 1)).alias("customer_count")
    ).withColumn(
        "window_start", col("window.start")
    ).withColumn(
        "window_end", col("window.end")
    ).withColumn(
        "hour_of_day", hour_func(col("window_start"))
    ).withColumn(
        "day_of_week", dayofweek(col("window_start"))
    ).drop("window")

    # Taxi hourly aggregation
    taxi_hourly = taxi_df.groupBy(
        window(col("datetime_ts"), window_duration)
    ).agg(
        count("*").alias("taxi_count"),
        avg("price").alias("avg_taxi_price"),
        spark_sum("price").alias("total_taxi_revenue"),
        avg("surge_multiplier").alias("avg_surge"),
        count(when(col("cab_type") == "Uber", 1)).alias("uber_count"),
        count(when(col("cab_type") == "Lyft", 1)).alias("lyft_count")
    ).withColumn(
        "window_start", col("window.start")
    ).withColumn(
        "window_end", col("window.end")
    ).withColumn(
        "hour_of_day", hour_func(col("window_start"))
    ).withColumn(
        "day_of_week", dayofweek(col("window_start"))
    ).drop("window")

    # Join bike and taxi
    combined = bike_hourly.join(taxi_hourly, ["window_start", "window_end", "hour_of_day", "day_of_week"], "full_outer")

    # Fill nulls
    combined = combined.fillna({
        "bike_count": 0,
        "taxi_count": 0,
        "subscriber_count": 0,
        "customer_count": 0,
        "uber_count": 0,
        "lyft_count": 0
    })

    # Total transport usage
    combined = combined.withColumn(
        "total_transport_count",
        col("bike_count") + col("taxi_count")
    )

    # Mode share percentages
    combined = combined.withColumn(
        "bike_mode_share_pct",
        when(col("total_transport_count") > 0,
             (col("bike_count") / col("total_transport_count")) * 100)
        .otherwise(0)
    ).withColumn(
        "taxi_mode_share_pct",
        when(col("total_transport_count") > 0,
             (col("taxi_count") / col("total_transport_count")) * 100)
        .otherwise(0)
    )

    # Peak hour indicator
    combined = combined.withColumn(
        "is_peak_hour",
        when(col("hour_of_day").isin([7, 8, 9, 17, 18, 19]), True)
        .otherwise(False)
    )

    # Weekend indicator
    combined = combined.withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), True)  # 1 = Sunday, 7 = Saturday in dayofweek()
        .otherwise(False)
    )

    return combined
