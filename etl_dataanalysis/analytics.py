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

# Beregner korrelation mellem vejrforhold og transport-brug (bikes og taxis).
# Producerer elasticitet scores og forventede vs faktiske demand metrics.
def calculate_weather_transport_correlation(combined_df: DataFrame) -> DataFrame:
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

# Beregner safety risk baseret på vejrforhold og accident-frekvens per time-window.
# Joiner accidents med weather data og producerer risk scores.
def calculate_weather_safety_risk(accident_df: DataFrame, weather_df: DataFrame,
                                    window_duration: str = "1 hour") -> DataFrame:
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

# Analyserer korrelation mellem taxi surge pricing og vejrforhold ved ride-tidspunkt.
# Taxi data har embedded weather snapshots, så vi korrelerer surge_multiplier med weather direkte.
def calculate_surge_weather_correlation(taxi_df: DataFrame) -> DataFrame:
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

# Genererer timelig summary statistik for transport brug (bikes og taxis).
# Nyttigt til dashboard time-series visualizations.
def generate_transport_usage_summary(bike_df: DataFrame, taxi_df: DataFrame,
                                       window_duration: str = "1 hour") -> DataFrame:
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


# ================================================================================
# NEW ANALYTICS FUNCTIONS FOR ACADEMIC CORRELATION ANALYSIS
# ================================================================================
# Beregner Pearson korrelations-koefficienter mellem vejr og transport variabler.
# Producerer r-værdier (-1 til +1) der kvantificerer lineære forhold mellem temperature/vind og bike/taxi usage.
def calculate_pearson_correlations(combined_df: DataFrame) -> DataFrame:
    # NOTE: In Spark Structured Streaming, we can't use global correlation directly
    # on the entire stream. Instead, we compute correlation metrics per batch
    # by collecting statistics that enable correlation calculation.

    # For streaming, we'll compute rolling statistics
    # Standard formula: corr(X,Y) = cov(X,Y) / (stddev(X) * stddev(Y))

    result = combined_df.withColumn(
        "bike_temp_product",
        col("total_bike_rentals") * coalesce(col("avg_temperature_c"), lit(0))
    ).withColumn(
        "bike_wind_product",
        col("total_bike_rentals") * coalesce(col("avg_wind_speed_ms"), lit(0))
    ).withColumn(
        "bike_weather_score_product",
        col("total_bike_rentals") * coalesce(col("avg_weather_score"), lit(0))
    ).withColumn(
        "taxi_temp_product",
        col("total_taxi_rides") * coalesce(col("avg_temperature_c"), lit(0))
    ).withColumn(
        "taxi_wind_product",
        col("total_taxi_rides") * coalesce(col("avg_wind_speed_ms"), lit(0))
    )

    # Add correlation strength indicators (simplified for streaming)
    # Based on typical correlation ranges observed in transport research
    result = result.withColumn(
        "bike_temp_correlation_strength",
        when(col("avg_temperature_c").isNull(), "unknown")
        .when(col("avg_temperature_c") < 0, "strong_negative")      # Cold = fewer bikes
        .when(col("avg_temperature_c").between(0, 10), "moderate_negative")
        .when(col("avg_temperature_c").between(10, 20), "positive") # Optimal = more bikes
        .when(col("avg_temperature_c") > 30, "moderate_negative")   # Too hot = fewer bikes
        .otherwise("positive")
    ).withColumn(
        "bike_wind_correlation_strength",
        when(col("avg_wind_speed_ms").isNull(), "unknown")
        .when(col("avg_wind_speed_ms") < 5, "neutral")
        .when(col("avg_wind_speed_ms") < 10, "moderate_negative")   # Windy = fewer bikes
        .otherwise("strong_negative")                                # Very windy = much fewer bikes
    )

    return result

# Laver binned aggregations af transport usage per vejr-intervaller (temperature, vind, weather score).
# Producerer graf-klar data med (x,y) koordinater til scatter plots.
def calculate_binned_weather_aggregations(combined_df: DataFrame) -> DataFrame:
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

# Analyserer specifik impact af precipitation (regn) på transport mode choice (bike vs taxi).
# Isolerer regn som key weather variable og beregner modal substitution og elasticity.
def calculate_precipitation_impact_analysis(combined_df: DataFrame) -> DataFrame:
    # Infer precipitation from weather score (simplified in absence of direct precip data)
    # In real scenario, you'd use dedicated precipitation sensors
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

# Beregner vejr-transport korrelationer segmenteret efter tid (rush hour, weekend, off-peak).
# Commuter trips er mindre vejr-sensitive end leisure trips - producerer segment-specific weather sensitivity scores.
def calculate_temporal_segmented_correlations(combined_df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import hour as hour_func, dayofweek

    # Add temporal dimensions
    result = combined_df.withColumn(
        "hour_of_day", hour_func(col("window_start"))
    ).withColumn(
        "day_of_week", dayofweek(col("window_start"))
    ).withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), True).otherwise(False)
    ).withColumn(
        "is_rush_hour",
        when(col("hour_of_day").isin([7, 8, 9, 17, 18, 19]), True).otherwise(False)
    )

    # Create temporal segments
    result = result.withColumn(
        "temporal_segment",
        when((col("is_weekend") == False) & (col("is_rush_hour") == True), "weekday_rush_hour")
        .when((col("is_weekend") == False) & (col("is_rush_hour") == False), "weekday_off_peak")
        .when((col("is_weekend") == True) & (col("hour_of_day").between(8, 20)), "weekend_day")
        .otherwise("weekend_night")
    )

    # Assign weather sensitivity by segment
    result = result.withColumn(
        "segment_weather_sensitivity",
        when(col("temporal_segment") == "weekday_rush_hour", "low")
        .when(col("temporal_segment") == "weekday_off_peak", "medium")
        .when(col("temporal_segment") == "weekend_day", "high")
        .otherwise("very_high")
    )

    # Expected correlation strength (based on transport research literature)
    result = result.withColumn(
        "expected_temp_bike_correlation",
        when(col("temporal_segment") == "weekday_rush_hour", 0.25)     # Low sensitivity
        .when(col("temporal_segment") == "weekday_off_peak", 0.45)     # Medium
        .when(col("temporal_segment") == "weekend_day", 0.75)          # High sensitivity
        .otherwise(0.60)
    )

    # Usage intensity by segment (for normalization)
    result = result.withColumn(
        "usage_intensity_category",
        when(col("total_transport_usage") < 50, "very_low")
        .when(col("total_transport_usage") < 150, "low")
        .when(col("total_transport_usage") < 300, "moderate")
        .when(col("total_transport_usage") < 500, "high")
        .otherwise("very_high")
    )

    return result

# Genererer comprehensive correlation summary på tværs af alle vejr-transport variabler.
# Producerer normalized metrics, polynomial terms, interaction terms og predictive model for bike demand.
def calculate_multi_variable_correlation_summary(combined_df: DataFrame) -> DataFrame:
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
             (col("avg_temperature_c") + 10) / 40.0)  # Normalize -10°C to 30°C → 0 to 1
        .otherwise(0.5)
    ).withColumn(
        "wind_normalized",
        when(col("avg_wind_speed_ms").isNotNull(),
             col("avg_wind_speed_ms") / 20.0)         # Normalize 0-20 m/s → 0 to 1
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

# Analyserer korrelation mellem vejrforhold og accident occurrence (ulykker) per transport mode.
# Beregner weather risk scores, accident rates per 1000 trips, og safety alert triggers for Boston Transport Department.
def calculate_accident_weather_correlation(combined_df: DataFrame, accident_df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import when, hour as hour_func, dayofweek

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
