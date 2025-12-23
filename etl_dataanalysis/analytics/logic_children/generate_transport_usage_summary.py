from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, avg, sum as spark_sum, when,
    hour as hour_func, dayofweek
)


def generate_transport_usage_summary(bike_df: DataFrame, taxi_df: DataFrame,
                                       window_duration: str = "1 hour") -> DataFrame:
    """
    Genererer timelig summary statistik for transport brug (bikes og taxis).
    Nyttigt til dashboard time-series visualizations.

    Args:
        bike_df: Bike rental DataFrame with start_time_ts
        taxi_df: Taxi ride DataFrame with datetime_ts
        window_duration: Time window duration (default: "1 hour")

    Returns:
        DataFrame with combined transport usage summary
    """
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
