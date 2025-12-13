from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    window,
    count,
    avg,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    stddev,
    when,
)


def aggregate_bike_data_by_window(
    df: DataFrame, window_duration: str = "15 minutes", slide_duration: str = None
) -> DataFrame:
    """
    Aggregate bike rental data in time windows.

    Args:
        df: Bike DataFrame with start_time_ts column
        window_duration: Window size (e.g., "5 minutes", "15 minutes", "1 hour")
        slide_duration: Slide interval (None = tumbling window, e.g., "5 minutes" = sliding)

    Returns:
        DataFrame with aggregated bike metrics per time window
    """
    # Group by time window
    if slide_duration:
        windowed = df.groupBy(
            window(col("start_time_ts"), window_duration, slide_duration), "user_type"
        )
    else:
        windowed = df.groupBy(
            window(col("start_time_ts"), window_duration), "user_type"
        )

    # Compute aggregations
    result = windowed.agg(
        count("*").alias("rental_count"),
        avg("duration_seconds").alias("avg_duration_seconds"),
        spark_min("duration_seconds").alias("min_duration_seconds"),
        spark_max("duration_seconds").alias("max_duration_seconds"),
        stddev("duration_seconds").alias("stddev_duration"),
        # Station popularity
        count("start_station_id").alias("total_trips_started"),
        # User demographics
        avg("birth_year").alias("avg_birth_year"),
        count(when(col("gender") == 2, 1)).alias("female_count"),
        count(when(col("gender") == 1, 1)).alias("male_count"),
        count(when(col("gender") == 0, 1)).alias("unknown_gender_count"),
    )

    # Add derived metrics
    result = (
        result.withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("avg_duration_minutes", col("avg_duration_seconds") / 60.0)
        .withColumn(
            "gender_diversity_ratio",
            when(
                col("male_count") + col("female_count") > 0,
                col("female_count") / (col("male_count") + col("female_count")),
            ).otherwise(None),
        )
    )

    return result
