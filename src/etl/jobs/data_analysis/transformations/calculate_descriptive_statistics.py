"""
Descriptive Statistics Calculations for Weather-Transport Analysis

Dette modul beregner beskrivende statistik for at forstå:
- Central tendencies (mean, median)
- Variability (std dev, variance, range)
- Distribution shape (skewness, kurtosis)
- Percentiles

Følger Single Responsibility Principle - kun statistiske beregninger.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, mean, stddev, variance,
    min as spark_min, max as spark_max,
    count, percentile_approx,
    skewness, kurtosis, sum as spark_sum
)


def calculate_overall_statistics(df: DataFrame) -> DataFrame:
    """
    Beregn overordnet beskrivende statistik for hele datasættet.

    Returnerer:
    - Central tendency: mean, median
    - Variability: std dev, variance, range
    - Distribution: skewness, kurtosis
    - Percentiles: 25%, 50%, 75%
    - Sample size
    """

    stats_df = df.select(
        # Central tendency for speed
        mean("average_speed_kmh").alias("mean_speed"),
        percentile_approx("average_speed_kmh", 0.5).alias("median_speed"),

        # Variability for speed
        stddev("average_speed_kmh").alias("stddev_speed"),
        variance("average_speed_kmh").alias("variance_speed"),
        spark_min("average_speed_kmh").alias("min_speed"),
        spark_max("average_speed_kmh").alias("max_speed"),

        # Distribution shape for speed
        skewness("average_speed_kmh").alias("skewness_speed"),
        kurtosis("average_speed_kmh").alias("kurtosis_speed"),

        # Percentiles for speed
        percentile_approx("average_speed_kmh", 0.25).alias("p25_speed"),
        percentile_approx("average_speed_kmh", 0.75).alias("p75_speed"),

        # Central tendency for temperature
        mean("temp_c").alias("mean_temp"),
        percentile_approx("temp_c", 0.5).alias("median_temp"),
        stddev("temp_c").alias("stddev_temp"),
        spark_min("temp_c").alias("min_temp"),
        spark_max("temp_c").alias("max_temp"),

        # Central tendency for distance
        mean("distance_meters").alias("mean_distance"),
        percentile_approx("distance_meters", 0.5).alias("median_distance"),
        stddev("distance_meters").alias("stddev_distance"),

        # Central tendency for duration
        mean("tripduration").alias("mean_duration"),
        percentile_approx("tripduration", 0.5).alias("median_duration"),
        stddev("tripduration").alias("stddev_duration"),

        # Sample size
        count("*").alias("total_trips")
    )

    # Add calculated fields
    stats_df = stats_df.withColumn(
        "range_speed",
        col("max_speed") - col("min_speed")
    ).withColumn(
        "cv_speed",  # Coefficient of variation
        col("stddev_speed") / col("mean_speed")
    ).withColumn(
        "iqr_speed",  # Interquartile range
        col("p75_speed") - col("p25_speed")
    )

    return stats_df


def calculate_statistics_by_weather_condition(df: DataFrame) -> DataFrame:
    """
    Beregn statistik grupperet efter vejr-betingelser.

    Dette viser hvordan speed, distance og duration varierer
    baseret på weather_condition (good/fair/poor).
    """

    stats_by_weather = df.groupBy("weather_condition").agg(
        # Speed statistics
        mean("average_speed_kmh").alias("mean_speed"),
        percentile_approx("average_speed_kmh", 0.5).alias("median_speed"),
        stddev("average_speed_kmh").alias("stddev_speed"),
        spark_min("average_speed_kmh").alias("min_speed"),
        spark_max("average_speed_kmh").alias("max_speed"),
        percentile_approx("average_speed_kmh", 0.25).alias("p25_speed"),
        percentile_approx("average_speed_kmh", 0.75).alias("p75_speed"),

        # Temperature statistics
        mean("temp_c").alias("mean_temp"),
        stddev("temp_c").alias("stddev_temp"),
        spark_min("temp_c").alias("min_temp"),
        spark_max("temp_c").alias("max_temp"),

        # Distance statistics
        mean("distance_meters").alias("mean_distance"),
        stddev("distance_meters").alias("stddev_distance"),

        # Duration statistics
        mean("tripduration").alias("mean_duration"),
        stddev("tripduration").alias("stddev_duration"),

        # Precipitation statistics
        mean("precip_daily_mm").alias("mean_precip"),
        spark_max("precip_daily_mm").alias("max_precip"),

        # Sample size
        count("*").alias("sample_size")
    )

    # Add derived metrics
    stats_by_weather = stats_by_weather.withColumn(
        "range_speed",
        col("max_speed") - col("min_speed")
    ).withColumn(
        "cv_speed",
        col("stddev_speed") / col("mean_speed")
    ).withColumn(
        "iqr_speed",
        col("p75_speed") - col("p25_speed")
    )

    return stats_by_weather


def calculate_statistics_by_temperature_bucket(df: DataFrame) -> DataFrame:
    """
    Beregn statistik grupperet efter temperatur-intervaller.

    Temperature buckets:
    - Below 0°C (Freezing)
    - 0-10°C (Cold)
    - 10-20°C (Mild)
    - 20-30°C (Warm)
    - Above 30°C (Hot)
    """

    # Create temperature buckets
    from pyspark.sql.functions import when

    df_with_buckets = df.withColumn(
        "temp_bucket",
        when(col("temp_c") < 0, "Below 0°C (Freezing)")
        .when((col("temp_c") >= 0) & (col("temp_c") < 10), "0-10°C (Cold)")
        .when((col("temp_c") >= 10) & (col("temp_c") < 20), "10-20°C (Mild)")
        .when((col("temp_c") >= 20) & (col("temp_c") < 30), "20-30°C (Warm)")
        .otherwise("Above 30°C (Hot)")
    )

    stats_by_temp = df_with_buckets.groupBy("temp_bucket").agg(
        mean("average_speed_kmh").alias("mean_speed"),
        stddev("average_speed_kmh").alias("stddev_speed"),
        percentile_approx("average_speed_kmh", 0.5).alias("median_speed"),
        spark_min("average_speed_kmh").alias("min_speed"),
        spark_max("average_speed_kmh").alias("max_speed"),

        mean("distance_meters").alias("mean_distance"),
        mean("tripduration").alias("mean_duration"),

        mean("temp_c").alias("avg_temp_in_bucket"),

        count("*").alias("sample_size")
    )

    return stats_by_temp


def calculate_statistics_by_precipitation_level(df: DataFrame) -> DataFrame:
    """
    Beregn statistik grupperet efter nedbørs-niveau.

    Precipitation levels:
    - None (0mm)
    - Light (< 1mm)
    - Moderate (1-5mm)
    - Heavy (> 5mm)
    """

    from pyspark.sql.functions import when

    df_with_precip_level = df.withColumn(
        "precip_level",
        when(col("precip_daily_mm") == 0, "None (0mm)")
        .when((col("precip_daily_mm") > 0) & (col("precip_daily_mm") < 1), "Light (<1mm)")
        .when((col("precip_daily_mm") >= 1) & (col("precip_daily_mm") < 5), "Moderate (1-5mm)")
        .otherwise("Heavy (>5mm)")
    )

    stats_by_precip = df_with_precip_level.groupBy("precip_level").agg(
        mean("average_speed_kmh").alias("mean_speed"),
        stddev("average_speed_kmh").alias("stddev_speed"),
        percentile_approx("average_speed_kmh", 0.5).alias("median_speed"),

        mean("distance_meters").alias("mean_distance"),
        mean("tripduration").alias("mean_duration"),

        mean("precip_daily_mm").alias("avg_precip_in_level"),
        mean("temp_c").alias("mean_temp"),

        count("*").alias("sample_size")
    )

    return stats_by_precip
