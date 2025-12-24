"""
Join Bike and Weather Data - Single Responsibility Principle (SRP)

Dette modul har ÉT ansvar: At join bike trip data med vejr data baseret på tidspunkt.
Følger SOLID principper for nem vedligeholdelse og test.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, date_format, hour as get_hour, concat_ws, expr
from typing import Tuple


def join_bike_weather_data(
    bike_df: DataFrame,
    weather_df: DataFrame,
    time_window_minutes: int = 30
) -> DataFrame:
    """
    Join bike trip data med vejr data baseret på tidspunkt.
    
    Args:
        bike_df: DataFrame med bike trip data (skal have 'start_time' kolonne)
        weather_df: DataFrame med vejr data (skal have 'datetime_ts' kolonne)
        time_window_minutes: Tidsvindues tolerence for at matche vejr med ture
        
    Returns:
        DataFrame: Joined bike + weather data med alle relevante kolonner
        
    Bemærk:
        Dette er en SIMPLE join uden komplekse beregninger.
        Beregninger (distance, speed, statistik) håndteres af andre moduler (SRP!).
    """
    
    # Ensure bike data has proper timestamp format
    bike_prepared = bike_df.withColumn(
        "trip_start_ts",
        to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss")
    )
    
    # Extract date and hour for partitioning (dashboard bruger disse!)
    bike_prepared = bike_prepared \
        .withColumn("date", date_format(col("trip_start_ts"), "yyyy-MM-dd")) \
        .withColumn("hour", get_hour(col("trip_start_ts")))
    
    # Generate unique trip_id for tracking
    bike_prepared = bike_prepared.withColumn(
        "trip_id",
        concat_ws("_",
            col("start_station_id"),
            date_format(col("trip_start_ts"), "yyyyMMddHHmmss")
        )
    )
    
    # Rename weather columns to avoid ambiguity after join
    # Both bike and weather have: date, hour, kafka_timestamp
    weather_for_join = weather_df.select(
        col("datetime_ts").alias("weather_datetime_ts"),
        col("temp_c"),
        col("wind_speed_ms"),
        col("visibility_m"),
        col("precip_daily_mm")
    )

    # Join med weather data baseret på tidspunkt
    # Vi bruger en time-based join med tolerance window
    # Dette sikrer at hver cykeltur får den tættest vejrdata

    joined_df = bike_prepared.join(
        weather_for_join,
        expr(f"""
            trip_start_ts >= weather_datetime_ts - INTERVAL {time_window_minutes} MINUTES
            AND trip_start_ts <= weather_datetime_ts + INTERVAL {time_window_minutes} MINUTES
        """),
        "left"  # Left join så vi beholder alle bike trips selv uden vejr data
    )

    # Select kun de kolonner dashboard forventer
    result_df = joined_df.select(
        # Trip identifiers
        col("trip_id"),
        col("start_time").alias("starttime"),
        col("stop_time").alias("stoptime"),
        col("duration_seconds").alias("tripduration"),

        # Station information
        col("start_station_id"),
        col("start_station_name"),
        col("start_station_latitude"),
        col("start_station_longitude"),
        col("end_station_id"),
        col("end_station_name"),
        col("end_station_latitude"),
        col("end_station_longitude"),

        # Weather data (kan være NULL hvis ingen match)
        col("temp_c"),
        col("wind_speed_ms"),
        col("visibility_m"),
        col("precip_daily_mm"),

        # Partitioning columns (vigtige for dashboard queries!)
        col("date"),  # From bike data
        col("hour"),  # From bike data

        # Keep timestamp for videre beregninger
        col("trip_start_ts"),
        col("kafka_timestamp")  # From bike data
    )
    
    return result_df


def add_weather_condition_category(df: DataFrame) -> DataFrame:
    """
    Tilføj simpel vejr kategori (good/fair/poor) baseret på temperature og visibility.
    
    Dette er en DESCRIPTIVE kategorisering - ikke en kompleks scoring.
    Nem at forklare til eksamen!
    
    Args:
        df: DataFrame med temp_c og visibility_m kolonner
        
    Returns:
        DataFrame: Med tilføjet 'weather_condition' kolonne
    """
    
    return df.withColumn(
        "weather_condition",
        expr("""
            CASE
                WHEN temp_c IS NULL THEN 'unknown'
                WHEN temp_c >= 15 AND temp_c <= 25 AND visibility_m > 10000 THEN 'good'
                WHEN temp_c >= 10 AND temp_c <= 30 AND visibility_m > 5000 THEN 'fair'
                ELSE 'poor'
            END
        """)
    )


def add_precipitation_indicator(df: DataFrame, weather_df: DataFrame) -> DataFrame:
    """
    Tilføj nedbørs indikator til joined data.
    
    Simpel aggregering af daglig nedbør fra vejr data.
    
    Args:
        df: Joined bike-weather DataFrame
        weather_df: Weather DataFrame med precipitation data
        
    Returns:
        DataFrame: Med tilføjet 'precip_daily_mm' kolonne
    """
    
    # Aggregate daily precipitation fra weather data
    daily_precip = weather_df.groupBy(
        date_format(col("datetime_ts"), "yyyy-MM-dd").alias("precip_date")
    ).agg(
        expr("SUM(COALESCE(precipitation, 0))").alias("precip_daily_mm")
    )
    
    # Join back til hoveddata
    result = df.join(
        daily_precip,
        col("date") == col("precip_date"),
        "left"
    ).drop("precip_date")
    
    return result
