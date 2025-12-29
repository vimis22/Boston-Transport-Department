from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, hour, date_format
from .decode_avro_payload import decode_avro_payload

# Import weather enrichment functions from the enrichments module
from data_analysis.enrichments.enrich_weather_data import enrich_weather_data
from data_analysis.enrichments.add_precipitation_indicator import (
    add_precipitation_indicator,
)


# Parser weather streaming data fra Kafka (NOAA NCEI format med UPPERCASE fields).
# Dekoder Avro payload og ekstraherer temperature/vind/visibility som comma-separated coded strings og konverterer ISO timestamps.
def parse_weather_stream(df: DataFrame, schema: str) -> DataFrame:
    """
    Parse weather stream from Kafka with Avro encoding.

    Args:
        df: Raw Kafka DataFrame with binary value column
        schema: Avro schema string from Schema Registry
    """
    # Decode Avro payload (skipping 5-byte Confluent header)
    decoded_df = df.select(
        decode_avro_payload("value", schema).alias("weather"),
        col("timestamp").alias("kafka_timestamp"),
    )

    # Extract fields from decoded Avro structure (lowercase field names)
    result_df = decoded_df.select(
        col("weather.station_id").alias("station"),
        col("weather.observation_date").alias("datetime"),
        col("weather.source_code").alias("data_source"),
        col("weather.latitude").cast("double").alias("latitude"),
        col("weather.longitude").cast("double").alias("longitude"),
        col("weather.elevation_meters").cast("double").alias("elevation"),
        col("weather.station_name").alias("station_name"),
        col("weather.report_type").alias("report_type"),
        col("weather.call_sign").alias("call_sign"),
        col("weather.quality_control").alias("quality_control"),
        col("weather.wind_observation").alias("wind"),
        col("weather.sky_condition_observation").alias("ceiling"),
        col("weather.visibility_observation").alias("visibility"),
        col("weather.dry_bulb_temperature_celsius").alias("temperature"),
        col("weather.dew_point_temperature_celsius").alias("dew_point"),
        col("weather.sea_level_pressure").alias("sea_level_pressure"),
        col("weather.precip_daily_mm").alias("precip_daily_mm"),
        col("kafka_timestamp"),
    )

    # Convert datetime to timestamp and extract date parts for partitioning
    final_df = (
        result_df.withColumn(
            "datetime_ts", to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ss")
        )
        .withColumn("year", year(col("datetime_ts")))
        .withColumn("month", month(col("datetime_ts")))
        .withColumn("date", date_format(col("datetime_ts"), "yyyy-MM-dd"))
        .withColumn("hour", hour(col("datetime_ts")))
    )

    # Apply weather enrichment to parse and categorize weather data
    final_df = enrich_weather_data(final_df)
    final_df = add_precipitation_indicator(final_df)

    # Add watermark for late data handling (following src/etl pattern)
    final_df = final_df.withWatermark("datetime_ts", "10 minutes")

    return final_df
