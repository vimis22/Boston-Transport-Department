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

    # Extract fields from decoded Avro structure (UPPERCASE field names from NCEI)
    result_df = decoded_df.select(
        col("weather.STATION").alias("station"),
        col("weather.DATE").alias("datetime"),
        col("weather.SOURCE").alias("data_source"),
        col("weather.LATITUDE").cast("double").alias("latitude"),
        col("weather.LONGITUDE").cast("double").alias("longitude"),
        col("weather.ELEVATION").cast("double").alias("elevation"),
        col("weather.NAME").alias("station_name"),
        col("weather.REPORT_TYPE").alias("report_type"),
        col("weather.CALL_SIGN").alias("call_sign"),
        col("weather.QUALITY_CONTROL").alias("quality_control"),
        col("weather.WND").alias("wind"),
        col("weather.CIG").alias("ceiling"),
        col("weather.VIS").alias("visibility"),
        col("weather.TMP").alias("temperature"),
        col("weather.DEW").alias("dew_point"),
        col("weather.SLP").alias("sea_level_pressure"),
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
