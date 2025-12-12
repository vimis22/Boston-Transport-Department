from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    date_format,
    get_json_object,
    when,
    expr
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
from pyspark.sql.avro.functions import from_avro

# Import weather enrichment functions
from .weather_enrichment import enrich_weather_data, add_precipitation_indicator


# Helper function to decode Avro payload, skipping the 5-byte Confluent header
def decode_avro_payload(col_name: str, schema: str):
    """Decode Avro payload, skipping the 5-byte Confluent header (Magic Byte + Schema ID)."""
    return from_avro(expr(f"substring({col_name}, 6, length({col_name})-5)"), schema)

#What initially is happening here is that we are trying to format the data from JSON-Format to a Column-Based Format.
#In ths context, we parse the JSON String to Column-Based Format, via. the bike_data_schema.
#This results, that we get the following structural change.

# +----------------------------------------+
# | value                                  |
# +----------------------------------------+
# | {"trip_id": "123", "duration": 600...} |
# +----------------------------------------+

# To this structural change:
# +---------+------------------+-------------+
# | trip_id | duration_seconds | start_time  |
# +---------+------------------+-------------+
# | 123     | 600              | 2018-01-... |
# +---------+------------------+-------------+


# Parser bike streaming data fra Kafka (Avro format via Schema Registry).
# Dekoder Avro payload og ekstraherer fields med mellemrum og konverterer timestamps til partitions.
def parse_bike_stream(df: DataFrame, schema: str) -> DataFrame:
    """
    Parse bike stream from Kafka with Avro encoding.

    Args:
        df: Raw Kafka DataFrame with binary value column
        schema: Avro schema string from Schema Registry
    """
    # Decode Avro payload (skipping 5-byte Confluent header)
    decoded_df = df.select(
        decode_avro_payload("value", schema).alias("bike"),
        col("timestamp").alias("kafka_timestamp")
    )

    # Extract fields from decoded Avro structure
    result_df = decoded_df.select(
        col("bike.tripduration").cast("integer").alias("duration_seconds"),
        col("bike.starttime").alias("start_time"),
        col("bike.stoptime").alias("stop_time"),
        col("bike.`start station id`").alias("start_station_id"),
        col("bike.`start station name`").alias("start_station_name"),
        col("bike.`start station latitude`").cast("double").alias("start_station_latitude"),
        col("bike.`start station longitude`").cast("double").alias("start_station_longitude"),
        col("bike.`end station id`").alias("end_station_id"),
        col("bike.`end station name`").alias("end_station_name"),
        col("bike.`end station latitude`").cast("double").alias("end_station_latitude"),
        col("bike.`end station longitude`").cast("double").alias("end_station_longitude"),
        col("bike.bikeid").alias("bike_id"),
        col("bike.usertype").alias("user_type"),
        col("bike.`birth year`").cast("integer").alias("birth_year"),
        col("bike.gender").alias("gender"),
        col("kafka_timestamp")
    )

    # Convert start_time to timestamp and extract date parts for partitioning
    final_df = result_df.withColumn(
        "start_time_ts", to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "stop_time_ts", to_timestamp(col("stop_time"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "year", year(col("start_time_ts"))
    ).withColumn(
        "month", month(col("start_time_ts"))
    ).withColumn(
        "date", date_format(col("start_time_ts"), "yyyy-MM-dd")
    ).withColumn(
        "hour", hour(col("start_time_ts"))
    )

    return final_df

# Parser taxi streaming data fra Kafka med embedded weather snapshots (camelCase fields).
# Dekoder Avro payload og ekstraherer flat structure og konverterer datetime til timestamp med partitions.
def parse_taxi_stream(df: DataFrame, schema: str) -> DataFrame:
    """
    Parse taxi stream from Kafka with Avro encoding.

    Args:
        df: Raw Kafka DataFrame with binary value column
        schema: Avro schema string from Schema Registry
    """
    # Decode Avro payload (skipping 5-byte Confluent header)
    decoded_df = df.select(
        decode_avro_payload("value", schema).alias("taxi"),
        col("timestamp").alias("kafka_timestamp")
    )

    # Extract fields from decoded Avro structure
    result_df = decoded_df.select(
        col("taxi.id").alias("trip_id"),
        col("taxi.datetime").alias("datetime"),
        col("taxi.source").alias("pickup_location"),
        col("taxi.destination").alias("dropoff_location"),
        col("taxi.cab_type").alias("cab_type"),
        col("taxi.product_id").alias("product_id"),
        col("taxi.name").alias("product_name"),
        col("taxi.price").cast("double").alias("price"),
        col("taxi.distance").cast("double").alias("distance"),
        col("taxi.surge_multiplier").cast("double").alias("surge_multiplier"),
        col("taxi.latitude").cast("double").alias("latitude"),
        col("taxi.longitude").cast("double").alias("longitude"),
        col("taxi.temperature").cast("double").alias("temperature"),
        col("taxi.apparentTemperature").cast("double").alias("apparent_temperature"),
        col("taxi.short_summary").alias("weather_summary"),
        col("taxi.precipIntensity").cast("double").alias("precip_intensity"),
        col("taxi.humidity").cast("double").alias("humidity"),
        col("taxi.windSpeed").cast("double").alias("wind_speed"),
        col("kafka_timestamp")
    )

    # Convert datetime to timestamp and extract date parts for partitioning
    final_df = result_df.withColumn(
        "datetime_ts", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "year", year(col("datetime_ts"))
    ).withColumn(
        "month", month(col("datetime_ts"))
    ).withColumn(
        "date", date_format(col("datetime_ts"), "yyyy-MM-dd")
    ).withColumn(
        "hour", hour(col("datetime_ts"))
    )

    return final_df

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
        col("timestamp").alias("kafka_timestamp")
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
        col("kafka_timestamp")
    )

    # Convert datetime to timestamp and extract date parts for partitioning
    final_df = result_df.withColumn(
        "datetime_ts", to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ss")
    ).withColumn(
        "year", year(col("datetime_ts"))
    ).withColumn(
        "month", month(col("datetime_ts"))
    ).withColumn(
        "date", date_format(col("datetime_ts"), "yyyy-MM-dd")
    ).withColumn(
        "hour", hour(col("datetime_ts"))
    )

    # Apply weather enrichment to parse and categorize weather data
    final_df = enrich_weather_data(final_df)
    final_df = add_precipitation_indicator(final_df)

    return final_df

# Parser accident streaming data fra Kafka med mode_type (bike/mv/ped) og location info.
# Dekoder Avro payload og ekstraherer flat structure og konverterer dispatch timestamps til partitions.
def parse_accident_stream(df: DataFrame, schema: str) -> DataFrame:
    """
    Parse accident stream from Kafka with Avro encoding.

    Args:
        df: Raw Kafka DataFrame with binary value column
        schema: Avro schema string from Schema Registry
    """
    # Decode Avro payload (skipping 5-byte Confluent header)
    decoded_df = df.select(
        decode_avro_payload("value", schema).alias("accident"),
        col("timestamp").alias("kafka_timestamp")
    )

    # Extract fields from decoded Avro structure
    result_df = decoded_df.select(
        col("accident.dispatch_ts").alias("dispatch_ts"),
        col("accident.mode_type").alias("mode_type"),
        col("accident.location_type").alias("location_type"),
        col("accident.street").alias("street"),
        col("accident.xstreet1").alias("xstreet1"),
        col("accident.xstreet2").alias("xstreet2"),
        col("accident.x_cord").cast("double").alias("x_cord"),
        col("accident.y_cord").cast("double").alias("y_cord"),
        col("accident.lat").cast("double").alias("lat"),
        col("accident.long").cast("double").alias("long"),
        col("kafka_timestamp")
    )

    # Convert dispatch_ts to timestamp and extract date parts for partitioning
    final_df = result_df.withColumn(
        "dispatch_timestamp", to_timestamp(col("dispatch_ts"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "year", year(col("dispatch_timestamp"))
    ).withColumn(
        "month", month(col("dispatch_timestamp"))
    ).withColumn(
        "date", date_format(col("dispatch_timestamp"), "yyyy-MM-dd")
    ).withColumn(
        "hour", hour(col("dispatch_timestamp"))
    )

    return final_df
