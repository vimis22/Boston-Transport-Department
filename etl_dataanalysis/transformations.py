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
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

# Import weather enrichment functions
from .weather_enrichment import enrich_weather_data, add_precipitation_indicator

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


# Parser bike streaming data fra Kafka (Avro format via REST Proxy).
# Ekstraherer fields med mellemrum via bracket notation og konverterer timestamps til partitions.
def parse_bike_stream(df: DataFrame) -> DataFrame:
    # Cast Kafka value to string
    string_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")

    # Extract fields from Avro format (note: field names have spaces!)
    result_df = string_df.select(
        get_json_object(col("json_str"), "$.value.tripduration").cast("integer").alias("duration_seconds"),
        get_json_object(col("json_str"), "$.value.starttime").alias("start_time"),
        get_json_object(col("json_str"), "$.value.stoptime").alias("stop_time"),
        get_json_object(col("json_str"), "$['value']['start station id']").alias("start_station_id"),
        get_json_object(col("json_str"), "$['value']['start station name']").alias("start_station_name"),
        get_json_object(col("json_str"), "$['value']['start station latitude']").cast("double").alias("start_station_latitude"),
        get_json_object(col("json_str"), "$['value']['start station longitude']").cast("double").alias("start_station_longitude"),
        get_json_object(col("json_str"), "$['value']['end station id']").alias("end_station_id"),
        get_json_object(col("json_str"), "$['value']['end station name']").alias("end_station_name"),
        get_json_object(col("json_str"), "$['value']['end station latitude']").cast("double").alias("end_station_latitude"),
        get_json_object(col("json_str"), "$['value']['end station longitude']").cast("double").alias("end_station_longitude"),
        get_json_object(col("json_str"), "$.value.bikeid").alias("bike_id"),
        get_json_object(col("json_str"), "$.value.usertype").alias("user_type"),
        get_json_object(col("json_str"), "$['value']['birth year']").cast("integer").alias("birth_year"),
        get_json_object(col("json_str"), "$.value.gender").alias("gender"),
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
# Ekstraherer flat Avro structure og konverterer datetime til timestamp med partitions.
def parse_taxi_stream(df: DataFrame) -> DataFrame:
    # Cast Kafka value to string
    string_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")

    # Extract fields from Avro format (flat structure with camelCase weather fields)
    result_df = string_df.select(
        get_json_object(col("json_str"), "$.value.id").alias("trip_id"),
        get_json_object(col("json_str"), "$.value.datetime").alias("datetime"),
        get_json_object(col("json_str"), "$.value.source").alias("pickup_location"),
        get_json_object(col("json_str"), "$.value.destination").alias("dropoff_location"),
        get_json_object(col("json_str"), "$.value.cab_type").alias("cab_type"),
        get_json_object(col("json_str"), "$.value.product_id").alias("product_id"),
        get_json_object(col("json_str"), "$.value.name").alias("product_name"),
        get_json_object(col("json_str"), "$.value.price").cast("double").alias("price"),
        get_json_object(col("json_str"), "$.value.distance").cast("double").alias("distance"),
        get_json_object(col("json_str"), "$.value.surge_multiplier").cast("double").alias("surge_multiplier"),
        get_json_object(col("json_str"), "$.value.latitude").cast("double").alias("latitude"),
        get_json_object(col("json_str"), "$.value.longitude").cast("double").alias("longitude"),
        get_json_object(col("json_str"), "$.value.temperature").cast("double").alias("temperature"),
        get_json_object(col("json_str"), "$.value.apparentTemperature").cast("double").alias("apparent_temperature"),
        get_json_object(col("json_str"), "$.value.short_summary").alias("weather_summary"),
        get_json_object(col("json_str"), "$.value.precipIntensity").cast("double").alias("precip_intensity"),
        get_json_object(col("json_str"), "$.value.humidity").cast("double").alias("humidity"),
        get_json_object(col("json_str"), "$.value.windSpeed").cast("double").alias("wind_speed"),
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
# Ekstraherer temperature/vind/visibility som comma-separated coded strings og konverterer ISO timestamps.
def parse_weather_stream(df: DataFrame) -> DataFrame:
    # Cast Kafka value to string
    string_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")

    # Extract fields from Avro format (UPPERCASE field names from NCEI)
    result_df = string_df.select(
        get_json_object(col("json_str"), "$.value.STATION").alias("station"),
        get_json_object(col("json_str"), "$.value.DATE").alias("datetime"),
        get_json_object(col("json_str"), "$.value.SOURCE").alias("data_source"),
        get_json_object(col("json_str"), "$.value.LATITUDE").cast("double").alias("latitude"),
        get_json_object(col("json_str"), "$.value.LONGITUDE").cast("double").alias("longitude"),
        get_json_object(col("json_str"), "$.value.ELEVATION").cast("double").alias("elevation"),
        get_json_object(col("json_str"), "$.value.NAME").alias("station_name"),
        get_json_object(col("json_str"), "$.value.REPORT_TYPE").alias("report_type"),
        get_json_object(col("json_str"), "$.value.CALL_SIGN").alias("call_sign"),
        get_json_object(col("json_str"), "$.value.QUALITY_CONTROL").alias("quality_control"),
        get_json_object(col("json_str"), "$.value.WND").alias("wind"),
        get_json_object(col("json_str"), "$.value.CIG").alias("ceiling"),
        get_json_object(col("json_str"), "$.value.VIS").alias("visibility"),
        get_json_object(col("json_str"), "$.value.TMP").alias("temperature"),
        get_json_object(col("json_str"), "$.value.DEW").alias("dew_point"),
        get_json_object(col("json_str"), "$.value.SLP").alias("sea_level_pressure"),
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
# Ekstraherer flat Avro structure og konverterer dispatch timestamps til partitions.
def parse_accident_stream(df: DataFrame) -> DataFrame:
    # Cast Kafka value to string
    string_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")

    # Extract fields from Avro format (flat structure)
    result_df = string_df.select(
        get_json_object(col("json_str"), "$.value.dispatch_ts").alias("dispatch_ts"),
        get_json_object(col("json_str"), "$.value.mode_type").alias("mode_type"),
        get_json_object(col("json_str"), "$.value.location_type").alias("location_type"),
        get_json_object(col("json_str"), "$.value.street").alias("street"),
        get_json_object(col("json_str"), "$.value.xstreet1").alias("xstreet1"),
        get_json_object(col("json_str"), "$.value.xstreet2").alias("xstreet2"),
        get_json_object(col("json_str"), "$.value.x_cord").cast("double").alias("x_cord"),
        get_json_object(col("json_str"), "$.value.y_cord").cast("double").alias("y_cord"),
        get_json_object(col("json_str"), "$.value.lat").cast("double").alias("lat"),
        get_json_object(col("json_str"), "$.value.long").cast("double").alias("long"),
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
