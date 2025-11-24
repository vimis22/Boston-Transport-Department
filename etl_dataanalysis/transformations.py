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
    get_json_object
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


def parse_bike_stream(df: DataFrame) -> DataFrame:
    """
    Parse bike streaming data from Kafka

    Kafka message format:
    {
        "data": {<transformed_record>},
        "timestamp": "ISO timestamp",
        "source": "bike-streamer"
    }

    Args:
        df: Raw Kafka stream DataFrame

    Returns:
        Parsed and transformed bike DataFrame
    """
    # Cast Kafka value to string
    string_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")

    # Extract nested fields from JSON
    result_df = string_df.select(
        get_json_object(col("json_str"), "$.data.trip_id").alias("trip_id"),
        get_json_object(col("json_str"), "$.data.duration_seconds").cast("integer").alias("duration_seconds"),
        get_json_object(col("json_str"), "$.data.start_time").alias("start_time"),
        get_json_object(col("json_str"), "$.data.stop_time").alias("stop_time"),
        get_json_object(col("json_str"), "$.data.start_station.id").alias("start_station_id"),
        get_json_object(col("json_str"), "$.data.start_station.name").alias("start_station_name"),
        get_json_object(col("json_str"), "$.data.start_station.latitude").cast("double").alias("start_station_latitude"),
        get_json_object(col("json_str"), "$.data.start_station.longitude").cast("double").alias("start_station_longitude"),
        get_json_object(col("json_str"), "$.data.end_station.id").alias("end_station_id"),
        get_json_object(col("json_str"), "$.data.end_station.name").alias("end_station_name"),
        get_json_object(col("json_str"), "$.data.end_station.latitude").cast("double").alias("end_station_latitude"),
        get_json_object(col("json_str"), "$.data.end_station.longitude").cast("double").alias("end_station_longitude"),
        get_json_object(col("json_str"), "$.data.bike_id").alias("bike_id"),
        get_json_object(col("json_str"), "$.data.user_type").alias("user_type"),
        get_json_object(col("json_str"), "$.data.birth_year").cast("integer").alias("birth_year"),
        get_json_object(col("json_str"), "$.data.gender").cast("integer").alias("gender"),
        get_json_object(col("json_str"), "$.timestamp").alias("event_timestamp"),
        get_json_object(col("json_str"), "$.source").alias("source"),
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


def parse_taxi_stream(df: DataFrame) -> DataFrame:
    """
    Parse taxi streaming data from Kafka

    Kafka message format:
    {
        "data": {<transformed_record>},
        "timestamp": "ISO timestamp",
        "source": "taxi-streamer"
    }

    Args:
        df: Raw Kafka stream DataFrame

    Returns:
        Parsed and transformed taxi DataFrame
    """
    # Cast Kafka value to string
    string_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")

    # Extract nested fields from JSON
    result_df = string_df.select(
        get_json_object(col("json_str"), "$.data.trip_id").alias("trip_id"),
        get_json_object(col("json_str"), "$.data.datetime").alias("datetime"),
        get_json_object(col("json_str"), "$.data.pickup_location").alias("pickup_location"),
        get_json_object(col("json_str"), "$.data.dropoff_location").alias("dropoff_location"),
        get_json_object(col("json_str"), "$.data.cab_type").alias("cab_type"),
        get_json_object(col("json_str"), "$.data.product.id").alias("product_id"),
        get_json_object(col("json_str"), "$.data.product.name").alias("product_name"),
        get_json_object(col("json_str"), "$.data.price").cast("double").alias("price"),
        get_json_object(col("json_str"), "$.data.distance").cast("double").alias("distance"),
        get_json_object(col("json_str"), "$.data.surge_multiplier").cast("double").alias("surge_multiplier"),
        get_json_object(col("json_str"), "$.data.location.latitude").cast("double").alias("latitude"),
        get_json_object(col("json_str"), "$.data.location.longitude").cast("double").alias("longitude"),
        get_json_object(col("json_str"), "$.data.weather_snapshot.temperature").cast("double").alias("temperature"),
        get_json_object(col("json_str"), "$.data.weather_snapshot.apparent_temperature").cast("double").alias("apparent_temperature"),
        get_json_object(col("json_str"), "$.data.weather_snapshot.summary").alias("weather_summary"),
        get_json_object(col("json_str"), "$.data.weather_snapshot.precip_intensity").cast("double").alias("precip_intensity"),
        get_json_object(col("json_str"), "$.data.weather_snapshot.humidity").cast("double").alias("humidity"),
        get_json_object(col("json_str"), "$.data.weather_snapshot.wind_speed").cast("double").alias("wind_speed"),
        get_json_object(col("json_str"), "$.timestamp").alias("event_timestamp"),
        get_json_object(col("json_str"), "$.source").alias("source"),
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


def parse_weather_stream(df: DataFrame) -> DataFrame:
    """
    Parse weather streaming data from Kafka (NCEI format)

    Kafka message format:
    {
        "data": {
            "station": "...",
            "datetime": "...",
            "location": {...},
            "station_info": {...},
            "observations": {...}
        },
        "timestamp": "ISO timestamp",
        "source": "weather-streamer"
    }

    Args:
        df: Raw Kafka stream DataFrame

    Returns:
        Parsed and transformed weather DataFrame
    """
    # Cast Kafka value to string
    string_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")

    # Extract nested fields from JSON (NCEI format)
    result_df = string_df.select(
        get_json_object(col("json_str"), "$.data.station").alias("station"),
        get_json_object(col("json_str"), "$.data.datetime").alias("datetime"),
        get_json_object(col("json_str"), "$.data.source").alias("data_source"),
        get_json_object(col("json_str"), "$.data.location.latitude").cast("double").alias("latitude"),
        get_json_object(col("json_str"), "$.data.location.longitude").cast("double").alias("longitude"),
        get_json_object(col("json_str"), "$.data.location.elevation").cast("double").alias("elevation"),
        get_json_object(col("json_str"), "$.data.station_info.name").alias("station_name"),
        get_json_object(col("json_str"), "$.data.station_info.report_type").alias("report_type"),
        get_json_object(col("json_str"), "$.data.station_info.call_sign").alias("call_sign"),
        get_json_object(col("json_str"), "$.data.station_info.quality_control").alias("quality_control"),
        get_json_object(col("json_str"), "$.data.observations.wind").alias("wind"),
        get_json_object(col("json_str"), "$.data.observations.ceiling").alias("ceiling"),
        get_json_object(col("json_str"), "$.data.observations.visibility").alias("visibility"),
        get_json_object(col("json_str"), "$.data.observations.temperature").alias("temperature"),
        get_json_object(col("json_str"), "$.data.observations.dew_point").alias("dew_point"),
        get_json_object(col("json_str"), "$.data.observations.sea_level_pressure").alias("sea_level_pressure"),
        get_json_object(col("json_str"), "$.timestamp").alias("event_timestamp"),
        get_json_object(col("json_str"), "$.source").alias("source"),
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

    return final_df


def parse_accident_stream(df: DataFrame) -> DataFrame:
    """
    Parse accident streaming data from Kafka

    Kafka message format:
    {
        "data": {<transformed_record>},
        "timestamp": "ISO timestamp",
        "source": "accident-streamer"
    }

    Args:
        df: Raw Kafka stream DataFrame

    Returns:
        Parsed and transformed accident DataFrame
    """
    # Cast Kafka value to string
    string_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")

    # Extract nested fields from JSON
    result_df = string_df.select(
        get_json_object(col("json_str"), "$.data.dispatch_ts").alias("dispatch_ts"),
        get_json_object(col("json_str"), "$.data.mode_type").alias("mode_type"),
        get_json_object(col("json_str"), "$.data.location_type").alias("location_type"),
        get_json_object(col("json_str"), "$.data.street").alias("street"),
        get_json_object(col("json_str"), "$.data.xstreet1").alias("xstreet1"),
        get_json_object(col("json_str"), "$.data.xstreet2").alias("xstreet2"),
        get_json_object(col("json_str"), "$.data.x_cord").cast("double").alias("x_cord"),
        get_json_object(col("json_str"), "$.data.y_cord").cast("double").alias("y_cord"),
        get_json_object(col("json_str"), "$.data.lat").cast("double").alias("lat"),
        get_json_object(col("json_str"), "$.data.long").cast("double").alias("long"),
        get_json_object(col("json_str"), "$.timestamp").alias("event_timestamp"),
        get_json_object(col("json_str"), "$.source").alias("source"),
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
