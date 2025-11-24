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
    Parse weather streaming data from Kafka

    Kafka message format:
    {
        "data": {<transformed_record>},
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

    # Extract nested fields from JSON
    result_df = string_df.select(
        get_json_object(col("json_str"), "$.data.observation_id").alias("observation_id"),
        get_json_object(col("json_str"), "$.data.datetime").alias("datetime"),
        get_json_object(col("json_str"), "$.data.temperature.max_f").cast("double").alias("temp_max_f"),
        get_json_object(col("json_str"), "$.data.temperature.min_f").cast("double").alias("temp_min_f"),
        get_json_object(col("json_str"), "$.data.temperature.avg_f").cast("double").alias("temp_avg_f"),
        get_json_object(col("json_str"), "$.data.temperature.feels_like_max_f").cast("double").alias("feels_like_max_f"),
        get_json_object(col("json_str"), "$.data.temperature.feels_like_min_f").cast("double").alias("feels_like_min_f"),
        get_json_object(col("json_str"), "$.data.temperature.feels_like_avg_f").cast("double").alias("feels_like_avg_f"),
        get_json_object(col("json_str"), "$.data.temperature.dew_point_f").cast("double").alias("dew_point_f"),
        get_json_object(col("json_str"), "$.data.precipitation.amount_inches").cast("double").alias("precip_amount_inches"),
        get_json_object(col("json_str"), "$.data.precipitation.probability_pct").cast("double").alias("precip_prob_pct"),
        get_json_object(col("json_str"), "$.data.precipitation.coverage_pct").cast("double").alias("precip_coverage_pct"),
        get_json_object(col("json_str"), "$.data.precipitation.type").alias("precip_type"),
        get_json_object(col("json_str"), "$.data.snow.amount_inches").cast("double").alias("snow_amount_inches"),
        get_json_object(col("json_str"), "$.data.snow.depth_inches").cast("double").alias("snow_depth_inches"),
        get_json_object(col("json_str"), "$.data.wind.speed_mph").cast("double").alias("wind_speed_mph"),
        get_json_object(col("json_str"), "$.data.wind.gust_mph").cast("double").alias("wind_gust_mph"),
        get_json_object(col("json_str"), "$.data.wind.direction_deg").cast("double").alias("wind_direction_deg"),
        get_json_object(col("json_str"), "$.data.atmosphere.humidity_pct").cast("double").alias("humidity_pct"),
        get_json_object(col("json_str"), "$.data.atmosphere.pressure_mb").cast("double").alias("pressure_mb"),
        get_json_object(col("json_str"), "$.data.atmosphere.visibility_miles").cast("double").alias("visibility_miles"),
        get_json_object(col("json_str"), "$.data.atmosphere.cloud_cover_pct").cast("double").alias("cloud_cover_pct"),
        get_json_object(col("json_str"), "$.data.solar.radiation").cast("double").alias("solar_radiation"),
        get_json_object(col("json_str"), "$.data.solar.energy").cast("double").alias("solar_energy"),
        get_json_object(col("json_str"), "$.data.solar.uv_index").cast("double").alias("uv_index"),
        get_json_object(col("json_str"), "$.data.conditions").alias("conditions"),
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
