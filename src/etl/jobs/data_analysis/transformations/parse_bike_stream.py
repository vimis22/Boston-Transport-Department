from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, hour, date_format
from .decode_avro_payload import decode_avro_payload


# What initially is happening here is that we are trying to format the data from JSON-Format to a Column-Based Format.
# In ths context, we parse the JSON String to Column-Based Format, via. the bike_data_schema.
# This results, that we get the following structural change.

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
        col("timestamp").alias("kafka_timestamp"),
    )

    # Extract fields from decoded Avro structure
    result_df = decoded_df.select(
        col("bike.tripduration").cast("integer").alias("duration_seconds"),
        col("bike.starttime").alias("start_time"),
        col("bike.stoptime").alias("stop_time"),
        col("bike.`start station id`").alias("start_station_id"),
        col("bike.`start station name`").alias("start_station_name"),
        col("bike.`start station latitude`")
        .cast("double")
        .alias("start_station_latitude"),
        col("bike.`start station longitude`")
        .cast("double")
        .alias("start_station_longitude"),
        col("bike.`end station id`").alias("end_station_id"),
        col("bike.`end station name`").alias("end_station_name"),
        col("bike.`end station latitude`").cast("double").alias("end_station_latitude"),
        col("bike.`end station longitude`")
        .cast("double")
        .alias("end_station_longitude"),
        col("bike.bikeid").alias("bike_id"),
        col("bike.usertype").alias("user_type"),
        col("bike.`birth year`").cast("integer").alias("birth_year"),
        col("bike.gender").alias("gender"),
        col("kafka_timestamp"),
    )

    # Convert start_time to timestamp and extract date parts for partitioning
    final_df = (
        result_df.withColumn(
            "start_time_ts", to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn(
            "stop_time_ts", to_timestamp(col("stop_time"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("year", year(col("start_time_ts")))
        .withColumn("month", month(col("start_time_ts")))
        .withColumn("date", date_format(col("start_time_ts"), "yyyy-MM-dd"))
        .withColumn("hour", hour(col("start_time_ts")))
    )

    # Add watermark for late data handling (following src/etl pattern)
    final_df = final_df.withWatermark("start_time_ts", "10 minutes")

    return final_df
