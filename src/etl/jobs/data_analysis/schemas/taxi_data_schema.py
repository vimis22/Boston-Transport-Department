from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

taxi_data_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("hour", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("datetime", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("source", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("cab_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("distance", DoubleType(), True),
        StructField("surge_multiplier", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("apparentTemperature", DoubleType(), True),
        StructField("short_summary", StringType(), True),
        StructField("long_summary", StringType(), True),
        StructField("precipIntensity", DoubleType(), True),
        StructField("precipProbability", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("windGust", DoubleType(), True),
        StructField("visibility", DoubleType(), True),
    ]
)
