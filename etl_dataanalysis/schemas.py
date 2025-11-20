from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
    DoubleType,
)

bike_data_schema = StructType([
    StructField("tripduration", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),

    StructField("start station id", StringType(), True),
    StructField("start station name", StringType(), True),
    StructField("start station latitude", DoubleType(), True),
    StructField("start station longitude", DoubleType(), True),

    StructField("end station id", StringType(), True),
    StructField("end station name", StringType(), True),
    StructField("end station latitude", DoubleType(), True),
    StructField("end station longitude", DoubleType(), True),

    StructField("bikeid", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("birth year", IntegerType(), True),
    StructField("gender", StringType(), True),
])

taxi_data_schema = StructType([
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
])

weather_data_schema = StructType([
    StructField("STATION", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("SOURCE", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEVATION", DoubleType(), True),
    StructField("NAME", StringType(), True),
    StructField("REPORT_TYPE", StringType(), True),
    StructField("CALL_SIGN", StringType(), True),
    StructField("QUALITY_CONTROL", StringType(), True),

    StructField("WND", StringType(), True),
    StructField("CIG", StringType(), True),
    StructField("VIS", StringType(), True),
    StructField("TMP", StringType(), True),
    StructField("DEW", StringType(), True),
    StructField("SLP", StringType(), True),
])

accident_data_schema = StructType([
    StructField("dispatch_ts", StringType(), True),
    StructField("mode_type", StringType(), True),
    StructField("location_type", StringType(), True),
    StructField("street", StringType(), True),
    StructField("xstreet1", StringType(), True),
    StructField("xstreet2", StringType(), True),
    StructField("x_cord", DoubleType(), True),
    StructField("y_cord", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
])
