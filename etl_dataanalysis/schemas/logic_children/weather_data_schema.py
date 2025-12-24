from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

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
