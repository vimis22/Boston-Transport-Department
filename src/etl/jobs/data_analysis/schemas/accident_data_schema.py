from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

accident_data_schema = StructType(
    [
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
    ]
)
