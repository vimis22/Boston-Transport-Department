from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# The purpose with this code is to show how the data looks when they arrive from Kafka.
# In this context, we see that all the data values are wrapped in a JSON object.
# This means, that all these data values will turn into an envelop for each dataset that is stream.
# Example can be taken here, where bike_data_schema would have envelope: {"data": {...}, "timestamp": "ISO timestamp", "source": "bike-streamer""}
bike_data_schema = StructType(
    [
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
    ]
)
