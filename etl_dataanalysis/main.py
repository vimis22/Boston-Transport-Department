import logging
from pyspark.sql import SparkSession

#Here we begin all 3 streams.
from . import config
from .transformations import (
    parse_bike_stream,
    parse_taxi_stream,
    parse_weather_stream,
    parse_accident_stream,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.config(conf=config.spark_config).getOrCreate()
    )

def read_kafka_stream(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

def write_parquest_stream(df, subfolder: str):
    output_path = f"{config.OUTPUT_BASE_PATH}/{subfolder}"
    checkpoint_path = f"{config.CHECKPOINT_BASE_PATH}/{subfolder}"

    logger.info(f"Writing to {output_path}")
    return (
        df.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("year", "month", "date", "hour")
        .outputMode("append")
        .trigger(processingTime=config.BATCH_INTERVAL)
        .start()
    )

def main():
    logger.info("=== Starting Boston Transport Simple ETL")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Spark Version: {spark.version}")
    logger.info(f"Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Output base: {config.OUTPUT_BASE_PATH}")

    try:
        bike_raw = read_kafka_stream(spark, config.KAFKA_TOPICS["bike"])
        bike_df = parse_bike_stream(bike_raw)
        bike_query = write_parquest_stream(bike_df, "bike_trips")

        taxi_raw = read_kafka_stream(spark, config.KAFKA_TOPICS["taxi"])
        taxi_df = parse_taxi_stream(taxi_raw)
        taxi_query = write_parquest_stream(taxi_df, "taxi_trips")

        weather_raw = read_kafka_stream(spark, config.KAFKA_TOPICS["weather"])
        weather_df = parse_weather_stream(weather_raw)
        weather_query = write_parquest_stream(weather_df, "weather_data")

        accident_raw = read_kafka_stream(spark, "accidents")
        accident_df = parse_accident_stream(accident_raw)
        accident_query = write_parquest_stream(accident_df, "accidents")

        bike_query.awaitTermination()
        taxi_query.awaitTermination()
        weather_query.awaitTermination()
        accident_query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Stopping ETL (KeyboardInterrupt)")
    except Exception:
        logger.exception("Error in ETL")
    finally:
        spark.stop()
        logger.info("=== ETL finished")

if __name__ == "__main__":
    main()

