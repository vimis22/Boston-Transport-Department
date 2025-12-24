import logging
from pyspark.sql import DataFrame
from ..parent import config

logger = logging.getLogger(__name__)


# Beskriv kort, hvad metoden gør i to sætninger.
def write_parquest_stream(df: DataFrame, subfolder: str, query_name: str = None):
    """
    Write streaming DataFrame to Parquet with partitioning.

    Args:
        df: DataFrame to write
        subfolder: Output subfolder name
        query_name: Optional query name for monitoring (defaults to subfolder)
    """
    output_path = f"{config.OUTPUT_BASE_PATH}/{subfolder}"
    checkpoint_path = f"{config.CHECKPOINT_BASE_PATH}/{subfolder}"

    # Use subfolder as query name if not specified
    if query_name is None:
        query_name = subfolder.replace("/", "_")

    logger.info(f"Writing to {output_path} (query: {query_name})")
    return (
        df.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("year", "month", "date", "hour")
        .outputMode("append")
        .trigger(processingTime=config.BATCH_INTERVAL)
        .queryName(query_name)
        .start()
    )
