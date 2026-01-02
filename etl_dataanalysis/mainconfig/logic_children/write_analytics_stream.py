import logging
from pyspark.sql import DataFrame
from ..parent import config

logger = logging.getLogger(__name__)


# Beskriv kort, hvad metoden gør i to sætninger.
def write_analytics_stream(df: DataFrame, subfolder: str, output_mode: str = "append", query_name: str = None):
    """
    Write analytics results to a separate analytics folder.

    Args:
        df: DataFrame to write
        subfolder: Subfolder name under analytics path
        output_mode: Spark output mode ("append", "update", or "complete")
        query_name: Optional query name for monitoring (defaults to subfolder)
    """
    output_path = f"{config.ANALYTICS_OUTPUT_PATH}/{subfolder}"
    checkpoint_path = f"{config.ANALYTICS_CHECKPOINT_PATH}/{subfolder}"

    # Use subfolder as query name if not specified
    if query_name is None:
        query_name = f"analytics_{subfolder}".replace("/", "_")

    logger.info(f"Writing analytics to {output_path} (mode: {output_mode}, query: {query_name})")
    return (
        df.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode(output_mode)
        .trigger(processingTime=config.BATCH_INTERVAL)
        .queryName(query_name)
        .start()
    )
