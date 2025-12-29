import logging
from pyspark.sql import SparkSession
from data_analysis import config

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """
    Create Spark session - supports both local and Spark Connect modes.

    If USE_SPARK_CONNECT=true, connects to remote Spark cluster (Kubernetes).
    Otherwise, creates local SparkSession.

    Returns:
        SparkSession: Configured Spark session
    """
    if config.USE_SPARK_CONNECT:
        logger.info(f"Using Spark Connect: {config.SPARK_CONNECT_URL}")
        return (
            SparkSession.builder.remote(config.SPARK_CONNECT_URL)
            .appName(config.SPARK_APP_NAME)
            .getOrCreate()
        )
    else:
        logger.info("Using local SparkSession")
        return SparkSession.builder.config(conf=config.spark_config).getOrCreate()
