import logging
import requests
from typing import Tuple
from ..parent import config

logger = logging.getLogger(__name__)


def get_latest_schema(subject: str) -> Tuple[str, int]:
    """
    Fetch the latest Avro schema from Schema Registry.

    Args:
        subject: Schema subject name (e.g., "bike-trips-value")

    Returns:
        Tuple of (schema_json_string, schema_id)
    """
    url = f"{config.SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    logger.info(f"Fetching schema from: {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    return data["schema"], data["id"]
