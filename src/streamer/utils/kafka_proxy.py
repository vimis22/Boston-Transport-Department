"""Utility class for interacting with Kafka via REST Proxy API"""

from typing import Any, Optional
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class KafkaProxy:
    """Utility class for creating topics and posting data to Kafka via REST Proxy API"""
    
    def __init__(
        self,
        base_url: str = "http://localhost:8082",
        cluster_id: Optional[str] = None,
        timeout: int = 60,
    ):
        """
        Initialize Kafka Proxy client.
        
        Args:
            base_url: Base URL for Kafka REST Proxy (default: http://localhost:8082)
            cluster_id: Optional cluster ID. If not provided, will be auto-discovered.
            timeout: Request timeout in seconds (default: 30)
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._cluster_id = cluster_id
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    @property
    def cluster_id(self) -> str:
        """Get or discover cluster ID"""
        if self._cluster_id is None:
            self._cluster_id = self._discover_cluster_id()
        return self._cluster_id
    
    def _discover_cluster_id(self) -> str:
        """
        Discover the Kafka cluster ID from the REST Proxy.
        
        Returns:
            Cluster ID string
            
        Raises:
            RuntimeError: If cluster discovery fails
        """
        url = f"{self.base_url}/v3/clusters"
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            clusters = data.get("data", [])
            if not clusters:
                raise RuntimeError("No Kafka clusters found")
            
            cluster_id = clusters[0].get("cluster_id")
            if not cluster_id:
                raise RuntimeError("Cluster ID not found in response")
            
            return cluster_id
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to discover cluster ID: {e}") from e
    
    def topic_exists(self, topic_name: str) -> bool:
        """
        Check if a topic exists.
        
        Args:
            topic_name: Name of the topic to check
            
        Returns:
            True if topic exists, False otherwise
        """
        url = f"{self.base_url}/v3/clusters/{self.cluster_id}/topics/{topic_name}"
        try:
            response = self.session.get(url, timeout=self.timeout)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def get_topic_info(self, topic_name: str) -> dict[str, Any]:
        """
        Get information about a topic including partition count and offsets.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Topic information dictionary
        """
        url = f"{self.base_url}/v3/clusters/{self.cluster_id}/topics/{topic_name}"
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to get topic info for '{topic_name}': {e}") from e
    
    def create_topic(
        self,
        topic_name: str,
        partitions: int = 1,
        replication_factor: int = 1,
        configs: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Create a Kafka topic.
        
        Args:
            topic_name: Name of the topic to create
            partitions: Number of partitions (default: 1)
            replication_factor: Replication factor (default: 1)
            configs: Optional topic configuration dictionary
            
        Returns:
            Response data from the API
            
        Raises:
            RuntimeError: If topic creation fails
        """
        url = f"{self.base_url}/v3/clusters/{self.cluster_id}/topics"
        
        payload = {
            "topic_name": topic_name,
            "partitions_count": partitions,
            "replication_factor": replication_factor,
        }
        
        if configs:
            payload["configs"] = [
                {
                    "name": k,
                    "value": str(v)
                }
                for k, v in configs.items()
            ]
        
        try:
            response = self.session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to create topic '{topic_name}': {e}") from e
    
    def ensure_topic(
        self,
        topic_name: str,
        partitions: int = 1,
        replication_factor: int = 1,
        configs: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Ensure a topic exists, creating it if it doesn't.
        
        Args:
            topic_name: Name of the topic
            partitions: Number of partitions (default: 1)
            replication_factor: Replication factor (default: 1)
            configs: Optional topic configuration dictionary
            
        Returns:
            True if topic was created, False if it already existed
        """
        if self.topic_exists(topic_name):
            return False
        
        self.create_topic(topic_name, partitions, replication_factor, configs)
        return True

    def post_records_avro(
        self,
        topic_name: str,
        records: list[dict[str, Any]],
        schema: Optional[dict[str, Any]] = None,
        schema_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Post multiple records to a Kafka topic as Avro using inline schema or schema ID.
        
        Args:
            topic_name: Name of the topic
            records: List of record dictionaries matching the schema
            schema: Optional Avro schema dictionary (used if schema_id not provided)
            schema_id: Optional Schema ID from Schema Registry (preferred)
            
        Returns:
            List of response data from the API for each record
            
        Raises:
            RuntimeError: If posting fails
            ValueError: If neither schema nor schema_id is provided
        """
        from .avro_serializer import filter_record_to_schema
        
        if not records:
            return []
        
        if schema_id is None and schema is None:
            raise ValueError("Either schema or schema_id must be provided")
        
        url = f"{self.base_url}/topics/{topic_name}"
        
        # Use provided schema for filtering if available, otherwise we can't filter
        # If only schema_id is provided, we assume records are already correct
        records_array = []
        for record in records:
            if schema:
                filtered_record = filter_record_to_schema(record, schema)
            else:
                filtered_record = record
            records_array.append({"value": filtered_record})
        
        payload: dict[str, Any] = {
            "records": records_array
        }
        
        if schema_id is not None:
            payload["value_schema_id"] = schema_id
        else:
            payload["value_schema"] = json.dumps(schema)
        
        content_type = "application/vnd.kafka.avro.v2+json"
        
        try:
            response = self.session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": content_type,
                    "Accept": "application/vnd.kafka.v2+json"
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            result = response.json()
            import logging
            logger = logging.getLogger(__name__)
            
            # v2 API response format: {"offsets": [{"partition": N, "offset": M, ...}], ...}
            if 'offsets' in result and len(result.get('offsets', [])) > 0:
                offsets = result['offsets']
                
                # Check for errors
                for i, offset_info in enumerate(offsets):
                    if offset_info.get('error_code'):
                        error_msg = offset_info.get('error', 'Unknown error')
                        logger.error(f"Kafka API error for record {i} in {topic_name}: {error_msg}")
                
                # Convert to list of results (one per record)
                results = []
                for offset_info in offsets:
                    results.append({
                        "partition_id": offset_info.get('partition'),
                        "offset": offset_info.get('offset'),
                        "value_schema_id": result.get('value_schema_id')
                    })
                
                logger.info(
                    f"Published {len(records)} records to {topic_name}"
                )
                return results
            else:
                error_msg = "No offsets in response"
                logger.error(f"Kafka API error for {topic_name}: {error_msg}")
                raise RuntimeError(f"Kafka API returned error: {error_msg}")
        except requests.exceptions.RequestException as e:
            import logging
            logger = logging.getLogger(__name__)
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Kafka API error response: {e.response.status_code} - {e.response.text}")
            raise RuntimeError(f"Failed to post Avro records to topic '{topic_name}': {e}") from e

