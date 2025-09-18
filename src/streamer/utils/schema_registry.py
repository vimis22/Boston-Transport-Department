"""Utility class for interacting with Confluent Schema Registry"""

from typing import Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SchemaRegistry:
    """Utility class for registering and managing Avro schemas with Confluent Schema Registry"""
    
    def __init__(
        self,
        base_url: str = "http://localhost:8081",
        timeout: int = 30,
    ):
        """
        Initialize Schema Registry client.
        
        Args:
            base_url: Base URL for Schema Registry (default: http://localhost:8081)
            timeout: Request timeout in seconds (default: 30)
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        
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
    
    def get_schema(self, subject: str, version: Optional[int] = None) -> dict[str, Any]:
        """
        Get a schema from the Schema Registry.
        
        Args:
            subject: Subject name
            version: Schema version (default: latest)
            
        Returns:
            Schema info dictionary with 'id', 'schema', 'subject', 'version'
        """
        if version is None:
            url = f"{self.base_url}/subjects/{subject}/versions/latest"
        else:
            url = f"{self.base_url}/subjects/{subject}/versions/{version}"
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            if hasattr(e, 'response') and e.response is not None:
                error_msg = f"{e.response.status_code} - {e.response.text}"
            raise RuntimeError(f"Failed to get schema for subject '{subject}': {error_msg}") from e
    
    def schema_exists(self, subject: str) -> bool:
        """
        Check if a schema exists for a subject.
        
        Args:
            subject: Subject name
            
        Returns:
            True if schema exists, False otherwise
        """
        try:
            self.get_schema(subject)
            return True
        except RuntimeError:
            return False
