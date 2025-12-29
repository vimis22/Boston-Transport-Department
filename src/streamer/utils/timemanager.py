"""Utility class for interacting with Time Manager API"""

from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class TimeManager:
    """Utility class for reading time and detecting run_id changes from Time Manager API"""
    
    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout: int = 30,
    ):
        """
        Initialize Time Manager client.
        
        Args:
            base_url: Base URL for Time Manager API (default: http://localhost:8000)
            timeout: Request timeout in seconds (default: 30)
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._last_run_id: str | None = None
        
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
    
    def get_current_time_and_check_run_id(self) -> tuple[datetime, bool]:
        """
        Get current simulation time and check if run_id changed atomically.
        This prevents race conditions where run_id changes between separate calls.
        
        Returns:
            Tuple of (current_time, run_id_changed)
            - current_time: Current simulation time as datetime
            - run_id_changed: True if run_id changed since last call, False otherwise
            
        Raises:
            RuntimeError: If request fails
        """
        url = f"{self.base_url}/api/v1/clock/state"
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            current_run_id = data["run_id"]
            
            # Check if run_id changed BEFORE updating _last_run_id
            run_id_changed = False
            if self._last_run_id is not None and current_run_id != self._last_run_id:
                run_id_changed = True
            
            # Update last run_id
            self._last_run_id = current_run_id
            
            # Parse and return current time
            time_str = data["current_time"]
            current_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            
            return (current_time, run_id_changed)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to get current time: {e}") from e
    
    def get_current_time(self) -> datetime:
        """
        Get current simulation time.
        Note: For detecting run_id changes, use get_current_time_and_check_run_id() instead.
        
        Returns:
            Current simulation time as datetime
            
        Raises:
            RuntimeError: If request fails
        """
        current_time, _ = self.get_current_time_and_check_run_id()
        return current_time
    
    def has_run_id_changed(self) -> bool:
        """
        Check if the run_id has changed since last check.
        Note: This makes a separate API call. For atomic checking, use get_current_time_and_check_run_id() instead.
        
        Returns:
            True if run_id changed, False otherwise
            
        Raises:
            RuntimeError: If request fails
        """
        _, run_id_changed = self.get_current_time_and_check_run_id()
        return run_id_changed
