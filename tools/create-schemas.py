#!/usr/bin/env python3
"""Upload Avro schemas to the Schema Registry"""

import sys
import json
import argparse
import requests
import socket
import subprocess
import time
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def load_schema(schema_path: str) -> dict:
    """
    Load Avro schema from file.
    
    Args:
        schema_path: Path to Avro schema file (.avsc)
        
    Returns:
        Schema dictionary
    """
    with open(schema_path, "r") as f:
        return json.load(f)


def get_schema_topic_mapping() -> dict[str, str]:
    """
    Map schema file names to Kafka topic names.
    
    Returns:
        Dictionary mapping schema file paths to topic names
    """
    schemas_dir = Path(__file__).parent.parent / "src" / "schemas"
    
    return {
        str(schemas_dir / "weather_data_raw.avsc"): "weather-data",
        str(schemas_dir / "taxi_data_raw.avsc"): "taxi-data",
        str(schemas_dir / "bike_data_raw.avsc"): "bike-data",
        str(schemas_dir / "bike_weather_aggregate.avsc"): "bike-weather-aggregate",
        str(schemas_dir / "bike_weather_distance.avsc"): "bike-weather-distance",
        str(schemas_dir / "accident_data_raw.avsc"): "accident-data",
        # Mulighed 2 - Descriptive Statistics schemas
        str(schemas_dir / "weather_transport_statistics_overall.avsc"): "weather-transport-statistics-overall",
        str(schemas_dir / "weather_transport_statistics_by_condition.avsc"): "weather-transport-statistics-by-condition",
        str(schemas_dir / "weather_transport_statistics_by_temperature.avsc"): "weather-transport-statistics-by-temperature",
        str(schemas_dir / "weather_transport_statistics_by_precipitation.avsc"): "weather-transport-statistics-by-precipitation",
    }


def schema_exists(session: requests.Session, base_url: str, subject: str, timeout: int = 30) -> bool:
    """
    Check if a schema exists for a subject.
    
    Args:
        session: Requests session
        base_url: Schema Registry base URL
        subject: Subject name
        timeout: Request timeout in seconds
        
    Returns:
        True if schema exists, False otherwise
    """
    url = f"{base_url}/subjects/{subject}/versions/latest"
    try:
        response = session.get(url, timeout=timeout)
        if response.status_code == 200:
            return True
        return False
    except requests.exceptions.RequestException:
        return False


def register_schema(
    session: requests.Session,
    base_url: str,
    subject: str,
    schema: dict,
    schema_type: str = "AVRO",
    timeout: int = 30,
) -> int:
    """
    Register a schema with the Schema Registry.
    
    Args:
        session: Requests session
        base_url: Schema Registry base URL
        subject: Subject name
        schema: Avro schema dictionary
        schema_type: Schema type (default: "AVRO")
        timeout: Request timeout in seconds
        
    Returns:
        Schema ID assigned by the registry
        
    Raises:
        RuntimeError: If registration fails
    """
    url = f"{base_url}/subjects/{subject}/versions"
    
    payload = {
        "schema": json.dumps(schema),
        "schemaType": schema_type,
    }
    
    try:
        response = session.post(
            url,
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=timeout,
        )
        response.raise_for_status()
        result = response.json()
        schema_id = result.get("id")
        if schema_id is None:
            raise RuntimeError("Schema ID not found in response")
        return schema_id
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_msg = f"{e.response.status_code} - {e.response.text}"
        raise RuntimeError(f"Failed to register schema for subject '{subject}': {error_msg}") from e


def delete_subject(
    session: requests.Session,
    base_url: str,
    subject: str,
    timeout: int = 30,
) -> None:
    """
    Delete a subject and all its versions from Schema Registry.
    
    Args:
        session: Requests session
        base_url: Schema Registry base URL
        subject: Subject name to delete
        timeout: Request timeout in seconds
        
    Raises:
        RuntimeError: If deletion fails
    """
    url = f"{base_url}/subjects/{subject}"
    try:
        response = session.delete(url, timeout=timeout)
        # 404 is OK - subject doesn't exist
        if response.status_code == 404:
            return
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            if e.response.status_code == 404:
                return  # Subject doesn't exist, that's fine
            error_msg = f"{e.response.status_code} - {e.response.text}"
        raise RuntimeError(f"Failed to delete subject '{subject}': {error_msg}") from e


def upload_schemas(
    schema_registry_url: str = "http://localhost:8081",
    recreate: bool = False,
) -> None:
    """
    Upload all Avro schemas to the Schema Registry.
    
    Args:
        schema_registry_url: Base URL for Schema Registry
        recreate: If True, delete existing schemas before registering new ones
    """
    base_url = schema_registry_url.rstrip("/")
    timeout = 30
    
    # Setup session with retry strategy
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    schema_mapping = get_schema_topic_mapping()
    
    print(f"Connecting to Schema Registry at {base_url}")
    print(f"Found {len(schema_mapping)} schemas to upload\n")
    
    for schema_path, topic_name in schema_mapping.items():
        schema_file = Path(schema_path)
        
        if not schema_file.exists():
            print(f"⚠️  Warning: Schema file not found: {schema_path}")
            continue
        
        subject_name = f"{topic_name}-value"
        
        try:
            # Load schema from file
            print(f"Loading schema from {schema_file.name}...")
            schema = load_schema(str(schema_file))
            
            # Register schema
            if recreate:
                print(f"  Recreating schema for subject '{subject_name}'...")
                delete_subject(session, base_url, subject_name, timeout=timeout)
                schema_id = register_schema(session, base_url, subject_name, schema, timeout=timeout)
            else:
                # Check if schema already exists
                if schema_exists(session, base_url, subject_name, timeout=timeout):
                    print(f"  Schema already exists for subject '{subject_name}', skipping...")
                    continue
                
                print(f"  Registering schema for subject '{subject_name}'...")
                schema_id = register_schema(session, base_url, subject_name, schema, timeout=timeout)
            
            print(f"  ✅ Successfully registered schema (ID: {schema_id})")
            print(f"     Subject: {subject_name}")
            print(f"     Topic: {topic_name}\n")
            
        except Exception as e:
            print(f"  ❌ Failed to register schema for '{subject_name}': {e}\n")
            raise
    
    print("✅ All schemas uploaded successfully!")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Upload Avro schemas to Schema Registry"
    )
    parser.add_argument(
        "--schema-registry-url",
        type=str,
        default=None,
        help="Schema Registry base URL (default: auto-detect via kubectl port-forward)",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Delete existing schemas before registering new ones",
    )
    
    args = parser.parse_args()
    
    # If no URL provided, set up kubectl port-forward
    if args.schema_registry_url is None:
        # Find available port
        with socket.socket() as s:
            s.bind(('', 0))
            local_port = s.getsockname()[1]
        
        # Start port-forward
        proc = subprocess.Popen(
            ["kubectl", "-n", "bigdata", "port-forward", "svc/schema-registry", f"{local_port}:8081"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(2)
        schema_registry_url = f"http://localhost:{local_port}"
        
        try:
            upload_schemas(schema_registry_url=schema_registry_url, recreate=args.recreate)
        except Exception as e:
            print(f"\n❌ Error: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            proc.terminate()
            proc.wait()
    else:
        try:
            upload_schemas(
                schema_registry_url=args.schema_registry_url,
                recreate=args.recreate,
            )
        except Exception as e:
            print(f"\n❌ Error: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
