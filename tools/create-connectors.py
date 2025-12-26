"""Create Kafka Connectors using the Kafka Connect REST API"""

import sys
import argparse
import requests
import socket
import subprocess
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Connector configurations
CONNECTORS = [
    {
        "name": "hdfs-sink",
        "config": {
            "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
            "tasks.max": "1",
            "topics": "weather-data, bike-data, taxi-data, bike-weather-aggregate, bike-weather-distance",
            "hdfs.url": "hdfs://hdfs-namenode:8020",
            "flush.size": "500",
            "hadoop.conf.dir": "/etc/hadoop/",
            "format.class": "io.confluent.connect.hdfs.parquet.ParquetFormat",
            "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
            "rotate.interval.ms": "120000",
            "hadoop.home": "/opt/hadoop",
            "logs.dir": "/tmp",
            "hive.integration": "true",
            "hive.metastore.uris": "thrift://hive-metastore:9083",
            "hive.database": "default",
            "confluent.license": "",
            "confluent.topic.bootstrap.servers": "kafka-broker:9092",
            "confluent.topic.replication.factor": "1",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "http://schema-registry:8081",
            "schema.compatibility": "BACKWARD"
        }
    }
]

def connector_exists(base_url: str, connector_name: str, session: requests.Session) -> bool:
    """Check if a connector exists."""
    url = f"{base_url}/connectors/{connector_name}"
    try:
        response = session.get(url, timeout=30)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def create_or_update_connector(base_url: str, connector_name: str, config: dict, session: requests.Session):
    """Create or update a Kafka connector."""
    url = f"{base_url}/connectors/{connector_name}/config"
    try:
        response = session.put(
            url,
            json=config,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_msg = f"{e.response.status_code} - {e.response.text}"
        raise RuntimeError(f"Failed to create/update connector '{connector_name}': {error_msg}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Create Kafka Connectors using the Kafka Connect REST API"
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default="bigdata",
        help="Kubernetes namespace to use (default: bigdata)",
    )
    parser.add_argument(
        "--kubeconfig",
        type=str,
        default=None,
        help="Path to kubeconfig file",
    )
    parser.add_argument(
        "--context",
        type=str,
        default=None,
        help="Kubernetes context to use",
    )
    parser.add_argument(
        "--kafka-connect-url",
        type=str,
        default=None,
        help="Kafka Connect base URL (default: auto-detect via kubectl port-forward)",
    )
    
    args = parser.parse_args()
    
    # Setup session with retry strategy
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # If no URL provided, set up kubectl port-forward
    proc = None
    if args.kafka_connect_url is None:
        print(f"Auto-detecting Kafka Connect in namespace '{args.namespace}'...")
        # Find available port
        with socket.socket() as s:
            s.bind(('', 0))
            local_port = s.getsockname()[1]
        
        # Start port-forward
        cmd = ["kubectl"]
        if args.kubeconfig:
            cmd.extend(["--kubeconfig", args.kubeconfig])
        if args.context:
            cmd.extend(["--context", args.context])
        cmd.extend(["-n", args.namespace, "port-forward", "svc/kafka-connect", f"{local_port}:8083"])

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Give it a moment to check if it started successfully
        time.sleep(2)
        if proc.poll() is not None:
            stdout, stderr = proc.communicate()
            print(f"❌ Failed to start kubectl port-forward for Kafka Connect:\n{stderr}\n{stdout}", file=sys.stderr)
            sys.exit(1)

        kafka_connect_url = f"http://localhost:{local_port}"
    else:
        kafka_connect_url = args.kafka_connect_url.rstrip("/")

    try:
        print(f"Connecting to Kafka Connect at {kafka_connect_url}")
        
        # Wait for Kafka Connect to be ready
        max_retries = 10
        for i in range(max_retries):
            try:
                response = session.get(kafka_connect_url, timeout=5)
                if response.status_code == 200:
                    break
            except Exception:
                pass
            print(f"Waiting for Kafka Connect to be ready ({i+1}/{max_retries})...")
            time.sleep(5)
        else:
            raise RuntimeError("Kafka Connect not ready after 50 seconds")

        print(f"Found {len(CONNECTORS)} connectors to ensure\n")
        
        for connector in CONNECTORS:
            name = connector["name"]
            config = connector["config"]
            try:
                print(f"Creating/Updating connector '{name}'...")
                create_or_update_connector(kafka_connect_url, name, config, session)
                print(f"✅ Successfully processed connector '{name}'")
            except Exception as e:
                print(f"❌ Failed to handle connector '{name}': {e}")
                
        print("\n✅ All connectors processed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if proc:
            proc.terminate()
            proc.wait()

if __name__ == "__main__":
    main()

