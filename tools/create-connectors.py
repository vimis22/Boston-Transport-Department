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
    
    # If URL provided, use it. Otherwise, port-forward will be set up in the retry loop.
    kafka_connect_url = args.kafka_connect_url.rstrip("/") if args.kafka_connect_url else None
    proc = None

    try:
        # Wait for Kafka Connect to be ready
        max_retries = 30 # Increased from 10 as Kafka Connect can be slow to start with plugins
        for i in range(max_retries):
            # If no URL provided, set up a new kubectl port-forward for each retry
            if args.kafka_connect_url is None:
                # Clean up previous port-forward if any
                if proc:
                    proc.terminate()
                    try:
                        proc.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                
                print(f"Auto-detecting Kafka Connect in namespace '{args.namespace}' (attempt {i+1}/{max_retries})...")
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
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Wait for the port to be ready locally
                timeout = 10
                start_time = time.time()
                port_ready = False
                while time.time() - start_time < timeout:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        if s.connect_ex(('localhost', local_port)) == 0:
                            port_ready = True
                            break
                    if proc.poll() is not None:
                        break
                    time.sleep(0.5)
                
                if not port_ready:
                    print(f"⚠️  Port-forward failed to establish on attempt {i+1}")
                    continue

                kafka_connect_url = f"http://localhost:{local_port}"

            try:
                print(f"Connecting to Kafka Connect at {kafka_connect_url}...")
                sys.stdout.flush()
                response = session.get(kafka_connect_url, timeout=5)
                if response.status_code == 200:
                    print("✅ Kafka Connect is ready!")
                    break
            except Exception:
                pass
            
            print(f"Waiting for Kafka Connect to be ready ({i+1}/{max_retries})...")
            sys.stdout.flush()
            time.sleep(5)
        else:
            raise RuntimeError("Kafka Connect not ready after multiple retries")

        print(f"Found {len(CONNECTORS)} connectors to ensure\n")
        sys.stdout.flush()
        
        for connector in CONNECTORS:
            name = connector["name"]
            config = connector["config"]
            try:
                print(f"Creating/Updating connector '{name}'...")
                sys.stdout.flush()
                create_or_update_connector(kafka_connect_url, name, config, session)
                print(f"✅ Successfully processed connector '{name}'")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ Failed to handle connector '{name}': {e}")
                sys.stdout.flush()
                
        print("\n✅ All connectors processed successfully!")
        sys.stdout.flush()
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if proc:
            print("Cleaning up port-forward process...")
            sys.stdout.flush()
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("Port-forward process did not terminate, killing it...")
                sys.stdout.flush()
                proc.kill()
                proc.wait()

if __name__ == "__main__":
    main()

