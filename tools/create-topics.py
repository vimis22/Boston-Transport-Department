"""Create Kafka topics using the Kafka REST Proxy"""

import sys
import argparse
import requests
import socket
import subprocess
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Topics previously managed by Confluent Operator CRDs
TOPICS = [
    "weather-data",
    "taxi-data",
    "bike-data",
    "bike-weather-aggregate",
    "bike-weather-distance",
    "accident-data",
]

def get_cluster_id(base_url: str, session: requests.Session) -> str:
    """
    Discover the Kafka cluster ID from the REST Proxy.
    
    Args:
        base_url: Kafka REST Proxy base URL
        session: Requests session
        
    Returns:
        Cluster ID string
    """
    url = f"{base_url}/v3/clusters"
    try:
        response = session.get(url, timeout=30)
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
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_msg = f"{e.response.status_code} - {e.response.text}"
        raise RuntimeError(f"Failed to discover cluster ID: {error_msg}")

def topic_exists(base_url: str, cluster_id: str, topic_name: str, session: requests.Session) -> bool:
    """
    Check if a topic exists.
    
    Args:
        base_url: Kafka REST Proxy base URL
        cluster_id: Kafka cluster ID
        topic_name: Name of the topic
        session: Requests session
        
    Returns:
        True if topic exists, False otherwise
    """
    url = f"{base_url}/v3/clusters/{cluster_id}/topics/{topic_name}"
    try:
        response = session.get(url, timeout=30)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def create_topic(
    base_url: str, 
    cluster_id: str, 
    topic_name: str, 
    session: requests.Session,
    partitions: int = 1,
    replication_factor: int = 1
):
    """
    Create a Kafka topic.
    
    Args:
        base_url: Kafka REST Proxy base URL
        cluster_id: Kafka cluster ID
        topic_name: Name of the topic to create
        session: Requests session
        partitions: Number of partitions (default: 1)
        replication_factor: Replication factor (default: 1)
    """
    url = f"{base_url}/v3/clusters/{cluster_id}/topics"
    payload = {
        "topic_name": topic_name,
        "partitions_count": partitions,
        "replication_factor": replication_factor,
    }
    
    try:
        response = session.post(
            url, 
            json=payload, 
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_msg = f"{e.response.status_code} - {e.response.text}"
        raise RuntimeError(f"Failed to create topic '{topic_name}': {error_msg}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Create Kafka topics using the Kafka REST Proxy"
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
        "--kafka-rest-url",
        type=str,
        default=None,
        help="Kafka REST Proxy base URL (default: auto-detect via kubectl port-forward)",
    )
    
    args = parser.parse_args()
    
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
    
    # If no URL provided, set up kubectl port-forward
    proc = None
    if args.kafka_rest_url is None:
        print(f"Auto-detecting Kafka REST Proxy in namespace '{args.namespace}'...")
        # Find available port
        with socket.socket() as s:
            s.bind(('', 0))
            local_port = s.getsockname()[1]
        
        # Start port-forward
        # Service name is 'kafka-rest-proxy' based on kubectl get svc output
        cmd = ["kubectl"]
        if args.kubeconfig:
            cmd.extend(["--kubeconfig", args.kubeconfig])
        if args.context:
            cmd.extend(["--context", args.context])
        cmd.extend(["-n", args.namespace, "port-forward", "svc/kafka-rest-proxy", f"{local_port}:8082"])
        
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for the port to be ready
        print(f"Waiting for port {local_port} to be ready...")
        timeout = 10
        start_time = time.time()
        while time.time() - start_time < timeout:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(('localhost', local_port)) == 0:
                    break
            if proc.poll() is not None:
                _, stderr = proc.communicate()
                print(f"❌ Failed to start kubectl port-forward:\n{stderr}", file=sys.stderr)
                sys.exit(1)
            time.sleep(0.5)
        else:
            print(f"❌ Timeout waiting for port {local_port}", file=sys.stderr)
            proc.terminate()
            sys.exit(1)

        kafka_rest_url = f"http://localhost:{local_port}"
    else:
        kafka_rest_url = args.kafka_rest_url.rstrip("/")

    try:
        print(f"Connecting to Kafka REST Proxy at {kafka_rest_url}")
        cluster_id = get_cluster_id(kafka_rest_url, session)
        print(f"Connected to Kafka cluster: {cluster_id}")
        print(f"Found {len(TOPICS)} topics to ensure\n")
        
        for topic in TOPICS:
            try:
                if topic_exists(kafka_rest_url, cluster_id, topic, session):
                    print(f"ℹ️  Topic '{topic}' already exists, skipping...")
                else:
                    print(f"Creating topic '{topic}'...")
                    create_topic(kafka_rest_url, cluster_id, topic, session)
                    print(f"✅ Successfully created topic '{topic}'")
            except Exception as e:
                print(f"❌ Failed to handle topic '{topic}': {e}")
                
        print("\n✅ All topics processed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if proc:
            proc.terminate()
            proc.wait()

if __name__ == "__main__":
    main()

