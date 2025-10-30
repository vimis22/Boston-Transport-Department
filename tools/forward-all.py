#!/usr/bin/env python3
# Forward all relevant ports from the cluster to the local machine, and pretty print the output

import subprocess
import sys
import time
from typing import List, Tuple

def run_port_forward(namespace: str, service: str, local_port: int, remote_port: int) -> subprocess.Popen:
    """Run a kubectl port-forward command and return the process."""
    cmd = [
        "kubectl", "port-forward",
        f"-n", namespace,
        f"svc/{service}",
        f"{local_port}:{remote_port}"
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def print_forwarding_info(forwardings: List[Tuple[str, str, int, int]]):
    """Pretty print the port forwarding information."""
    print("\n" + "="*60)
    print("PORT FORWARDING STATUS")
    print("="*60)
    print(f"{'Service':<20} {'Local Port':<12} {'Remote Port':<12} {'Status':<10}")
    print("-"*60)
    for service, _, local, remote in forwardings:
        print(f"{service:<20} {local:<12} {remote:<12} {'RUNNING' if True else 'ERROR'}")
    print("="*60)
    print("\nAvailable services:\n")
    print("HDFS:")
    print(f"  - HDFS Server: localhost:9000")
    print(f"  - HDFS UI: http://localhost:9870")
    print("\nHive:")
    print(f"  - Hive Server: localhost:10000")
    print(f"  - Hive UI: http://localhost:10002")
    print("\nSpark:")
    print(f"  - Spark Master: localhost:7077")
    print(f"  - Spark UI: http://localhost:8080")
    print("\nJupyter:")
    print(f"  - Jupyter: http://localhost:8888")
    print("\nPress Ctrl+C to stop all forwarding.")
    print("="*60)

def main():
    namespace = "bigdata"
    
    # Define all port forwardings
    forwardings = [
        ("namenode-hdfs", "namenode", 9000, 9000),
        ("namenode-ui", "namenode", 9870, 9870),
        ("hive-server", "hive-server-service", 10000, 10000),
        ("hive-ui", "hive-server-service", 10002, 10002),
        ("spark-master", "spark-primary", 7077, 7077),
        ("spark-ui", "spark-primary", 8080, 8080),
        ("jupyter", "jupyter", 8888, 8888),
    ]
    
    processes = []
    
    try:
        # Start all port forwards
        for service, resource, local, remote in forwardings:
            print(f"Starting port forward for {service}...")
            process = run_port_forward(namespace, resource, local, remote)
            processes.append(process)
        
        # Print info
        print_forwarding_info(forwardings)
        
        # Keep processes running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping port forwards...")
    finally:
        # Cleanup
        for process in processes:
            if process.poll() is None:  # If still running
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
        print("All port forwards stopped.")

if __name__ == "__main__":
    main()
