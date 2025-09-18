#!/usr/bin/env python3
# Forward all relevant ports from the cluster to the local machine, and pretty print the output

import subprocess
import time
import argparse
from typing import List, Tuple


def run_port_forward(
    namespace: str, service: str, local_port: int, remote_port: int
) -> subprocess.Popen:
    """Run a kubectl port-forward command and return the process."""
    cmd = [
        "kubectl",
        "port-forward",
        "-n",
        namespace,
        f"svc/{service}",
        f"{local_port}:{remote_port}",
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def print_forwarding_info(forwardings: List[Tuple[str, str, int, int]]):
    """Pretty print the port forwarding information."""
    print("\n" + "=" * 60)
    print("PORT FORWARDING STATUS")
    print("=" * 60)
    print(f"{'Service':<20} {'Local Port':<12} {'Remote Port':<12} {'Status':<10}")
    print("-" * 60)
    for service, _, local, remote in forwardings:
        print(
            f"{service:<20} {local:<12} {remote:<12} {'RUNNING'}"
        )
    print("=" * 60)
    print("\nPress Ctrl+C to stop all forwarding.")


def main():
    parser = argparse.ArgumentParser(description="Forward all relevant ports from the cluster to the local machine")
    parser.add_argument("--namespace", type=str, default="bigdata", help="Kubernetes namespace")
    args = parser.parse_args()

    namespace = args.namespace

    # Define all port forwardings
    forwardings = [
        ("webhdfs", "hdfs-namenode", 9870, 9870),
        ("webhdfs-datanode", "hdfs-datanode", 9864, 9864),
        ("spark-connect", "spark-connect", 15002, 15002),
        ("spark-ui", "spark-connect-server", 4040, 4040),
        ("jupyter", "jupyterlab", 8080, 8080),
        ("hive-metastore", "hive-metastore", 9083, 9083),
        ("kafka-rest-proxy", "kafka-rest-proxy", 8082, 8082),
        ("kafka-ui", "kafka-ui", 8083, 8080),
        ("kafka-connect", "kafka-connect", 8084, 8083),
        ("schema-registry", "schema-registry", 8081, 8081),
        ("hive", "spark-thrift", 10000, 10000),
        ("timemanager", "timemanager", 8000, 8000),
        ("dashboard", "dashboard", 3000, 3000),
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
