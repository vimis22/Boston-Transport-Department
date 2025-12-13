#!/usr/bin/env bash
set -euo pipefail

HDFS_BIN="${HADOOP_HOME:-/opt/hadoop}/bin/hdfs"

function wait_for_hdfs() {
  local retries=30
  local count=0
  echo "[bootstrap-hdfs] Waiting for HDFS..."
  until ${HDFS_BIN} dfsadmin -report >/dev/null 2>&1; do
    count=$((count + 1))
    if [[ ${count} -ge ${retries} ]]; then
      echo "[bootstrap-hdfs] HDFS is not ready after ${retries} attempts." >&2
      exit 1
    fi
    sleep 5
  done
  echo "[bootstrap-hdfs] HDFS is reachable."
}

function ensure_dir() {
  local path="$1"
  if ! ${HDFS_BIN} dfs -test -d "${path}"; then
    ${HDFS_BIN} dfs -mkdir -p "${path}"
  fi
}

wait_for_hdfs

ensure_dir /warehouse
ensure_dir /warehouse/raw
ensure_dir /warehouse/enriched
ensure_dir /warehouse/model
ensure_dir /warehouse/errors
ensure_dir /tmp/hive
ensure_dir /tmp/hive-local

${HDFS_BIN} dfs -chmod -R 1777 /tmp/hive /tmp/hive-local
# Set owner to 'hive' and group to 'supergroup' to avoid group mapping issues across containers
${HDFS_BIN} dfs -chown -R hive:supergroup /warehouse
${HDFS_BIN} dfs -chmod -R 775 /warehouse

echo "[bootstrap-hdfs] Warehouse directories created and permissions set."
