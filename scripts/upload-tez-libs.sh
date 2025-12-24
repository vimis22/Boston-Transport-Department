#!/usr/bin/env bash
set -euo pipefail

# Resolve hdfs binary path even if HADOOP_HOME is not /opt/hadoop (BDE images use /opt/hadoop-3.x)
if [[ -n "${HADOOP_HOME:-}" && -x "${HADOOP_HOME}/bin/hdfs" ]]; then
  HDFS_BIN="${HADOOP_HOME}/bin/hdfs"
else
  HDFS_BIN="$(command -v hdfs || true)"
fi

if [[ -z "${HDFS_BIN}" || ! -x "${HDFS_BIN}" ]]; then
  for candidate in /opt/hadoop /opt/hadoop-* /usr/local/hadoop*; do
    if [[ -x "${candidate}/bin/hdfs" ]]; then
      export HADOOP_HOME="${candidate}"
      HDFS_BIN="${candidate}/bin/hdfs"
      break
    fi
  done
fi

if [[ -z "${HDFS_BIN}" || ! -x "${HDFS_BIN}" ]]; then
  echo "[upload-tez-libs] Unable to find the hdfs command. Set HADOOP_HOME or ensure hdfs is on PATH." >&2
  exit 1
fi

TEZ_HOME="${TEZ_HOME:-/opt/tez}"
TARGET=/apps/tez

if [[ ! -d "${TEZ_HOME}" ]]; then
  echo "[upload-tez-libs] TEZ_HOME (${TEZ_HOME}) does not exist. Mount the Tez distribution into the container." >&2
  exit 1
fi

if ! ls "${TEZ_HOME}"/*.jar >/dev/null 2>&1; then
  echo "[upload-tez-libs] No JAR files found in ${TEZ_HOME}. Place the Tez binary jars there first." >&2
  exit 1
fi

echo "[upload-tez-libs] Uploading Tez libraries from ${TEZ_HOME} to ${TARGET}..."
"${HDFS_BIN}" dfs -mkdir -p "${TARGET}"
"${HDFS_BIN}" dfs -put -f "${TEZ_HOME}"/*.jar "${TARGET}"
echo "[upload-tez-libs] Done."
