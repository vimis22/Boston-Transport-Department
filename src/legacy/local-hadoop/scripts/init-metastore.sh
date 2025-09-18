#!/usr/bin/env bash
set -euo pipefail

SCHEMATOOL="${HIVE_HOME:-/opt/hive}/bin/schematool"
DB_TYPE="${HIVE_DB_TYPE:-postgres}"
JDBC_URL="${HIVE_JDBC_URL:-jdbc:postgresql://metastore-db:5432/hive}"
DB_USER="${HIVE_DB_USER:-hive}"
DB_PASS="${HIVE_DB_PASS:-hivepassword}"

echo "[init-metastore] Checking for existing Hive schema..."
if ${SCHEMATOOL} -dbType "${DB_TYPE}" -info \
  -userName "${DB_USER}" -passWord "${DB_PASS}" -url "${JDBC_URL}" >/dev/null 2>&1; then
  echo "[init-metastore] Schema already initialized."
  exit 0
fi

echo "[init-metastore] Initializing Hive schema for ${DB_TYPE}..."
${SCHEMATOOL} -dbType "${DB_TYPE}" -initSchema --verbose \
  -userName "${DB_USER}" -passWord "${DB_PASS}" -url "${JDBC_URL}"

echo "[init-metastore] Done."
