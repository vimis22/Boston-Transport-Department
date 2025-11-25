#!/usr/bin/env bash
set -euo pipefail

BEELINE="${HIVE_HOME:-/opt/hive}/bin/beeline"
JDBC_URL="${HIVE_JDBC_URL:-jdbc:hive2://localhost:10000/default}"
HIVE_USER="${HIVE_JDBC_USER:-hive}"

${BEELINE} -u "${JDBC_URL}" -n "${HIVE_USER}" -f /scripts/smoke-test.sql
