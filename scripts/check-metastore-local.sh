#!/usr/bin/env bash
set -euo pipefail

# Host-side helper to run basic metastore health checks using Docker Compose.
# Run from the repository root as: ./scripts/check-metastore-local.sh

COMPOSE="docker compose"

echo "[check-metastore-local] Schematool info (inside hive-metastore):"
${COMPOSE} exec hive-metastore /opt/hive/bin/schematool -dbType postgres -info -userName hive -passWord hivepassword -url jdbc:postgresql://metastore-db:5432/hive || true

echo "\n[check-metastore-local] Postgres tables in hive DB:"
${COMPOSE} exec metastore-db psql -U hive -d hive -c "\dt" || true

echo "\n[check-metastore-local] VERSION table (if present):"
${COMPOSE} exec metastore-db psql -U hive -d hive -c "select * from version;" || true

echo "\n[check-metastore-local] If you need to init the schema, run: docker compose run --rm metastore-init"
