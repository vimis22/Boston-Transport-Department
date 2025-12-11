# Hive MVP Runbook

This document explains how to operate the Hive-only stack that underpins the Boston Transport Department architecture. It covers first-time bootstrap, routine operations, health checks, and smoke tests. Spark, Kafka, and downstream tooling intentionally sit outside this scope.

## Components
- **HDFS**: single NameNode/DataNode pair (Hadoop 3.3.0 images) hosting `/warehouse` and scratch paths.
- **Hive Metastore DB**: PostgreSQL 13 storing metadata.
- **Hive Metastore Service**: exposes `thrift://localhost:9083`.
- **HiveServer2**: JDBC/ODBC endpoint on `localhost:10000`.
- **Tez**: execution engine; jars uploaded to `hdfs:///apps/tez`.
- **Utility jobs**: `hdfs-bootstrap`, `metastore-init`, `tez-installer`, and smoke-test scripts.

## Directory Layout
```
conf/                # core-site.xml, hdfs-site.xml, hive-site.xml, tez-site.xml
scripts/             # bootstrap + init scripts and smoke-test tooling
tez/                 # drop the Tez binary distribution here before uploading to HDFS
docker-compose.yml   # service definitions
RUNBOOK.md           # this file
```

## Prerequisites
1. Docker Desktop (or compatible Compose v2 runtime).
2. ~6 GB RAM and 10 GB disk available.
3. Download Apache Tez 0.10.x and extract its `*.jar` files into `tez/`. The upload script assumes the jars live directly in that directory.
4. Download the PostgreSQL JDBC driver (e.g., `postgresql-42.7.3.jar`) into `lib/postgresql-jdbc.jar` as described in `lib/README.md`.

## First-Time Bootstrap
1. Pull containers (optional but recommended):
   ```
   docker compose pull
   ```
2. Start HDFS and PostgreSQL:
   ```
   docker compose up -d namenode datanode metastore-db
   ```
   - Confirm NameNode UI at http://localhost:9870.
3. Initialize warehouse paths and permissions:
   ```
   docker compose run --rm hdfs-bootstrap
   ```
4. Upload Tez libraries to HDFS:
   ```
   docker compose run --rm tez-installer
   ```
   (Ensure `tez/` contains the jars before running.)
5. Initialize the Hive Metastore schema (idempotent):
   ```
   docker compose run --rm metastore-init
   ```
6. Start the long-running Hive services:
   ```
   docker compose up -d hive-metastore hive-server2
   ```
7. Create the logical databases once. Either run Beeline interactively (commands below) or execute the helper script:
   ```
   docker compose exec hive-server2 /opt/hive/bin/beeline -u jdbc:hive2://hive-server2:10000/default -n hive -f /scripts/create-mvp-dbs.sql
   ```
   The script creates `raw`, `enriched`, `model`, `errors` and finishes with `SHOW DATABASES`.

## Daily Operations
- **Start everything**: `docker compose up -d`.
- **Stop everything**: `docker compose down`.
- **Check logs**: `docker compose logs -f hive-server2` (or service name of choice).
- **Restart a single component**: `docker compose restart hive-server2`.

## Smoke Test
1. Ensure HiveServer2 is running.
2. Execute (connects via `hive-server2` DNS so IPv4/IPv6 bindings are tested):
  ```
  docker compose exec hive-server2 /scripts/smoke-test.sh
  ```
   (The script uses `/scripts/smoke-test.sql` to create/drop a temporary database and verify Inserts/Selects.)

## Backups & Maintenance
- **Metastore DB**: take a nightly dump from the host.
  ```
  docker compose exec metastore-db pg_dump -U hive -Fc hive > backups/hive_$(date +%F).dump
  ```
- **HDFS snapshots** (once raw data exists):
  ```
  docker compose exec namenode hdfs dfsadmin -allowSnapshot /warehouse
  docker compose exec namenode hdfs dfs -createSnapshot /warehouse daily-$(date +%F)
  ```
- **Statistics & housekeeping**: defer until tables exist (ANALYZE TABLE, MSCK REPAIR, compaction jobs).

## Troubleshooting Tips
- If NameNode is not reachable, verify ports 8020/9870 are unused and restart the service.
- `metastore-init` can be re-run safely; it aborts if the schema already exists.
- Tez upload failures usually mean the `tez/` directory is empty or contains nested folders. Place the jar files directly in `tez/`.
- For Beeline access (inside or outside Docker), point JDBC to `jdbc:hive2://localhost:10000/default` and user `hive` (no password, because `hive.server2.enable.doAs=false`). The container also exposes the endpoint to the other services via the Compose network hostname `hive-server2`.

## Next Steps (outside this MVP)
- Wire Kafka Connect to HDFS landing zones under `/warehouse/raw`.
- Define raw/enriched/model/error tables in Hive.
- Grant fine-grained access through Ranger/Sentry or table-level privileges.
- Integrate Spark, notebooks, and dashboards with the shared Metastore.
