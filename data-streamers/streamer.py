import os
import time
import requests
import json
from datetime import datetime, timedelta
import duckdb
from pathlib import Path
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import re

# ==========================================================
# CONFIGURATION
# ==========================================================
TIME_MANAGER_URL = os.getenv("TIME_MANAGER_URL", "http://time-manager:5000")
SYNC_INTERVAL = float(os.getenv("SYNC_INTERVAL", 1))
OUTPUT_INTERVAL = float(os.getenv("OUTPUT_INTERVAL", 1))
DATA_FOLDER = os.getenv("DATA_FOLDER", "./data")
TIME_COLUMN = os.getenv("TIME_COLUMN", "dispatch_ts")
TIME_FORMAT = os.getenv("TIME_FORMAT", "%Y-%m-%d %H:%M:%S")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "default_topic")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

print("========== STREAMER CONFIG ==========")
print("TIME_MANAGER_URL:", TIME_MANAGER_URL)
print("SYNC_INTERVAL:", SYNC_INTERVAL)
print("OUTPUT_INTERVAL:", OUTPUT_INTERVAL)
print("DATA_FOLDER:", DATA_FOLDER)
print("TIME_COLUMN:", TIME_COLUMN)
print("TIME_FORMAT:", TIME_FORMAT)
print("KAFKA_BROKER:", KAFKA_BROKER)
print("KAFKA_TOPIC:", KAFKA_TOPIC)
print("SCHEMA_REGISTRY_URL:", SCHEMA_REGISTRY_URL)
print("====================================")

# ==========================================================
# TIME MANAGER SYNC
# ==========================================================
class TimeManagerSync:
    def __init__(self, url, poll_interval=SYNC_INTERVAL):
        self.url = url
        self.poll_interval = poll_interval
        self.last_tm_time = None
        self.local_time = None
        self.last_real = None
        self.tm_speed = 1.0
        self.state = "stopped"

    def fetch_status(self):
        try:
            r = requests.get(f"{self.url}/api/v1/clock/status", timeout=3)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            print("Error fetching clock status:", e, flush=True)
            return None

    def parse_tm_time(self, tm_time_str):
        try:
            return datetime.fromisoformat(tm_time_str.replace('Z', '+00:00'))
        except Exception as e:
            print(f"Error parsing time: {tm_time_str} -> {e}", flush=True)
            return None

    def update(self):
        status = self.fetch_status()
        if not status:
            time.sleep(self.poll_interval)
            return None
        self.state = status.get("state")
        tm_time_str = status.get("current_time")
        self.tm_speed = status.get("speed", 1.0)

        if self.state != "running":
            print(f"Simulation state: {self.state}. Streamer paused...", flush=True)
            self.last_tm_time = None
            self.local_time = None
            self.last_real = None
            time.sleep(self.poll_interval)
            return None

        tm_time = self.parse_tm_time(tm_time_str)
        if tm_time is None:
            time.sleep(self.poll_interval)
            return None

        now_real = time.time()
        if self.last_tm_time is None:
            self.local_time = tm_time
            self.last_real = now_real
        else:
            elapsed_real = now_real - self.last_real
            self.local_time += timedelta(seconds=elapsed_real * self.tm_speed)
            self.last_real = now_real

        self.last_tm_time = tm_time
        return self.local_time

# ==========================================================
# CSV STREAMER
# ==========================================================
class CSVStreamer:
    def __init__(self, folder, time_column):
        self.folder = Path(folder)
        self.time_column = time_column
        self.conn = duckdb.connect(database=":memory:")
        self.last_time = None
        self.columns = []

        self._load_csvs()

        if not self.columns:
            raise RuntimeError("No columns detected — CSV loading failed")
        if self.time_column not in self.columns:
            raise RuntimeError(f"TIME_COLUMN '{self.time_column}' not found in {self.columns}")

    def _load_csvs(self):
        csv_files = list(self.folder.glob("*.csv*"))
        if not csv_files:
            raise RuntimeError(f"No CSV files found in {self.folder}")

        files_str = ", ".join(f"'{f}'" for f in csv_files)

        self.conn.execute(f"""
            CREATE TABLE events AS
            SELECT * FROM read_csv_auto([{files_str}], union_by_name=true);
        """)

        # SAFE COLUMN NORMALIZATION
        raw_cols = self.conn.execute("PRAGMA table_info(events)").fetchall()
        original_cols = [row[1] for row in raw_cols]

        normalized_map = {}
        drop_cols = []

        for col in original_cols:
            # Replace illegal characters
            new_col = col.replace(" ", "_").replace(".", "_")

            if new_col in normalized_map:
                # If duplicate after normalization → DROP this one
                drop_cols.append(col)
            else:
                normalized_map[new_col] = col

        # Drop duplicates FIRST
        for col in drop_cols:
            print(f"[INFO] Dropping duplicate column: {col}", flush=True)
            self.conn.execute(f'ALTER TABLE events DROP COLUMN "{col}"')

        # Then safely rename remaining columns
        for new_col, old_col in normalized_map.items():
            if new_col != old_col:
                print(f"[INFO] Renaming column: {old_col} -> {new_col}", flush=True)
                self.conn.execute(
                    f'ALTER TABLE events RENAME COLUMN "{old_col}" TO "{new_col}"'
                )

        # TIMESTAMP NORMALIZATION
        self._normalize_time_column()

        # Refresh column metadata
        info = self.conn.execute("PRAGMA table_info(events)").fetchall()
        self.columns = [col[1] for col in info]

        print(f"[INFO] Detected CSV columns after normalization: {self.columns}", flush=True)


    def _normalize_time_column(self):
        col = self.time_column
        cols = [r[0] for r in self.conn.execute("DESCRIBE events").fetchall()]
        if col not in cols:
            raise RuntimeError(f"Time column '{col}' not present in CSV. Columns: {cols}")

        print(f"[INFO] Normalizing time column '{col}' using TIME_FORMAT='{TIME_FORMAT}'", flush=True)

        self.conn.execute(f"ALTER TABLE events ADD COLUMN _ts_tmp TIMESTAMP;")

        if TIME_FORMAT.lower() == "epoch":
            self.conn.execute(f"""
                ALTER TABLE events
                ALTER COLUMN {col} SET DATA TYPE TIMESTAMP
                USING TO_TIMESTAMP(CAST({col} AS DOUBLE))
            """)
            print("[INFO] Parsed epoch timestamps", flush=True)
        else:
            fmt_with_micro = TIME_FORMAT
            fmt_without_micro = TIME_FORMAT.replace(".%f", "")
            self.conn.execute(f"""
                UPDATE events
                SET _ts_tmp =
                    CASE
                        WHEN POSITION('.' IN CAST({col} AS VARCHAR)) > 0 THEN strptime(CAST({col} AS VARCHAR), '{fmt_with_micro}')
                        ELSE strptime(CAST({col} AS VARCHAR), '{fmt_without_micro}')
                    END;
            """)
            print("[INFO] Parsed string timestamps flexibly (with/without fractional seconds)", flush=True)

        self.conn.execute(f"ALTER TABLE events DROP COLUMN {col};")
        self.conn.execute(f"ALTER TABLE events RENAME COLUMN _ts_tmp TO {col};")
        print("[INFO] Timestamp normalization complete", flush=True)

    def fetch_rows_up_to(self, up_to_time):
        if self.last_time is None:
            self.last_time = up_to_time - timedelta(seconds=1)

        up_to_iso = up_to_time.isoformat()
        query = f"""
            SELECT * FROM events
            WHERE {self.time_column} > TIMESTAMP '{self.last_time.isoformat()}'
              AND {self.time_column} <= TIMESTAMP '{up_to_iso}'
            ORDER BY {self.time_column} ASC
        """

        rows = self.conn.execute(query).fetchall()
        if rows:
            time_idx = self.columns.index(self.time_column)
            self.last_time = rows[-1][time_idx]
        return rows

# ==========================================================
# KAFKA + AUTO AVRO
# ==========================================================
class KafkaAvroProducer:
    def __init__(self, duckdb_conn, table_name):
        self.conn = duckdb_conn
        self.table = table_name

        self.schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        self.columns, self.avro_schema = self._build_schema_from_duckdb()
        schema_str = json.dumps(self.avro_schema)

        self.avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry,
            schema_str=schema_str,
            to_dict=self._to_dict
        )

        self.producer = SerializingProducer({
            "bootstrap.servers": KAFKA_BROKER,
            "value.serializer": self.avro_serializer
        })

        print("[INFO] Kafka Avro Producer initialized with AUTO schema", flush=True)

    def _build_schema_from_duckdb(self):
        rows = self.conn.execute(f"DESCRIBE {self.table}").fetchall()

        fields = []
        columns = []
        seen = set()  # track sanitized names

        for row in rows:
            col_name = row[0]

            # Sanitize for Avro: replace spaces, lowercase
            clean_name = col_name.strip().replace(" ", "_").lower()

            if clean_name in seen:
                print(f"[WARN] Skipping duplicate or conflicting column '{col_name}' -> '{clean_name}'")
                continue

            seen.add(clean_name)
            columns.append(clean_name)

            col_type = row[1]
            avro_type = self._map_duckdb_to_avro(col_type)

            fields.append({
                "name": clean_name,
                "type": avro_type,
                "default": None
            })

        schema = {
            "type": "record",
            "name": "StreamerRecord",
            "namespace": "auto.streamer",
            "fields": fields
        }

        return columns, schema


    def _map_duckdb_to_avro(self, duck_type: str):
        t = duck_type.upper()
        if "DOUBLE" in t or "FLOAT" in t:
            return ["null", "double"]
        elif "INT" in t:
            return ["null", "long"]
        elif "BOOLEAN" in t:
            return ["null", "boolean"]
        elif "TIMESTAMP" in t or "DATE" in t:
            return ["null", "string"]
        else:
            return ["null", "string"]

    def _to_dict(self, obj, ctx):
        return obj

    def send(self, row):
        record = {}
        for col, val in zip(self.columns, row):
            if val is None:
                record[col] = None
            elif isinstance(val, datetime):
                record[col] = val.isoformat()
            else:
                record[col] = val
        try:
            self.producer.produce(topic=KAFKA_TOPIC, value=record)
            self.producer.poll(0)
        except Exception as e:
            print("[KAFKA ERROR]", e, flush=True)

# ==========================================================
# MAIN LOOP
# ==========================================================
def main():
    tm_sync = TimeManagerSync(url=TIME_MANAGER_URL)
    streamer = CSVStreamer(folder=DATA_FOLDER, time_column=TIME_COLUMN)
    kafka_producer = KafkaAvroProducer(streamer.conn, "events")
    
    last_output = time.time()

    while True:
        local_time = tm_sync.update()
        if local_time is None:
            continue

        now = time.time()
        if now - last_output >= OUTPUT_INTERVAL:
            rows = streamer.fetch_rows_up_to(local_time)
            for row in rows:
                kafka_producer.send(row)
            if rows:
                print(f"[SENT] {len(rows)} records at sim-time {local_time}", flush=True)
            last_output = now

        time.sleep(0.05)

if __name__ == "__main__":
    main()
