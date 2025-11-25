import os
import time
import requests
import duckdb
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro

# ==========================================================
# CONFIGURATION
# ==========================================================
DATA_PATH = os.getenv("DATA_FILE", "data/*.csv.gz")
UPDATE_DELAY = float(os.getenv("UPDATE_DELAY", 1))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "default_topic")
TIME_MANAGER_URL = os.getenv("TIME_MANAGER_URL", "http://time-manager:5000")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

# Defaults (will be overwritten by time-manager API)
DELAY = 1
TIME_VALUE = 0

print(f"Starting dataset streamer...")
print(f"Data path: {DATA_PATH}")
print(f"Kafka broker: {KAFKA_BROKER}")
print(f"Kafka topic: {KAFKA_TOPIC}")
print(f"Schema Registry : {SCHEMA_REGISTRY_URL}")
print(f"Update delay: {UPDATE_DELAY}s")
print(f"Time manager URL: {TIME_MANAGER_URL}")

# ==========================================================
# AVRO SCHEMA DEFINITION
# ==========================================================

value_schema_str = """
{
  "namespace": "accidents.avro",
  "type": "record",
  "name": "AccidentRecord",
  "fields": [
    { "name": "timestamp", "type": "long" },
    { "name": "lat",       "type": "double" },
    { "name": "lon",       "type": "double" },
    { "name": "severity",  "type": "int" },
    { "name": "city",      "type": "string" }
  ]
}
"""

value_schema = avro.loads(value_schema_str)

# ==========================================================
# DUCKDB — LOAD COMPRESSED CSV DATA
# ==========================================================
duckdbConnection = duckdb.connect()

print(f"Loading dataset with DuckDB from: {DATA_PATH}")

# Register the compressed CSV files as a DuckDB view
duckdbConnection.execute(f"""
    CREATE OR REPLACE VIEW data AS 
    SELECT * FROM read_csv('{DATA_PATH}', AUTO_DETECT=TRUE, SAMPLE_SIZE=20000000);
""")

print("[INFO] DuckDB view created over CSV files")

# ==========================================================
# TIME MANAGER STATUS FETCH
# ==========================================================
def update_clock_values():
    global DELAY, TIME_VALUE

    try:
        response = requests.get(f"{TIME_MANAGER_URL}/api/v1/clock/status", timeout=3)
        if response.status_code == 200:
            data = response.json()
            DELAY = float(data.get("delay", DELAY))
            TIME_VALUE = float(data.get("time", TIME_VALUE))
            print(f"[CLOCK] Updated DELAY={DELAY}, TIME={TIME_VALUE}")
        else:
            print(f"[WARN] Clock API returned status {response.status_code}")
    except Exception as e:
        print(f"[WARN] Failed to fetch clock status: {e}")

# ==========================================================
# KAFKA AVRO PRODUCER with retry (Schema Registry)
# ==========================================================
producer = None
while producer is None:
    try:
        producer = AvroProducer(
            {
                "bootstrap.servers": KAFKA_BROKER,
                "schema.registry.url": SCHEMA_REGISTRY_URL,
            },
            default_value_schema=value_schema,
        )
        print("[INFO] Connected to Kafka and Schema Registry")
    except Exception as e:
        print(f"[WARN] Kafka/Schema registry unavailable: {e}. Retrying...")
        time.sleep(5)

# ==========================================================
# MAIN STREAMING LOOP
# ==========================================================
last_update = 0
last_sent_timestamp = -1 

while True:
    now = time.time()

    # Sync clock
    if now - last_update >= UPDATE_DELAY:
        update_clock_values()
        last_update = now

    # Query next row AFTER the last data sent
    query = f"""
        SELECT *
        FROM data
        WHERE timestamp > {last_sent_timestamp}
          AND timestamp >= {TIME_VALUE}
        ORDER BY timestamp
        LIMIT 1
    """

    rows = con.execute(query).fetchall()

    if not rows:
        print("[INFO] No new matching rows yet… waiting...")
        time.sleep(DELAY)
        continue

    row = rows[0]

    # Convert row → dict for Avro
    record = dict(zip(col_names, row))

    # Send to Kafka
    try:
        producer.produce(topic=KAFKA_TOPIC, value=record)
        producer.flush()
        print(f"[KAFKA-AVRO] Sent row (timestamp={record['timestamp']}): {record}")
    except Exception as e:
        print(f"[ERROR] Failed to send Avro record: {e}")

    # Update progress marker
    last_sent_timestamp = record["timestamp"]

    time.sleep(DELAY)
