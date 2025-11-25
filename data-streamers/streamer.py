import os
import time
import csv
import json
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Configuration ---
DATA_FILE = os.getenv("DATA_FILE", "data/accident/tmpw8i0zd4_.csv")
UPDATE_DELAY = float(os.getenv("UPDATE_DELAY", 1))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "default_topic")
TIME_MANAGER_URL = os.getenv("TIME_MANAGER_URL", "http://localhost:5000")

# Defaults (will be overwritten by time-manager API)
DELAY = 1
TIME_VALUE = 0

print(f"Starting dataset streamer...")
print(f"Data file: {DATA_FILE}")
print(f"Kafka broker: {KAFKA_BROKER}")
print(f"Kafka topic: {KAFKA_TOPIC}")
print(f"Update delay: {UPDATE_DELAY}s")
print(f"Time manager URL: {TIME_MANAGER_URL}")

# --- Function to fetch DELAY & TIME from the API ---
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

# --- Kafka Producer with retry ---
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print(f"[INFO] Connected to Kafka broker at {KAFKA_BROKER}")
    except KafkaError as e:
        print(f"[WARN] Kafka not ready yet: {e}. Retrying in 5 seconds...")
        time.sleep(5)

# --- Stream data to Kafka ---
try:
    with open(DATA_FILE, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        row_count = 0
        last_update = 0

        for row in reader:

            # Update DELAY/TIME every UPDATE_DELAY seconds
            now = time.time()
            if now - last_update >= UPDATE_DELAY:
                update_clock_values()
                last_update = now

            # Send row to Kafka
            json_msg = dict(row)
            try:
                producer.send(KAFKA_TOPIC, json_msg)
                producer.flush()
                row_count += 1
                print(f"[KAFKA] Sent row {row_count} with DELAY={DELAY}: {json_msg}")
            except KafkaError as e:
                print(f"[ERROR] Failed to send message: {e}")
            time.sleep(DELAY)
    print(f"[INFO] Finished streaming {row_count} rows to Kafka topic '{KAFKA_TOPIC}'")
except FileNotFoundError:
    print(f"[ERROR] Data file not found: {DATA_FILE}")
except Exception as e:
    print(f"[ERROR] Unexpected error: {e}")