import os
import time
import csv
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Configuration ---
DATA_FILE = os.getenv("DATA_FILE", "data/accident/tmpw8i0zd4_.csv")
DELAY = float(os.getenv("STREAM_DELAY", 1))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "default_topic")

print(f"Starting dataset streamer...")
print(f"Data file: {DATA_FILE}")
print(f"Kafka broker: {KAFKA_BROKER}")
print(f"Kafka topic: {KAFKA_TOPIC}")
print(f"Stream delay: {DELAY}s")

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
        for row in reader:
            json_msg = dict(row)
            try:
                producer.send(KAFKA_TOPIC, json_msg)
                producer.flush()
                row_count += 1
                print(f"[KAFKA] Sent row {row_count}: {json_msg}")
            except KafkaError as e:
                print(f"[ERROR] Failed to send message: {e}")
            time.sleep(DELAY)
    print(f"[INFO] Finished streaming {row_count} rows to Kafka topic '{KAFKA_TOPIC}'")
except FileNotFoundError:
    print(f"[ERROR] Data file not found: {DATA_FILE}")
except Exception as e:
    print(f"[ERROR] Unexpected error: {e}")
