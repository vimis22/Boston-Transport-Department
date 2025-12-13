import os
import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker:9092')
TOPIC = 'taxi-topic'

# Sample locations in Boston
LOCATIONS = [
    "Back Bay",
    "Beacon Hill",
    "Boston University",
    "Fenway",
    "Financial District",
    "Harvard Square",
    "Haymarket Square",
    "Logan Airport",
    "North End",
    "South Station",
    "TD Garden",
    "Theatre District",
    "Northeastern University",
    "Seaport District",
    "Chinatown",
    "Cambridge",
    "MIT",
    "South Boston",
    "Charlestown",
    "Jamaica Plain"
]

CAB_TYPES = ["Uber", "Lyft"]
TIMEZONES = ["America/New_York"]

def generate_taxi_ride():
    """Generate a random taxi ride"""
    now = datetime.now()
    
    source = random.choice(LOCATIONS)
    destination = random.choice([loc for loc in LOCATIONS if loc != source])
    
    # Calculate distance (rough estimate based on location pairs)
    distance = random.uniform(0.5, 15.0)  # miles
    
    # Calculate price (base fare + distance-based)
    base_fare = random.uniform(2.5, 5.0)
    per_mile = random.uniform(1.5, 3.0)
    price = round(base_fare + (distance * per_mile), 2)
    
    ride = {
        "ride_id": f"RIDE-{int(time.time())}-{random.randint(1000, 9999)}",
        "timestamp": int(now.timestamp() * 1000),
        "hour": now.hour,
        "day_of_week": now.weekday(),
        "month": now.month,
        "date": now.strftime("%Y-%m-%d"),
        "timezone": random.choice(TIMEZONES),
        "source": source,
        "destination": destination,
        "cab_type": random.choice(CAB_TYPES),
        "price": price,
        "distance": round(distance, 2)
    }
    
    return ride

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def main():
    print(f"Starting Taxi Producer...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Configure Producer (simple JSON serialization)
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'taxi-producer'
    }
    
    producer = Producer(producer_config)
    
    print(f"Producing JSON messages to topic '{TOPIC}'...")
    
    try:
        while True:
            # Generate taxi ride
            ride = generate_taxi_ride()
            
            # Serialize to JSON
            message = json.dumps(ride).encode('utf-8')
            
            # Produce message
            producer.produce(
                topic=TOPIC,
                key=ride['ride_id'].encode('utf-8'),
                value=message,
                callback=delivery_report
            )
            
            producer.poll(0)
            
            # Wait before producing next message
            time.sleep(random.uniform(2, 5))
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        print("Producer shutdown complete.")

if __name__ == '__main__':
    main()