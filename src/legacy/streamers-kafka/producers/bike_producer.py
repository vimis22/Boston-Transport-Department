import os
import time
import json
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker:9092')
TOPIC = 'bike-topic'

# Sample station data for Boston
STATIONS = [
    {"id": 1, "name": "Charles Circle - Charles St. at Cambridge St.", "lat": 42.359434, "lon": -71.064254},
    {"id": 2, "name": "Boylston St. at Arlington St.", "lat": 42.352547, "lon": -71.070433},
    {"id": 3, "name": "TD Garden - Causeway St. at Portal Park", "lat": 42.366284, "lon": -71.062333},
    {"id": 4, "name": "South Station - 700 Atlantic Ave.", "lat": 42.352271, "lon": -71.055242},
    {"id": 5, "name": "Cambridge St. at Joy St.", "lat": 42.361285, "lon": -71.066952},
    {"id": 6, "name": "Post Office Square", "lat": 42.356395, "lon": -71.054870},
    {"id": 7, "name": "Boston Public Library - 700 Boylston St.", "lat": 42.349210, "lon": -71.077774},
    {"id": 8, "name": "Newbury St. at Hereford St.", "lat": 42.349631, "lon": -71.085191},
]

USER_TYPES = ["Registered", "Casual"]
GENDERS = ["Male", "Female", "Unknown"]

def generate_bike_trip():
    """Generate a random bike trip"""
    start_station = random.choice(STATIONS)
    end_station = random.choice([s for s in STATIONS if s["id"] != start_station["id"]])
    
    start_time = datetime.now()
    duration = random.randint(300, 3600)  # 5 minutes to 1 hour
    end_time = start_time + timedelta(seconds=duration)
    
    user_type = random.choice(USER_TYPES)
    
    trip = {
        "trip_id": f"TRIP-{int(time.time())}-{random.randint(1000, 9999)}",
        "duration": duration,
        "start_time": int(start_time.timestamp() * 1000),
        "end_time": int(end_time.timestamp() * 1000),
        "start_station_id": start_station["id"],
        "start_station_name": start_station["name"],
        "start_station_latitude": start_station["lat"],
        "start_station_longitude": start_station["lon"],
        "end_station_id": end_station["id"],
        "end_station_name": end_station["name"],
        "end_station_latitude": end_station["lat"],
        "end_station_longitude": end_station["lon"],
        "bike_id": f"BIKE-{random.randint(1000, 5000)}",
        "user_type": user_type,
        "birth_year": random.randint(1960, 2000) if user_type == "Registered" else None,
        "gender": random.choice(GENDERS) if user_type == "Registered" else None,
        "zip_code": f"021{random.randint(10, 99)}" if user_type == "Registered" else None
    }
    
    return trip

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def main():
    print(f"Starting Bike Producer...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Configure Producer (simple JSON serialization)
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'bike-producer'
    }
    
    producer = Producer(producer_config)
    
    print(f"Producing JSON messages to topic '{TOPIC}'...")
    
    try:
        while True:
            # Generate bike trip
            trip = generate_bike_trip()
            
            # Serialize to JSON
            message = json.dumps(trip).encode('utf-8')
            
            # Produce message
            producer.produce(
                topic=TOPIC,
                key=trip['trip_id'].encode('utf-8'),
                value=message,
                callback=delivery_report
            )
            
            producer.poll(0)
            
            # Wait before producing next message
            time.sleep(random.uniform(1, 3))
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        print("Producer shutdown complete.")

if __name__ == '__main__':
    main()