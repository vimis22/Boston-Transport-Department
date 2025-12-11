import os
import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker:9092')
TOPIC = 'weather-topic'

def generate_weather_data():
    """Generate realistic weather data for Boston"""
    now = datetime.now()
    
    # Seasonal temperature ranges for Boston (in Celsius)
    month = now.month
    if month in [12, 1, 2]:  # Winter
        temp_base = random.uniform(-5, 5)
    elif month in [3, 4, 5]:  # Spring
        temp_base = random.uniform(5, 18)
    elif month in [6, 7, 8]:  # Summer
        temp_base = random.uniform(18, 30)
    else:  # Fall
        temp_base = random.uniform(8, 20)
    
    # Temperature variations
    temp_variation = random.uniform(3, 8)
    temp_min = round(temp_base - temp_variation/2, 1)
    temp_max = round(temp_base + temp_variation/2, 1)
    temp_avg = round((temp_min + temp_max) / 2, 1)
    
    # Precipitation (mm) - more in spring/fall
    if month in [4, 5, 9, 10, 11]:
        precipitation = round(random.uniform(0, 15), 1)
    else:
        precipitation = round(random.uniform(0, 8), 1)
    
    # Wind
    wind_direction = round(random.uniform(0, 360), 1)
    wind_speed = round(random.uniform(5, 25), 1)  # km/h
    
    # Pressure (hectopascals)
    pressure = round(random.uniform(990, 1030), 1)
    
    weather = {
        "date": now.strftime("%Y-%m-%d"),
        "timestamp": int(now.timestamp() * 1000),
        "temp_avg": temp_avg,
        "temp_min": temp_min,
        "temp_max": temp_max,
        "precipitation": precipitation,
        "wind_direction": wind_direction,
        "wind_speed": wind_speed,
        "pressure": pressure
    }
    
    return weather

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def main():
    print(f"Starting Weather Producer...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Configure Producer (simple JSON serialization)
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'weather-producer'
    }
    
    producer = Producer(producer_config)
    
    print(f"Producing JSON messages to topic '{TOPIC}'...")
    
    try:
        while True:
            # Generate weather data
            weather = generate_weather_data()
            
            # Use date as key
            key = weather['date'].encode('utf-8')
            
            # Serialize to JSON
            message = json.dumps(weather).encode('utf-8')
            
            # Produce message
            producer.produce(
                topic=TOPIC,
                key=key,
                value=message,
                callback=delivery_report
            )
            
            producer.poll(0)
            
            # Weather updates less frequently (every 10-30 seconds)
            time.sleep(random.uniform(10, 30))
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        print("Producer shutdown complete.")

if __name__ == '__main__':
    main()