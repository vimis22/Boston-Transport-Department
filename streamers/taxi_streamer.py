"""
Taxi Streamer
Streams Boston taxi/rideshare data to Kafka.
Data format based on Uber/Lyft Boston dataset.
"""

import os
from base_streamer import BaseStreamer
from typing import Dict


class TaxiStreamer(BaseStreamer):
    """Streamer for taxi/rideshare data"""

    def __init__(self):
        data_file = os.getenv('DATA_FILE', '/data/taxi_trips.csv')
        super().__init__(
            name='taxi-streamer',
            data_file=data_file,
            kafka_topic='taxi-trips',
            time_column='datetime',
            time_format='%Y-%m-%d %H:%M:%S'
        )

    def transform_record(self, record: Dict) -> Dict:
        """
        Transform taxi trip record

        Expected CSV columns:
        - datetime: Timestamp
        - source: Pickup location
        - destination: Dropoff location
        - cab_type: Uber/Lyft
        - product_id: Product type ID
        - name: Product name (UberX, Lyft, etc.)
        - price: Trip price
        - distance: Trip distance
        - surge_multiplier: Surge pricing multiplier
        - latitude: Pickup latitude
        - longitude: Pickup longitude
        - temperature: Temperature at time
        - apparentTemperature: Feels-like temperature
        - short_summary: Weather summary
        - precipIntensity: Precipitation intensity
        - humidity: Humidity
        - windSpeed: Wind speed
        """
        try:
            return {
                'trip_id': f"{record.get('datetime', '')}_{record.get('source', '')}_{record.get('destination', '')}",
                'datetime': record.get('datetime'),
                'pickup_location': record.get('source'),
                'dropoff_location': record.get('destination'),
                'cab_type': record.get('cab_type'),
                'product': {
                    'id': record.get('product_id'),
                    'name': record.get('name')
                },
                'price': float(record.get('price', 0)) if record.get('price') else None,
                'distance': float(record.get('distance', 0)) if record.get('distance') else None,
                'surge_multiplier': float(record.get('surge_multiplier', 1.0)) if record.get('surge_multiplier') else 1.0,
                'location': {
                    'latitude': float(record.get('latitude', 0)) if record.get('latitude') else None,
                    'longitude': float(record.get('longitude', 0)) if record.get('longitude') else None
                },
                # Weather data included in taxi dataset
                'weather_snapshot': {
                    'temperature': float(record.get('temperature', 0)) if record.get('temperature') else None,
                    'apparent_temperature': float(record.get('apparentTemperature', 0)) if record.get('apparentTemperature') else None,
                    'summary': record.get('short_summary'),
                    'precip_intensity': float(record.get('precipIntensity', 0)) if record.get('precipIntensity') else None,
                    'humidity': float(record.get('humidity', 0)) if record.get('humidity') else None,
                    'wind_speed': float(record.get('windSpeed', 0)) if record.get('windSpeed') else None
                }
            }
        except Exception as e:
            # If transformation fails, return raw record
            return record


if __name__ == '__main__':
    streamer = TaxiStreamer()
    streamer.run()
