"""
Bike Streamer
Streams Boston bike sharing data to Kafka.
Data format based on Hubway/Bluebikes historical data.
"""

import os
from base_streamer import BaseStreamer
from typing import Dict
from datetime import datetime


class BikeStreamer(BaseStreamer):
    """Streamer for bike sharing data"""

    def __init__(self):
        data_file = os.getenv('DATA_FILE', '/data/bike_trips.csv')
        super().__init__(
            name='bike-streamer',
            data_file=data_file,
            kafka_topic='bike-trips',
            time_column='starttime',
            time_format='%Y-%m-%d %H:%M:%S'
        )

    def transform_record(self, record: Dict) -> Dict:
        """
        Transform bike trip record

        Expected CSV columns:
        - tripduration: Duration in seconds
        - starttime: Start timestamp
        - stoptime: Stop timestamp
        - start station id: Start station ID
        - start station name: Start station name
        - start station latitude: Start latitude
        - start station longitude: Start longitude
        - end station id: End station ID
        - end station name: End station name
        - end station latitude: End latitude
        - end station longitude: End longitude
        - bikeid: Bike ID
        - usertype: User type (Customer/Subscriber)
        - birth year: Birth year
        - gender: Gender (0=unknown, 1=male, 2=female)
        """
        try:
            return {
                'trip_id': f"{record.get('starttime', '')}_{record.get('bikeid', '')}",
                'duration_seconds': int(record.get('tripduration', 0)) if record.get('tripduration') else None,
                'start_time': record.get('starttime'),
                'stop_time': record.get('stoptime'),
                'start_station': {
                    'id': record.get('start station id'),
                    'name': record.get('start station name'),
                    'latitude': float(record.get('start station latitude', 0)) if record.get('start station latitude') else None,
                    'longitude': float(record.get('start station longitude', 0)) if record.get('start station longitude') else None
                },
                'end_station': {
                    'id': record.get('end station id'),
                    'name': record.get('end station name'),
                    'latitude': float(record.get('end station latitude', 0)) if record.get('end station latitude') else None,
                    'longitude': float(record.get('end station longitude', 0)) if record.get('end station longitude') else None
                },
                'bike_id': record.get('bikeid'),
                'user_type': record.get('usertype'),
                'birth_year': int(record.get('birth year', 0)) if record.get('birth year') else None,
                'gender': int(record.get('gender', 0)) if record.get('gender') else 0
            }
        except Exception as e:
            # If transformation fails, return raw record
            return record


if __name__ == '__main__':
    streamer = BikeStreamer()
    streamer.run()
