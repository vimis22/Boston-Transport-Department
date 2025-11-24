"""
Weather Streamer
Streams Boston weather data to Kafka.
Data format based on NCEI/historical weather datasets.
"""

import os
from base_streamer import BaseStreamer
from typing import Dict


class WeatherStreamer(BaseStreamer):
    """Streamer for weather observation data"""

    def __init__(self):
        data_file = os.getenv('DATA_FILE', '/data/weather_observations.csv')
        super().__init__(
            name='weather-streamer',
            data_file=data_file,
            kafka_topic='weather-data',
            time_column='DATE',
            time_format='%Y-%m-%dT%H:%M:%S'
        )

    def transform_record(self, record: Dict) -> Dict:
        """
        Transform weather observation record (NCEI format)

        Expected CSV columns (NCEI):
        - STATION: Station identifier
        - DATE: Observation timestamp (ISO format)
        - SOURCE: Data source
        - LATITUDE: Station latitude
        - LONGITUDE: Station longitude
        - ELEVATION: Station elevation
        - NAME: Station name
        - REPORT_TYPE: Type of report
        - CALL_SIGN: Station call sign
        - QUALITY_CONTROL: QC flag
        - WND: Wind observation
        - CIG: Ceiling height
        - VIS: Visibility
        - TMP: Temperature
        - DEW: Dew point
        - SLP: Sea level pressure
        """
        try:
            return {
                'station': record.get('STATION'),
                'datetime': record.get('DATE'),
                'source': record.get('SOURCE'),
                'location': {
                    'latitude': float(record.get('LATITUDE', 0)) if record.get('LATITUDE') else None,
                    'longitude': float(record.get('LONGITUDE', 0)) if record.get('LONGITUDE') else None,
                    'elevation': float(record.get('ELEVATION', 0)) if record.get('ELEVATION') else None
                },
                'station_info': {
                    'name': record.get('NAME'),
                    'report_type': record.get('REPORT_TYPE'),
                    'call_sign': record.get('CALL_SIGN'),
                    'quality_control': record.get('QUALITY_CONTROL')
                },
                'observations': {
                    'wind': record.get('WND'),
                    'ceiling': record.get('CIG'),
                    'visibility': record.get('VIS'),
                    'temperature': record.get('TMP'),
                    'dew_point': record.get('DEW'),
                    'sea_level_pressure': record.get('SLP')
                }
            }
        except Exception as e:
            # If transformation fails, return raw record
            return record


if __name__ == '__main__':
    streamer = WeatherStreamer()
    streamer.run()
