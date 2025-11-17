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
            time_column='datetime',
            time_format='%Y-%m-%d %H:%M:%S'
        )

    def transform_record(self, record: Dict) -> Dict:
        """
        Transform weather observation record

        Expected CSV columns:
        - datetime: Observation timestamp
        - tempmax: Maximum temperature (F)
        - tempmin: Minimum temperature (F)
        - temp: Average temperature (F)
        - feelslikemax: Max feels-like temperature (F)
        - feelslikemin: Min feels-like temperature (F)
        - feelslike: Average feels-like temperature (F)
        - dew: Dew point (F)
        - humidity: Humidity (%)
        - precip: Precipitation (inches)
        - precipprob: Precipitation probability (%)
        - precipcover: Precipitation coverage (%)
        - preciptype: Type of precipitation
        - snow: Snow amount (inches)
        - snowdepth: Snow depth (inches)
        - windgust: Wind gust (mph)
        - windspeed: Wind speed (mph)
        - winddir: Wind direction (degrees)
        - sealevelpressure: Sea level pressure (mb)
        - cloudcover: Cloud cover (%)
        - visibility: Visibility (miles)
        - solarradiation: Solar radiation
        - solarenergy: Solar energy
        - uvindex: UV index
        - conditions: Weather conditions description
        """
        try:
            return {
                'observation_id': f"{record.get('datetime', '')}",
                'datetime': record.get('datetime'),
                'temperature': {
                    'max_f': float(record.get('tempmax', 0)) if record.get('tempmax') else None,
                    'min_f': float(record.get('tempmin', 0)) if record.get('tempmin') else None,
                    'avg_f': float(record.get('temp', 0)) if record.get('temp') else None,
                    'feels_like_max_f': float(record.get('feelslikemax', 0)) if record.get('feelslikemax') else None,
                    'feels_like_min_f': float(record.get('feelslikemin', 0)) if record.get('feelslikemin') else None,
                    'feels_like_avg_f': float(record.get('feelslike', 0)) if record.get('feelslike') else None,
                    'dew_point_f': float(record.get('dew', 0)) if record.get('dew') else None
                },
                'precipitation': {
                    'amount_inches': float(record.get('precip', 0)) if record.get('precip') else None,
                    'probability_pct': float(record.get('precipprob', 0)) if record.get('precipprob') else None,
                    'coverage_pct': float(record.get('precipcover', 0)) if record.get('precipcover') else None,
                    'type': record.get('preciptype')
                },
                'snow': {
                    'amount_inches': float(record.get('snow', 0)) if record.get('snow') else None,
                    'depth_inches': float(record.get('snowdepth', 0)) if record.get('snowdepth') else None
                },
                'wind': {
                    'speed_mph': float(record.get('windspeed', 0)) if record.get('windspeed') else None,
                    'gust_mph': float(record.get('windgust', 0)) if record.get('windgust') else None,
                    'direction_deg': float(record.get('winddir', 0)) if record.get('winddir') else None
                },
                'atmosphere': {
                    'humidity_pct': float(record.get('humidity', 0)) if record.get('humidity') else None,
                    'pressure_mb': float(record.get('sealevelpressure', 0)) if record.get('sealevelpressure') else None,
                    'visibility_miles': float(record.get('visibility', 0)) if record.get('visibility') else None,
                    'cloud_cover_pct': float(record.get('cloudcover', 0)) if record.get('cloudcover') else None
                },
                'solar': {
                    'radiation': float(record.get('solarradiation', 0)) if record.get('solarradiation') else None,
                    'energy': float(record.get('solarenergy', 0)) if record.get('solarenergy') else None,
                    'uv_index': float(record.get('uvindex', 0)) if record.get('uvindex') else None
                },
                'conditions': record.get('conditions', '')
            }
        except Exception as e:
            # If transformation fails, return raw record
            return record


if __name__ == '__main__':
    streamer = WeatherStreamer()
    streamer.run()
