# Import schema definitions from schemas modules
from data_analysis.schemas.bike_data_schema import bike_data_schema
from data_analysis.schemas.taxi_data_schema import taxi_data_schema
from data_analysis.schemas.weather_data_schema import weather_data_schema
from data_analysis.schemas.accident_data_schema import accident_data_schema

# Re-export all schemas for backward compatibility
__all__ = [
    'bike_data_schema',
    'taxi_data_schema',
    'weather_data_schema',
    'accident_data_schema',
]
