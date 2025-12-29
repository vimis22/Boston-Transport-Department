"""Taxi data streamer"""

from datetime import datetime
from typing import Any
import duckdb


def fahrenheit_to_celsius(fahrenheit: float | None) -> float | None:
    """
    Convert temperature from Fahrenheit to Celsius.
    Returns None if input is None.
    
    Formula: C = (F - 32) * 5/9
    """
    if fahrenheit is None:
        return None
    try:
        return (fahrenheit - 32) * 5 / 9
    except (TypeError, ValueError):
        return None


def get_rows_from_taxi_data(dataset_path: str, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
    """Get the rows from the taxi data in the interval between the start and end time"""
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"SELECT * FROM read_parquet('{dataset_path}') "
            f"WHERE datetime >= '{start_time}' AND datetime <= '{end_time}'"
        )
        rows = conn.fetchall()
        columns = [desc[0] for desc in conn.description]
        
        # Temperature fields to convert from Fahrenheit to Celsius
        temperature_fields = [
            "temperature",
            "apparentTemperature",
            "temperatureHigh",
            "temperatureLow",
            "apparentTemperatureHigh",
            "apparentTemperatureLow",
            "temperatureMin",
            "temperatureMax",
            "apparentTemperatureMin",
            "apparentTemperatureMax",
            "dewPoint",
        ]
        
        # Convert rows to dictionaries and convert temperatures
        result = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            
            # Map field names for schema compatibility
            field_name_mapping = {
                "apparentTemperature": "apparent_temperature",
                "windSpeed": "wind_speed",
                "precipIntensity": "precip_intensity"
            }
            
            mapped_dict = {}
            for key, value in row_dict.items():
                mapped_key = field_name_mapping.get(key, key)
                mapped_dict[mapped_key] = value

            # Convert all temperature fields from Fahrenheit to Celsius (using original names from row_dict)
            for field in temperature_fields:
                mapped_field = field_name_mapping.get(field, field)
                if mapped_field in mapped_dict:
                    mapped_dict[mapped_field] = fahrenheit_to_celsius(mapped_dict[mapped_field])
            
            # Ensure required numeric fields are float/double and not null
            for field in ["price", "distance", "surge_multiplier", "temperature", "apparent_temperature", "humidity", "wind_speed", "precip_intensity"]:
                if mapped_dict.get(field) is None or mapped_dict.get(field) == "NA":
                    mapped_dict[field] = 0.0
                else:
                    try:
                        mapped_dict[field] = float(mapped_dict[field])
                    except (ValueError, TypeError):
                        mapped_dict[field] = 0.0

            # Convert timestamp to string if it exists (for Avro schema compatibility)
            if "timestamp" in mapped_dict and mapped_dict["timestamp"] is not None:
                mapped_dict["timestamp"] = str(mapped_dict["timestamp"])
            
            # Convert other numeric fields that should be strings in the schema
            # Convert id to string if it exists
            if "id" in mapped_dict and mapped_dict["id"] is not None:
                mapped_dict["id"] = str(mapped_dict["id"])
            
            # Convert hour, day, month to int if they exist (schema expects int)
            for field in ["hour", "day", "month"]:
                if field in mapped_dict and mapped_dict[field] is not None:
                    try:
                        mapped_dict[field] = int(mapped_dict[field])
                    except (ValueError, TypeError):
                        mapped_dict[field] = 0
                else:
                    mapped_dict[field] = 0
            
            # Convert string fields to ensure they're strings
            for field in ["datetime", "timezone", "source", "destination", "cab_type", "product_id"]:
                if field in mapped_dict and mapped_dict[field] is not None:
                    mapped_dict[field] = str(mapped_dict[field])
                else:
                    mapped_dict[field] = "Unknown"
            
            result.append(mapped_dict)
        
        return result


class TaxiStreamer:
    """Streamer for taxi data"""
    
    def __init__(self, dataset_path: str = "./boston_datasets/bigdata/taxi_data.parquet"):
        self.dataset_path = dataset_path
    
    def get_rows(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        """Get taxi data rows for the specified time range"""
        return get_rows_from_taxi_data(self.dataset_path, start_time, end_time)

