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
            
            # Convert all temperature fields from Fahrenheit to Celsius
            for field in temperature_fields:
                if field in row_dict:
                    row_dict[field] = fahrenheit_to_celsius(row_dict[field])
            
            # Convert timestamp to string if it exists (for Avro schema compatibility)
            if "timestamp" in row_dict and row_dict["timestamp"] is not None:
                row_dict["timestamp"] = str(row_dict["timestamp"])
            
            # Convert other numeric fields that should be strings in the schema
            # Convert id to string if it exists
            if "id" in row_dict and row_dict["id"] is not None:
                row_dict["id"] = str(row_dict["id"])
            
            # Convert hour, day, month to int if they exist (schema expects int)
            for field in ["hour", "day", "month"]:
                if field in row_dict and row_dict[field] is not None:
                    try:
                        row_dict[field] = int(row_dict[field])
                    except (ValueError, TypeError):
                        pass
            
            # Convert string fields to ensure they're strings
            for field in ["datetime", "timezone", "source", "destination", "cab_type", "product_id"]:
                if field in row_dict and row_dict[field] is not None:
                    row_dict[field] = str(row_dict[field])
            
            result.append(row_dict)
        
        return result


class TaxiStreamer:
    """Streamer for taxi data"""
    
    def __init__(self, dataset_path: str = "./boston_datasets/bigdata/taxi_data.parquet"):
        self.dataset_path = dataset_path
    
    def get_rows(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        """Get taxi data rows for the specified time range"""
        return get_rows_from_taxi_data(self.dataset_path, start_time, end_time)

