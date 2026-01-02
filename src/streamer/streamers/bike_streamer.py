"""Bike data streamer"""

from datetime import datetime
from typing import Any
import duckdb


def get_rows_from_bike_data(dataset_path: str, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
    """Get the rows from the bike data in the interval between the start and end time"""
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"SELECT * FROM read_parquet('{dataset_path}') "
            f"WHERE starttime >= '{start_time}' AND starttime <= '{end_time}'"
        )
        rows = conn.fetchall()
        columns = [desc[0] for desc in conn.description]
        
        # Convert rows to dictionaries and convert types for Avro schema compatibility
        # Map field names with spaces to schema field names with underscores
        field_name_mapping = {
            "start station id": "start_station_id",
            "start station name": "start_station_name",
            "start station latitude": "start_station_latitude",
            "start station longitude": "start_station_longitude",
            "end station id": "end_station_id",
            "end station name": "end_station_name",
            "end station latitude": "end_station_latitude",
            "end station longitude": "end_station_longitude",
            "birth year": "birth_year",
            "gender": "gender",
            "usertype": "usertype",
        }
        
        result = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            
            # Map field names to match schema (spaces -> underscores)
            mapped_dict = {}
            for key, value in row_dict.items():
                mapped_key = field_name_mapping.get(key, key)
                mapped_dict[mapped_key] = value
            
            # Convert datetime fields to strings (schema expects strings)
            for field in ["starttime", "stoptime"]:
                if field in mapped_dict and mapped_dict[field] is not None:
                    if isinstance(mapped_dict[field], datetime):
                        mapped_dict[field] = mapped_dict[field].isoformat()
                    else:
                        mapped_dict[field] = str(mapped_dict[field])
            
            # Convert string fields to ensure they're strings
            for field in ["start_station_id", "start_station_name", "end_station_id", "end_station_name"]:
                if field in mapped_dict and mapped_dict[field] is not None:
                    mapped_dict[field] = str(mapped_dict[field])
            
            # Ensure numeric fields are the right type and handle non-nullable fields
            if "tripduration" in mapped_dict and mapped_dict["tripduration"] is not None:
                try:
                    mapped_dict["tripduration"] = float(mapped_dict["tripduration"])
                except (ValueError, TypeError):
                    pass

            # Handle non-nullable fields for Avro schema
            if mapped_dict.get("usertype") is None:
                mapped_dict["usertype"] = "Unknown"
            
            if mapped_dict.get("birth_year") is None:
                mapped_dict["birth_year"] = 0
            else:
                try:
                    mapped_dict["birth_year"] = int(float(mapped_dict["birth_year"]))
                except (ValueError, TypeError):
                    mapped_dict["birth_year"] = 0
            
            if mapped_dict.get("gender") is None:
                mapped_dict["gender"] = 0
            else:
                try:
                    mapped_dict["gender"] = int(float(mapped_dict["gender"]))
                except (ValueError, TypeError):
                    mapped_dict["gender"] = 0

            for field in ["start_station_latitude", "start_station_longitude", "end_station_latitude", "end_station_longitude"]:
                if field in mapped_dict and mapped_dict[field] is not None:
                    try:
                        mapped_dict[field] = float(mapped_dict[field])
                    except (ValueError, TypeError):
                        pass
            
            result.append(mapped_dict)
        
        return result


class BikeStreamer:
    """Streamer for bike data"""
    
    def __init__(self, dataset_path: str = "./boston_datasets/bigdata/bike_data.parquet"):
        self.dataset_path = dataset_path
    
    def get_rows(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        """Get bike data rows for the specified time range"""
        return get_rows_from_bike_data(self.dataset_path, start_time, end_time)

