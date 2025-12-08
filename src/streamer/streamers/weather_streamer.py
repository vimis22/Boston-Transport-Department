"""Weather data streamer"""

from datetime import datetime
from typing import Any
import duckdb

def parse_temperature(temp_str: str | None) -> float | None:
    """
    Parse temperature from format like '-0128,1' or '+9999,9'
    Returns temperature in degrees Celsius, or None if missing data
    
    Format: [sign][4 digits],[quality_flag]
    - Sign: + or - (or missing for positive)
    - 4 digits: Temperature in tenths of a degree Celsius
    - Quality flag: Single digit (ignored for parsing)
    
    Examples:
        '-0128,1' -> -12.8°C
        '-0211,1' -> -21.1°C
        '+9999,9' -> None (missing data)
    """
    if not temp_str:
        return None
    
    # Check for missing data indicators
    if temp_str.startswith('+9999') or temp_str.startswith('-9999'):
        return None
    
    try:
        # Extract sign and numeric part (before comma)
        sign = -1 if temp_str.startswith('-') else 1
        numeric_part = temp_str.split(',')[0].lstrip('+-')
        
        # Convert to integer (tenths of degree) then divide by 10
        temp_tenths = int(numeric_part)
        temp_celsius = sign * (temp_tenths / 10.0)
        
        return temp_celsius
    except (ValueError, IndexError):
        return None




def get_rows_from_weather_data(dataset_path: str, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
    """Get weather observations within the interval [start_time, end_time]."""
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"SELECT * FROM read_parquet('{dataset_path}') "
            f"WHERE date >= '{start_time}' AND date <= '{end_time}' "
            f"ORDER BY date"
        )
        rows = conn.fetchall()
        columns = [desc[0] for desc in conn.description]
        
        # Convert rows to dictionaries with parsed temperatures
        result = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            
            # Parse and convert to Avro schema format
            # Convert DATE to string (handle datetime objects)
            observation_date = None
            if row_dict.get("DATE"):
                date_val = row_dict["DATE"]
                if isinstance(date_val, datetime):
                    observation_date = date_val.isoformat()
                else:
                    observation_date = str(date_val)
            
            # Convert types to match Avro schema (all fields are now required, no union types)
            # Use default values for missing fields
            parsed_row = {
                "station_id": int(row_dict["STATION"]) if row_dict.get("STATION") is not None else 0,
                "observation_date": observation_date if observation_date else "",
                "source_code": int(row_dict["SOURCE"]) if row_dict.get("SOURCE") is not None else 0,
                "latitude": float(row_dict["LATITUDE"]) if row_dict.get("LATITUDE") is not None else 0.0,
                "longitude": float(row_dict["LONGITUDE"]) if row_dict.get("LONGITUDE") is not None else 0.0,
                "elevation_meters": float(row_dict["ELEVATION"]) if row_dict.get("ELEVATION") is not None else 0.0,
                "station_name": str(row_dict["NAME"]) if row_dict.get("NAME") is not None else "",
                "report_type": str(row_dict["REPORT_TYPE"]) if row_dict.get("REPORT_TYPE") is not None else "",
                "call_sign": str(row_dict["CALL_SIGN"]) if row_dict.get("CALL_SIGN") is not None else "",
                "quality_control": str(row_dict["QUALITY_CONTROL"]) if row_dict.get("QUALITY_CONTROL") is not None else "",
                "wind_observation": str(row_dict["WND"]) if row_dict.get("WND") is not None else "",
                "sky_condition_observation": str(row_dict["CIG"]) if row_dict.get("CIG") is not None else "",
                "visibility_observation": str(row_dict["VIS"]) if row_dict.get("VIS") is not None else "",
                "dry_bulb_temperature_celsius": parse_temperature(row_dict.get("TMP")) or 0.0,
                "dew_point_temperature_celsius": parse_temperature(row_dict.get("DEW")) or 0.0,
                "sea_level_pressure": str(row_dict["SLP"]) if row_dict.get("SLP") is not None else "",
            }
            result.append(parsed_row)
        
        return result


class WeatherStreamer:
    """Streamer for weather data"""
    
    def __init__(self, dataset_path: str = "./boston_datasets/bigdata/weather_data.parquet"):
        self.dataset_path = dataset_path
    
    def get_rows(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        """Get weather data rows for the specified time range"""
        return get_rows_from_weather_data(self.dataset_path, start_time, end_time)

