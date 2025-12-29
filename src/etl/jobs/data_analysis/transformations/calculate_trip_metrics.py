"""
Calculate Trip Metrics - Single Responsibility Principle (SRP)

Dette modul har ÉT ansvar: At beregne distance og speed for bike trips.
Bruger standard Haversine formula (nem at forklare til eksamen!).
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sin, cos, atan2, sqrt, radians, when


# Earth radius i meter (standard konstant)
EARTH_RADIUS_METERS = 6371000


def calculate_trip_distance_and_speed(df: DataFrame) -> DataFrame:
    """
    Beregn distance (meters) og average speed (km/h) for bike trips.
    
    Bruger Haversine formula til at beregne distance mellem to GPS koordinater.
    
    Formula:
        a = sin²(Δlat/2) + cos(lat1) * cos(lat2) * sin²(Δlon/2)
        c = 2 * atan2(√a, √(1-a))
        distance = R * c
        
    hvor R = Earth radius (6371 km)
    
    Args:
        df: DataFrame med start/end station latitude/longitude kolonner
        
    Returns:
        DataFrame: Med tilføjede 'distance_meters' og 'average_speed_kmh' kolonner
        
    Eksempel:
        >>> bike_weather_df = join_bike_weather_data(bike_df, weather_df)
        >>> enriched_df = calculate_trip_distance_and_speed(bike_weather_df)
    """
    
    # Konverter latitude/longitude til radians (krævet af trigonometriske funktioner)
    lat1 = radians(col("start_station_latitude"))
    lon1 = radians(col("start_station_longitude"))
    lat2 = radians(col("end_station_latitude"))
    lon2 = radians(col("end_station_longitude"))
    
    # Beregn forskelle
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    # Haversine formula implementation
    a = (sin(dlat / 2) ** 2) + cos(lat1) * cos(lat2) * (sin(dlon / 2) ** 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = EARTH_RADIUS_METERS * c
    
    # Tilføj distance column
    df_with_distance = df.withColumn("distance_meters", distance)
    
    # Beregn average speed i km/h
    # speed (km/h) = (distance_meters / duration_seconds) * 3.6
    # Vi tjekker også at duration > 0 for at undgå division by zero
    df_with_speed = df_with_distance.withColumn(
        "average_speed_kmh",
        when(
            (col("tripduration") > 0) & (col("distance_meters") > 0),
            (col("distance_meters") / col("tripduration")) * 3.6
        ).otherwise(0.0)
    )
    
    return df_with_speed


def filter_outliers(df: DataFrame, max_speed_kmh: float = 50.0) -> DataFrame:
    """
    Filtrer outliers baseret på speed.
    
    Dashboard query'er med 'WHERE average_speed_kmh > 0 AND average_speed_kmh < 50'
    så vi filtrerer her for at matche dashboard forventninger.
    
    Args:
        df: DataFrame med 'average_speed_kmh' kolonne
        max_speed_kmh: Maximum realistisk cykel hastighed (default 50 km/h)
        
    Returns:
        DataFrame: Filtreret data uden outliers
    """
    
    return df.filter(
        (col("average_speed_kmh") > 0) & 
        (col("average_speed_kmh") < max_speed_kmh)
    )


def validate_coordinates(df: DataFrame) -> DataFrame:
    """
    Validér at GPS koordinater er indenfor realistiske grænser.
    
    Boston area bounds (approx):
        Latitude: 42.2 - 42.5
        Longitude: -71.2 - -70.9
        
    Args:
        df: DataFrame med station latitude/longitude kolonner
        
    Returns:
        DataFrame: Kun rækker med valide koordinater
    """
    
    return df.filter(
        # Start station coordinates
        (col("start_station_latitude").between(42.0, 42.6)) &
        (col("start_station_longitude").between(-71.3, -70.8)) &
        # End station coordinates
        (col("end_station_latitude").between(42.0, 42.6)) &
        (col("end_station_longitude").between(-71.3, -70.8))
    )
