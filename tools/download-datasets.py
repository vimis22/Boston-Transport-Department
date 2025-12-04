#!/usr/bin/env python3
# Upload datasets to hadoop for the data streamers

import gdown
import zipfile
import duckdb
import os
import requests
import urllib.parse

DATASETS_DIR = "./boston_datasets/bigdata"

def download_datasets():
    output = "boston_datasets.zip"
    # Download the boston datasets zip file from google drive: https://drive.google.com/file/d/1vpr1K0D_YVqKeT7pH7d1aoAaGxsK8vSk/view?usp=sharing
    gdown.download(id="1vpr1K0D_YVqKeT7pH7d1aoAaGxsK8vSk", output=output, quiet=False, resume=True, )
    with zipfile.ZipFile(output, 'r') as zip_ref:
        zip_ref.extractall("boston_datasets")

def convert_to_parquet():
    db = duckdb.connect(":memory:")

    # Convert bike data to parquet, parsing lat/lon as DOUBLE, sort by starttime
    if not os.path.exists(f"{DATASETS_DIR}/bike_data.parquet"):
        print("Converting bike data to parquet...")
        db.execute(f"""
            COPY (
                SELECT 
                    tripduration,
                    starttime,
                    stoptime,
                    "start station id",
                    "start station name",
                    TRY_CAST("start station latitude" AS DOUBLE) AS "start station latitude",
                    TRY_CAST("start station longitude" AS DOUBLE) AS "start station longitude",
                    "end station id",
                    "end station name",
                    TRY_CAST("end station latitude" AS DOUBLE) AS "end station latitude",
                    TRY_CAST("end station longitude" AS DOUBLE) AS "end station longitude",
                    bikeid,
                    usertype
                FROM read_csv_auto('{DATASETS_DIR}/bikedata/*.csv', header=true, union_by_name=true, nullstr='\\N')
                ORDER BY starttime
            )
            TO '{DATASETS_DIR}/bike_data.parquet'
            (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
        """)
    else:
        print("Bike data already converted to parquet")

    # Convert weather data to parquet, parsing lat/lon as DOUBLE, sort by DATE
    if not os.path.exists(f"{DATASETS_DIR}/weather_data.parquet"):
        print("Converting weather data to parquet...")
        db.execute(f"""
            COPY (
                SELECT 
                    STATION,
                    DATE,
                    SOURCE,
                    TRY_CAST(LATITUDE AS DOUBLE) AS LATITUDE,
                    TRY_CAST(LONGITUDE AS DOUBLE) AS LONGITUDE,
                    ELEVATION,
                    NAME,
                    REPORT_TYPE,
                    CALL_SIGN,
                    QUALITY_CONTROL,
                    WND,
                    CIG,
                    VIS,
                    TMP,
                    DEW,
                    SLP,
                    AA1, AA2, AA3, AB1, AD1, AE1, AH1, AH2, AH3, AH4, AH5, AH6,
                    AI1, AI2, AI3, AI4, AI5, AI6, AL1, AM1, AN1,
                    AT1, AT2, AT3, AT4, AT5, AT6, AT7,
                    AU1, AU2, AU3, AU4, AW1, AW2, AW3, AW4, AW5,
                    AX1, AX2, AX3, AX4, ED1,
                    GA1, GA2, GA3, GA4, GD1, GD2, GD3, GD4, GE1, GF1,
                    KA1, KA2, KB1, KB2, KB3, KC1, KC2, KD1, KD2, KE1, KG1, KG2,
                    MA1, MD1, MF1, MG1, MH1, MK1, MV1, MW1, MW2, MW3,
                    OC1, OD1, OE1, OE2, OE3,
                    RH1, RH2, RH3, REM, EQD
                FROM read_csv_auto('{DATASETS_DIR}/weather/*.csv', header=true, union_by_name=true, nullstr='\\N')
                ORDER BY DATE
            )
            TO '{DATASETS_DIR}/weather_data.parquet'
            (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
        """)
    else:
        print("Weather data already converted to parquet")

    # Convert taxi data to parquet, parsing lat/lon as DOUBLE, sort by datetime
    if not os.path.exists(f"{DATASETS_DIR}/taxi_data.parquet"):
        print("Converting taxi data to parquet...")
        db.execute(f"""
            COPY (
                SELECT 
                    id,
                    timestamp,
                    hour,
                    day,
                    month,
                    datetime,
                    timezone,
                    source,
                    destination,
                    cab_type,
                    product_id,
                    name,
                    price,
                    distance,
                    surge_multiplier,
                    TRY_CAST(latitude AS DOUBLE) AS latitude,
                    TRY_CAST(longitude AS DOUBLE) AS longitude,
                    temperature,
                    apparentTemperature,
                    short_summary,
                    long_summary,
                    precipIntensity,
                    precipProbability,
                    humidity,
                    windSpeed,
                    windGust,
                    windGustTime,
                    visibility,
                    temperatureHigh,
                    temperatureHighTime,
                    temperatureLow,
                    temperatureLowTime,
                    apparentTemperatureHigh,
                    apparentTemperatureHighTime,
                    apparentTemperatureLow,
                    apparentTemperatureLowTime,
                    icon,
                    dewPoint,
                    pressure,
                    windBearing,
                    cloudCover,
                    uvIndex,
                    "visibility.1" AS visibility_alt,
                    ozone,
                    sunriseTime,
                    sunsetTime,
                    moonPhase,
                    precipIntensityMax,
                    uvIndexTime,
                    temperatureMin,
                    temperatureMinTime,
                    temperatureMax,
                    temperatureMaxTime,
                    apparentTemperatureMin,
                    apparentTemperatureMinTime,
                    apparentTemperatureMax,
                    apparentTemperatureMaxTime
                FROM read_csv_auto('{DATASETS_DIR}/taxi/*.csv', header=true, union_by_name=true, nullstr='\\N')
                ORDER BY datetime
            )
            TO '{DATASETS_DIR}/taxi_data.parquet'
            (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
        """)
    else:
        print("Taxi data already converted to parquet")

    print("Done converting to parquet")

    db.close()

def upload_to_hadoop():
    print("Uploading to hadoop...")
    
    def upload_file(local_path: str, hdfs_path: str):
        """Upload a file to HDFS using WebHDFS REST API."""
        # Step 1: Initial request to get redirect URL
        url = f"http://localhost:9870/webhdfs/v1{hdfs_path}?user.name=stackable&op=CREATE&overwrite=true"
        
        response = requests.put(url, allow_redirects=False)
        
        if response.status_code != 307:
            print(f"Failed to get redirect: {response.status_code}")
            return False
        
        # Step 2: Rewrite redirect URL to use localhost:9864
        redirect_url = response.headers["Location"]
        parts = urllib.parse.urlparse(redirect_url)
        new_redirect_url = parts._replace(netloc="localhost:9864").geturl()
        
        print(f"Uploading {local_path} to {hdfs_path}...")
        
        # Step 3: Follow redirect to actually create the file
        with open(local_path, "rb") as f:
            response = requests.put(new_redirect_url, data=f, allow_redirects=False)
        
        if response.status_code == 201:
            print(f"Successfully uploaded {local_path}")
            return True
        else:
            print(f"Failed to upload file: {response.status_code}")
            return False
    
    print("Uploading bike data to hadoop...")
    upload_file(f"{DATASETS_DIR}/bike_data.parquet", "/bigdata/bike_data.parquet")
    print("Uploading weather data to hadoop...")
    upload_file(f"{DATASETS_DIR}/weather_data.parquet", "/bigdata/weather_data.parquet")
    print("Uploading taxi data to hadoop...")
    upload_file(f"{DATASETS_DIR}/taxi_data.parquet", "/bigdata/taxi_data.parquet")
    print("Uploading crime data to hadoop...")
    upload_file(f"{DATASETS_DIR}/accident/tmpw8i0zd4_.csv", "/bigdata/accidents.csv")
    print("Done uploading to hadoop")

if __name__ == "__main__":
    download_datasets()
    convert_to_parquet()
    upload_to_hadoop()
