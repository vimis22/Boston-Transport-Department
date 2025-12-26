#!/usr/bin/env python3
# Upload datasets to hadoop for the data streamers

import gdown
import zipfile
import duckdb
import os
import sys
import argparse
import requests
import socket
import subprocess
import time
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
                WITH raw_data AS (
                    SELECT * 
                    FROM read_csv_auto('{DATASETS_DIR}/weather/*.csv', header=true, union_by_name=true, nullstr='\\N')
                )
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
                    RH1, RH2, RH3, REM, EQD,
                    LAST_VALUE(
                        CASE 
                            WHEN AA1 LIKE '24,%' THEN CAST(str_split(AA1, ',')[2] AS INTEGER)
                            ELSE NULL
                        END IGNORE NULLS
                    ) OVER (
                        PARTITION BY STATION 
                        ORDER BY DATE
                    ) AS PRECIP_DAILY_MM

                FROM raw_data
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

def upload_to_hadoop(namenode_url: str, datanode_netloc: str):
    print(f"Uploading to hadoop (NameNode: {namenode_url}, DataNode: {datanode_netloc})...")
    
    def upload_file(local_path: str, hdfs_path: str):
        """Upload a file to HDFS using WebHDFS REST API."""
        # Step 1: Initial request to get redirect URL
        url = f"{namenode_url}/webhdfs/v1{hdfs_path}?user.name=stackable&op=CREATE&overwrite=true"
        
        response = requests.put(url, allow_redirects=False)
        
        if response.status_code != 307:
            print(f"Failed to get redirect: {response.status_code}")
            return False
        
        # Step 2: Rewrite redirect URL to use local datanode address
        redirect_url = response.headers["Location"]
        parts = urllib.parse.urlparse(redirect_url)
        new_redirect_url = parts._replace(netloc=datanode_netloc).geturl()
        
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

def main():
    parser = argparse.ArgumentParser(
        description="Upload datasets to hadoop for the data streamers"
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default="bigdata",
        help="Kubernetes namespace to use (default: bigdata)",
    )
    parser.add_argument(
        "--kubeconfig",
        type=str,
        default=None,
        help="Path to kubeconfig file",
    )
    parser.add_argument(
        "--context",
        type=str,
        default=None,
        help="Kubernetes context to use",
    )
    parser.add_argument(
        "--namenode-url",
        type=str,
        default=None,
        help="HDFS NameNode WebHDFS URL (default: auto-detect via kubectl port-forward)",
    )
    parser.add_argument(
        "--datanode-url",
        type=str,
        default=None,
        help="HDFS DataNode WebHDFS URL (default: auto-detect via kubectl port-forward)",
    )
    
    args = parser.parse_args()
    
    download_datasets()
    convert_to_parquet()
    
    # Port forwarding setup
    nn_proc = None
    dn_proc = None
    
    try:
        if args.namenode_url is None:
            print(f"Auto-detecting HDFS NameNode in namespace '{args.namespace}'...")
            with socket.socket() as s:
                s.bind(('', 0))
                nn_port = s.getsockname()[1]
            
            cmd = ["kubectl"]
            if args.kubeconfig:
                cmd.extend(["--kubeconfig", args.kubeconfig])
            if args.context:
                cmd.extend(["--context", args.context])
            cmd.extend(["-n", args.namespace, "port-forward", "svc/hdfs-namenode", f"{nn_port}:9870"])

            nn_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Give it a moment to check if it started successfully
            time.sleep(2)
            if nn_proc.poll() is not None:
                stdout, stderr = nn_proc.communicate()
                print(f"❌ Failed to start kubectl port-forward for NameNode:\n{stderr}\n{stdout}", file=sys.stderr)
                sys.exit(1)
                
            namenode_url = f"http://localhost:{nn_port}"
        else:
            namenode_url = args.namenode_url.rstrip("/")
            
        if args.datanode_url is None:
            print(f"Auto-detecting HDFS DataNode in namespace '{args.namespace}'...")
            with socket.socket() as s:
                s.bind(('', 0))
                dn_port = s.getsockname()[1]
            
            cmd = ["kubectl"]
            if args.kubeconfig:
                cmd.extend(["--kubeconfig", args.kubeconfig])
            if args.context:
                cmd.extend(["--context", args.context])
            cmd.extend(["-n", args.namespace, "port-forward", "svc/hdfs-datanode", f"{dn_port}:9864"])

            dn_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Give it a moment to check if it started successfully
            time.sleep(2)
            if dn_proc.poll() is not None:
                stdout, stderr = dn_proc.communicate()
                print(f"❌ Failed to start kubectl port-forward for DataNode:\n{stderr}\n{stdout}", file=sys.stderr)
                sys.exit(1)
                
            datanode_netloc = f"localhost:{dn_port}"
        else:
            parts = urllib.parse.urlparse(args.datanode_url)
            datanode_netloc = parts.netloc
            
        time.sleep(1) # Wait a bit more for port-forwards to fully establish
        
        upload_to_hadoop(namenode_url, datanode_netloc)
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if nn_proc:
            nn_proc.terminate()
            nn_proc.wait()
        if dn_proc:
            dn_proc.terminate()
            dn_proc.wait()

if __name__ == "__main__":
    main()
