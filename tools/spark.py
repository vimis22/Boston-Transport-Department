#!/usr/bin/env python3
"""
PySpark Notebook-Style Example: Interactive Data Analysis with HDFS and Hive

This script demonstrates a notebook-style workflow using PySpark to:
1. Connect to Spark cluster with Hive support
2. Explore Hive tables (openbeer.breweries)
3. Read data from HDFS
4. Perform transformations and analysis
5. Save results back to HDFS
6. Generate basic visualizations (if matplotlib available)

Prerequisites:
- PySpark installed: pip install pyspark==4.0.1
- Port forwards running:
  - kubectl port-forward -n bigdata svc/spark-primary 7077:7077
  - kubectl port-forward -n bigdata svc/hive-server-service 10000:10000
  - kubectl port-forward -n bigdata svc/namenode 9000:9000

Usage:
    python spark-notebook-example.py

Sections:
[1] Setup Spark Session with Hive Integration
[2] Explore Hive Metastore (Databases and Tables)
[3] Load and Explore Breweries Data from Hive
[4] Read Additional Data from HDFS
[5] Data Transformation and Analysis
[6] Create Aggregations and Insights
[7] Save Results to HDFS
[8] Generate Visualizations (Optional)
[9] Cleanup
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, when, length
from pyspark.sql.types import StringType, IntegerType
import matplotlib.pyplot as plt
import pandas as pd

# Optional: for advanced visualization
try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False
    print("Seaborn not available - basic matplotlib plots will be used")

print("=" * 60)
print("SPARK NOTEBOOK EXAMPLE: BREWERY DATA ANALYSIS")
print("=" * 60)

# =============================================================================
# [1] SETUP SPARK SESSION WITH HIVE INTEGRATION
# =============================================================================
print("\n[1] SETUP SPARK SESSION WITH HIVE INTEGRATION")
print("-" * 50)

# Create Spark session with Hive support
# spark = SparkSession.builder \
#     .appName("BreweryAnalysis-Notebook") \
#     .master("spark://localhost:7077") \
#     .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:10000") \
#     .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/spark-warehouse") \
#     .config("spark.sql.adaptive.enabled", "true") \
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#     .enableHiveSupport() \
#     .getOrCreate()
spark = SparkSession.builder.master("spark://localhost:7077").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

# Set log level to reduce verbosity
# spark.sparkContext.setLogLevel("WARN")

print(f"✓ Spark Session created: {spark.version}")
print(f"✓ Connected to Spark Master: spark://localhost:7077")
print(f"✓ Hive Metastore: thrift://localhost:10000")
print(f"✓ Warehouse: hdfs://localhost:9000/spark-warehouse")
print(f"✓ Default parallelism: {spark.sparkContext.defaultParallelism}")

# =============================================================================
# [2] EXPLORE HIVE METASTORE (DATABASES AND TABLES)
# =============================================================================
print("\n[2] EXPLORE HIVE METASTORE")
print("-" * 50)

# Show all databases
print("\nAvailable Databases:")
databases_df = spark.sql("SHOW DATABASES")
databases_df.show(20, truncate=False)
num_databases = databases_df.count()
print(f"Total databases: {num_databases}")

# Use the openbeer database (from docker setup)
spark.sql("USE openbeer")

# Show tables in openbeer database
print("\nTables in 'openbeer' database:")
tables_df = spark.sql("SHOW TABLES IN openbeer")
tables_df.show(truncate=False)

# Describe the breweries table
print("\nBreweries Table Schema:")
breweries_desc = spark.sql("DESCRIBE breweries")
breweries_desc.show(truncate=False)

# Get basic stats
print("\nBreweries Table Stats:")
stats_df = spark.sql("SELECT COUNT(*) as total_breweries FROM breweries")
stats_df.show()

# =============================================================================
# [3] LOAD AND EXPLORE BREWERIES DATA FROM HIVE
# =============================================================================
print("\n[3] LOAD AND EXPLORE BREWERIES DATA FROM HIVE")
print("-" * 50)

# Load breweries data from Hive table
breweries_df = spark.table("openbeer.breweries")

print(f"✓ Loaded {breweries_df.count()} brewery records")
print("\nFirst 10 breweries:")
breweries_df.show(10, truncate=False)

print("\nColumn summary:")
breweries_df.printSchema()

# Basic data profiling
print("\nData Profiling:")
print(f"  - Total rows: {breweries_df.count()}")
print(f"  - Columns: {len(breweries_df.columns)}")
print(f"  - Missing values per column:")
for col_name in breweries_df.columns:
    null_count = breweries_df.filter(col(col_name).isNull()).count()
    print(f"    {col_name}: {null_count} nulls")

# State distribution
print("\nTop 10 States by Brewery Count:")
state_counts = breweries_df.groupBy("state") \
    .agg(count("*").alias("count")) \
    .orderBy(desc("count")) \
    .limit(10)
state_counts.show(truncate=False)

# =============================================================================
# [4] READ ADDITIONAL DATA FROM HDFS
# =============================================================================
print("\n[4] READ ADDITIONAL DATA FROM HDFS")
print("-" * 50)

# Try to read the CSV file directly from HDFS (if available)
hdfs_path = "hdfs://localhost:9000/data/openbeer/breweries/breweries.csv"
try:
    print(f"Attempting to read raw CSV from HDFS: {hdfs_path}")
    raw_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(hdfs_path)
    
    print(f"✓ Successfully read {raw_df.count()} raw records from HDFS")
    print("Raw CSV sample:")
    raw_df.show(5, truncate=False)
    
    # Compare with Hive table
    print(f"\nRaw HDFS rows: {raw_df.count()}, Hive table rows: {breweries_df.count()}")
    
except Exception as e:
    print(f"⚠ Could not read from HDFS path: {e}")
    print("Using Hive table data only...")
    raw_df = None

# =============================================================================
# [5] DATA TRANSFORMATION AND ANALYSIS
# =============================================================================
print("\n[5] DATA TRANSFORMATION AND ANALYSIS")
print("-" * 50)

# Clean the data
clean_breweries = breweries_df.filter(
    col("name").isNotNull() & 
    col("city").isNotNull() & 
    col("state").isNotNull()
).withColumn(
    "name_length", 
    length(col("name"))
).withColumn(
    "is_large_name", 
    when(col("name_length") > 20, "Long").otherwise("Short")
)

print(f"✓ Cleaned data: {clean_breweries.count()} rows (removed nulls)")
print("\nSample of cleaned data:")
clean_breweries.select("name", "city", "state", "name_length", "is_large_name").show(10, truncate=False)

# Create a view for SQL queries
clean_breweries.createOrReplaceTempView("clean_breweries")

# Analysis 1: Brewery names by state
print("\nAnalysis 1: Brewery Name Length Analysis by State")
name_analysis = clean_breweries.groupBy("state", "is_large_name") \
    .agg(
        count("*").alias("count"),
        avg("name_length").alias("avg_name_length")
    ) \
    .orderBy(desc("count")) \
    .limit(15)

name_analysis.show(truncate=False)

# Analysis 2: Top cities by brewery count
print("\nAnalysis 2: Top 10 Cities by Brewery Count")
top_cities = clean_breweries.groupBy("city", "state") \
    .agg(count("*").alias("brewery_count")) \
    .orderBy(desc("brewery_count")) \
    .limit(10)

top_cities.show(truncate=False)

# SQL example using the temp view
print("\nAnalysis 3: SQL Query Example")
sql_result = spark.sql("""
    SELECT 
        state,
        COUNT(*) as brewery_count,
        AVG(name_length) as avg_name_length,
        MIN(name) as shortest_name,
        MAX(name) as longest_name
    FROM clean_breweries
    GROUP BY state
    HAVING brewery_count > 5
    ORDER BY brewery_count DESC
    LIMIT 10
""")
sql_result.show(truncate=False)

# =============================================================================
# [6] CREATE AGGREGATIONS AND INSIGHTS
# =============================================================================
print("\n[6] CREATE AGGREGATIONS AND INSIGHTS")
print("-" * 50)

# State-level insights
state_insights = clean_breweries.groupBy("state") \
    .agg(
        count("*").alias("total_breweries"),
        avg("name_length").alias("avg_name_length"),
        countDistinct("city").alias("unique_cities")
    ) \
    .orderBy(desc("total_breweries"))

print("State-level insights (top 10):")
state_insights.show(10, truncate=False)

# Find breweries with longest names
print("\nBreweries with longest names (top 5):")
long_names = clean_breweries.select("name", "city", "state", "name_length") \
    .filter(col("name_length") > 25) \
    .orderBy(desc("name_length")) \
    .limit(5)

long_names.show(truncate=False)

# =============================================================================
# [7] SAVE RESULTS TO HDFS
# =============================================================================
print("\n[7] SAVE RESULTS TO HDFS")
print("-" * 50)

# Create output directory in HDFS
output_path = "hdfs://localhost:9000/spark-notebook-output"
spark.sql(f"CREATE DATABASE IF NOT EXISTS notebook_results")
spark.sql(f"DROP TABLE IF EXISTS notebook_results.state_insights")

# Save state insights as Hive table
state_insights.write \
    .mode("overwrite") \
    .saveAsTable("notebook_results.state_insights")

print(f"✓ State insights saved as Hive table: notebook_results.state_insights")

# Save as Parquet to HDFS
parquet_path = f"{output_path}/state_insights.parquet"
state_insights.write \
    .mode("overwrite") \
    .parquet(parquet_path)

print(f"✓ Parquet file saved to: {parquet_path}")

# Save top cities as JSON
json_path = f"{output_path}/top_cities.json"
top_cities.write \
    .mode("overwrite") \
    .json(json_path)

print(f"✓ JSON file saved to: {json_path}")

# Verify HDFS output
print("\nVerify HDFS output:")
try:
    parquet_count = spark.read.parquet(parquet_path).count()
    json_count = spark.read.json(json_path).count()
    print(f"  - Parquet file: {parquet_count} rows")
    print(f"  - JSON file: {json_count} rows")
    print(f"  - Hive table: {spark.sql('SELECT COUNT(*) FROM notebook_results.state_insights').collect()[0][0]} rows")
except Exception as e:
    print(f"  - Verification error: {e}")

# =============================================================================
# [8] GENERATE VISUALIZATIONS (OPTIONAL)
# =============================================================================
print("\n[8] GENERATE VISUALIZATIONS")
print("-" * 50)

try:
    # Convert to Pandas for plotting
    state_pd = state_insights.limit(20).toPandas()
    
    plt.figure(figsize=(12, 6))
    
    if HAS_SEABORN:
        sns.barplot(data=state_pd, x="total_breweries", y="state", palette="viridis")
        plt.title("Top 20 States by Number of Breweries")
    else:
        plt.barh(range(len(state_pd)), state_pd["total_breweries"])
        plt.yticks(range(len(state_pd)), state_pd["state"])
        plt.xlabel("Number of Breweries")
        plt.title("Top 20 States by Number of Breweries")
    
    plt.tight_layout()
    plt.savefig("brewery_analysis.png", dpi=300, bbox_inches='tight')
    plt.show()
    
    print("✓ Visualization saved as 'brewery_analysis.png'")
    
    # Additional plot: Name length distribution
    name_lengths = clean_breweries.select("name_length").rdd.flatMap(lambda x: x).collect()
    plt.figure(figsize=(10, 6))
    plt.hist(name_lengths, bins=30, alpha=0.7, edgecolor='black')
    plt.xlabel("Brewery Name Length (characters)")
    plt.ylabel("Frequency")
    plt.title("Distribution of Brewery Name Lengths")
    plt.grid(True, alpha=0.3)
    plt.savefig("name_length_distribution.png", dpi=300, bbox_inches='tight')
    plt.show()
    
    print("✓ Name length distribution saved as 'name_length_distribution.png'")
    
except Exception as e:
    print(f"⚠ Visualization error (matplotlib/pandas may not be installed): {e}")
    print("Install with: pip install matplotlib pandas seaborn")

# =============================================================================
# [9] CLEANUP AND SUMMARY
# =============================================================================
print("\n[9] CLEANUP AND SUMMARY")
print("-" * 50)

# Summary statistics
total_processed = clean_breweries.count()
unique_states = clean_breweries.select("state").distinct().count()
unique_cities = clean_breweries.select("city").distinct().count()

print(f"SUMMARY:")
print(f"  • Total breweries processed: {total_processed:,}")
print(f"  • Unique states: {unique_states}")
print(f"  • Unique cities: {unique_cities}")
print(f"  • Average name length: {clean_breweries.agg(avg('name_length')).collect()[0][0]:.1f} characters")
print(f"  • Output saved to: hdfs://localhost:9000/spark-notebook-output/")
print(f"  • Hive table created: notebook_results.state_insights")
print(f"  • Visualizations: brewery_analysis.png, name_length_distribution.png")

# Cleanup: Stop Spark session
spark.stop()
print("\n✓ Spark session stopped. Analysis complete!")

print("\n" + "=" * 60)
print("Next Steps:")
print("1. Verify results in Hive: SELECT * FROM notebook_results.state_insights LIMIT 10;")
print("2. Check HDFS output: hdfs dfs -ls hdfs://localhost:9000/spark-notebook-output/")
print("3. View in Spark UI: http://localhost:8080 (port-forward svc/spark-master 8080:8080)")
print("4. Extend this script with your own analysis sections")
print("=" * 60)
