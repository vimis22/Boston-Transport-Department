# Readme for ETL_DATA ANALYSIS - Main & Config

## main.py

### What is main.py?
The `main.py` file is the **orchestrator** (dirigent) of the entire ETL pipeline. It's like a conductor in an orchestra - it doesn't play music itself, but it tells everyone when to start, what to do, and coordinates everything.

When you run `python -m etl_dataanalysis.main`, this file starts everything!

---

### What does main.py do?

#### 1. Create Spark Session
First, `main.py` creates a Spark Session - this is the connection to Apache Spark that lets us process streaming data.

```python
def create_spark_session():
    return SparkSession.builder.config(conf=config.spark_config).getOrCreate()
```

**What happens here:**
- Takes configuration from `config.py` (Kafka settings, checkpoint paths, etc.)
- Creates a Spark session that can read Kafka streams
- Returns the session so we can use it

---

#### 2. Read Kafka Streams
Next, it connects to Kafka topics and starts reading data streams.

```python
def read_kafka_stream(spark, topic):
    return spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
```

**What happens here:**
- Connects to Kafka broker (e.g., `localhost:9092`)
- Subscribes to a specific topic (e.g., `bike-trips`, `taxi-trips`, `weather-data`)
- Reads from the earliest available message
- Returns a streaming DataFrame

**Important:** This doesn't download all data at once - it continuously reads new messages as they arrive!

---

#### 3. Parse the Streams
After reading raw Kafka messages, it parses them into structured data.

```python
bike_raw = read_kafka_stream(spark, config.KAFKA_TOPICS["bike"])
bike_df = parse_bike_stream(bike_raw)
```

**What happens here:**
- `bike_raw` contains raw JSON strings from Kafka
- `parse_bike_stream()` (from transformations.py) converts JSON → columns
- `bike_df` now has structured columns: `trip_id`, `duration_seconds`, `start_time`, etc.

This happens for all 4 data sources:
- Bikes → `parse_bike_stream()`
- Taxis → `parse_taxi_stream()`
- Weather → `parse_weather_stream()` (also enriches weather!)
- Accidents → `parse_accident_stream()`

---

#### 4. Write to Parquet (Basic ETL)
The parsed data is written to disk as Parquet files.

```python
def write_parquet_stream(df, subfolder):
    output_path = f"{config.OUTPUT_BASE_PATH}/{subfolder}"
    checkpoint_path = f"{config.CHECKPOINT_BASE_PATH}/{subfolder}"

    return df.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("year", "month", "date", "hour")
        .outputMode("append")
        .trigger(processingTime=config.BATCH_INTERVAL)
        .start()
```

**What happens here:**
- `output_path`: Where to save the Parquet files (e.g., `/data/processed_simple/bike_trips/`)
- `checkpoint_path`: Where Spark saves its state (for fault tolerance)
- `partitionBy`: Organizes files by year/month/date/hour (makes queries faster!)
- `outputMode("append")`: Only add new data (don't rewrite everything)
- `trigger(processingTime="10 seconds")`: Process new data every 10 seconds

**Result:** Clean, structured data saved on disk for later analysis.

---

#### 5. Run Analytics Streams
This is where the magic happens! After basic ETL, `main.py` runs all the analytics functions.

##### Original Analytics (4 streams):

```python
# Analytics 1: Weather-Transport Correlation
combined_df = create_combined_transport_weather_window(bike_df, taxi_df, weather_df)
correlation_df = calculate_weather_transport_correlation(combined_df)
write_analytics_stream(correlation_df, "weather_transport_correlation")

# Analytics 2: Weather-Safety Risk
safety_df = calculate_weather_safety_risk(accident_df, weather_df)
write_analytics_stream(safety_df, "weather_safety_analysis")

# Analytics 3: Surge-Weather Correlation
surge_df = calculate_surge_weather_correlation(taxi_df)
write_analytics_stream(surge_df, "surge_weather_correlation")

# Analytics 4: Transport Usage Summary
summary_df = generate_transport_usage_summary(bike_df, taxi_df)
write_analytics_stream(summary_df, "transport_usage_summary")
```

##### New Enhanced Analytics (5 streams) ✨:

```python
# Analytics 5: Pearson Correlations
pearson_df = calculate_pearson_correlations(combined_df)
write_analytics_stream(pearson_df, "pearson_correlations")

# Analytics 6: Binned Aggregations (for Oskar's graphs)
binned_df = calculate_binned_weather_aggregations(combined_df)
write_analytics_stream(binned_df, "weather_binned_metrics")

# Analytics 7: Precipitation Impact
precip_df = calculate_precipitation_impact_analysis(combined_df)
write_analytics_stream(precip_df, "precipitation_impact")

# Analytics 8: Temporal Correlations
temporal_df = calculate_temporal_segmented_correlations(combined_df)
write_analytics_stream(temporal_df, "temporal_correlations")

# Analytics 9: Multi-Variable Summary
multi_var_df = calculate_multi_variable_correlation_summary(combined_df)
write_analytics_stream(multi_var_df, "multi_variable_summary")
```

**What happens here:**
- Takes the parsed data (bikes, taxis, weather, accidents)
- Runs analytics functions from `analytics.py`
- Writes results to separate folders in `/data/analytics/`
- Each analytics stream runs **continuously** and updates as new data arrives

---

#### 6. Track All Queries
All streaming queries are tracked in a list so we can monitor and stop them.

```python
queries = []
queries.append(bike_query)
queries.append(taxi_query)
queries.append(weather_query)
# ... etc for all 13 streams

# Wait for all queries to finish (they run forever until you stop them)
for query in queries:
    query.awaitTermination()
```

**What happens here:**
- Each `.start()` returns a query object
- We store all queries in a list
- `awaitTermination()` keeps the program running
- Press `Ctrl+C` to stop all streams gracefully

---

#### 7. Handle Errors and Cleanup
If something goes wrong or you stop the pipeline, it cleans up properly.

```python
try:
    # Run all streams
    ...
except KeyboardInterrupt:
    logger.info("Stopping ETL (KeyboardInterrupt)")
    for query in queries:
        query.stop()
except Exception:
    logger.exception("Error in ETL")
finally:
    spark.stop()
    logger.info("ETL finished")
```

**What happens here:**
- `KeyboardInterrupt`: You pressed Ctrl+C → stop all streams gracefully
- `Exception`: Something crashed → log the error
- `finally`: Always close Spark session (cleanup)

---

### The Complete Flow in main.py

```
START
  │
  ├─> Create Spark Session
  │
  ├─> Read 4 Kafka Topics
  │   ├─ bike-trips
  │   ├─ taxi-trips
  │   ├─ weather-data
  │   └─ accidents
  │
  ├─> Parse Each Stream (JSON → Columns)
  │   ├─ parse_bike_stream()
  │   ├─ parse_taxi_stream()
  │   ├─ parse_weather_stream() + enrichment
  │   └─ parse_accident_stream()
  │
  ├─> Write Basic ETL to Parquet
  │   ├─ /data/processed_simple/bike_trips/
  │   ├─ /data/processed_simple/taxi_trips/
  │   ├─ /data/processed_simple/weather_data/
  │   └─ /data/processed_simple/accidents/
  │
  ├─> Run 4 Original Analytics Streams
  │   ├─ weather_transport_correlation
  │   ├─ weather_safety_analysis
  │   ├─ surge_weather_correlation
  │   └─ transport_usage_summary
  │
  ├─> Run 5 Enhanced Analytics Streams ✨
  │   ├─ pearson_correlations
  │   ├─ weather_binned_metrics
  │   ├─ precipitation_impact
  │   ├─ temporal_correlations
  │   └─ multi_variable_summary
  │
  ├─> Wait for Streams (run forever)
  │
  └─> Cleanup on Stop/Error
```

---

### What You See When Running main.py

When you run the pipeline, you'll see this output:

```
=== Starting Boston Transport ETL with Data Analysis ===
Spark Version: 3.5.0
Kafka: localhost:9092
Output base: /data/processed_simple
Analytics output: /data/analytics

Step 1: Reading and parsing Kafka streams...
✓ Basic ETL streams started

Step 2: Setting up analytics streams...
  - Creating weather-transport correlation stream...
  ✓ Weather-transport correlation stream started
  - Creating weather-safety analysis stream...
  ✓ Weather-safety analysis stream started
  - Creating surge-weather correlation stream...
  ✓ Surge-weather correlation stream started
  - Creating transport usage summary stream...
  ✓ Transport usage summary stream started

Step 3: Setting up enhanced academic analytics streams...
  - Creating Pearson correlation metrics stream...
  ✓ Pearson correlation stream started
  - Creating binned weather aggregations stream...
  ✓ Binned aggregations stream started
  - Creating precipitation impact analysis stream...
  ✓ Precipitation impact stream started
  - Creating temporal segmented correlations stream...
  ✓ Temporal correlation stream started
  - Creating multi-variable correlation summary stream...
  ✓ Multi-variable summary stream started

=== ALL STREAMS STARTED (13 total) ===
Raw data → /data/processed_simple/
Analytics → /data/analytics/

📊 ENHANCED ACADEMIC ANALYTICS ENABLED 📊
  ✓ Pearson correlations
  ✓ Binned aggregations (graph-ready)
  ✓ Precipitation impact analysis
  ✓ Temporal segmentation (rush hour vs leisure)
  ✓ Multi-variable correlation summary

Streaming queries are running. Press Ctrl+C to stop.
```

---

### Key Functions in main.py

#### `create_spark_session()`
- **Purpose**: Create Spark connection
- **Returns**: SparkSession object

#### `read_kafka_stream(spark, topic)`
- **Purpose**: Connect to Kafka and read a topic
- **Input**: Spark session + topic name (e.g., "bike-trips")
- **Returns**: Streaming DataFrame with raw Kafka messages

#### `write_parquet_stream(df, subfolder)`
- **Purpose**: Write parsed data to Parquet files
- **Input**: DataFrame + subfolder name (e.g., "bike_trips")
- **Output**: Parquet files in `/data/processed_simple/{subfolder}/`

#### `write_analytics_stream(df, subfolder, output_mode)`
- **Purpose**: Write analytics results to Parquet files
- **Input**: DataFrame + subfolder name + output mode
- **Output**: Parquet files in `/data/analytics/{subfolder}/`
- **Note**: Similar to `write_parquet_stream()` but for analytics outputs

#### `main()`
- **Purpose**: The main orchestrator function
- **Does everything**: Creates session → reads streams → parses → writes ETL → runs analytics → waits

---

### Why main.py Matters for Your Exam

When explaining the pipeline, you should mention:

1. **Orchestration**: `main.py` coordinates all components (transformations, analytics, windowing)

2. **Streaming Architecture**: Uses Spark Structured Streaming (not batch processing!)
   - Continuously reads from Kafka
   - Processes micro-batches every 10 seconds
   - Never stops (runs 24/7 in production)

3. **Fault Tolerance**: Checkpointing ensures no data loss
   - If pipeline crashes, it resumes from last checkpoint
   - Uses write-ahead logs

4. **Separation of Concerns**:
   - Raw ETL → `/data/processed_simple/`
   - Analytics → `/data/analytics/`
   - Dashboard queries analytics, not raw data (faster!)

5. **Scalability**: 13 independent streams running in parallel
   - Each stream can scale independently
   - Can disable streams via config flags

---

## config.py

### What is config.py?
The `config.py` file contains all the **configuration settings** for the ETL pipeline. Think of it as the "control panel" where you can adjust how the pipeline behaves without changing the actual code.

It's like the settings menu in a video game - you can change things without rewriting the game!

---

### What does config.py contain?

#### 1. Kafka Configuration

```python
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = {
    "bike": "bike-trips",
    "taxi": "taxi-trips",
    "weather": "weather-data",
}
```

**What this means:**
- `KAFKA_BOOTSTRAP_SERVERS`: Where is Kafka running?
  - Default: `localhost:9092` (local development)
  - Production: Could be `kafka-broker:9092` or IP address
  - `os.getenv()` reads from environment variable (lets you change without editing code!)

- `KAFKA_TOPICS`: Which Kafka topics to read from?
  - Maps friendly names ("bike") to actual topic names ("bike-trips")
  - Makes code cleaner: `config.KAFKA_TOPICS["bike"]` instead of hardcoding "bike-trips"

**For your exam:** Explain that Kafka is the **message broker** that streams receive data from in real-time.

---

#### 2. Spark Configuration

```python
SPARK_APP_NAME = "ETL Data Analysis"
BATCH_INTERVAL = "10 seconds"
```

**What this means:**
- `SPARK_APP_NAME`: Name shown in Spark UI (http://localhost:4040)
  - Helps identify your application when multiple Spark jobs are running

- `BATCH_INTERVAL`: How often to process new data?
  - `"10 seconds"`: Check Kafka every 10 seconds for new messages
  - Faster = more real-time, but more processing overhead
  - Slower = less overhead, but less real-time

**For your exam:** This is a **micro-batch** processing interval. Spark Structured Streaming processes data in small batches, not true real-time (which would be event-by-event).

---

#### 3. Output Paths

```python
OUTPUT_BASE_PATH = os.getenv("OUTPUT_BASE_PATH", "/data/processed_simple")
CHECKPOINT_BASE_PATH = os.getenv("CHECKPOINT_BASE_PATH", "/tmp/spark_checkpoints_simple")

ANALYTICS_OUTPUT_PATH = os.getenv("ANALYTICS_OUTPUT_PATH", "/data/analytics")
ANALYTICS_CHECKPOINT_PATH = os.getenv("ANALYTICS_CHECKPOINT_PATH", "/tmp/spark_checkpoints_analytics")
```

**What this means:**

- `OUTPUT_BASE_PATH`: Where to save raw processed data (basic ETL)
  - Example: `/data/processed_simple/bike_trips/year=2018/month=01/...`

- `CHECKPOINT_BASE_PATH`: Where Spark saves its internal state
  - Used for fault tolerance (if pipeline crashes, resume from here)
  - Should be on reliable storage (HDFS, S3, etc.)

- `ANALYTICS_OUTPUT_PATH`: Where to save analytics results
  - Example: `/data/analytics/weather_binned_metrics/year=2018/...`

- `ANALYTICS_CHECKPOINT_PATH`: Checkpoints for analytics streams
  - Separate from raw ETL checkpoints

**Why separate paths?**
- Raw data is large → different storage tier
- Analytics are smaller but more valuable → maybe faster storage
- Easier to manage and query separately

**For your exam:** Checkpointing is essential for **exactly-once processing** guarantee - no duplicate data, no missing data.

---

#### 4. Window Durations

```python
WINDOW_DURATION_SHORT = "5 minutes"   # For real-time metrics
WINDOW_DURATION_MEDIUM = "15 minutes" # For correlations
WINDOW_DURATION_LONG = "1 hour"       # For safety analysis
SLIDE_DURATION = "5 minutes"          # Sliding window interval
```

**What this means:**

- `WINDOW_DURATION_SHORT` (5 minutes): Fast-moving metrics
  - Use case: Real-time dashboard updates
  - Example: "Show current bike rentals (last 5 minutes)"

- `WINDOW_DURATION_MEDIUM` (15 minutes): Most analytics
  - Use case: Weather-transport correlations
  - Balance between granularity and statistical significance
  - Enough trips in 15 minutes to detect patterns

- `WINDOW_DURATION_LONG` (1 hour): Slower-moving metrics
  - Use case: Safety risk analysis, hourly summaries
  - Accidents are rare events → need longer window for meaningful counts

- `SLIDE_DURATION` (5 minutes): How often windows update
  - If you have 15-minute window sliding every 5 minutes:
    ```
    Window 1: 08:00-08:15
    Window 2: 08:05-08:20  (overlaps with Window 1!)
    Window 3: 08:10-08:25
    ```
  - More frequent updates, smoother trends

**For your exam:** Explain **tumbling windows** (no overlap) vs **sliding windows** (overlap). Our analytics use tumbling windows (slide = window duration) for simplicity.

---

#### 5. Analytics Feature Flags

```python
# Original Analytics
ENABLE_WEATHER_TRANSPORT_CORRELATION = os.getenv("...", "true").lower() == "true"
ENABLE_WEATHER_SAFETY_ANALYSIS = os.getenv("...", "true").lower() == "true"
ENABLE_SURGE_WEATHER_CORRELATION = os.getenv("...", "true").lower() == "true"
ENABLE_TRANSPORT_USAGE_SUMMARY = os.getenv("...", "true").lower() == "true"

# New Enhanced Analytics ✨
ENABLE_PEARSON_CORRELATIONS = os.getenv("...", "true").lower() == "true"
ENABLE_BINNED_AGGREGATIONS = os.getenv("...", "true").lower() == "true"
ENABLE_PRECIPITATION_ANALYSIS = os.getenv("...", "true").lower() == "true"
ENABLE_TEMPORAL_CORRELATIONS = os.getenv("...", "true").lower() == "true"
ENABLE_MULTI_VARIABLE_SUMMARY = os.getenv("...", "true").lower() == "true"
```

**What this means:**

Each analytics stream can be turned on/off independently!

- **Default**: All are `"true"` (enabled)
- **To disable**: Set environment variable to `"false"`
  ```bash
  export ENABLE_PEARSON_CORRELATIONS=false
  python -m etl_dataanalysis.main
  ```

**Why feature flags?**
- **Development**: Test one analytics stream at a time
- **Debugging**: Disable problematic streams without deleting code
- **Performance**: Disable expensive analytics if not needed
- **Gradual rollout**: Enable new analytics gradually in production

**For your exam:** This is a **feature toggle** pattern - common in production systems for safe deployment.

---

#### 6. Spark Advanced Configuration

```python
spark_config = (
    SparkConf()
    .setAppName(SPARK_APP_NAME)
    .set("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE_PATH)
    .set("spark.sql.streaming.schemaInference", "true")
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
)
```

**What each setting does:**

1. **`setAppName(SPARK_APP_NAME)`**
   - Sets application name to "ETL Data Analysis"
   - Visible in Spark UI

2. **`spark.sql.streaming.checkpointLocation`**
   - Default checkpoint location for all streams
   - Can be overridden per stream

3. **`spark.sql.streaming.schemaInference = "true"`**
   - Automatically detect schema from Kafka JSON
   - Alternative: Define schema explicitly (more robust but more code)

4. **`spark.streaming.stopGracefullyOnShutdown = "true"`**
   - When you press Ctrl+C, finish current micro-batch before stopping
   - Prevents data loss during shutdown

5. **`spark.jars.packages`**
   - Downloads Kafka connector library
   - `spark-sql-kafka-0-10_2.12:3.5.0` means:
     - Kafka 0.10+ support
     - Scala 2.12 (Spark is written in Scala)
     - Spark version 3.5.0

6. **`spark.sql.adaptive.enabled = "true"`**
   - **Adaptive Query Execution (AQE)**
   - Spark optimizes query plan at runtime based on actual data statistics
   - Makes queries faster automatically!

7. **`spark.sql.adaptive.coalescePartitions.enabled = "true"`**
   - Automatically reduces number of partitions if data is small
   - Prevents creating too many tiny files (wasteful)

**For your exam:** Mention that Spark has built-in **query optimization** that happens automatically. AQE is part of this.

---

### How config.py is Used

In `main.py`, you'll see:

```python
from . import config

# Use Kafka settings
kafka_servers = config.KAFKA_BOOTSTRAP_SERVERS
topics = config.KAFKA_TOPICS

# Use window durations
window_duration = config.WINDOW_DURATION_MEDIUM

# Use feature flags
if config.ENABLE_PEARSON_CORRELATIONS:
    # Run pearson correlation analytics
    ...
```

**Benefits:**
- **Single source of truth**: All settings in one place
- **Easy to change**: Edit one file, affects entire pipeline
- **Environment-aware**: Different settings for dev/test/prod using environment variables
- **Type-safe**: Python validates values at import time

---

### Changing Configuration

#### Method 1: Edit config.py directly
```python
# In config.py
WINDOW_DURATION_MEDIUM = "30 minutes"  # Changed from 15 to 30
```

#### Method 2: Use environment variables (better for production!)
```bash
# In terminal before running
export WINDOW_DURATION_MEDIUM="30 minutes"
export ENABLE_PEARSON_CORRELATIONS=false
python -m etl_dataanalysis.main
```

#### Method 3: Docker/Kubernetes config
```yaml
# In docker-compose.yml or Kubernetes ConfigMap
environment:
  - KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092
  - OUTPUT_BASE_PATH=/data/processed_simple
  - ENABLE_PEARSON_CORRELATIONS=true
```

---

### Configuration Best Practices

#### 1. Use Environment Variables for Deployment
```python
# ✅ Good: Can change without code edit
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ❌ Bad: Hardcoded
KAFKA_SERVERS = "localhost:9092"
```

#### 2. Provide Sensible Defaults
```python
# ✅ Good: Works out-of-the-box for development
OUTPUT_PATH = os.getenv("OUTPUT_BASE_PATH", "/data/processed_simple")

# ❌ Bad: Requires environment variable
OUTPUT_PATH = os.getenv("OUTPUT_BASE_PATH")  # Crashes if not set!
```

#### 3. Group Related Settings
```python
# ✅ Good: Easy to understand
WINDOW_DURATION_SHORT = "5 minutes"
WINDOW_DURATION_MEDIUM = "15 minutes"
WINDOW_DURATION_LONG = "1 hour"

# ❌ Bad: Scattered throughout file
WINDOW_1 = "5 minutes"
SOME_OTHER_SETTING = "..."
WINDOW_2 = "15 minutes"
```

#### 4. Use Feature Flags for New Features
```python
# ✅ Good: Can enable/disable without deleting code
ENABLE_NEW_ANALYTICS = os.getenv("ENABLE_NEW_ANALYTICS", "false").lower() == "true"

if ENABLE_NEW_ANALYTICS:
    # New experimental analytics
    ...
```

---

### Summary: How main.py and config.py Work Together

```
config.py (Settings)
    │
    ├─ KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    ├─ KAFKA_TOPICS = {"bike": "bike-trips", ...}
    ├─ OUTPUT_BASE_PATH = "/data/processed_simple"
    ├─ WINDOW_DURATION_MEDIUM = "15 minutes"
    ├─ ENABLE_PEARSON_CORRELATIONS = True
    └─ spark_config = SparkConf(...)

    ↓ (imported by)

main.py (Orchestrator)
    │
    ├─ spark = create_spark_session()  → uses config.spark_config
    │
    ├─ read_kafka_stream(spark, config.KAFKA_TOPICS["bike"])
    │
    ├─ write_parquet_stream(df, ...) → uses config.OUTPUT_BASE_PATH
    │
    ├─ if config.ENABLE_PEARSON_CORRELATIONS:
    │      run_pearson_analytics() → uses config.WINDOW_DURATION_MEDIUM
    │
    └─ awaitTermination()
```

---

### Key Points for Your Exam

#### About main.py:
1. **Orchestrator**: Coordinates all components (parsing, enrichment, analytics)
2. **Streaming**: Uses Spark Structured Streaming (continuous processing)
3. **Fault-tolerant**: Checkpointing ensures no data loss
4. **Separation of concerns**: Raw ETL separate from analytics
5. **13 streams**: 4 basic ETL + 4 original analytics + 5 enhanced analytics

#### About config.py:
1. **Single source of truth**: All settings in one place
2. **Environment-aware**: Uses `os.getenv()` for flexible deployment
3. **Feature flags**: Enable/disable analytics independently
4. **Window configurations**: Different windows for different use cases
5. **Spark tuning**: Optimized settings for streaming performance

#### Together:
- **config.py** = "What to do and where to do it"
- **main.py** = "Actually doing it"
- **Separation** = Easy to change settings without touching orchestration logic

---

## Complete Example: One Data Point Through Both Files

Let's trace one bike rental through the entire system:

### 1. Configuration (config.py)
```python
KAFKA_TOPICS = {"bike": "bike-trips"}
OUTPUT_BASE_PATH = "/data/processed_simple"
ANALYTICS_OUTPUT_PATH = "/data/analytics"
WINDOW_DURATION_MEDIUM = "15 minutes"
ENABLE_BINNED_AGGREGATIONS = True
```

### 2. Execution (main.py)

```
08:07:23 - Bike rental happens in Boston
    ↓
08:07:24 - bike-streamer sends JSON to Kafka topic "bike-trips"
    ↓
main.py reads Kafka (every 10 seconds per BATCH_INTERVAL)
    ↓
08:07:30 - Micro-batch processing starts
    ↓
parse_bike_stream() converts JSON → structured DataFrame
    ↓
write_parquet_stream() saves to /data/processed_simple/bike_trips/
    ↓
Window aggregation: groups into [08:00-08:15] window
    ↓
ENABLE_BINNED_AGGREGATIONS is True → runs analytics
    ↓
calculate_binned_weather_aggregations() adds temp_bin="15_to_20C"
    ↓
write_analytics_stream() saves to /data/analytics/weather_binned_metrics/
    ↓
08:07:35 - Micro-batch complete, wait for next batch
```

### 3. Result
- **Raw data**: `/data/processed_simple/bike_trips/year=2018/month=01/date=2018-01-15/hour=08/part-00000.parquet`
- **Analytics**: `/data/analytics/weather_binned_metrics/year=2018/month=01/date=2018-01-15/hour=08/part-00000.parquet`

---

## Troubleshooting Guide

### Common Issues and Solutions

#### Issue 1: "Cannot connect to Kafka"
```
ERROR: Failed to construct kafka consumer
```

**Solution:** Check `config.py`:
```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Is Kafka actually running here?
```

Test Kafka:
```bash
# Check if Kafka is running
kubectl get pods -n bigdata | grep kafka
```

---

#### Issue 2: "Checkpoint directory not empty"
```
ERROR: Checkpoint directory already exists
```

**Solution:** Clear checkpoints (only safe if you want to restart from scratch!):
```bash
# Clear basic ETL checkpoints
rm -rf /tmp/spark_checkpoints_simple/*

# Clear analytics checkpoints
rm -rf /tmp/spark_checkpoints_analytics/*
```

Or in config.py, change checkpoint paths:
```python
CHECKPOINT_BASE_PATH = "/tmp/spark_checkpoints_v2"
```

---

#### Issue 3: "No analytics output"
```
# Streams started but no files in /data/analytics/
```

**Solution:** Check feature flags in `config.py`:
```python
# Make sure analytics are enabled
ENABLE_PEARSON_CORRELATIONS = True  # Was this False?
```

Also check logs:
```python
logger.info("✓ Pearson correlation stream started")  # Do you see this?
```

---

#### Issue 4: "Out of memory"
```
ERROR: Java heap space
```

**Solution:** Reduce window durations in `config.py`:
```python
# Shorter windows = less data in memory at once
WINDOW_DURATION_MEDIUM = "5 minutes"  # Instead of 15 minutes
```

Or increase Spark memory:
```python
spark_config.set("spark.driver.memory", "4g")
spark_config.set("spark.executor.memory", "4g")
```

---

## Summary: What You Should Remember

### main.py (Orchestrator)
- **Creates** Spark session
- **Reads** 4 Kafka topics
- **Parses** JSON → structured data
- **Writes** basic ETL to Parquet
- **Runs** 9 analytics streams
- **Tracks** all queries
- **Handles** errors and cleanup

### config.py (Control Panel)
- **Kafka** connection settings
- **Spark** configuration and tuning
- **Paths** for output and checkpoints
- **Windows** durations for different analytics
- **Feature flags** to enable/disable analytics
- **Defaults** that work for development

### Together
- config.py defines **WHAT** and **WHERE**
- main.py executes **HOW**
- Clean separation makes code maintainable and testable
