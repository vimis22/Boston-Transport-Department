# Readme for ETL_DATA ANALYSIS - Transformations

## transformations.py

### What is transformations.py?
The `transformations.py` file is responsible for **parsing** (læsning og konvertering) raw JSON messages from Kafka into structured DataFrames with clean columns.

Think of it like unpacking boxes:
- **Input**: Box full of mixed items (JSON string)
- **Output**: Neatly organized shelves with labeled items (DataFrame with columns)

---

### Why Do We Need Transformations?

When data arrives from Kafka, it looks like this:
```json
{
  "value": "{\"data\": {\"trip_id\": \"123\", \"duration_seconds\": 600, ...}, \"timestamp\": \"...\", \"source\": \"bike-streamer\"}"
}
```

This is a **string** (text) - Spark can't work with it directly!

We need to:
1. **Extract** the JSON from the Kafka message
2. **Parse** the JSON structure
3. **Convert** to typed columns (integers, doubles, timestamps)
4. **Add** time-based partitioning columns (year, month, date, hour)

---

## The 4 Parse Functions

`transformations.py` has **4 main functions**, one for each data source:

1. `parse_bike_stream()` - For bike rental data
2. `parse_taxi_stream()` - For taxi ride data
3. `parse_weather_stream()` - For weather observations
4. `parse_accident_stream()` - For accident reports

Let's look at each one in detail!

---

## 1. parse_bike_stream(df)

### Purpose
Converts raw Kafka messages about bike rentals into structured bike trip data.

### Input
A Kafka DataFrame with one column:
```
+----------------------------------------+
| value                                  |
+----------------------------------------+
| {"data": {"trip_id": "123", ...}, ...} |
+----------------------------------------+
```

### What does it do?

#### Step 1: Cast to String
```python
string_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
```

**What happens:**
- Kafka stores messages as binary (bytes)
- We convert to readable string
- Also grab Kafka's timestamp (when message arrived)

---

#### Step 2: Extract JSON Fields
```python
result_df = string_df.select(
    get_json_object(col("json_str"), "$.data.trip_id").alias("trip_id"),
    get_json_object(col("json_str"), "$.data.duration_seconds").cast("integer").alias("duration_seconds"),
    get_json_object(col("json_str"), "$.data.start_time").alias("start_time"),
    ...
)
```

**What happens:**
- `get_json_object()` extracts specific fields from JSON string
- `"$.data.trip_id"` means: "Go into 'data', then get 'trip_id'"
- `.cast("integer")` converts string numbers to actual integers
- `.alias("trip_id")` names the column

**Example:**
```
JSON: {"data": {"trip_id": "123", "duration_seconds": 600}}
  ↓
Columns: trip_id="123", duration_seconds=600
```

---

#### Step 3: Extract Station Information
```python
get_json_object(col("json_str"), "$.data.start_station.id").alias("start_station_id"),
get_json_object(col("json_str"), "$.data.start_station.name").alias("start_station_name"),
get_json_object(col("json_str"), "$.data.start_station.latitude").cast("double").alias("start_station_latitude"),
get_json_object(col("json_str"), "$.data.start_station.longitude").cast("double").alias("start_station_longitude"),
```

**What happens:**
- Bike trips have start and end stations
- Each station has: id, name, latitude, longitude
- We extract all fields and make them separate columns

---

#### Step 4: Convert Timestamps
```python
final_df = result_df.withColumn(
    "start_time_ts", to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "stop_time_ts", to_timestamp(col("stop_time"), "yyyy-MM-dd HH:mm:ss")
)
```

**What happens:**
- `start_time` arrives as string: "2018-01-15 09:30:00"
- `to_timestamp()` converts to actual timestamp type
- Now Spark knows it's a time, not just text!
- Format `"yyyy-MM-dd HH:mm:ss"` tells Spark how to read the string

---

#### Step 5: Add Partitioning Columns
```python
.withColumn("year", year(col("start_time_ts")))
.withColumn("month", month(col("start_time_ts")))
.withColumn("date", date_format(col("start_time_ts"), "yyyy-MM-dd"))
.withColumn("hour", hour(col("start_time_ts")))
```

**What happens:**
- Extracts year, month, date, hour from timestamp
- Used for **partitioning** Parquet files later

**Why partition?**
- Faster queries: "Give me all trips in January 2018" → only reads January files
- Organized storage: Files grouped by time
- Example structure:
  ```
  /bike_trips/
    year=2018/
      month=01/
        date=2018-01-15/
          hour=09/
            part-00000.parquet
  ```

---

### Output Schema (Bike)
After `parse_bike_stream()`, we have these columns:
```
trip_id: string
duration_seconds: integer
start_time: string
stop_time: string
start_time_ts: timestamp
stop_time_ts: timestamp
start_station_id: string
start_station_name: string
start_station_latitude: double
start_station_longitude: double
end_station_id: string
end_station_name: string
end_station_latitude: double
end_station_longitude: double
bike_id: string
user_type: string (e.g., "Subscriber", "Customer")
birth_year: integer
gender: integer (0=unknown, 1=male, 2=female)
event_timestamp: string (from bike-streamer)
source: string (always "bike-streamer")
kafka_timestamp: timestamp (when Kafka received message)
year: integer
month: integer
date: string
hour: integer
```

---

## 2. parse_taxi_stream(df)

### Purpose
Converts raw Kafka messages about taxi rides into structured taxi trip data.

### Input
Kafka DataFrame with taxi trip JSON.

### What does it do?

#### Key Difference from Bikes:
Taxi data includes **embedded weather snapshot** at ride time!

```python
get_json_object(col("json_str"), "$.data.weather_snapshot.temperature").cast("double").alias("temperature"),
get_json_object(col("json_str"), "$.data.weather_snapshot.apparent_temperature").cast("double").alias("apparent_temperature"),
get_json_object(col("json_str"), "$.data.weather_snapshot.summary").alias("weather_summary"),
get_json_object(col("json_str"), "$.data.weather_snapshot.precip_intensity").cast("double").alias("precip_intensity"),
get_json_object(col("json_str"), "$.data.weather_snapshot.humidity").cast("double").alias("humidity"),
get_json_object(col("json_str"), "$.data.weather_snapshot.wind_speed").cast("double").alias("wind_speed"),
```

**Why is this useful?**
- Taxi data already has weather at the exact time of the ride
- Used in `calculate_surge_weather_correlation()` analytics
- No need to join with separate weather stream!

---

### Taxi-Specific Fields
```python
get_json_object(col("json_str"), "$.data.cab_type").alias("cab_type"),  # "Uber" or "Lyft"
get_json_object(col("json_str"), "$.data.product.name").alias("product_name"),  # "UberX", "Lyft Line", etc.
get_json_object(col("json_str"), "$.data.price").cast("double").alias("price"),
get_json_object(col("json_str"), "$.data.distance").cast("double").alias("distance"),
get_json_object(col("json_str"), "$.data.surge_multiplier").cast("double").alias("surge_multiplier"),
```

**Important fields:**
- `surge_multiplier`: 1.0 = no surge, 1.5 = 50% extra charge
- `cab_type`: Uber vs Lyft (for comparison analytics)
- `price`, `distance`: For revenue calculations

---

### Output Schema (Taxi)
```
trip_id: string
datetime: string
datetime_ts: timestamp
pickup_location: string
dropoff_location: string
cab_type: string ("Uber", "Lyft")
product_id: string
product_name: string
price: double
distance: double
surge_multiplier: double
latitude: double
longitude: double
temperature: double (from weather snapshot!)
apparent_temperature: double
weather_summary: string
precip_intensity: double
humidity: double
wind_speed: double
event_timestamp: string
source: string (always "taxi-streamer")
kafka_timestamp: timestamp
year: integer
month: integer
date: string
hour: integer
```

---

## 3. parse_weather_stream(df)

### Purpose
Converts raw NOAA weather observations into structured weather data **with enrichment**.

### Input
Kafka DataFrame with NOAA weather JSON.

### What does it do?

#### Special Feature: Calls Weather Enrichment!
```python
# Parse basic fields
final_df = result_df.withColumn(...)

# THEN enrich the weather data
final_df = enrich_weather_data(final_df)
final_df = add_precipitation_indicator(final_df)
```

This is the **only parse function** that does enrichment inside the parsing step!

---

#### Step 1: Parse NOAA Format
```python
get_json_object(col("json_str"), "$.data.station").alias("station"),
get_json_object(col("json_str"), "$.data.datetime").alias("datetime"),
get_json_object(col("json_str"), "$.data.observations.wind").alias("wind"),
get_json_object(col("json_str"), "$.data.observations.temperature").alias("temperature"),
get_json_object(col("json_str"), "$.data.observations.visibility").alias("visibility"),
```

**What we get:**
- `station`: Weather station ID (e.g., "72509014739")
- `datetime`: Observation time
- `wind`: Raw NOAA wind string (e.g., "160,1,N,0046,1")
- `temperature`: Raw NOAA temp string (e.g., "+0056,1")
- `visibility`: Raw NOAA visibility string (e.g., "016000,1,9,9")

**Problem:** These are encoded strings, not usable numbers!

---

#### Step 2: Enrich Weather Data
```python
final_df = enrich_weather_data(final_df)
```

This calls `weather_enrichment.py` which:
1. **Parses** NOAA strings → Celsius, m/s, meters
2. **Creates buckets**: "5_to_10C", "light_breeze", "good"
3. **Calculates weather_condition_score** (0-100)
4. **Adds flags**: is_good_weather, is_bad_weather

(See WEATHER_ENRICHMENT.md for full details!)

---

#### Step 3: Add Precipitation Indicators
```python
final_df = add_precipitation_indicator(final_df)
```

Adds:
- `has_precipitation`: Boolean (likely raining?)
- `precip_category`: "none", "light", "moderate", "heavy"

---

### Output Schema (Weather)
```
station: string
datetime: string
datetime_ts: timestamp
data_source: string
latitude: double
longitude: double
elevation: double
station_name: string
wind: string (raw NOAA)
temperature: string (raw NOAA)
visibility: string (raw NOAA)
dew_point: string (raw NOAA)
sea_level_pressure: string (raw NOAA)

--- ENRICHED COLUMNS (from weather_enrichment.py) ---
temperature_celsius: double (e.g., 5.6)
wind_speed_ms: double (e.g., 4.6)
visibility_m: double (e.g., 16000)
temp_bucket: string (e.g., "5_to_10C")
wind_category: string (e.g., "light_breeze")
visibility_category: string (e.g., "excellent")
weather_condition_score: double (0-100)
is_good_weather: boolean
is_bad_weather: boolean
has_precipitation: boolean
precip_category: string

event_timestamp: string
source: string (always "weather-streamer")
kafka_timestamp: timestamp
year: integer
month: integer
date: string
hour: integer
```

**Key point:** Weather DataFrame has both raw NOAA strings AND parsed/enriched values!

---

## 4. parse_accident_stream(df)

### Purpose
Converts raw Kafka messages about traffic accidents into structured accident data.

### Input
Kafka DataFrame with accident report JSON.

### What does it do?

#### Extract Accident Details
```python
get_json_object(col("json_str"), "$.data.dispatch_ts").alias("dispatch_ts"),
get_json_object(col("json_str"), "$.data.mode_type").alias("mode_type"),
get_json_object(col("json_str"), "$.data.location_type").alias("location_type"),
get_json_object(col("json_str"), "$.data.street").alias("street"),
get_json_object(col("json_str"), "$.data.xstreet1").alias("xstreet1"),
get_json_object(col("json_str"), "$.data.xstreet2").alias("xstreet2"),
```

**Important fields:**
- `dispatch_ts`: When emergency services were dispatched (timestamp of accident)
- `mode_type`: Type of accident
  - "bike" = bicycle accident
  - "ped" = pedestrian accident
  - "mv" = motor vehicle accident
- `location_type`: "Intersection" vs "Street"
- `street`, `xstreet1`, `xstreet2`: Location description

---

#### Extract Coordinates
```python
get_json_object(col("json_str"), "$.data.x_cord").cast("double").alias("x_cord"),
get_json_object(col("json_str"), "$.data.y_cord").cast("double").alias("y_cord"),
get_json_object(col("json_str"), "$.data.lat").cast("double").alias("lat"),
get_json_object(col("json_str"), "$.data.long").cast("double").alias("long"),
```

**What we get:**
- `x_cord`, `y_cord`: Local coordinate system (city-specific)
- `lat`, `long`: GPS coordinates (latitude, longitude)

**Used for:** Mapping accident locations on dashboard

---

#### Convert Dispatch Timestamp
```python
final_df = result_df.withColumn(
    "dispatch_timestamp", to_timestamp(col("dispatch_ts"), "yyyy-MM-dd HH:mm:ss")
)
```

**Why important:**
- Used to join with weather data (what was weather when accident happened?)
- Used for time-based aggregations

---

### Output Schema (Accidents)
```
dispatch_ts: string
dispatch_timestamp: timestamp
mode_type: string ("bike", "ped", "mv")
location_type: string ("Intersection", "Street")
street: string
xstreet1: string (cross street 1)
xstreet2: string (cross street 2)
x_cord: double
y_cord: double
lat: double (latitude)
long: double (longitude)
event_timestamp: string
source: string (always "accident-streamer")
kafka_timestamp: timestamp
year: integer
month: integer
date: string
hour: integer
```

---

## Common Pattern Across All Parse Functions

All 4 functions follow the same structure:

```
Step 1: Cast Kafka value to string
    ↓
Step 2: Extract JSON fields with get_json_object()
    ↓
Step 3: Cast fields to correct types (integer, double, timestamp)
    ↓
Step 4: Convert datetime strings to timestamp type
    ↓
Step 5: Add partitioning columns (year, month, date, hour)
    ↓
Step 6: (Weather only) Enrich with weather_enrichment.py
    ↓
Return: Clean DataFrame with typed columns
```

---

## Why Transformations Matter

### 1. Type Safety
**Before transformation:**
```python
duration = "600"  # String - can't do math!
```

**After transformation:**
```python
duration_seconds = 600  # Integer - can calculate average, sum, etc.
```

---

### 2. Easy Querying
**Before transformation:**
```python
# Would have to parse JSON in every query - slow!
SELECT get_json_object(value, '$.data.trip_id') FROM kafka_table
```

**After transformation:**
```python
# Direct column access - fast!
SELECT trip_id FROM bike_trips WHERE duration_seconds > 300
```

---

### 3. Partitioning for Performance
**Without partitioning:**
```
/bike_trips/
  part-00000.parquet  (all data in one file - 10 GB!)
  part-00001.parquet
```

Query: "Give me trips from January 15, 2018"
- Must scan entire 10 GB file 😞

**With partitioning:**
```
/bike_trips/
  year=2018/
    month=01/
      date=2018-01-15/
        hour=09/part-00000.parquet (10 MB)
        hour=10/part-00001.parquet (10 MB)
```

Query: "Give me trips from January 15, 2018"
- Only reads date=2018-01-15 folder (~240 MB for 24 hours) 😊
- **40x faster!**

---

### 4. Enables Analytics
Without proper transformations, analytics would be impossible:

**Can't do this without transformations:**
```python
# Join bikes with weather by timestamp
bikes.join(weather, bikes.start_time_ts == weather.datetime_ts)

# Calculate average duration
bikes.groupBy("user_type").agg(avg("duration_seconds"))

# Time windows
bikes.groupBy(window("start_time_ts", "15 minutes"))
```

---

## Data Flow: Kafka → Transformations → Analytics

```
1. KAFKA MESSAGE (Raw JSON string)
   {"data": {"trip_id": "123", "duration_seconds": 600, ...}, ...}
        ↓
2. TRANSFORMATION (parse_bike_stream)
   trip_id="123", duration_seconds=600, start_time_ts=2018-01-15 09:30:00
        ↓
3. PARQUET FILES (Structured, partitioned)
   /data/processed_simple/bike_trips/year=2018/month=01/...
        ↓
4. ANALYTICS (Can now calculate correlations!)
   Window [09:30-09:45]: 145 bike rentals, avg temp 18.2°C
        ↓
5. DASHBOARD (Visualization)
   Graph: Temperature vs Bike Rentals
```

---

## Special Case: Weather Enrichment in Parsing

**Question:** Why does `parse_weather_stream()` call enrichment functions, but other parsers don't?

**Answer:**
1. **NOAA format is complex**: Raw strings like "+0056,1" need special parsing
2. **Weather is used immediately**: Other streams join with weather, so it must be enriched early
3. **Self-contained**: Weather enrichment doesn't depend on other streams

**Other streams don't enrich because:**
- Bike/taxi/accident data is already in clean format from streamers
- Cross-stream enrichment happens in `windowed_aggregations.py` (combining bikes + weather)

---

## Important Functions Used in Transformations

### 1. `get_json_object(column, path)`
**Purpose:** Extract field from JSON string

**Example:**
```python
get_json_object(col("json_str"), "$.data.trip_id")
```
- `$` = root of JSON
- `.data` = go into "data" object
- `.trip_id` = get "trip_id" field

**JSON path examples:**
- `"$.data.trip_id"` → `"123"`
- `"$.data.start_station.name"` → `"MIT at Mass Ave"`
- `"$.timestamp"` → `"2018-01-15T09:30:00"`

---

### 2. `to_timestamp(column, format)`
**Purpose:** Convert string to timestamp

**Example:**
```python
to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss")
```
- Input: `"2018-01-15 09:30:00"` (string)
- Output: `2018-01-15 09:30:00` (timestamp type)

**Format codes:**
- `yyyy` = 4-digit year (2018)
- `MM` = 2-digit month (01)
- `dd` = 2-digit day (15)
- `HH` = 24-hour (09)
- `mm` = minute (30)
- `ss` = second (00)

---

### 3. `withColumn(name, expression)`
**Purpose:** Add or modify a column

**Example:**
```python
df.withColumn("year", year(col("start_time_ts")))
```
- Creates new column called "year"
- Value = year extracted from start_time_ts

**Can chain multiple:**
```python
df.withColumn("year", year(col("start_time_ts")))
  .withColumn("month", month(col("start_time_ts")))
  .withColumn("hour", hour(col("start_time_ts")))
```

---

### 4. `cast(type)`
**Purpose:** Convert column type

**Example:**
```python
col("duration_seconds").cast("integer")
col("price").cast("double")
```

**Common types:**
- `"string"` = text
- `"integer"` = whole number
- `"double"` = decimal number
- `"boolean"` = true/false
- `"timestamp"` = date and time

---

### 5. `alias(name)`
**Purpose:** Rename a column

**Example:**
```python
get_json_object(col("json_str"), "$.data.trip_id").alias("trip_id")
```
- Without alias: Column name would be long and ugly
- With alias: Clean name "trip_id"

---

## Error Handling in Transformations

### What if JSON field is missing?
```python
get_json_object(col("json_str"), "$.data.optional_field").alias("optional_field")
```

**Result:** `null` (not an error!)
- Spark handles missing fields gracefully
- Column exists but value is NULL

---

### What if timestamp format is wrong?
```python
to_timestamp(col("bad_time"), "yyyy-MM-dd HH:mm:ss")
```

**Result:** `null`
- If string doesn't match format, returns NULL
- Doesn't crash the pipeline

**How to check:**
```python
# In your data, check for nulls
df.filter(col("start_time_ts").isNull()).count()
```

---

### What if cast fails?
```python
col("bad_number").cast("integer")
```

**Result:** `null`
- If string is not a number (e.g., "abc"), returns NULL

---

## Performance Considerations

### 1. JSON Parsing is Expensive
**Why:** Spark must parse JSON string character-by-character

**Impact:** Slowest part of the pipeline

**Optimization:** Done only once during transformation, then saved as Parquet

---

### 2. Partitioning Helps
**Without partitioning:**
- One big file per topic
- Queries scan entire file

**With partitioning (year/month/date/hour):**
- Many small files organized by time
- Queries only read relevant files
- **10-100x faster queries!**

---

### 3. Parquet Format Benefits
**Why Parquet?**
- **Columnar storage**: Only read columns you need
- **Compression**: Smaller files (often 10x smaller than JSON)
- **Schema included**: No need to parse on every read
- **Predicate pushdown**: Filter before reading data

**Example:**
```python
# JSON: Must read entire file (10 GB)
df = spark.read.json("/data/raw/bike_trips.json")
df.filter(col("user_type") == "Subscriber").count()

# Parquet: Only reads "user_type" column (~100 MB)
df = spark.read.parquet("/data/processed_simple/bike_trips/")
df.filter(col("user_type") == "Subscriber").count()
```

---

## Summary: Key Points for Your Exam

### About transformations.py:
1. **Purpose**: Convert raw JSON strings → structured DataFrames
2. **Functions**: 4 parsers (bikes, taxis, weather, accidents)
3. **Process**: Extract → Cast → Timestamp → Partition
4. **Special case**: Weather enrichment happens during parsing
5. **Output**: Clean, typed, partitioned data ready for analytics

### Why it matters:
1. **Type safety**: Strings → integers/doubles/timestamps
2. **Performance**: JSON → Parquet (10x compression + faster queries)
3. **Partitioning**: Organized by time (year/month/date/hour)
4. **Analytics-ready**: Can join, aggregate, calculate correlations
5. **Fault-tolerant**: Null handling for missing/malformed data

### Technical terms to mention:
- **JSON parsing**: Converting text → structured data
- **Type casting**: String → integer/double/timestamp
- **Partitioning**: Organizing files by columns (year/month/date/hour)
- **Parquet format**: Columnar storage (faster than JSON)
- **Schema inference**: Spark detects data types automatically

---

## Example: Tracing One Bike Trip

Let's follow one bike rental through transformation:

### 1. Kafka Message (Raw)
```json
{
  "value": "{\"data\": {\"trip_id\": \"12345\", \"duration_seconds\": 600, \"start_time\": \"2018-01-15 09:30:00\", \"user_type\": \"Subscriber\"}, \"timestamp\": \"2018-01-15T09:30:05\", \"source\": \"bike-streamer\"}"
}
```

### 2. After CAST to String
```
json_str: {"data": {"trip_id": "12345", "duration_seconds": 600, ...}, ...}
kafka_timestamp: 2018-01-15 09:30:05
```

### 3. After get_json_object (Extract)
```
trip_id: "12345" (string)
duration_seconds: "600" (still string!)
start_time: "2018-01-15 09:30:00" (string)
user_type: "Subscriber" (string)
```

### 4. After cast (Type Conversion)
```
trip_id: "12345" (string - keep as text)
duration_seconds: 600 (integer - now can do math!)
start_time: "2018-01-15 09:30:00" (still string)
user_type: "Subscriber" (string)
```

### 5. After to_timestamp
```
trip_id: "12345"
duration_seconds: 600
start_time: "2018-01-15 09:30:00"
start_time_ts: 2018-01-15 09:30:00 (timestamp type!)
user_type: "Subscriber"
```

### 6. After Partitioning Columns
```
trip_id: "12345"
duration_seconds: 600
start_time_ts: 2018-01-15 09:30:00
user_type: "Subscriber"
year: 2018
month: 1
date: "2018-01-15"
hour: 9
```

### 7. Written to Parquet
```
File: /data/processed_simple/bike_trips/year=2018/month=01/date=2018-01-15/hour=09/part-00000.parquet

Contents (columnar):
  trip_id        duration_seconds    start_time_ts           user_type
  -------        ----------------    ---------------         -----------
  12345          600                 2018-01-15 09:30:00    Subscriber
  12346          450                 2018-01-15 09:32:15    Customer
  ...
```

Now this data can be queried efficiently and used in analytics! ✅
