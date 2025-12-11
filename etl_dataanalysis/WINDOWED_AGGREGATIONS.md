# Readme for ETL_DATA ANALYSIS - Windowed Aggregations

## windowed_aggregations.py

### What is windowed_aggregations.py?
The `windowed_aggregations.py` file is responsible for **time-based aggregation** of streaming data - grouping events that happen in the same time window (e.g., 15-minute intervals).

Think of it like organizing photos by hour:
- **Without windows**: 10,000 individual bike trips (messy!)
- **With windows**: Grouped into hourly summaries (organized!)

---

### Why Do We Need Time Windows?

#### Problem 1: Too Much Detail
Raw streaming data has individual events:
```
08:03:12 - Bike rental
08:05:47 - Bike rental
08:07:23 - Bike rental
08:09:01 - Bike rental
... (thousands per hour!)
```

**Issues:**
- Too many data points to visualize
- Hard to see patterns
- Can't correlate with weather (weather updates every hour, bikes every second!)

---

#### Problem 2: Asynchronous Data
Different streams have different timestamps:

```
Bikes:     08:03:12, 08:05:47, 08:07:23, ...
Taxis:     08:02:34, 08:06:15, 08:08:42, ...
Weather:   08:00:00, 08:15:00, 08:30:00, ...  ← Every 15 minutes!
```

**How do we correlate them?** → **Time windows!**

---

#### Solution: Time Windows
Group all events in same time period:

```
Window [08:00 - 08:15]:
  ├─ Bikes: 145 rentals
  ├─ Taxis: 67 rides
  └─ Weather: avg temp 18.2°C

Window [08:15 - 08:30]:
  ├─ Bikes: 132 rentals
  ├─ Taxis: 71 rides
  └─ Weather: avg temp 18.5°C
```

Now we can correlate: "In the 08:00-08:15 window, there were 145 bike rentals when temperature was 18.2°C"

---

## Types of Windows in Spark

### 1. Tumbling Windows (No Overlap)
```
[08:00 - 08:15]  [08:15 - 08:30]  [08:30 - 08:45]
     ↑                ↑                ↑
  Window 1         Window 2         Window 3
```

**Characteristics:**
- Each event belongs to **exactly one** window
- No overlap between windows
- Simpler to reason about

**Our pipeline uses tumbling windows.**

---

### 2. Sliding Windows (Overlap)
```
[08:00 - 08:15]
     [08:05 - 08:20]
          [08:10 - 08:25]
```

**Characteristics:**
- Windows overlap (same event in multiple windows)
- Smoother trends
- More computationally expensive

**We could enable sliding by setting `slide_duration` parameter.**

---

## The 5 Main Aggregation Functions

`windowed_aggregations.py` has 5 aggregation functions:

1. `aggregate_bike_data_by_window()` - Bike rental metrics per window
2. `aggregate_taxi_data_by_window()` - Taxi ride metrics per window
3. `aggregate_weather_data_by_window()` - Weather metrics per window
4. `aggregate_accident_data_by_window()` - Accident metrics per window
5. `create_combined_transport_weather_window()` - **Combines all!** (Most important)

Plus one new function:
6. `create_weather_binned_aggregations()` ✨ - Graph-ready binned data

Let's look at each one!

---

## 1. aggregate_bike_data_by_window(df, window_duration, slide_duration)

### Purpose
Groups bike rental data into time windows and calculates statistics.

### Input
```python
df: Bike DataFrame with start_time_ts column
window_duration: "15 minutes" (how long each window is)
slide_duration: None (tumbling window) or "5 minutes" (sliding window)
```

### What does it do?

#### Step 1: Group by Time Window
```python
if slide_duration:
    windowed = df.groupBy(
        window(col("start_time_ts"), window_duration, slide_duration),
        "user_type"
    )
else:
    windowed = df.groupBy(
        window(col("start_time_ts"), window_duration),
        "user_type"
    )
```

**What this does:**
- `window(col("start_time_ts"), "15 minutes")` groups trips into 15-min buckets
- Also groups by `user_type` (Subscriber vs Customer)

**Example:**
```
Individual trips:
  08:03:12 - Subscriber
  08:05:47 - Customer
  08:07:23 - Subscriber
  08:17:01 - Subscriber

Grouped:
  Window [08:00-08:15], Subscriber: 2 trips
  Window [08:00-08:15], Customer: 1 trip
  Window [08:15-08:30], Subscriber: 1 trip
```

---

#### Step 2: Calculate Aggregations
```python
result = windowed.agg(
    count("*").alias("rental_count"),
    avg("duration_seconds").alias("avg_duration_seconds"),
    min("duration_seconds").alias("min_duration_seconds"),
    max("duration_seconds").alias("max_duration_seconds"),
    stddev("duration_seconds").alias("stddev_duration"),

    # Station popularity
    count("start_station_id").alias("total_trips_started"),

    # User demographics
    avg("birth_year").alias("avg_birth_year"),
    count(when(col("gender") == 2, 1)).alias("female_count"),
    count(when(col("gender") == 1, 1)).alias("male_count"),
    count(when(col("gender") == 0, 1)).alias("unknown_gender_count"),
)
```

**What each does:**
- `count("*")`: Total number of rentals in window
- `avg("duration_seconds")`: Average trip duration
- `min/max`: Shortest and longest trips
- `stddev`: How much durations vary
- `count(when(...))`: Conditional counting (e.g., count females)

---

#### Step 3: Add Derived Metrics
```python
result = result.withColumn(
    "window_start", col("window.start")
).withColumn(
    "window_end", col("window.end")
).withColumn(
    "avg_duration_minutes", col("avg_duration_seconds") / 60.0
).withColumn(
    "gender_diversity_ratio",
    when(col("male_count") + col("female_count") > 0,
         col("female_count") / (col("male_count") + col("female_count")))
    .otherwise(None)
)
```

**What this does:**
- Extracts window boundaries (start/end timestamps)
- Converts seconds → minutes (easier to read)
- Calculates gender ratio (female / total)

---

### Output Schema (Bike Aggregation)
```
window_start: timestamp (e.g., 2018-01-15 08:00:00)
window_end: timestamp (e.g., 2018-01-15 08:15:00)
user_type: string ("Subscriber" or "Customer")
rental_count: integer (e.g., 145)
avg_duration_seconds: double (e.g., 623.5)
avg_duration_minutes: double (e.g., 10.4)
min_duration_seconds: integer
max_duration_seconds: integer
stddev_duration: double
total_trips_started: integer
avg_birth_year: double
female_count: integer
male_count: integer
unknown_gender_count: integer
gender_diversity_ratio: double (e.g., 0.32 = 32% female)
```

---

## 2. aggregate_taxi_data_by_window(df, window_duration, slide_duration)

### Purpose
Groups taxi ride data into time windows and calculates statistics.

### Key Differences from Bikes
- Groups by `cab_type` (Uber vs Lyft) instead of user_type
- Includes pricing metrics (price, surge_multiplier)
- Includes **embedded weather** from taxi data

### Aggregations
```python
result = windowed.agg(
    count("*").alias("ride_count"),
    avg("price").alias("avg_price"),
    sum("price").alias("total_revenue"),
    avg("distance").alias("avg_distance"),
    avg("surge_multiplier").alias("avg_surge"),
    max("surge_multiplier").alias("max_surge"),

    # Weather at ride time (from embedded snapshot)
    avg("temperature").alias("avg_temperature"),
    avg("apparent_temperature").alias("avg_feels_like_temp"),
    avg("humidity").alias("avg_humidity"),
    avg("wind_speed").alias("avg_wind_speed"),
    avg("precip_intensity").alias("avg_precip_intensity"),
)
```

**Important:** Taxi data has weather embedded in each trip!
- Averages weather across all trips in window
- Used later for surge-weather correlation

### Derived Metrics
```python
result = result.withColumn(
    "revenue_per_ride", col("total_revenue") / col("ride_count")
).withColumn(
    "high_surge_indicator",
    when(col("avg_surge") > 1.5, True).otherwise(False)
)
```

---

### Output Schema (Taxi Aggregation)
```
window_start: timestamp
window_end: timestamp
cab_type: string ("Uber" or "Lyft")
ride_count: integer
avg_price: double
total_revenue: double
avg_distance: double
avg_surge: double
max_surge: double
revenue_per_ride: double
high_surge_indicator: boolean
avg_temperature: double (from embedded weather!)
avg_feels_like_temp: double
avg_humidity: double
avg_wind_speed: double
avg_precip_intensity: double
```

---

## 3. aggregate_weather_data_by_window(df, window_duration, slide_duration)

### Purpose
Aggregates weather observations into time windows (weather stations report multiple times per window).

### Aggregations
```python
result = windowed.agg(
    count("*").alias("observation_count"),

    # Temperature stats
    avg("temperature_celsius").alias("avg_temperature_c"),
    min("temperature_celsius").alias("min_temperature_c"),
    max("temperature_celsius").alias("max_temperature_c"),

    # Wind stats
    avg("wind_speed_ms").alias("avg_wind_speed_ms"),
    max("wind_speed_ms").alias("max_wind_speed_ms"),

    # Visibility stats
    avg("visibility_m").alias("avg_visibility_m"),
    min("visibility_m").alias("min_visibility_m"),

    # Weather quality
    avg("weather_condition_score").alias("avg_weather_score"),
    count(when(col("is_good_weather") == True, 1)).alias("good_weather_obs_count"),
    count(when(col("is_bad_weather") == True, 1)).alias("bad_weather_obs_count"),

    # Most common categories
    first("temp_bucket").alias("dominant_temp_bucket"),
    first("wind_category").alias("dominant_wind_category"),
    first("visibility_category").alias("dominant_visibility_category"),
)
```

**Why multiple observations per window?**
- Weather station reports every 5-15 minutes
- In a 15-minute window, might have 2-3 observations
- We average them for more stable metrics

### Derived Metrics
```python
result = result.withColumn(
    "temperature_range_c", col("max_temperature_c") - col("min_temperature_c")
).withColumn(
    "good_weather_ratio",
    when(col("observation_count") > 0,
         col("good_weather_obs_count") / col("observation_count"))
    .otherwise(0.0)
)
```

---

### Output Schema (Weather Aggregation)
```
window_start: timestamp
window_end: timestamp
observation_count: integer (how many weather readings in window)
avg_temperature_c: double
min_temperature_c: double
max_temperature_c: double
temperature_range_c: double (max - min)
avg_wind_speed_ms: double
max_wind_speed_ms: double
avg_visibility_m: double
min_visibility_m: double
avg_weather_score: double (0-100)
good_weather_obs_count: integer
bad_weather_obs_count: integer
good_weather_ratio: double (0.0-1.0)
dominant_temp_bucket: string (e.g., "15_to_20C")
dominant_wind_category: string (e.g., "light_breeze")
dominant_visibility_category: string (e.g., "excellent")
```

---

## 4. aggregate_accident_data_by_window(df, window_duration, slide_duration)

### Purpose
Groups accident reports into time windows (accidents are rare, so usually 1-hour windows).

### Aggregations
```python
result = windowed.agg(
    count("*").alias("accident_count"),
    count(when(col("location_type") == "Intersection", 1)).alias("intersection_accidents"),
    count(when(col("location_type") == "Street", 1)).alias("street_accidents"),
)
```

**Why simple aggregations?**
- Accidents are infrequent (maybe 0-5 per hour)
- Main metric is just **count**
- Breakdown by mode_type (bike/ped/mv) and location_type

### Derived Metrics
```python
result = result.withColumn(
    "intersection_accident_ratio",
    when(col("accident_count") > 0,
         col("intersection_accidents") / col("accident_count"))
    .otherwise(0.0)
)
```

**What this shows:**
- What % of accidents happen at intersections vs streets?
- Intersections typically more dangerous (crossing traffic)

---

### Output Schema (Accident Aggregation)
```
window_start: timestamp
window_end: timestamp
mode_type: string ("bike", "ped", "mv")
accident_count: integer
intersection_accidents: integer
street_accidents: integer
intersection_accident_ratio: double (0.0-1.0)
```

---

## 5. create_combined_transport_weather_window() ⭐ MOST IMPORTANT

### Purpose
**This is the KEY function!** It combines bikes, taxis, and weather into a single DataFrame aligned by time windows.

This is what enables **correlation analysis** between transport and weather!

### Input
```python
bike_df: Bike DataFrame
taxi_df: Taxi DataFrame
weather_df: Weather DataFrame
window_duration: "15 minutes" (same window for all!)
```

### What does it do?

#### Step 1: Aggregate Each Stream Separately
```python
bike_agg = aggregate_bike_data_by_window(bike_df, window_duration)
taxi_agg = aggregate_taxi_data_by_window(taxi_df, window_duration)
weather_agg = aggregate_weather_data_by_window(weather_df, window_duration)
```

Now we have 3 separate aggregated DataFrames.

---

#### Step 2: Sum Across Subcategories
```python
# Sum bike rentals across user types
bike_total = bike_agg.groupBy("window_start", "window_end").agg(
    sum("rental_count").alias("total_bike_rentals"),
    avg("avg_duration_minutes").alias("avg_bike_duration_min")
)

# Sum taxi rides across cab types
taxi_total = taxi_agg.groupBy("window_start", "window_end").agg(
    sum("ride_count").alias("total_taxi_rides"),
    avg("avg_price").alias("avg_taxi_price"),
    avg("avg_surge").alias("avg_taxi_surge")
)
```

**Why sum?**
- Original aggregation is per user_type (Subscriber/Customer)
- We want **total** bike rentals (Subscriber + Customer)
- Same for taxis (Uber + Lyft → total)

---

#### Step 3: Join All Three on Time Window
```python
# Join bikes + taxis
combined = bike_total.join(taxi_total, ["window_start", "window_end"], "full_outer")

# Join with weather
combined = combined.join(weather_agg.select(
    "window_start", "window_end",
    "avg_temperature_c", "avg_wind_speed_ms", "avg_weather_score",
    "good_weather_ratio", "dominant_temp_bucket", "dominant_wind_category"
), ["window_start", "window_end"], "left")
```

**Join type:**
- `"full_outer"` for bikes + taxis (include windows with only bikes OR only taxis)
- `"left"` for weather (always keep transport windows, even if weather missing)

---

#### Step 4: Fill Missing Values
```python
combined = combined.fillna({
    "total_bike_rentals": 0,
    "total_taxi_rides": 0
})
```

**Why?**
- Some windows might have no bikes (late night)
- Some windows might have no taxis
- Fill with 0 instead of NULL (easier for analytics)

---

#### Step 5: Calculate Combined Metrics
```python
# Total transport usage
combined = combined.withColumn(
    "total_transport_usage",
    col("total_bike_rentals") + col("total_taxi_rides")
)

# Mode share percentages
combined = combined.withColumn(
    "bike_share_pct",
    when(col("total_transport_usage") > 0,
         (col("total_bike_rentals") / col("total_transport_usage")) * 100)
    .otherwise(0.0)
)
```

---

### Output Schema (Combined Transport-Weather) ⭐
This is the **most important** output - used by all analytics functions!

```
window_start: timestamp
window_end: timestamp
total_bike_rentals: integer
total_taxi_rides: integer
total_transport_usage: integer (bikes + taxis)
avg_bike_duration_min: double
avg_taxi_price: double
avg_taxi_surge: double
bike_share_pct: double (% of transport that is bikes)
avg_temperature_c: double
avg_wind_speed_ms: double
avg_weather_score: double
good_weather_ratio: double
dominant_temp_bucket: string
dominant_wind_category: string
```

**This DataFrame is input to ALL analytics functions!**

---

## 6. create_weather_binned_aggregations(combined_df) ✨ NEW

### Purpose
Creates graph-ready binned aggregations for scatter plots (directly addresses Oskar's request!).

### Input
```python
combined_df: Output from create_combined_transport_weather_window()
```

### What does it do?

#### Step 1: Add Bins to Each Row
```python
binned_df = combined_df.withColumn(
    "temp_bin",
    when(col("avg_temperature_c") < 0, "-5_to_0C")
    .when(col("avg_temperature_c") < 5, "0_to_5C")
    .when(col("avg_temperature_c") < 10, "5_to_10C")
    .when(col("avg_temperature_c") < 15, "10_to_15C")
    .when(col("avg_temperature_c") < 20, "15_to_20C")
    .when(col("avg_temperature_c") < 25, "20_to_25C")
    .otherwise("25_to_30C")
).withColumn(
    "wind_bin",
    when(col("avg_wind_speed_ms") < 3, "0_to_3ms")
    .when(col("avg_wind_speed_ms") < 6, "3_to_6ms")
    .when(col("avg_wind_speed_ms") < 9, "6_to_9ms")
    .when(col("avg_wind_speed_ms") < 12, "9_to_12ms")
    .otherwise("12+ms")
).withColumn(
    "weather_score_bin",
    when(col("avg_weather_score") < 30, "poor_0-30")
    .when(col("avg_weather_score") < 50, "fair_30-50")
    .when(col("avg_weather_score") < 70, "good_50-70")
    .otherwise("excellent_70+")
)
```

---

#### Step 2: Aggregate by Temperature Bins
```python
temp_binned = binned_df.filter(col("temp_bin") != "unknown").groupBy("temp_bin").agg(
    avg("total_bike_rentals").alias("avg_bike_rentals_in_temp_bin"),
    avg("total_taxi_rides").alias("avg_taxi_rides_in_temp_bin"),
    avg("total_transport_usage").alias("avg_total_usage_in_temp_bin"),
    count("*").alias("sample_count"),
    stddev("total_bike_rentals").alias("stddev_bike_rentals")
)
```

**What this does:**
Groups all windows by temperature bin, then calculates average bike rentals per bin.

**Example:**
```
Input (individual windows):
  [08:00-08:15], temp=7°C, bikes=145
  [08:15-08:30], temp=8°C, bikes=132
  [08:30-08:45], temp=9°C, bikes=150
  [16:00-16:15], temp=6°C, bikes=95

Output (aggregated by temp_bin):
  temp_bin="5_to_10C", avg_bikes=130.5, sample_count=4
```

---

### Output Schema (Binned Aggregations) - For Oskar! 🎯
```
temp_bin: string (e.g., "15_to_20C")
avg_bike_rentals_in_temp_bin: double
avg_taxi_rides_in_temp_bin: double
avg_total_usage_in_temp_bin: double
sample_count: integer (how many windows in this bin)
stddev_bike_rentals: double (spread of values)
aggregation_type: "by_temperature_bin"
```

**How Oskar uses this:**
```sql
SELECT
  temp_bin,
  avg_bike_rentals_in_temp_bin
FROM weather_binned_metrics
ORDER BY temp_bin;
```

Result creates scatter plot:
- X-axis: Temperature bins
- Y-axis: Average bike rentals
- Shows peak at 15-20°C!

---

## Time Window Synchronization Explained

### The Problem
```
Bike rental at 08:03:12
Weather observation at 08:00:00
Bike rental at 08:05:47
Weather observation at 08:15:00
Taxi ride at 08:07:23
```

**Question:** What weather was it when bike was rented at 08:03:12?

**Answer:** We don't have weather at exactly 08:03:12!

---

### The Solution: Time Windows
```
Window [08:00 - 08:15]:
  ├─ Bike at 08:03:12  ┐
  ├─ Bike at 08:05:47  ├─ All belong to same window!
  ├─ Taxi at 08:07:23  ┘
  └─ Weather at 08:00:00

Average bike rentals in [08:00-08:15] = 2 rentals
Average temperature in [08:00-08:15] = 18.2°C

Correlation: "When temp is 18.2°C, we see ~2 bike rentals per window"
```

---

## Window Functions in Spark

### Syntax
```python
window(timeColumn, windowDuration, slideDuration)
```

**Parameters:**
- `timeColumn`: Column with timestamp (e.g., `start_time_ts`)
- `windowDuration`: Length of window (e.g., `"15 minutes"`)
- `slideDuration`: (Optional) How often windows start (for sliding windows)

**Examples:**
```python
# Tumbling window (no overlap)
window(col("start_time_ts"), "15 minutes")

# Sliding window (overlap)
window(col("start_time_ts"), "15 minutes", "5 minutes")
```

---

### Window Duration Options
```python
"5 minutes"
"15 minutes"
"1 hour"
"2 hours"
"1 day"
```

**From config.py:**
- `WINDOW_DURATION_SHORT = "5 minutes"` (real-time)
- `WINDOW_DURATION_MEDIUM = "15 minutes"` (correlations)
- `WINDOW_DURATION_LONG = "1 hour"` (safety, summaries)

---

## Aggregation Functions Used

### Count
```python
count("*")  # Count all rows
count("column_name")  # Count non-null values
count(when(condition, 1))  # Conditional count
```

### Statistical Functions
```python
avg("column")  # Average
sum("column")  # Sum
min("column")  # Minimum
max("column")  # Maximum
stddev("column")  # Standard deviation
```

### First/Last
```python
first("column")  # First value in group
last("column")  # Last value in group
```

**Note:** For categories, `first()` gives "dominant" category (most recent observation).

---

## Performance Considerations

### 1. Window Size vs Performance
**Smaller windows (5 minutes):**
- ✅ More granular data
- ✅ Faster updates
- ❌ More windows → more output data
- ❌ Less data per window → noisier statistics

**Larger windows (1 hour):**
- ✅ More stable statistics
- ✅ Fewer windows → less output data
- ❌ Less granular
- ❌ Slower to reflect changes

**Our choice: 15 minutes** (balance)

---

### 2. Join Performance
```python
combined = bike_total.join(taxi_total, ["window_start", "window_end"])
```

**Why join on window boundaries?**
- Both DataFrames have same windows (synchronized)
- Simple equality join (fast!)
- No complex time-range joins needed

---

### 3. Tumbling vs Sliding
**Tumbling (what we use):**
- Each event in exactly 1 window
- Less computation
- Simpler to reason about

**Sliding:**
- Each event in multiple windows (overlap)
- More computation (same data aggregated multiple times)
- Smoother trends

**Our choice: Tumbling** (simpler, faster)

---

## Why Windowed Aggregations Matter

### 1. Enables Correlation Analysis
Without windows:
```
❌ Can't correlate individual bike trip with weather reading
```

With windows:
```
✅ Can correlate avg bikes per window with avg weather per window
```

---

### 2. Reduces Data Volume
**Before windowing:**
```
Raw events: 10,000 bike trips/hour
            5,000 taxi rides/hour
            4 weather observations/hour
Total: 15,004 records/hour
```

**After windowing (15-min windows):**
```
Bike aggregations: 4 windows/hour
Taxi aggregations: 4 windows/hour
Weather aggregations: 4 windows/hour
Combined: 4 records/hour (with all info!)

96% data reduction!
```

---

### 3. Time Alignment
**Before:**
- Bikes timestamp: second precision (08:03:12)
- Weather timestamp: minute precision (08:00:00)
- Can't join directly!

**After:**
- Both: 15-minute window precision ([08:00-08:15])
- Can join on window boundaries!

---

### 4. Statistical Stability
**Individual event:**
```
One bike trip at 08:03:12: duration = 1200 seconds
→ Not representative!
```

**Windowed average:**
```
145 bike trips in [08:00-08:15]: avg duration = 623 seconds
→ Much more reliable statistic!
```

---

## Complete Data Flow Example

Let's trace one hour through windowed aggregations:

### 1. Raw Events (08:00 - 09:00)
```
Bikes:    145 + 132 + 150 + 138 = 565 trips (4 windows × ~140/window)
Taxis:    67 + 71 + 65 + 69 = 272 rides (4 windows × ~68/window)
Weather:  4 observations (one per 15-min)
```

---

### 2. After aggregate_bike_data_by_window()
```
[08:00-08:15]: 145 bikes, avg duration 10.4 min, 89 Subscriber, 56 Customer
[08:15-08:30]: 132 bikes, avg duration 11.2 min, 85 Subscriber, 47 Customer
[08:30-08:45]: 150 bikes, avg duration 9.8 min, 95 Subscriber, 55 Customer
[08:45-09:00]: 138 bikes, avg duration 10.1 min, 87 Subscriber, 51 Customer
```

---

### 3. After aggregate_taxi_data_by_window()
```
[08:00-08:15]: 67 taxis, avg price $8.50, avg surge 1.2
[08:15-08:30]: 71 taxis, avg price $9.20, avg surge 1.3
[08:30-08:45]: 65 taxis, avg price $8.80, avg surge 1.1
[08:45-09:00]: 69 taxis, avg price $9.00, avg surge 1.2
```

---

### 4. After aggregate_weather_data_by_window()
```
[08:00-08:15]: avg temp 18.2°C, avg wind 4.5 m/s, avg score 72
[08:15-08:30]: avg temp 18.5°C, avg wind 4.8 m/s, avg score 71
[08:30-08:45]: avg temp 19.0°C, avg wind 5.2 m/s, avg score 69
[08:45-09:00]: avg temp 19.5°C, avg wind 5.5 m/s, avg score 68
```

---

### 5. After create_combined_transport_weather_window()
```
[08:00-08:15]: 145 bikes, 67 taxis, 212 total, 68% bike share, 18.2°C, 4.5 m/s, score 72
[08:15-08:30]: 132 bikes, 71 taxis, 203 total, 65% bike share, 18.5°C, 4.8 m/s, score 71
[08:30-08:45]: 150 bikes, 65 taxis, 215 total, 70% bike share, 19.0°C, 5.2 m/s, score 69
[08:45-09:00]: 138 bikes, 69 taxis, 207 total, 67% bike share, 19.5°C, 5.5 m/s, score 68
```

Now this combined data goes to analytics functions!

---

## Key Points for Your Exam

### About windowed_aggregations.py:
1. **Purpose**: Group streaming events into time windows for aggregation
2. **5 functions**: Aggregate bikes, taxis, weather, accidents separately, then combine
3. **Key function**: `create_combined_transport_weather_window()` aligns all data
4. **Window type**: Tumbling windows (no overlap)
5. **Window size**: 15 minutes for correlations (configurable)

### Why it matters:
1. **Time alignment**: Synchronizes asynchronous streams
2. **Data reduction**: 10,000 events → 4 windows (96% reduction)
3. **Statistical stability**: Averages over window more reliable than individual events
4. **Enables correlation**: Can join bikes + weather on window boundaries
5. **Graph-ready**: Binned aggregations create x-y pairs for scatter plots

### Technical details:
- **Tumbling windows**: Each event in exactly 1 window
- **Join on window boundaries**: Simple equality join
- **Full outer join**: Include windows with only bikes OR only taxis
- **fillna(0)**: Replace NULL counts with 0
- **Composite metrics**: total_transport_usage, bike_share_pct

### For correlation analysis:
- Combined DataFrame is input to ALL analytics functions
- Each window has bikes, taxis, AND weather aligned
- Can calculate: "When temp is X, we see Y bike rentals"
- Enables Oskar's scatter plot: temp vs bikes

---

## Summary: The Key Insight

**Without windowing:**
```
❌ 10,000 bike trips with different timestamps
❌ 4 weather observations with different timestamps
❌ Can't correlate them!
```

**With windowing:**
```
✅ 4 windows × 15 minutes = 1 hour
✅ Each window has: avg bikes, avg taxis, avg weather
✅ Can correlate: "In window with temp 18.2°C, we see 145 bikes"
✅ This is what enables ALL the weather-transport analytics!
```

**The combined DataFrame from `create_combined_transport_weather_window()` is the foundation for all correlation analysis in analytics.py!** 🎯
