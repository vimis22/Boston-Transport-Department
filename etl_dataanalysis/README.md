# ETL Data Analysis Pipeline

## Overview

This ETL (Extract, Transform, Load) pipeline consumes streaming data from Kafka topics and performs comprehensive data analysis to support the Boston Transport Department dashboard.

### What This Pipeline Does

1. **Extraction**: Reads real-time data from Kafka topics (bike rentals, taxi rides, weather, accidents)
2. **Transformation**: Parses, enriches, and validates streaming data
3. **Analytics**: Computes meaningful metrics and correlations
4. **Loading**: Writes processed data and analytics to HDFS/Hive for dashboard visualization

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  DATA FLOW PIPELINE                      │
└─────────────────────────────────────────────────────────┘

Kafka Topics                ETL Processing              Output
─────────────               ────────────────            ────────

bike-trips      ─┐         ┌─> Parse & Enrich      ─> /data/processed/bike_trips/
                 │         │
taxi-trips      ─┼─> ETL ─>├─> Parse & Enrich      ─> /data/processed/taxi_trips/
                 │         │
weather-data    ─┤         ├─> Parse & Enrich      ─> /data/processed/weather_data/
                 │         │   + Weather Categories
accidents       ─┘         │   + Buckets & Scores
                           │
                           ├─> Window Aggregations  ─> /data/analytics/
                           │   (5min, 15min, 1hour)     - weather_transport_correlation
                           │                             - weather_safety_analysis
                           └─> Correlation Metrics      - surge_weather_correlation
                               + Risk Scores             - transport_usage_summary
```

---

## Key Features

### 🌤️ Weather Enrichment

The pipeline enriches raw NOAA weather data with:

- **Temperature parsing**: Converts coded strings to Celsius
- **Wind speed extraction**: Parses wind observations to m/s
- **Visibility calculation**: Extracts visibility in meters
- **Weather categories**:
  - Temperature buckets (5°C intervals)
  - Wind categories (calm/breezy/windy/stormy)
  - Visibility levels (poor/moderate/good/excellent)
- **Weather quality score**: 0-100 scale for overall conditions

**Example:**
```
Raw: TMP = "+0156,1"  →  Parsed: 15.6°C  →  Bucket: "15_to_20C"
Raw: WND = "160,1,N,0046,1"  →  Wind: 4.6 m/s  →  Category: "light_breeze"
```

### 📊 Time-Based Windows

Multiple time windows for different analytics needs:

| Window Type | Duration | Use Case |
|-------------|----------|----------|
| Short | 5 minutes | Real-time metrics |
| Medium | 15 minutes | Weather-transport correlation |
| Long | 1 hour | Safety analysis, hourly summaries |
| Sliding | 5 min slide | Trend detection |

### 🔬 Analytics Computations

#### 1. Weather-Transport Correlation

**Location**: `/data/analytics/weather_transport_correlation/`

**Metrics**:
- Total bike rentals and taxi rides per time window
- Transport usage by weather category
- Weather impact score (-100 to +100)
- Transport elasticity (% change per weather condition)
- Expected vs. actual demand deviation

**Dashboard Usage**: Scatter plots showing bike/taxi demand vs. temperature, rain, wind

#### 2. Weather-Safety Risk Analysis

**Location**: `/data/analytics/weather_safety_analysis/`

**Metrics**:
- Accident count by weather condition
- Weather risk multiplier (1.0x to 3.0x)
- Safety risk index (0-10 scale)
- Risk categories (low/moderate/high/very_high)
- High-risk transport modes

**Dashboard Usage**: Heat maps showing accident risk by weather, safety alerts

#### 3. Surge-Weather Correlation

**Location**: `/data/analytics/surge_weather_correlation/`

**Metrics**:
- Surge multiplier trends
- Weather-driven surge score
- Surge classification (weather_driven/demand_driven/no_surge)
- Precipitation and temperature at surge time

**Dashboard Usage**: Line graphs showing surge pricing vs. weather conditions

#### 4. Transport Usage Summary

**Location**: `/data/analytics/transport_usage_summary/`

**Metrics**:
- Hourly bike and taxi counts
- Mode share percentages
- Peak hour indicators
- Weekend vs. weekday patterns
- User demographics (subscribers vs. customers)

**Dashboard Usage**: Time series visualizations, daily/weekly patterns

---

## Data Schemas

### Input Data (from Kafka)

#### Bike Data
```json
{
  "data": {
    "trip_id": "...",
    "duration_seconds": 600,
    "start_time": "2018-01-15 09:30:00",
    "stop_time": "2018-01-15 09:40:00",
    "start_station": { "id": "42", "name": "...", "latitude": 42.36, "longitude": -71.05 },
    "end_station": { ... },
    "bike_id": "1234",
    "user_type": "Subscriber",
    "birth_year": 1990,
    "gender": 2
  },
  "timestamp": "2024-12-04T...",
  "source": "bike-streamer"
}
```

#### Weather Data (NOAA Format)
```json
{
  "data": {
    "station": "72509014739",
    "datetime": "2018-01-15T09:00:00",
    "observations": {
      "temperature": "+0156,1",  // 15.6°C
      "wind": "160,1,N,0046,1",  // 4.6 m/s
      "visibility": "016000,1,9,9"  // 16000m
    }
  }
}
```

### Output Data (Analytics)

#### Weather-Transport Correlation
```
window_start | window_end | total_bike_rentals | total_taxi_rides | avg_temperature_c | avg_weather_score | weather_transport_impact_score | bike_weather_elasticity
-------------|------------|-------------------|------------------|-------------------|-------------------|-------------------------------|------------------------
2018-01-15 09:00 | 09:15 | 150 | 75 | 15.2 | 85.0 | 70.0 | 0.0
2018-01-15 09:15 | 09:30 | 95 | 120 | 2.1 | 35.0 | -30.0 | -0.3
```

---

## Configuration

### Environment Variables

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092

# Output Paths
OUTPUT_BASE_PATH=/data/processed_simple
ANALYTICS_OUTPUT_PATH=/data/analytics

# Checkpoint Paths (for fault tolerance)
CHECKPOINT_BASE_PATH=/tmp/spark_checkpoints_simple
ANALYTICS_CHECKPOINT_PATH=/tmp/spark_checkpoints_analytics

# Enable/Disable Analytics
ENABLE_WEATHER_TRANSPORT_CORRELATION=true
ENABLE_WEATHER_SAFETY_ANALYSIS=true
ENABLE_SURGE_WEATHER_CORRELATION=true
ENABLE_TRANSPORT_USAGE_SUMMARY=true
```

### Window Configuration

Edit `config.py` to adjust time windows:

```python
WINDOW_DURATION_SHORT = "5 minutes"   # Real-time metrics
WINDOW_DURATION_MEDIUM = "15 minutes" # Correlations
WINDOW_DURATION_LONG = "1 hour"       # Safety analysis
SLIDE_DURATION = "5 minutes"          # Sliding window interval
```

---

## Running the Pipeline

### Prerequisites

1. Kafka topics must be created and populated by streamers
2. Spark cluster must be running
3. HDFS must be accessible

### Start the ETL Pipeline

```bash
# From the project root
python -m etl_dataanalysis.main
```

### Expected Output

```
=== Starting Boston Transport ETL with Data Analysis ===
Spark Version: 3.5.0
Kafka: kafka-broker:9092
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

=== ALL STREAMS STARTED (8 total) ===
Raw data → /data/processed_simple/
Analytics → /data/analytics/

Streaming queries are running. Press Ctrl+C to stop.
```

---

## File Structure

```
etl_dataanalysis/
├── main.py                      # Main entry point, orchestrates all streams
├── config.py                    # Configuration settings
├── schemas.py                   # Kafka message schemas
├── transformations.py           # Parsing and basic transformations
├── weather_enrichment.py        # Weather parsing and categorization (NEW)
├── windowed_aggregations.py    # Time-based window operations (NEW)
├── analytics.py                 # Correlation metrics and risk scores (NEW)
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## Analytics Use Cases

### For Dashboard Visualizations

#### 1. **Scatter Plot: Temperature vs. Bike Rentals**
- **Data**: `weather_transport_correlation`
- **X-axis**: `avg_temperature_c`
- **Y-axis**: `total_bike_rentals`
- **Color**: `avg_weather_score`

#### 2. **Line Chart: Hourly Transport Usage**
- **Data**: `transport_usage_summary`
- **X-axis**: `window_start` (time)
- **Y-axis**: `bike_count`, `taxi_count`
- **Filter**: `is_peak_hour`, `is_weekend`

#### 3. **Heat Map: Accident Risk by Weather**
- **Data**: `weather_safety_analysis`
- **Rows**: `dominant_temp_bucket`
- **Columns**: `dominant_wind_category`
- **Color**: `safety_risk_index_normalized` (0-10)

#### 4. **Bar Chart: Weather Impact on Mode Share**
- **Data**: `weather_transport_correlation`
- **X-axis**: `dominant_temp_bucket`
- **Y-axis**: `bike_share_pct`, `100 - bike_share_pct` (taxi share)
- **Group by**: Weather category

---

## Technical Details

### Weather Parsing Functions

**Temperature Parsing**:
```python
TMP = "+0156,1"  # Raw NOAA format
→ Extract "+0156"
→ Convert to float: 156.0
→ Divide by 10: 15.6°C
```

**Wind Speed Parsing**:
```python
WND = "160,1,N,0046,1"  # Direction, quality, type, speed, quality
→ Extract speed: "0046"
→ Convert: 46.0
→ Divide by 10: 4.6 m/s
```

### Weather Scoring Algorithm

```python
weather_score = (
    temperature_component(0-40) +  # Ideal: 15-25°C
    wind_component(0-30) +         # Prefer calm to light breeze
    visibility_component(0-30)     # Prefer excellent visibility
)
# Result: 0-100 scale
```

### Elasticity Calculation

```python
elasticity = (actual_demand - baseline_demand) / baseline_demand

Examples:
- Cold weather: -30% bike demand (elasticity = -0.3)
- Rain: +40% taxi demand (elasticity = +0.4)
- Comfortable: 0% change (elasticity = 0.0)
```

---

## Troubleshooting

### Common Issues

1. **Import errors**:
   ```bash
   # Ensure you're running from project root
   python -m etl_dataanalysis.main
   ```

2. **Kafka connection timeout**:
   ```
   Check KAFKA_BOOTSTRAP_SERVERS in config.py
   Verify Kafka is running: kubectl get pods -n bigdata
   ```

3. **Empty analytics output**:
   ```
   - Check if streamers are producing data
   - Verify time windows match data timestamps (2018 for bikes!)
   - Look at Spark logs for errors
   ```

4. **Checkpoint errors**:
   ```bash
   # Clear checkpoints if schema changed
   hdfs dfs -rm -r /tmp/spark_checkpoints_analytics/*
   ```

---

## Performance Tuning

### Batch Interval

Adjust processing frequency in `config.py`:
```python
BATCH_INTERVAL = "10 seconds"  # Default
# For high volume: "5 seconds"
# For low volume: "30 seconds"
```

### Window Sizes

Larger windows = fewer updates, more data per batch:
```python
WINDOW_DURATION_MEDIUM = "15 minutes"  # Default
# For more granular: "5 minutes"
# For less frequent: "30 minutes"
```

---

## Future Enhancements

Potential additions:
- [ ] Machine learning models for demand prediction
- [ ] Anomaly detection for unusual patterns
- [ ] Real-time alerts for high-risk conditions
- [ ] Geographic clustering analysis
- [ ] Seasonal trend decomposition

---

## Contact

For questions or issues with the ETL pipeline:
- Check Spark UI at http://localhost:4040 for streaming query details
- Review logs for error messages
- Inspect checkpoint directories for state information

Last updated: December 2024
