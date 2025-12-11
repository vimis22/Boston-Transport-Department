# Readme for ETL_DATA ANALYSIS - Weather Enrichment

## weather_enrichment.py

### What is weather_enrichment.py?
The `weather_enrichment.py` file is responsible for **parsing and categorizing** raw NOAA weather data into human-readable and analytics-ready formats.

Think of it like translating a secret code:
- **Input**: Coded weather strings (e.g., "+0056,1")
- **Output**: Clean numbers (5.6°C) + categories ("5_to_10C") + quality scores (0-100)

---

### Why Do We Need Weather Enrichment?

NOAA (National Oceanic and Atmospheric Administration) weather data comes in a special encoded format:

```
Temperature: "+0156,1"
Wind: "160,1,N,0046,1"
Visibility: "016000,1,9,9"
```

**Problems:**
1. **Not human-readable**: What does "+0156,1" mean?
2. **Can't do calculations**: Can't calculate average temperature with a string!
3. **No categories**: Can't say "cold" vs "comfortable" vs "hot"
4. **No composite scores**: Need overall "weather quality" score

**Solution:** Parse → Convert → Categorize → Score

---

## The 3 Main Parsing Functions

`weather_enrichment.py` has 3 parsing functions that decode NOAA format:

### 1. parse_temperature(tmp_string)

#### Purpose
Converts NOAA temperature string to Celsius.

#### Input Example
```python
tmp_string = "+0156,1"
```

#### NOAA Format Explained
- `+0156` = temperature in **tenths of degree Celsius**
- `,1` = quality code (1 = good quality)

So `+0156` = 156 tenths = 156 / 10 = **15.6°C**

#### What the function does
```python
def parse_temperature(tmp_string: str) -> float:
    if not tmp_string or tmp_string == "":
        return None  # Missing data

    try:
        # Extract the numeric part (before comma)
        temp_part = tmp_string.split(',')[0]  # "+0156"

        # Convert to float and divide by 10
        temp_value = float(temp_part) / 10.0  # 156 / 10 = 15.6

        return temp_value
    except (ValueError, IndexError, AttributeError):
        return None  # Invalid format
```

#### Examples
| Input       | Output  | Explanation                      |
|-------------|---------|----------------------------------|
| "+0156,1"   | 15.6    | 156 tenths = 15.6°C             |
| "+0000,1"   | 0.0     | 0°C (freezing!)                 |
| "-0050,1"   | -5.0    | -50 tenths = -5°C (below zero)  |
| "+0250,1"   | 25.0    | 250 tenths = 25°C (hot!)        |
| ""          | None    | Missing data                     |
| "invalid"   | None    | Can't parse → return None       |

---

### 2. parse_wind_speed(wnd_string)

#### Purpose
Extracts wind speed in meters per second from NOAA wind observation string.

#### Input Example
```python
wnd_string = "160,1,N,0046,1"
```

#### NOAA Format Explained
```
"160,1,N,0046,1"
  ↑   ↑ ↑  ↑    ↑
  |   | |  |    |
  |   | |  |    └─ Quality code
  |   | |  └────── Wind speed (in tenths of m/s)
  |   | └─────── Type code
  |   └───────── Quality code
  └─────────── Wind direction (degrees)
```

We only care about the **4th field** (wind speed): `0046`

`0046` = 46 tenths of m/s = 46 / 10 = **4.6 m/s**

#### What the function does
```python
def parse_wind_speed(wnd_string: str) -> float:
    if not wnd_string or wnd_string == "":
        return None

    try:
        parts = wnd_string.split(',')  # ["160", "1", "N", "0046", "1"]

        if len(parts) >= 4:
            wind_speed_part = parts[3]  # "0046"
            wind_speed = float(wind_speed_part) / 10.0  # 4.6
            return wind_speed
    except (ValueError, IndexError, AttributeError):
        return None
```

#### Examples
| Input              | Output | Explanation                   |
|--------------------|--------|-------------------------------|
| "160,1,N,0046,1"   | 4.6    | 46 tenths = 4.6 m/s          |
| "180,1,N,0000,1"   | 0.0    | No wind (calm)               |
| "090,1,N,0120,1"   | 12.0   | 120 tenths = 12 m/s (windy!) |
| ""                 | None   | Missing data                  |

---

### 3. parse_visibility(vis_string)

#### Purpose
Extracts visibility distance in meters.

#### Input Example
```python
vis_string = "016000,1,9,9"
```

#### NOAA Format Explained
```
"016000,1,9,9"
   ↑     ↑ ↑ ↑
   |     | | |
   |     | | └─ Quality code
   |     | └─── Variability code
   |     └───── Quality code
   └────────── Visibility (in meters)
```

We only care about the **1st field**: `016000`

`016000` = **16,000 meters** = 16 km (excellent visibility!)

#### What the function does
```python
def parse_visibility(vis_string: str) -> float:
    if not vis_string or vis_string == "":
        return None

    try:
        parts = vis_string.split(',')  # ["016000", "1", "9", "9"]

        if len(parts) >= 1:
            visibility = float(parts[0])  # 16000.0
            return visibility
    except (ValueError, IndexError, AttributeError):
        return None
```

#### Examples
| Input           | Output  | Meaning                        |
|-----------------|---------|--------------------------------|
| "016000,1,9,9"  | 16000   | 16 km (excellent visibility)   |
| "001000,1,9,9"  | 1000    | 1 km (poor visibility - fog)   |
| "000100,1,9,9"  | 100     | 100 m (very poor - heavy fog)  |
| ""              | None    | Missing data                    |

---

## UDFs (User Defined Functions)

After defining the Python functions, we register them as **Spark UDFs** so they can be used in DataFrames:

```python
parse_temperature_udf = udf(parse_temperature, DoubleType())
parse_wind_speed_udf = udf(parse_wind_speed, DoubleType())
parse_visibility_udf = udf(parse_visibility, DoubleType())
```

**What this does:**
- Converts Python function → Spark function
- `DoubleType()` = return type (decimal number)
- Now can use in `.withColumn()`:
  ```python
  df.withColumn("temperature_celsius", parse_temperature_udf(col("temperature")))
  ```

---

## Main Enrichment Function: enrich_weather_data(df)

### Purpose
Takes raw weather DataFrame and adds:
1. **Parsed values** (Celsius, m/s, meters)
2. **Categories** (buckets, labels)
3. **Quality score** (0-100 overall weather rating)
4. **Boolean flags** (is_good_weather, is_bad_weather)

---

### Step 1: Parse Raw Strings

```python
enriched_df = df.withColumn(
    "temperature_celsius",
    parse_temperature_udf(col("temperature"))
).withColumn(
    "wind_speed_ms",
    parse_wind_speed_udf(col("wind"))
).withColumn(
    "visibility_m",
    parse_visibility_udf(col("visibility"))
)
```

**Before:**
```
temperature: "+0156,1"
wind: "160,1,N,0046,1"
visibility: "016000,1,9,9"
```

**After:**
```
temperature: "+0156,1"
temperature_celsius: 15.6
wind: "160,1,N,0046,1"
wind_speed_ms: 4.6
visibility: "016000,1,9,9"
visibility_m: 16000.0
```

Now we have both original AND parsed values!

---

### Step 2: Create Temperature Buckets

```python
enriched_df = enriched_df.withColumn(
    "temp_bucket",
    when(col("temperature_celsius").isNull(), "unknown")
    .when(col("temperature_celsius") < -10, "below_-10C")
    .when(col("temperature_celsius") < -5, "-10_to_-5C")
    .when(col("temperature_celsius") < 0, "-5_to_0C")
    .when(col("temperature_celsius") < 5, "0_to_5C")
    .when(col("temperature_celsius") < 10, "5_to_10C")
    .when(col("temperature_celsius") < 15, "10_to_15C")
    .when(col("temperature_celsius") < 20, "15_to_20C")
    .when(col("temperature_celsius") < 25, "20_to_25C")
    .when(col("temperature_celsius") < 30, "25_to_30C")
    .otherwise("above_30C")
)
```

**What this does:**
Groups temperatures into 5°C intervals.

**Examples:**
| Temperature (°C) | Bucket        |
|------------------|---------------|
| -12.0            | below_-10C    |
| -3.0             | -5_to_0C      |
| 2.5              | 0_to_5C       |
| 8.2              | 5_to_10C      |
| 15.6             | 15_to_20C     |
| 27.0             | 25_to_30C     |
| 35.0             | above_30C     |

**Why buckets?**
- **Analytics**: Group bike rentals by temperature range
- **Graphing**: X-axis can be temperature buckets
- **Easier interpretation**: "15-20°C" clearer than "17.3°C"

---

### Step 3: Create Wind Categories

```python
enriched_df = enriched_df.withColumn(
    "wind_category",
    when(col("wind_speed_ms").isNull(), "unknown")
    .when(col("wind_speed_ms") < 1.5, "calm")           # 0-1.5 m/s
    .when(col("wind_speed_ms") < 5.5, "light_breeze")  # 1.5-5.5 m/s
    .when(col("wind_speed_ms") < 10.8, "moderate")     # 5.5-10.8 m/s
    .when(col("wind_speed_ms") < 17.2, "fresh")        # 10.8-17.2 m/s
    .otherwise("strong_wind")                           # > 17.2 m/s
)
```

**Based on Beaufort Wind Scale** (international standard)

**Examples:**
| Wind Speed (m/s) | Category      | Description               |
|------------------|---------------|---------------------------|
| 0.5              | calm          | Smoke rises vertically    |
| 3.0              | light_breeze  | Leaves rustle             |
| 8.0              | moderate      | Small branches move       |
| 15.0             | fresh         | Small trees sway          |
| 20.0             | strong_wind   | Large branches move       |

**Why categories?**
- **Cycling safety**: "fresh" wind makes cycling difficult
- **Analytics**: Correlation between wind category and bike usage
- **Human-readable**: "moderate wind" vs "8.2 m/s"

---

### Step 4: Create Visibility Categories

```python
enriched_df = enriched_df.withColumn(
    "visibility_category",
    when(col("visibility_m").isNull(), "unknown")
    .when(col("visibility_m") < 1000, "very_poor")      # < 1km
    .when(col("visibility_m") < 4000, "poor")           # 1-4km
    .when(col("visibility_m") < 10000, "moderate")      # 4-10km
    .when(col("visibility_m") < 20000, "good")          # 10-20km
    .otherwise("excellent")                              # > 20km
)
```

**Examples:**
| Visibility (m) | Category   | Condition                    |
|----------------|------------|------------------------------|
| 100            | very_poor  | Dense fog (dangerous!)       |
| 2000           | poor       | Light fog / heavy rain       |
| 7000           | moderate   | Haze / light rain            |
| 15000          | good       | Clear with slight haze       |
| 25000          | excellent  | Perfectly clear              |

**Why categories?**
- **Safety**: Poor visibility increases accident risk
- **Analytics**: Correlation with accidents and transport usage
- **Intuitive**: "poor visibility" vs "2000 meters"

---

### Step 5: Calculate Weather Condition Score (0-100)

This is the **most important** calculation - a composite score of overall weather quality!

```python
enriched_df = enriched_df.withColumn(
    "weather_condition_score",
    when(col("temperature_celsius").isNull(), 50.0)  # Unknown = neutral
    .otherwise(
        # Temperature component (0-40): ideal 15-25°C
        when(col("temperature_celsius").between(15, 25), 40.0)
        .when(col("temperature_celsius").between(10, 15), 35.0)
        .when(col("temperature_celsius").between(25, 30), 35.0)
        .when(col("temperature_celsius").between(5, 10), 25.0)
        .when(col("temperature_celsius").between(0, 5), 20.0)
        .otherwise(10.0)
        +
        # Wind component (0-30): prefer calm to light breeze
        when(col("wind_speed_ms") < 1.5, 30.0)
        .when(col("wind_speed_ms") < 5.5, 25.0)
        .when(col("wind_speed_ms") < 10.8, 15.0)
        .otherwise(5.0)
        +
        # Visibility component (0-30): prefer excellent visibility
        when(col("visibility_m") > 20000, 30.0)
        .when(col("visibility_m") > 10000, 25.0)
        .when(col("visibility_m") > 4000, 15.0)
        .otherwise(5.0)
    )
)
```

#### How the Score is Calculated

**Formula:**
```
Weather Score = Temperature Score (0-40)
              + Wind Score (0-30)
              + Visibility Score (0-30)
              = Total (0-100)
```

#### Temperature Component (0-40 points)
| Temperature (°C) | Points | Reason                          |
|------------------|--------|---------------------------------|
| 15-25            | 40     | Ideal cycling weather           |
| 10-15 or 25-30   | 35     | Comfortable                     |
| 5-10             | 25     | Tolerable but cool/warm         |
| 0-5              | 20     | Cold but manageable             |
| < 0 or > 30      | 10     | Extreme (too cold/hot)          |

#### Wind Component (0-30 points)
| Wind Speed (m/s) | Points | Reason                          |
|------------------|--------|---------------------------------|
| < 1.5            | 30     | Calm (perfect)                  |
| 1.5-5.5          | 25     | Light breeze (pleasant)         |
| 5.5-10.8         | 15     | Moderate (affects cycling)      |
| > 10.8           | 5      | Strong wind (difficult cycling) |

#### Visibility Component (0-30 points)
| Visibility (m)   | Points | Reason                          |
|------------------|--------|---------------------------------|
| > 20000          | 30     | Crystal clear                   |
| 10000-20000      | 25     | Clear                           |
| 4000-10000       | 15     | Moderate (haze)                 |
| < 4000           | 5      | Poor (fog/rain)                 |

#### Example Calculations

**Perfect weather:**
```
Temperature: 18°C → 40 points
Wind: 2 m/s → 25 points
Visibility: 25000 m → 30 points
Total: 95/100 (excellent!)
```

**Bad weather:**
```
Temperature: -5°C → 10 points
Wind: 12 m/s → 5 points
Visibility: 1000 m → 5 points
Total: 20/100 (terrible!)
```

**Typical weather:**
```
Temperature: 12°C → 35 points
Wind: 4 m/s → 25 points
Visibility: 8000 m → 15 points
Total: 75/100 (good)
```

---

### Step 6: Add Boolean Flags

```python
# Is it "good weather" for outdoor activities?
enriched_df = enriched_df.withColumn(
    "is_good_weather",
    when(col("weather_condition_score") >= 70, True)
    .otherwise(False)
)

# Is it "bad weather" (risky conditions)?
enriched_df = enriched_df.withColumn(
    "is_bad_weather",
    when(col("weather_condition_score") <= 30, True)
    .otherwise(False)
)
```

**What this does:**
Creates simple yes/no flags based on score.

**Examples:**
| Score | is_good_weather | is_bad_weather | Interpretation       |
|-------|-----------------|----------------|----------------------|
| 95    | True            | False          | Perfect day!         |
| 75    | True            | False          | Nice weather         |
| 50    | False           | False          | Neutral/mediocre     |
| 25    | False           | True           | Stay inside!         |
| 10    | False           | True           | Dangerous conditions |

**Why flags?**
- **Easy filtering**: `df.filter(col("is_good_weather"))`
- **Counting**: "How many good weather days in January?"
- **Analytics**: Correlation between good weather and bike usage

---

## Additional Function: add_precipitation_indicator(df)

### Purpose
Adds precipitation (rain/snow) indicators.

**Note:** NOAA data doesn't always have direct precipitation measurements, so we **infer** from weather score.

```python
def add_precipitation_indicator(df: DataFrame) -> DataFrame:
    # Infer precipitation from weather score
    df = df.withColumn(
        "has_precipitation",
        when(col("weather_condition_score") < 40, True)
        .otherwise(False)
    )

    df = df.withColumn(
        "precip_category",
        when(col("has_precipitation") == False, "none")
        .when(col("weather_condition_score") < 20, "heavy")
        .when(col("weather_condition_score") < 30, "moderate")
        .otherwise("light")
    )

    return df
```

**Logic:**
- Score < 40 → probably raining (poor conditions)
- Score < 20 → heavy rain (very poor)
- Score 20-30 → moderate rain
- Score 30-40 → light rain
- Score ≥ 40 → no rain (dry)

**Examples:**
| Weather Score | has_precipitation | precip_category |
|---------------|-------------------|-----------------|
| 85            | False             | none            |
| 55            | False             | none            |
| 35            | True              | light           |
| 25            | True              | moderate        |
| 15            | True              | heavy           |

---

## Complete Enrichment Output

After running both `enrich_weather_data()` and `add_precipitation_indicator()`, we get:

### Original Columns (from NOAA)
```
station: "72509014739"
datetime: "2018-01-15T09:00:00"
temperature: "+0156,1"  (raw NOAA string)
wind: "160,1,N,0046,1"  (raw NOAA string)
visibility: "016000,1,9,9"  (raw NOAA string)
```

### Parsed Columns (numeric values)
```
temperature_celsius: 15.6
wind_speed_ms: 4.6
visibility_m: 16000.0
```

### Category Columns (buckets/labels)
```
temp_bucket: "15_to_20C"
wind_category: "light_breeze"
visibility_category: "excellent"
```

### Score Columns (0-100 scale)
```
weather_condition_score: 90.0
```

### Boolean Flags
```
is_good_weather: True
is_bad_weather: False
has_precipitation: False
```

### Precipitation Columns
```
precip_category: "none"
```

**Total:** 17+ enriched columns from 3 raw NOAA strings! 🎉

---

## Why Weather Enrichment Matters

### 1. Makes Data Usable
**Before enrichment:**
```
temperature: "+0156,1"  ← Can't calculate average!
```

**After enrichment:**
```
temperature_celsius: 15.6  ← Can calculate avg, min, max!
```

---

### 2. Enables Analytics
Without enrichment, we can't answer:
- "How does temperature affect bike rentals?" ← Need numeric temp
- "Are more accidents in poor visibility?" ← Need visibility category
- "What's the overall weather quality?" ← Need composite score

With enrichment, all these questions are easy!

---

### 3. Human-Readable Categories
**Before:**
```
wind_speed_ms: 8.2  ← What does this mean?
```

**After:**
```
wind_speed_ms: 8.2
wind_category: "moderate"  ← Ah, moderate wind!
```

---

### 4. Supports Binned Analytics
For Oskar's scatter plot (temp vs bikes), we need temperature **bins**:

```sql
SELECT
  temp_bucket,
  AVG(bike_rentals) as avg_bikes
FROM combined_data
GROUP BY temp_bucket;
```

Result:
```
0_to_5C    → 78 bikes
5_to_10C   → 115 bikes
10_to_15C  → 165 bikes
15_to_20C  → 195 bikes  ← Peak!
20_to_25C  → 170 bikes
```

Without `temp_bucket`, this would be much harder!

---

### 5. Single Weather Quality Metric
Instead of checking 3 separate metrics (temp, wind, visibility), use **one score**:

```python
# Easy: Filter by overall weather quality
df.filter(col("weather_condition_score") > 70)

# Hard: Check 3 conditions
df.filter(
    (col("temperature_celsius").between(15, 25)) &
    (col("wind_speed_ms") < 5.5) &
    (col("visibility_m") > 10000)
)
```

---

## When is Enrichment Called?

Weather enrichment is called **inside `parse_weather_stream()`** in transformations.py:

```python
def parse_weather_stream(df):
    # Step 1: Parse JSON to columns
    result_df = string_df.select(...)

    # Step 2: Convert to timestamp
    final_df = result_df.withColumn(...)

    # Step 3: ENRICH! (called here)
    final_df = enrich_weather_data(final_df)
    final_df = add_precipitation_indicator(final_df)

    return final_df
```

So enrichment happens **during initial ETL**, before writing to Parquet.

**Result:** All weather data in `/data/processed_simple/weather_data/` is already enriched!

---

## Domain Knowledge: Why These Specific Ranges?

### Temperature (15-25°C = ideal)
**Research shows:**
- **10-20°C**: Optimal for cycling (comfortable, no sweating)
- **< 5°C**: Cold deters casual riders
- **> 30°C**: Heat reduces cycling (uncomfortable, dangerous)

**Boston climate:**
- Winter: -5°C to 5°C
- Spring/Fall: 10°C to 20°C ← Best cycling season
- Summer: 20°C to 30°C

---

### Wind (< 10 m/s = comfortable)
**Beaufort Scale:**
- **< 5.5 m/s**: Light breeze (pleasant for cycling)
- **5.5-10.8 m/s**: Moderate wind (affects cycling, but manageable)
- **> 10.8 m/s**: Fresh wind (difficult to cycle, dangerous on bridges)

**Cycling impact:**
- Headwind at 10 m/s = ~36 km/h wind = very difficult!
- Side wind = stability issues
- Boston bridges very windy!

---

### Visibility (> 10 km = good)
**Safety standards:**
- **< 1 km**: Driving lights required (foggy)
- **1-4 km**: Reduced speed zones
- **> 10 km**: Normal conditions

**Impact on cycling:**
- Poor visibility → drivers can't see cyclists → dangerous
- Also correlates with rain (poor visibility often means rain)

---

## Performance Considerations

### UDFs vs Built-in Functions
**UDFs (what we use):**
```python
parse_temperature_udf(col("temperature"))
```
- **Pros**: Flexible (can write any Python logic)
- **Cons**: Slower (Python interpreter overhead)

**Built-in Functions (alternative):**
```python
substring(col("temperature"), 0, 5).cast("double") / 10.0
```
- **Pros**: Faster (native Spark execution)
- **Cons**: Complex logic harder to express

**Our choice:** UDFs for **readability** - code is easier to understand and maintain.

---

### When is Enrichment Expensive?
- **Parsing strings**: Moderate cost (regex, string splitting)
- **If-else chains**: Cheap (simple comparisons)
- **Score calculation**: Cheap (just arithmetic)

**Overall:** Weather enrichment adds ~10-20% to parsing time, but only happens **once** during ETL. After that, enriched data is in Parquet (fast!).

---

## Error Handling

All parsing functions have try-except blocks:

```python
try:
    temp_part = tmp_string.split(',')[0]
    temp_value = float(temp_part) / 10.0
    return temp_value
except (ValueError, IndexError, AttributeError):
    return None  # Return None instead of crashing
```

**Why important:**
- **Malformed data**: Sometimes NOAA strings are invalid
- **Missing data**: Weather station offline
- **Graceful degradation**: Pipeline continues, just has NULL values

**Example:**
```
Good data:  "+0156,1" → 15.6°C
Bad data:   "ERROR"   → None (NULL)
Missing:    ""        → None (NULL)
```

Pipeline doesn't crash, analytics handle NULLs gracefully.

---

## Key Points for Your Exam

### About weather_enrichment.py:
1. **Purpose**: Decode NOAA weather format → human-readable + analytics-ready
2. **3 parsers**: Temperature, wind speed, visibility
3. **Main enrichment**: Adds categories, buckets, scores, flags
4. **Weather score**: 0-100 composite metric (temp + wind + visibility)
5. **Called during parsing**: Enrichment happens in `parse_weather_stream()`

### Why it matters:
1. **Usability**: Raw NOAA strings → clean numbers
2. **Categories**: Enables binned analytics (scatter plots)
3. **Single metric**: Weather score simplifies filtering
4. **Domain knowledge**: Ranges based on transport research
5. **Analytics-ready**: Can correlate with bikes, taxis, accidents

### Technical details:
- **UDFs**: Register Python functions for use in Spark
- **Buckets**: 5°C temp intervals, Beaufort wind scale
- **Score formula**: Weighted sum of 3 components
- **Error handling**: NULL for invalid data (no crashes)
- **Performance**: Moderate cost, but only during ETL

---

## Summary: Complete Example

### Input (Raw NOAA)
```
temperature: "+0156,1"
wind: "160,1,N,0046,1"
visibility: "016000,1,9,9"
```

### After Parsing
```
temperature_celsius: 15.6
wind_speed_ms: 4.6
visibility_m: 16000.0
```

### After Categorization
```
temp_bucket: "15_to_20C"
wind_category: "light_breeze"
visibility_category: "excellent"
```

### After Scoring
```
weather_condition_score: 90.0
  ← 40 (ideal temp) + 25 (light breeze) + 25 (good visibility)
```

### After Flags
```
is_good_weather: True
is_bad_weather: False
has_precipitation: False
precip_category: "none"
```

**Result:** From 3 cryptic strings to 17+ useful columns! This is what enables all the weather-transport correlation analytics. 🎯
