# ETL Data Analysis - Boston Transport Department

**Real-time streaming ETL** der kombinerer transport data (bikes, taxis) med vejrdata og ulykker for at producere analytics og korrelations-analyser.

---

## 🎯 Hvad gør denne ETL?

Denne ETL læser **4 Kafka topics**, bearbejder dataen i real-time med **Spark Structured Streaming**, og producerer analytics:

```
Kafka Topics                    Streaming Processing              Output
─────────────                   ────────────────────              ──────
bike-trips      ─┐
taxi-trips      ─┤──> Avro decode ──> Parse ──> Enrich ──> Aggregate ──> Analytics ──> Parquet
weather-data    ─┤                                                                      │
accidents       ─┘                                                                      └──> Kafka
```

**Input:** 4 Kafka topics (Avro format)
**Processing:** Spark Structured Streaming
**Output:** Parquet filer (partitioneret) + Kafka topics (Avro)

---

## 🔗 Forhold til `src/etl`

Vi følger **samme mønster** som eksemplerne i `src/etl/jobs/`:

| Feature | `src/etl` | `etl_dataanalysis` |
|---------|-----------|-------------------|
| Kafka + Avro | ✅ | ✅ Identisk |
| Schema Registry | ✅ | ✅ Identisk |
| Confluent Wire Format | ✅ | ✅ Identisk (5-byte header) |
| Watermarking | ✅ | ✅ Alle streams (10 min) |
| Query names | ✅ | ✅ Alle streams navngivet |
| Checkpoints | ✅ | ✅ Fault tolerance |
| Spark Connect | ✅ | ✅ Remote cluster support |

**Ekstra:** Vi har tilføjet **Separation of Concerns** (SOLID) og **10+ analytics streams**.

---

## 📁 Projekt Struktur

```
etl_dataanalysis/
├── mainconfig/          # Spark session, Kafka læsning/skrivning, orchestration
├── transformations/     # Parse Avro data fra Kafka (decode + parse)
├── enrichments/         # Vejr parsing (temperature, wind, visibility UDFs)
├── aggregations/        # Time-windowed aggregations (15 min vinduer)
├── analytics/           # Korrelations-analyser (vejr vs transport)
└── schemas/             # Avro schema definitions

Hvert modul følger samme struktur:
├── parent/              # Facade klasse (orchestration)
└── logic_children/      # Individuelle funktioner (business logic)
```

**Design princip:** Parent kalder children → **Separation of Concerns** (SOLID).

---

## 🚀 Sådan kører du ETL'en

### 1. Environment Variables

```bash
# Kafka & Schema Registry (samme som src/etl)
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SCHEMA_REGISTRY_URL="http://localhost:8081"

# Spark Connect (hvis Kubernetes)
export USE_SPARK_CONNECT="true"
export SPARK_CONNECT_URL="sc://spark-connect-server:15002"

# Output paths
export OUTPUT_BASE_PATH="/data/processed_simple"
export ANALYTICS_OUTPUT_PATH="/data/analytics"
```

### 2. Kør ETL'en

```bash
# Lokal mode
python -m etl_dataanalysis.mainconfig.parent.main

# Med Spark Connect (Kubernetes)
USE_SPARK_CONNECT=true python -m etl_dataanalysis.mainconfig.parent.main
```

---

## 📊 Data Flow - Detaljeret

### Step 1: **Læs fra Kafka**
```python
# mainconfig/logic_children/read_kafka_stream.py
spark.readStream.format("kafka").option("subscribe", "bike-trips").load()
```

### Step 2: **Decode Avro** (Confluent Wire Format)
```python
# transformations/logic_children/decode_avro_payload.py
from_avro(substring(value, 6, length(value)-5), schema)  # Skip 5-byte header
```

### Step 3: **Parse & Watermark**
```python
# transformations/logic_children/parse_bike_stream.py
df.withWatermark("start_time_ts", "10 minutes")  # Late data tolerance
```

### Step 4: **Enrich** (kun vejr data)
```python
# enrichments/logic_children/enrich_weather_data.py
# Parse temperature, wind, visibility + categorize
```

### Step 5: **Aggregate** (time windows)
```python
# aggregations/logic_children/aggregate_bike_data_by_window.py
df.groupBy(window("start_time_ts", "15 minutes")).agg(...)
```

### Step 6: **Analytics** (korrelationer)
```python
# analytics/logic_children/calculate_weather_transport_correlation.py
# Beregn Pearson korrelationer, vejr-impact, etc.
```

### Step 7: **Write Output**
```python
# mainconfig/logic_children/write_parquet_stream.py
df.writeStream.format("parquet").partitionBy("year", "month").start()
```

---

## 🎓 Analytics Streams (10+)

Denne ETL producerer følgende analytics:

1. **Vejr-Transport Korrelation** - Hvordan påvirker vejret bike/taxi brug?
2. **Vejr-Safety Risk** - Ulykker vs vejrforhold
3. **Surge Pricing** - Taxi surge vs vejr
4. **Pearson Korrelationer** - Statistiske korrelationer (r-værdier)
5. **Precipitation Impact** - Regn → modal substitution (bike → taxi)
6. **Temporal Segmentation** - Rush hour vs weekend patterns
7. **Accident-Weather Correlation** - Ulykker per 1000 ture i dårligt vejr
8. **Binned Aggregations** - Graf-klar data (scatter plots)
9. **Multi-Variable Summary** - Kombinerede vejr faktorer
10. **Transport Usage Summary** - Time-series for dashboard

Alle analytics skrives til: `/data/analytics/{stream_name}/`

---

## 🔍 Nøgle Features

### 1. Production-Ready (som `src/etl`)
- ✅ **Watermarking:** Alle streams har 10 min late data tolerance
- ✅ **Checkpoints:** Fault recovery ved crash
- ✅ **Query Names:** Alle queries synlige i Spark UI
- ✅ **Partitioning:** Output partitioneret (år/måned/dag/time)

### 2. SOLID Architecture
- ✅ **36 logic_children filer** - Hver funktion i sin egen fil
- ✅ **Parent = Facade** - Kalder kun children, ingen business logic
- ✅ **77% reduktion** - Parent filer reduceret fra 2,172 → 501 linjer
- ✅ **Testbar** - Hver funktion kan unit testes isoleret

### 3. Schema Registry Integration
```python
# mainconfig/logic_children/get_latest_schema.py
schema, schema_id = get_latest_schema("bike-trips-value")
# Henter seneste schema fra Schema Registry
```

### 4. Confluent Wire Format (Avro)
```python
# mainconfig/logic_children/write_to_kafka_with_avro.py
header = bytearray([0]) + struct.pack(">I", schema_id)  # Magic byte + Schema ID
payload = concat(lit(header), to_avro(struct("*"), schema))
# Identisk med src/etl implementering
```

---

## 📈 Monitoring

### Spark UI
Alle queries er navngivet:
```
http://localhost:4040/StreamingQuery/
  - bike_trips
  - taxi_trips
  - weather_data
  - accidents
  - analytics_weather_transport_correlation
  - analytics_pearson_correlations
  ... (10+ analytics queries)
```

### Checkpoints
```
/tmp/spark_checkpoints_simple/
  ├── bike_trips/
  ├── taxi_trips/
  ├── weather_data/
  └── kafka_output/
```

---

## 🧪 Test Kompatibilitet med `src/etl`

```python
# 1. Test Avro decoding (identisk med src/etl)
from etl_dataanalysis.transformations.logic_children.decode_avro_payload import decode_avro_payload
# Same as: src/etl/jobs/bike-weather-data-aggregation.py:54

# 2. Test Schema Registry (identisk)
from etl_dataanalysis.mainconfig.logic_children.get_latest_schema import get_latest_schema
schema, schema_id = get_latest_schema("bike-trips-value")

# 3. Test Spark Connect support
from etl_dataanalysis.mainconfig.logic_children.create_spark_session import create_spark_session
spark = create_spark_session()  # Bruger Spark Connect hvis USE_SPARK_CONNECT=true
```

---

## 📊 Refaktorerings Statistik

Vi har refaktoreret **alle 6 moduler** til SOLID-compliance:

| Modul | Før (linjer) | Efter (linjer) | Logic Children | Reduktion |
|-------|--------------|----------------|----------------|-----------|
| mainconfig | 450+ | 300 | 6 filer | 33% |
| transformations | 259 | 37 | 5 filer | 86% |
| enrichments | 217 | 45 | 5 filer | 79% |
| aggregations | 412 | 45 | 6 filer | 89% |
| analytics | 735 | 51 | 10 filer | 93% |
| schemas | 99 | 23 | 4 filer | 77% |
| **TOTAL** | **2,172** | **501** | **36 filer** | **77%** |

**Resultat:** Koden er lettere at læse, vedligeholde og teste, samtidig med at funktionaliteten er 100% bevaret.

---

## ✅ Summary

**Denne ETL er:**
- ✅ **100% kompatibel med `src/etl`** - Samme Kafka/Avro/watermarking pattern
- ✅ **Production-ready** - Checkpoints, query names, fault tolerance
- ✅ **SOLID-compliant** - 36 logic_children filer, separation of concerns
- ✅ **Akademisk-klar** - 10+ analytics streams til research

**Kodebasen følger best practices fra `src/etl` men er bedre organiseret med SOLID principper.**

---

**Spørgsmål?** Se `src/etl/jobs/` for sammenlignelige eksempler eller kør ETL'en og tjek Spark UI på `http://localhost:4040`.
