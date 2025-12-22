# 🚀 BOSTON TRANSPORT - QUICK START GUIDE

## Prerequisites
- Docker Desktop with Kubernetes enabled ✅ (You have this)
- Python 3.11+ ✅ (You have this)
- Terraform (Need to install)

---

## STEP 1: Install Terraform (2 minutes)

### Windows - PowerShell (Run as Administrator):
```powershell
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# After Chocolatey installs:
choco install terraform -y
```

Verify:
```bash
terraform --version
```

---

## STEP 2: Deploy Everything (5-10 minutes)

```bash
cd C:\Users\vivek\Downloads\Boston-Transport-Department\infra\environments\local
terraform init
terraform apply -auto-approve
```

This deploys:
1. **Kafka** + Schema Registry + REST Proxy
2. **HDFS** (Hadoop) - NameNode + DataNode
3. **Spark** Connect Server + Workers
4. **Hive** Metastore + Thrift Server
5. **JupyterLab** (for notebooks)
6. **Time Manager** (simulated time for streaming)
7. **Streamer** (reads CSV → publishes to Kafka as Avro)
8. **Data Analysis ETL** (your Spark job)
9. **Dashboard** (visualizes results)

---

## STEP 3: Wait for Pods to Start (2-3 minutes)

```bash
kubectl get pods -n bigdata -w
```

Wait until all pods show `Running` status. Press `Ctrl+C` when done.

---

## STEP 4: Port Forward to Access Services

Open a NEW terminal and run:

```bash
cd C:\Users\vivek\Downloads\Boston-Transport-Department
python tools/forward-all.py
```

This forwards:
- **Dashboard**: http://localhost:3000 (commented out in script - uncomment line 56)
- **HDFS UI**: http://localhost:9870
- **Spark UI**: http://localhost:4040
- **JupyterLab**: http://localhost:8080 (token: `adminadmin`)
- **Kafka UI**: http://localhost:8083
- **Schema Registry**: http://localhost:8081
- **Time Manager**: http://localhost:8000
- **Hive**: localhost:10000

---

## STEP 5: Start the Data Pipeline

### 5.1 Check Time Manager Status
```bash
curl http://localhost:8000/api/v1/clock
```

### 5.2 Start Simulation (Streams Data)
```bash
curl -X POST http://localhost:8000/api/v1/clock/start
```

### 5.3 Watch Logs

**Streamer (produces to Kafka):**
```bash
kubectl logs -n bigdata -l app=streamer -f
```

**Data Analysis ETL (your calculations):**
```bash
kubectl logs -n bigdata -l app=data-analysis -f
```

---

## STEP 6: Verify Data Flow

### 6.1 Check Kafka Topics
```bash
kubectl exec -n bigdata svc/kafka-broker -- kafka-topics --bootstrap-server localhost:9092 --list
```

Should see: `bike-data`, `taxi-data`, `weather-data`, `accident-data`

### 6.2 Check Schemas in Registry
```bash
curl http://localhost:8081/subjects
```

Should see: `["bike-data-value", "taxi-data-value", "weather-data-value", "accident-data-value"]`

### 6.3 Check ETL Output in Spark Pod
```bash
# List output folders
kubectl exec -n bigdata deployment/data-analysis -- ls -la /data/processed_simple/
kubectl exec -n bigdata deployment/data-analysis -- ls -la /data/analytics/
```

Should see folders like:
- `/data/processed_simple/bike_trips/`
- `/data/processed_simple/taxi_trips/`
- `/data/processed_simple/weather_data/`
- `/data/analytics/weather_transport_correlation/`
- `/data/analytics/pearson_correlations/`

### 6.4 Check Parquet Files
```bash
kubectl exec -n bigdata deployment/data-analysis -- find /data/processed_simple -name "*.parquet" | head -10
kubectl exec -n bigdata deployment/data-analysis -- find /data/analytics -name "*.parquet" | head -10
```

---

## STEP 7: View Results in Dashboard

1. **Uncomment Dashboard in port-forward script:**
   - Edit `tools/forward-all.py` line 56, remove the `#` comment
   - Restart the port-forward script

2. **Open Dashboard:**
   - Go to: http://localhost:3000

3. **You should see:**
   - Live transport data (bike/taxi trips)
   - Weather correlations
   - Real-time analytics graphs

---

## UNDERSTANDING THE DATA FLOW

```
CSV Files (mock-data/)
    ↓
Time Manager (simulates time progression)
    ↓
Streamer Pod (reads CSV → Avro → Kafka)
    ↓
Kafka Topics (bike-data, taxi-data, weather-data, accident-data)
    ↓
Data Analysis ETL Pod (your Spark job in src/etl/)
    ├── Connects to Spark Connect Server
    ├── Spark reads from Kafka
    ├── Spark performs calculations (weather-transport correlations)
    └── Spark writes Parquet files:
        ├── /data/processed_simple/  (raw transformed data)
        └── /data/analytics/         (calculated correlations)
    ↓
Hive Metastore (indexes the Parquet files)
    ↓
Dashboard (queries via Hive HTTP Proxy → displays graphs)
```

---

## ANSWER TO YOUR QUESTIONS

### Q1: "Where is my output forwarded to?"

**Your ETL output is written to:**
- `/data/processed_simple/` - Raw transformed data as Parquet
- `/data/analytics/` - Calculated correlations as Parquet

These are **Persistent Volumes** in Kubernetes that:
1. Spark writes to directly
2. Hive reads from (via Hive Metastore)
3. Dashboard queries via Hive

**NOT directly to HDFS** - but Hive can be configured to use HDFS as backend (your setup uses PVC).

### Q2: "Does my ETL code forward to Spark or Hadoop?"

**Your ETL code IS a Spark application.** It:
- Connects to Spark Connect Server (`sc://spark-connect-server:15002`)
- Submits Spark Structured Streaming jobs
- Spark executors perform the calculations
- Spark executors write the results to `/data/...`

**It does NOT "forward" to Spark - it RUNS ON Spark!**

---

## TROUBLESHOOTING

### ETL Pod Crashes with "404 schema not found"
**Fix:**
```bash
# Re-run schema registration
cd C:\Users\vivek\Downloads\Boston-Transport-Department
python tools/create-schemas.py
```

### No Data Showing in Dashboard
**Check:**
1. Is time manager running? `curl http://localhost:8000/api/v1/clock`
2. Is streamer producing? `kubectl logs -n bigdata -l app=streamer`
3. Is ETL running? `kubectl logs -n bigdata -l app=data-analysis`
4. Are Parquet files created? `kubectl exec -n bigdata deployment/data-analysis -- ls /data/analytics/`

### Pods Not Starting
```bash
# Check pod status
kubectl get pods -n bigdata

# Check specific pod logs
kubectl logs -n bigdata <pod-name>

# Describe pod for events
kubectl describe pod -n bigdata <pod-name>
```

---

## CLEANUP

To delete everything:
```bash
cd C:\Users\vivek\Downloads\Boston-Transport-Department\infra\environments/local
terraform destroy -auto-approve
```

Or manually:
```bash
kubectl delete namespace bigdata
```

---

## NEXT STEPS

1. Explore JupyterLab: http://localhost:8080 (token: `adminadmin`)
2. Query Hive directly:
   ```bash
   kubectl exec -n bigdata svc/spark-thrift-service -- beeline -u jdbc:hive2://localhost:10000
   ```
3. View Spark UI: http://localhost:4040
4. Modify ETL code in `src/etl/jobs/data_analysis.py` and redeploy

---

**Good luck! 🚀**
