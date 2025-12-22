# 🚀 BOSTON TRANSPORT - HURTIG OPSTARTSGUIDE

## 📌 VIGTIGT AT FORSTÅ FØRST

**Dit projekt starter IKKE fra `src/`-mappen!**

- **`infra/`** → Her starter du projektet (Terraform deployer alt)
- **`src/`** → Indeholder kildekode til applikationer (deployes automatisk af Terraform)
- **`tools/`** → Hjælpescripts til port-forwarding og datahåndtering
- **`mock-data/`** → CSV-filer som Streamer læser

**Terraform læser modulerne i `infra/modules/` og deployer alt som Docker containers til Kubernetes.**

---

## 📋 FORUDSÆTNINGER

- ✅ Docker Desktop installeret
- ✅ Python 3.11+
- ⚠️ Kubernetes aktiveret i Docker Desktop (vigtigt!)
- ⚠️ Terraform (skal installeres)
- ⚠️ kubectl (installeres automatisk med Docker Desktop)

---

## TRIN 0: Start Kubernetes i Docker Desktop ⚠️ VIGTIGT!

**Før du kan køre projektet, skal Kubernetes være aktiveret i Docker Desktop:**

1. **Åbn Docker Desktop**
2. **Klik på tandhjul-ikonet** (⚙️ Settings) øverst til højre
3. **Klik på "Kubernetes"** i venstre menu
4. **Sæt flueben ved "Enable Kubernetes"**
5. **Klik "Apply & restart"**
6. **Vent 2-3 minutter** mens Kubernetes starter (du ser en grøn indikator nederst til venstre)

### Verificer at Kubernetes kører:
```bash
kubectl cluster-info
```

Du skulle se noget som:
```
Kubernetes control plane is running at https://kubernetes.docker.internal:6443
CoreDNS is running at https://kubernetes.docker.internal:6443/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy
```

**Hvis du får fejl "connection refused", er Kubernetes ikke startet endnu - vent lidt længere.**

---

## TRIN 1: Installér Terraform (2 minutter)

### Windows - PowerShell (Kør som Administrator):
```powershell
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Efter Chocolatey er installeret:
choco install terraform -y
```

Verificer:
```bash
terraform --version
```

---

## TRIN 2: Deploy Infrastrukturen (5-10 minutter)

**Dette er dit startpunkt!**

```bash
cd C:\Users\vivek\Downloads\Boston-Transport-Department\infra\environments\local
terraform init
terraform apply -auto-approve
```

### Hvad deployer Terraform?

Terraform læser moduler fra `infra/modules/` og deployer følgende services som Kubernetes pods:

1. **Kafka** + Schema Registry + REST Proxy (fra `infra/modules/kafka/`)
2. **HDFS** (Hadoop) - NameNode + DataNode (fra `infra/modules/hadoop/`)
3. **Spark** Connect Server + Workers (fra `infra/modules/bigdata/`)
4. **Hive** Metastore + Thrift Server (fra `infra/modules/hadoop/`)
5. **JupyterLab** (til notebooks)
6. **Time Manager** (simuleret tid til streaming, kode fra `src/timemanager/`)
7. **Streamer** (læser CSV → sender til Kafka som Avro, kode fra `src/streamer/`)
8. **Data Analysis ETL** (dit Spark job, kode fra `src/etl/`)
9. **Dashboard** (visualiserer resultater, kode fra `src/dashboard/`)

**Alt kører i Kubernetes namespace `bigdata`.**

---

## TRIN 3: Vent på at Pods Starter (2-3 minutter)

```bash
kubectl get pods -n bigdata -w
```

Vent til alle pods viser `Running` status. Tryk `Ctrl+C` når færdig.

---

## TRIN 4: Port-Forward til Services

**Åbn en NY terminal** og kør:

```bash
cd C:\Users\vivek\Downloads\Boston-Transport-Department
python tools/forward-all.py
```

Dette port-forwarder:
- **Dashboard**: http://localhost:3000
- **HDFS UI**: http://localhost:9870
- **Spark UI**: http://localhost:4040
- **JupyterLab**: http://localhost:8080 (token: `adminadmin`)
- **Kafka REST Proxy**: http://localhost:8083
- **Schema Registry**: http://localhost:8081
- **Time Manager API**: http://localhost:8000
- **Hive**: localhost:10000

---

## TRIN 5: Start Data Pipeline

### 5.1 Tjek Time Manager Status
```bash
curl http://localhost:8000/api/v1/clock
```

### 5.2 Start Simulering (Streamer Data)
```bash
curl -X POST http://localhost:8000/api/v1/clock/start
```

### 5.3 Overvåg Logs

**Streamer (producerer til Kafka):**
```bash
kubectl logs -n bigdata -l app=streamer -f
```

**Data Analysis ETL (dine beregninger):**
```bash
kubectl logs -n bigdata -l app=data-analysis -f
```

---

## 🔄 FORSTÅ DATAFLOWET

```
1. CSV-filer (mock-data/)
   ↓
2. Time Manager (src/timemanager/) → Simulerer tid
   ↓
3. Streamer Pod (src/streamer/) → Læser CSV → Konverterer til Avro → Kafka
   ↓
4. Kafka Topics (bike-data, taxi-data, weather-data, accident-data)
   ↓
5. Data Analysis ETL Pod (src/etl/)
   ├── Forbinder til Spark Connect Server
   ├── Spark læser fra Kafka
   ├── Spark udfører beregninger (vejr-transport korrelationer)
   └── Spark skriver Parquet-filer:
       ├── /data/processed_simple/  (transformeret rådata)
       └── /data/analytics/         (beregnede korrelationer)
   ↓
6. Hive Metastore → Indekserer Parquet-filerne
   ↓
7. Dashboard (src/dashboard/) → Henter data via Hive HTTP Proxy → Viser grafer
```

---

## ✅ VERIFICER DATAFLOW

### 6.1 Tjek Kafka Topics
```bash
kubectl exec -n bigdata svc/kafka-broker -- kafka-topics --bootstrap-server localhost:9092 --list
```

Skulle vise: `bike-data`, `taxi-data`, `weather-data`, `accident-data`

### 6.2 Tjek Schemas i Registry
```bash
curl http://localhost:8081/subjects
```

Skulle vise: `["bike-data-value", "taxi-data-value", "weather-data-value", "accident-data-value"]`

### 6.3 Tjek ETL Output i Spark Pod
```bash
# Vis output-mapper
kubectl exec -n bigdata deployment/data-analysis -- ls -la /data/processed_simple/
kubectl exec -n bigdata deployment/data-analysis -- ls -la /data/analytics/
```

Skulle vise mapper som:
- `/data/processed_simple/bike_trips/`
- `/data/processed_simple/taxi_trips/`
- `/data/processed_simple/weather_data/`
- `/data/analytics/weather_transport_correlation/`
- `/data/analytics/pearson_correlations/`

### 6.4 Tjek Parquet-filer
```bash
kubectl exec -n bigdata deployment/data-analysis -- find /data/processed_simple -name "*.parquet" | head -10
kubectl exec -n bigdata deployment/data-analysis -- find /data/analytics -name "*.parquet" | head -10
```

---

## 📊 VIS RESULTATER I DASHBOARD

1. **Åbn Dashboard:**
   - Gå til: http://localhost:3000

2. **Du skulle se:**
   - Live transportdata (cykel/taxi-ture)
   - Vejrkorrelationer
   - Realtidsanalyse-grafer

---

## 🧩 VIGTIGE SPØRGSMÅL & SVAR

### Q1: "Hvor skrives mit ETL output?"

**Dit ETL output skrives til:**
- `/data/processed_simple/` - Transformeret rådata som Parquet
- `/data/analytics/` - Beregnede korrelationer som Parquet

Disse er **Persistent Volumes** i Kubernetes som:
1. Spark skriver direkte til
2. Hive læser fra (via Hive Metastore)
3. Dashboard forespørger via Hive

**IKKE direkte til HDFS** - men Hive kan konfigureres til at bruge HDFS som backend (dit setup bruger PVC).

### Q2: "Forwarded min ETL-kode til Spark eller Hadoop?"

**Din ETL-kode ER en Spark-applikation.** Den:
- Forbinder til Spark Connect Server (`sc://spark-connect-server:15002`)
- Indsender Spark Structured Streaming jobs
- Spark executors udfører beregningerne
- Spark executors skriver resultaterne til `/data/...`

**Den "forwarder" IKKE til Spark - den KØRER PÅ Spark!**

### Q3: "Hvad er forskellen på `infra/` og `src/`?"

| Folder | Rolle |
|--------|-------|
| **`infra/`** | Terraform moduler - **DIT OPSTARTSPUNKT** |
| **`src/`** | Kildekode til applikationer (deployes af Terraform) |
| **`tools/`** | Hjælpescripts (port-forward, upload data osv.) |
| **`mock-data/`** | CSV-filer som Streamer læser |
| **`notebooks/`** | Jupyter notebooks til dataanalyse |

---

## 🔧 FEJLFINDING

### Kubernetes Connection Refused
**Fejl:** `dial tcp 127.0.0.1:6443: connectex: No connection could be made because the target machine actively refused it.`

**Årsag:** Kubernetes er ikke startet i Docker Desktop.

**Fix:**
1. Åbn Docker Desktop
2. Gå til Settings → Kubernetes
3. Aktivér "Enable Kubernetes"
4. Klik "Apply & restart"
5. Vent 2-3 minutter
6. Verificer: `kubectl cluster-info`

### ETL Pod Crasher med "404 schema not found"
**Fix:**
```bash
cd C:\Users\vivek\Downloads\Boston-Transport-Department
python tools/create-schemas.py
```

### Ingen Data Vises i Dashboard
**Tjek:**
1. Kører time manager? `curl http://localhost:8000/api/v1/clock`
2. Producerer streamer? `kubectl logs -n bigdata -l app=streamer`
3. Kører ETL? `kubectl logs -n bigdata -l app=data-analysis`
4. Er Parquet-filer oprettet? `kubectl exec -n bigdata deployment/data-analysis -- ls /data/analytics/`

### Pods Starter Ikke
```bash
# Tjek pod status
kubectl get pods -n bigdata

# Tjek specifikke pod logs
kubectl logs -n bigdata <pod-name>

# Beskriv pod for events
kubectl describe pod -n bigdata <pod-name>
```

---

## 🧹 RYDNING

For at slette alt:
```bash
cd C:\Users\vivek\Downloads\Boston-Transport-Department\infra\environments\local
terraform destroy -auto-approve
```

Eller manuelt:
```bash
kubectl delete namespace bigdata
```

---

## 🚀 NÆSTE SKRIDT

1. Udforsk JupyterLab: http://localhost:8080 (token: `adminadmin`)
2. Forespørg Hive direkte:
   ```bash
   kubectl exec -n bigdata svc/spark-thrift-service -- beeline -u jdbc:hive2://localhost:10000
   ```
3. Se Spark UI: http://localhost:4040
4. Modificer ETL-kode i `src/etl/jobs/data_analysis.py` og redeploy

---

**Held og lykke! 🎯**
