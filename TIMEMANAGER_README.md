# Boston Transport Department - Time Manager & Streamers

This directory contains the **Time Manager** service and **Data Streamers** (Bike, Taxi, Weather) for the Boston Transport Department Big Data project. These components fulfill the streaming data ingestion requirements (FR01-FR09) from the project specification.

## Architecture Overview

```
┌─────────────────┐
│  Time-Manager   │◄────── REST API control
│    (Redis)      │
└────────┬────────┘
         │ Coordinates simulation clock
    ┌────┼────┬─────────────┐
    │    │    │             │
    ▼    ▼    ▼             ▼
┌────────────────┐    ┌──────────┐
│  Bike-streamer │───►│          │
├────────────────┤    │          │
│  Taxi-streamer │───►│  Kafka   │──► To downstream components
├────────────────┤    │          │    (HDFS, Spark, Hive, etc.)
│Weather-streamer│───►│          │
└────────────────┘    └──────────┘
```

## Components

### 1. Time-Manager
Coordinates the simulation clock for replaying historical data as live streams.

**Technology**: Python Flask + Redis

**Key Features**:
- REST API for clock control (start/stop/reset/speed)
- Simulation clock with adjustable speed (1x, 10x, 100x, etc.)
- Redis-backed state management
- Clock ticker thread for automatic time advancement

**API Endpoints**:
- `GET /health` - Health check
- `GET /api/v1/clock/status` - Get full simulation status
- `GET /api/v1/clock/time` - Get current simulation time
- `POST /api/v1/clock/start` - Start simulation
- `POST /api/v1/clock/stop` - Stop simulation
- `POST /api/v1/clock/reset` - Reset to start time
- `PUT /api/v1/clock/speed` - Set speed multiplier
- `PUT /api/v1/clock/range` - Set time range

### 2. Streamers (Bike, Taxi, Weather)
Read historical CSV data and stream to Kafka topics, synchronized with the simulation clock.

**Technology**: Python + Kafka Producer

**Key Features**:
- Polls time-manager for current simulation time
- Publishes records to Kafka when their timestamp ≤ current sim time
- Partitions by date-hour (YYYY-MM-DD-HH) as per FR02
- JSON message format with metadata
- Automatic data sorting by timestamp
- Handles simulation reset

**Kafka Topics**:
- `bike-trips` - Bike sharing trip data
- `taxi-trips` - Taxi/rideshare trip data
- `weather-data` - Weather observations

**Message Format**:
```json
{
  "data": { /* transformed record */ },
  "timestamp": "2018-01-01T08:00:00",
  "source": "bike-streamer"
}
```

## Requirements Fulfillment

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| **FR01**: Ingest raw data from 4 sources | ✅ | Three streamers (bike/taxi/weather) + base architecture for accidents |
| **FR02**: Store in Parquet, partitioned by date/hour | ✅ | Kafka partition keys use YYYY-MM-DD-HH format |
| **FR03**: Normalize/validate data | ✅ | Transform methods in each streamer |
| **FR04**: Enrich transport with weather | 🔄 | Downstream ETL component (Spark/Hive) |
| **FR05**: Dynamic time-based queries | 🔄 | Downstream query layer (Hive/Spark) |
| **FR06**: Calculate accident exposure | 🔄 | Downstream analytics component |
| **FR07**: Generate 2-hour forecasts | 🔄 | Downstream ML component |
| **FR08**: Daily idempotent ETL | 🔄 | Downstream ETL orchestration |
| **FR09**: Access via SQL/API/Export | 🔄 | Downstream dashboard component |

✅ = Implemented in this component
🔄 = Handled by downstream components (HDFS, Spark, Hive, Dashboard)

## Local Development with Docker Compose

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (for local testing)

### Quick Start

1. **Clone and navigate to project**:
```bash
cd Boston-Transport-Department
```

2. **Build and start all services**:
```bash
docker-compose up --build
```

This will start:
- Redis (port 6379)
- Zookeeper (port 2181)
- Kafka (port 9092)
- Time-Manager (port 5000)
- Bike-Streamer
- Taxi-Streamer
- Weather-Streamer

3. **Control the simulation**:

Start simulation at 10x speed:
```bash
curl -X PUT http://localhost:5000/api/v1/clock/speed \
  -H "Content-Type: application/json" \
  -d '{"speed": 10.0}'

curl -X POST http://localhost:5000/api/v1/clock/start
```

Check status:
```bash
curl http://localhost:5000/api/v1/clock/status
```

4. **Monitor Kafka topics**:
```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume bike-trips
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bike-trips \
  --from-beginning \
  --max-messages 10
```

5. **Stop simulation**:
```bash
curl -X POST http://localhost:5000/api/v1/clock/stop
```

### Logs
```bash
# Time-manager logs
docker-compose logs -f time-manager

# All streamer logs
docker-compose logs -f bike-streamer taxi-streamer weather-streamer
```

## Kubernetes Deployment

### Prerequisites
- Kubernetes cluster (minikube, kind, or cloud provider)
- kubectl configured
- Docker images built and pushed to registry

### Build Docker Images

1. **Build time-manager**:
```bash
cd time-manager
docker build -t boston-transport/time-manager:latest .
```

2. **Build streamers**:
```bash
cd streamers
docker build -f Dockerfile.bike -t boston-transport/bike-streamer:latest .
docker build -f Dockerfile.taxi -t boston-transport/taxi-streamer:latest .
docker build -f Dockerfile.weather -t boston-transport/weather-streamer:latest .
```

3. **Tag and push to registry** (if using remote cluster):
```bash
# Example for Docker Hub
docker tag boston-transport/time-manager:latest youruser/time-manager:latest
docker push youruser/time-manager:latest
# Repeat for other images
```

### Deploy to Kubernetes

1. **Create namespace and infrastructure**:
```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/zookeeper.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/redis.yaml
```

2. **Wait for infrastructure to be ready**:
```bash
kubectl wait --for=condition=ready pod -l app=redis -n boston-transport --timeout=300s
kubectl wait --for=condition=ready pod -l app=zookeeper -n boston-transport --timeout=300s
kubectl wait --for=condition=ready pod -l app=kafka -n boston-transport --timeout=300s
```

3. **Deploy time-manager**:
```bash
kubectl apply -f k8s/time-manager.yaml
kubectl wait --for=condition=ready pod -l app=time-manager -n boston-transport --timeout=300s
```

4. **Deploy mock data and streamers**:
```bash
kubectl apply -f k8s/data-configmap.yaml
kubectl apply -f k8s/bike-streamer.yaml
kubectl apply -f k8s/taxi-streamer.yaml
kubectl apply -f k8s/weather-streamer.yaml
```

5. **Verify deployment**:
```bash
kubectl get pods -n boston-transport
kubectl get services -n boston-transport
```

### Access Services

**Port-forward time-manager API**:
```bash
kubectl port-forward -n boston-transport service/time-manager 5000:5000
```

Then use same curl commands as Docker Compose section.

**Port-forward Kafka**:
```bash
kubectl port-forward -n boston-transport service/kafka 9092:9092
```

### Monitor System

```bash
# Check logs
kubectl logs -n boston-transport -l app=time-manager -f
kubectl logs -n boston-transport -l app=bike-streamer -f

# Check resource usage
kubectl top pods -n boston-transport

# Describe pod issues
kubectl describe pod -n boston-transport <pod-name>
```

### Scale Streamers

```bash
# Not recommended for this use case (stateful data streaming)
# but possible if implementing proper offset management
kubectl scale deployment bike-streamer -n boston-transport --replicas=2
```

## Configuration

### Time-Manager Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | localhost | Redis hostname |
| `REDIS_PORT` | 6379 | Redis port |
| `PORT` | 5000 | Flask API port |

### Streamer Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TIME_MANAGER_URL` | http://localhost:5000 | Time-manager API URL |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka broker(s) |
| `DATA_FILE` | /data/*.csv | Path to CSV data file |
| `POLL_INTERVAL` | 1.0 | Seconds between time checks |

## Data Format

### Input: CSV Files

**Bike Trips** ([bike_trips.csv](mock-data/bike_trips.csv)):
```csv
tripduration,starttime,stoptime,start station id,start station name,...
600,2018-01-01 08:00:00,2018-01-01 08:10:00,22,Boylston St...
```

**Taxi Trips** ([taxi_trips.csv](mock-data/taxi_trips.csv)):
```csv
datetime,source,destination,cab_type,product_id,name,price,distance,...
2018-01-01 08:00:00,Back Bay,Financial District,Uber,lyft_line,Shared,12.5...
```

**Weather** ([weather_observations.csv](mock-data/weather_observations.csv)):
```csv
datetime,tempmax,tempmin,temp,feelslikemax,humidity,precip,...
2018-01-01 08:00:00,35.2,30.1,32.5,28.3,68.0,0.0...
```

### Output: Kafka Messages

Kafka messages use partition keys: `YYYY-MM-DD-HH`

**Example message**:
```json
{
  "data": {
    "trip_id": "2018-01-01 08:00:00_1234",
    "duration_seconds": 600,
    "start_time": "2018-01-01 08:00:00",
    "start_station": {
      "id": "22",
      "name": "Boylston St at Arlington St",
      "latitude": 42.35302,
      "longitude": -71.07069
    },
    "user_type": "Subscriber"
  },
  "timestamp": "2018-01-01T08:00:00",
  "source": "bike-streamer"
}
```

## Integration with Downstream Components

Your teammates' components should:

1. **Consume from Kafka topics**:
   - `bike-trips`
   - `taxi-trips`
   - `weather-data`

2. **Use partition key for date/hour grouping**:
   - Partition key format: `YYYY-MM-DD-HH`
   - Enables efficient time-based processing

3. **Poll time-manager for coordination** (optional):
   ```python
   import requests
   status = requests.get('http://time-manager:5000/api/v1/clock/status').json()
   current_time = status['current_time']
   ```

4. **Write to HDFS in Parquet format** (FR02):
   ```python
   df.write.partitionBy("date", "hour").parquet("/data/bike-trips")
   ```

## Testing

### Unit Tests (Example)

```python
# test_time_manager.py
import requests

def test_clock_control():
    base_url = "http://localhost:5000"

    # Reset
    resp = requests.post(f"{base_url}/api/v1/clock/reset")
    assert resp.status_code == 200

    # Set speed
    resp = requests.put(f"{base_url}/api/v1/clock/speed", json={"speed": 100})
    assert resp.status_code == 200

    # Start
    resp = requests.post(f"{base_url}/api/v1/clock/start")
    assert resp.status_code == 200

    # Check time advancement
    import time
    time.sleep(2)
    status = requests.get(f"{base_url}/api/v1/clock/status").json()
    assert status['state'] == 'running'
```

### Integration Test

```bash
# Start system
docker-compose up -d

# Wait for services
sleep 30

# Configure simulation for 2018-01-01 data
curl -X PUT http://localhost:5000/api/v1/clock/range \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2018-01-01T00:00:00",
    "end_time": "2018-01-01T23:59:59"
  }'

# Start at 100x speed
curl -X PUT http://localhost:5000/api/v1/clock/speed \
  -H "Content-Type: application/json" \
  -d '{"speed": 100.0}'

curl -X POST http://localhost:5000/api/v1/clock/start

# Consume Kafka messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bike-trips \
  --from-beginning \
  --max-messages 100

# Stop
curl -X POST http://localhost:5000/api/v1/clock/stop
```

## Troubleshooting

### Issue: Streamers not connecting to time-manager

**Check**:
```bash
kubectl logs -n boston-transport -l app=bike-streamer
```

**Solution**: Ensure time-manager is running and healthy:
```bash
kubectl get pods -n boston-transport -l app=time-manager
```

### Issue: Kafka not receiving messages

**Check Kafka topics**:
```bash
kubectl exec -it -n boston-transport kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --list
```

**Check streamer logs**:
```bash
kubectl logs -n boston-transport -l app=bike-streamer -f
```

### Issue: Simulation not advancing

**Check clock status**:
```bash
curl http://localhost:5000/api/v1/clock/status
```

**Verify state is "running"** and speed > 0.

### Issue: Out of memory

**Solution**: Increase resource limits in k8s manifests:
```yaml
resources:
  limits:
    memory: "1Gi"
    cpu: "1000m"
```

## Performance Tuning

### Time-Manager
- Adjust ticker interval in [app.py:133](time-manager/app.py#L133) (default: 100ms)
- Use Redis persistence for crash recovery

### Streamers
- Adjust `POLL_INTERVAL` (default: 1 second)
- Batch Kafka sends for higher throughput
- Use async Kafka producer

### Kafka
- Increase partitions for parallel processing:
  ```bash
  kafka-topics --alter --topic bike-trips --partitions 10
  ```
- Tune retention policy:
  ```bash
  kafka-configs --alter --entity-type topics --entity-name bike-trips \
    --add-config retention.ms=86400000
  ```

## Production Considerations

1. **Use real data sources**: Replace ConfigMaps with PersistentVolumes or S3
2. **Add monitoring**: Prometheus + Grafana for metrics
3. **Add alerting**: Alert on streamer failures, Kafka lag
4. **Implement authentication**: Secure time-manager API
5. **Use Kafka Connect**: For HDFS sink instead of custom consumers
6. **Add schema registry**: Avro schemas for Kafka messages
7. **Implement checkpointing**: Resume from last processed timestamp

## References

- [Project Requirements (PDF)](timemanager.pdf)
- [Architecture Diagram](src/assets/BD_Architecture.png)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Redis Documentation](https://redis.io/documentation)
- [Flask Documentation](https://flask.palletsprojects.com/)

## License

This is a university project for Big Data and Science Technologies (E25) course.

## Contributors

Boston Transport Department Big Data Team

---

**Questions?** Check logs first, then review the troubleshooting section above.
