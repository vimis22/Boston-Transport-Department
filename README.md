<h1>Boston Transport Department</h1>
<h2>The Objective</h2>
<p>
The project is apart of a 10 ECTS Course of Big Data and Science Technologies (E25). 
The objective with this course is to work different datasets where they overlap eachtother and thereby finding something valuable for the Customer.
Our main objective is to study the relationsship between transportation and weather.
Please note, that this project is also a part of our Scientific Methods Course (E25), where we will have a more theoretical approach there.
</p>

<h2>Who is our Customer</h2>
<p>Our Customer in this context is the Boston Transportation Department, since we are working with datasets of weather reports from different weather stations in Bostom City.</p>

<h2>How is our Architecture Structured?</h2>
<p>The following image shows how we have structured our architecture.</p>
<img src="src/assets/BD_Architecture.png">

<h2>Components</h2>

<h3>Time-Manager & Streamers (Implemented)</h3>
<p>
The time-manager coordinates simulation clock for replaying historical data as live streams. Three data streamers (bike, taxi, weather) read CSV files and publish to Kafka topics, synchronized with the simulation clock.
</p>

<ul>
  <li><strong>Time-Manager</strong>: REST API service (Flask + Redis) that manages simulation time</li>
  <li><strong>Bike-Streamer</strong>: Streams bike sharing trip data to Kafka topic 'bike-trips'</li>
  <li><strong>Taxi-Streamer</strong>: Streams taxi/rideshare data to Kafka topic 'taxi-trips'</li>
  <li><strong>Weather-Streamer</strong>: Streams weather observations to Kafka topic 'weather-data'</li>
</ul>

<p>
  <strong>See detailed documentation:</strong> <a href="TIMEMANAGER_README.md">Time-Manager & Streamers README</a>
</p>

<h3>Quick Start</h3>

<h4>Using Docker Compose (Local Development)</h4>
<pre>
# Start all services
docker-compose up --build

# In another terminal, start simulation at 10x speed
curl -X PUT http://localhost:5000/api/v1/clock/speed -H "Content-Type: application/json" -d '{"speed": 10.0}'
curl -X POST http://localhost:5000/api/v1/clock/start

# Check status
curl http://localhost:5000/api/v1/clock/status

# Run automated test
./test-local.sh
</pre>

<h4>Using Kubernetes</h4>
<pre>
# Deploy everything
./deploy.sh deploy

# Port-forward to access time-manager
kubectl port-forward -n boston-transport service/time-manager 5000:5000

# Start simulation
curl -X POST http://localhost:5000/api/v1/clock/start

# Clean up
./deploy.sh clean
</pre>

<h2>Project Structure</h2>
<pre>
Boston-Transport-Department/
├── time-manager/           # Time Manager service
│   ├── app.py             # Flask application
│   ├── requirements.txt
│   └── Dockerfile
├── streamers/             # Data streamers
│   ├── base_streamer.py   # Base class
│   ├── bike_streamer.py   # Bike data streamer
│   ├── taxi_streamer.py   # Taxi data streamer
│   ├── weather_streamer.py # Weather data streamer
│   ├── requirements.txt
│   └── Dockerfile.*       # Dockerfiles for each streamer
├── k8s/                   # Kubernetes manifests
│   ├── namespace.yaml
│   ├── redis.yaml
│   ├── kafka.yaml
│   ├── zookeeper.yaml
│   ├── time-manager.yaml
│   └── *-streamer.yaml
├── mock-data/             # Sample CSV data files
│   ├── bike_trips.csv
│   ├── taxi_trips.csv
│   └── weather_observations.csv
├── docker-compose.yaml    # Docker Compose for local testing
├── deploy.sh              # Kubernetes deployment script
├── test-local.sh          # Local testing script
├── TIMEMANAGER_README.md  # Detailed documentation
└── README.md              # This file
</pre>

<h2>Integration for Team Members</h2>
<p>
Your components (Kafka Connect, HDFS, Spark, Hive, Dashboard) should:
</p>

<ol>
  <li><strong>Consume from Kafka topics:</strong>
    <ul>
      <li><code>bike-trips</code> - Bike sharing trip records</li>
      <li><code>taxi-trips</code> - Taxi/rideshare trip records</li>
      <li><code>weather-data</code> - Weather observation records</li>
    </ul>
  </li>
  <li><strong>Use partition keys for time-based grouping:</strong>
    <ul>
      <li>Format: <code>YYYY-MM-DD-HH</code> (e.g., "2018-01-01-08")</li>
      <li>Enables efficient hourly processing as per FR02</li>
    </ul>
  </li>
  <li><strong>Message format:</strong>
    <pre>
{
  "data": { /* transformed record */ },
  "timestamp": "2018-01-01T08:00:00",
  "source": "bike-streamer"
}
    </pre>
  </li>
  <li><strong>Optional: Coordinate with time-manager:</strong>
    <ul>
      <li>GET http://time-manager:5000/api/v1/clock/status for current simulation time</li>
    </ul>
  </li>
</ol>
