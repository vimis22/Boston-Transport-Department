# Dataset Streamers

This folder contains **Python-based dataset streamers** that read CSV datasets and stream their contents to a Kafka-compatible broker line by line. Each streamer is containerized with Docker, allowing you to run multiple independent streams in parallel.

---

## Features

* Streams datasets to Kafka topics in real-time or at configurable intervals
* Supports multiple datasets with separate Kafka topics
* Dockerized for easy deployment and reproducibility
* Retry logic ensures streamers wait for Kafka to become ready

---

## Project Structure

```
.
├── .gitignore
├── Dockerfile
├── docker-compose.yaml
├── streamer.py
├── data/
│   ├── accident/tmpw8i0zd4_.csv
│   ├── bikedata/201501-hubway-tripdata.csv
│   ├── taxi/rideshare_kaggle.csv
│   └── weather/72509014739.csv
└── README.md
```
> **Data folder:** The `.data/` folder contains the `.csv` data that is being sent by this image and is mounted on with a volume. each container needs to be configured to use data inside this folder see [env structure](#environment-variables) for configuration.

> **Dockerfile:** The `Dockerfile` which can be build together with `streamer.py` to create an image see [setup](#setup), a published image can also be used see [setup](#setup).

> **docker-compose:** The `docker-compose.yaml` file is an example of how this image is used. 

## Notes
* The script retries connecting to the broker until it is ready.
* Multiple streamers can run in parallel, each streaming a separate dataset to a separate topic.
* CSV files are **mounted as volumes** to avoid rebuilding the image for every dataset change.
* The `kafka-data/` folder created by Kafka is ignored in version control via `.gitignore`.

---

## Requirements

* Docker >= 20.x
* Docker Compose >= 1.29.x
* Optional: Python 3.11+ if running outside Docker

---

## Setup

1. **Clone the repository and be in data-streamers folder**

```bash
cd .\data-streamers\
```

2. **Ensure your datasets are in the `data/` folder**

> Note: large datasets are mounted as volumes into the container instead of being copied into the image.

3. **Build the Docker image or use published**

Build yourself:
```bash
docker build -t dataset-streamer:latest .
```
or use public
```yaml
image: fearfulsnow/dataset-streamer
```
See image on [Docker hub](https://hub.docker.com/layers/fearfulsnow/dataset-streamer/latest/images/sha256:67213ad62ef7b37abb4be6b7da892a66c4664e9b0ca0ef6fa108143ea96f30de?uuid=F20DB87E-8921-47F5-9359-C9E87CB47AC4)

---

## Example with Docker Compose

We provide a `docker-compose.yml` that launches:

* **Kafka & Redpanda** as the Kafka broker and console
* **4 independent dataset streamers**

```bash
docker compose up
```

### Streamer containers

| Container Name     | Dataset                                  | Kafka Topic |
| ------------------ | ---------------------------------------- | ----------- |
| streamer-accidents | data/accident/tmpw8i0zd4_.csv            | accidents   |
| streamer-biketrips | data/bikedata/201501-hubway-tripdata.csv | biketrips   |
| streamer-taxirides | data/taxi/rideshare_kaggle.csv           | taxirides   |
| streamer-weather   | data/weather/72509014739.csv             | weather     |

Using **docker desktop** you can view logs of each container or go to the redpanda console here: http://localhost:8094

---

## Environment Variables

Each streamer can be configured using environment variables:

| Variable         | Description                                              | Example                         |
| ---------------- | -------------------------------------------------------- | ------------------------------- |
| `DATA_FILE`      | Path to the CSV dataset inside the container             | `data/accident/tmpw8i0zd4_.csv` |
| `KAFKA_TOPIC`    | Kafka topic to send data to                              | `accidents`                     |
| `KAFKA_BROKER`   | Kafka/Redpanda broker address                            | `kafka:9092`                 |
| `STREAM_DELAY`   | Delay in seconds between sending rows                    | `1`                             |
---

## Running Manually (without Compose)

```bash
docker run -it \
  -v ./data:/app/data \
  -e DATA_FILE=data/accident/tmpw8i0zd4_.csv \
  -e KAFKA_BROKER=redpanda:9092 \
  -e KAFKA_TOPIC=accidents \
  -e STREAM_DELAY=1 \
  dataset-streamer:latest
```

* The container will stream directly to Kafka.
* Logs will show each row sent.

---