# Use a lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy only the Python script
COPY streamer.py .

# Install Python dependencies
RUN pip install --no-cache-dir \
    duckdb \
    confluent-kafka[avro] \
    fastavro \
    requests

# Run the streamer
CMD ["python", "streamer.py"]