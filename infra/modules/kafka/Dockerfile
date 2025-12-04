FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    confluent-kafka[avro]==2.3.0 \
    requests==2.31.0

# Copy schemas and producer scripts
COPY schemas/ /app/schemas/
COPY streamers/producers/ /app/producers/

# Set Python path
ENV PYTHONUNBUFFERED=1

# Default command (override in k8s deployment)
CMD ["python", "producers/bike_producer.py"]