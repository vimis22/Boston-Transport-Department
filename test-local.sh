#!/bin/bash

# Boston Transport Department - Local Test Script
# Tests the time-manager and streamers using Docker Compose

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Boston Transport - Local Test${NC}"
echo "=============================="
echo ""

# Start services
echo -e "${YELLOW}Starting services...${NC}"
docker-compose up -d

# Wait for services to be healthy
echo "Waiting for services to be ready..."
sleep 30

# Check health
echo -e "${YELLOW}Checking time-manager health...${NC}"
HEALTH=$(curl -s http://localhost:5000/health | grep -o "healthy" || echo "unhealthy")
if [ "$HEALTH" = "healthy" ]; then
    echo -e "${GREEN}✓ Time-manager is healthy${NC}"
else
    echo -e "${RED}✗ Time-manager is unhealthy${NC}"
    docker-compose logs time-manager
    exit 1
fi

# Configure simulation
echo ""
echo -e "${YELLOW}Configuring simulation...${NC}"

# Set time range
curl -s -X PUT http://localhost:5000/api/v1/clock/range \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2018-01-01T08:00:00",
    "end_time": "2018-01-01T12:00:00"
  }' | grep -q "Time range updated" && echo -e "${GREEN}✓ Time range set${NC}"

# Set speed to 100x
curl -s -X PUT http://localhost:5000/api/v1/clock/speed \
  -H "Content-Type: application/json" \
  -d '{"speed": 100.0}' | grep -q "Speed set" && echo -e "${GREEN}✓ Speed set to 100x${NC}"

# Start simulation
echo ""
echo -e "${YELLOW}Starting simulation...${NC}"
curl -s -X POST http://localhost:5000/api/v1/clock/start | grep -q "started" && echo -e "${GREEN}✓ Simulation started${NC}"

# Wait a bit for data to stream
echo ""
echo "Waiting 15 seconds for data streaming..."
sleep 15

# Check status
echo ""
echo -e "${YELLOW}Checking simulation status...${NC}"
STATUS=$(curl -s http://localhost:5000/api/v1/clock/status)
echo "$STATUS" | python3 -m json.tool 2>/dev/null || echo "$STATUS"

# Check Kafka topics
echo ""
echo -e "${YELLOW}Checking Kafka topics...${NC}"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -E "bike-trips|taxi-trips|weather-data" && echo -e "${GREEN}✓ Kafka topics created${NC}"

# Consume sample messages
echo ""
echo -e "${YELLOW}Sample bike-trips messages:${NC}"
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bike-trips \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 10000 2>/dev/null | head -3 || echo "No messages yet"

echo ""
echo -e "${YELLOW}Sample taxi-trips messages:${NC}"
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic taxi-trips \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 10000 2>/dev/null | head -3 || echo "No messages yet"

echo ""
echo -e "${YELLOW}Sample weather-data messages:${NC}"
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather-data \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 10000 2>/dev/null | head -3 || echo "No messages yet"

# Stop simulation
echo ""
echo -e "${YELLOW}Stopping simulation...${NC}"
curl -s -X POST http://localhost:5000/api/v1/clock/stop | grep -q "stopped" && echo -e "${GREEN}✓ Simulation stopped${NC}"

# Show logs
echo ""
echo -e "${YELLOW}Streamer logs (last 20 lines):${NC}"
echo "--- Bike Streamer ---"
docker-compose logs --tail=20 bike-streamer | tail -10
echo ""
echo "--- Taxi Streamer ---"
docker-compose logs --tail=20 taxi-streamer | tail -10
echo ""
echo "--- Weather Streamer ---"
docker-compose logs --tail=20 weather-streamer | tail -10

# Summary
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Test Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "System is running. You can:"
echo "  - View logs: docker-compose logs -f [service-name]"
echo "  - Access API: curl http://localhost:5000/api/v1/clock/status"
echo "  - Stop system: docker-compose down"
echo ""
