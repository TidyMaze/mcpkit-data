#!/bin/bash
# Script to run Kafka integration tests with Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check for docker compose (v2) or docker-compose (v1)
if command -v docker &> /dev/null && docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "Error: docker compose or docker-compose not found"
    exit 1
fi

echo "Using: $DOCKER_COMPOSE"
echo ""

# Start services
echo "Starting Kafka services..."
$DOCKER_COMPOSE up -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
max_wait=60
waited=0
while [ $waited -lt $max_wait ]; do
    if $DOCKER_COMPOSE ps | grep -q "healthy\|running"; then
        echo "Services are ready!"
        break
    fi
    sleep 2
    waited=$((waited + 2))
    echo "  Waiting... ($waited/$max_wait seconds)"
done

if [ $waited -ge $max_wait ]; then
    echo "Warning: Services may not be fully ready"
fi

# Give extra time for Kafka to stabilize
sleep 5

# Run tests
echo ""
echo "Running integration tests..."
pytest tests/test_kafka_integration.py -v "$@"

# Optionally stop services (uncomment if desired)
# echo ""
# echo "Stopping services..."
# $DOCKER_COMPOSE down

