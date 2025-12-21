# Integration Tests Setup

The integration tests require Docker services (Kafka, Zookeeper, Schema Registry) to be running.

## Quick Setup

The Docker Compose setup may have permission issues on some systems. If you encounter Zookeeper data directory errors, try:

### Option 1: Use Docker Desktop (Recommended)

If using Docker Desktop, ensure it has proper permissions and try:

```bash
docker compose up -d
```

Wait 30-60 seconds for services to start, then verify:

```bash
docker compose ps
```

All services should show as "running" or "healthy".

### Option 2: Manual Service Start

If Docker Compose has issues, you can start services manually or use a different Kafka setup:

1. **Use a managed Kafka service** (Confluent Cloud, AWS MSK, etc.)
2. **Use local Kafka installation** (if available)
3. **Use testcontainers** (requires Java)

### Option 3: Skip Integration Tests

You can run only unit tests (which don't require Docker):

```bash
pytest -v -m "not integration"
```

## Running Integration Tests

Once services are running:

```bash
# Set environment variables
export MCPKIT_KAFKA_BOOTSTRAP=localhost:9092
export MCPKIT_SCHEMA_REGISTRY_URL=http://localhost:8081

# Run tests
pytest tests/test_kafka_integration.py -v
```

## Troubleshooting

### Zookeeper Permission Errors

If you see "Unable to create data directory" errors:

1. Check Docker permissions
2. Try running with `sudo` (not recommended for production)
3. Use Docker volumes explicitly
4. Check Docker Desktop resource settings

### Connection Refused

If tests fail with connection errors:

1. Verify services are running: `docker compose ps`
2. Check ports are not in use: `lsof -i :9092 -i :2181 -i :8081`
3. Wait longer for services to start (Kafka can take 30-60 seconds)
4. Check logs: `docker compose logs`

### Services Not Starting

```bash
# Check logs
docker compose logs zookeeper
docker compose logs kafka
docker compose logs schema-registry

# Restart services
docker compose restart

# Clean restart
docker compose down -v
docker compose up -d
```

## Alternative: Use Testcontainers

For a more reliable setup, consider using testcontainers-python which handles Docker automatically:

```python
from testcontainers.kafka import KafkaContainer

kafka = KafkaContainer()
kafka.start()
```

This would require adding testcontainers as a dependency.

