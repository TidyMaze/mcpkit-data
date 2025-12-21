# Integration Tests with Dockerized Kafka

This project includes integration tests that use a dockerized Kafka platform to test Kafka, Schema Registry, and Avro tools with real services.

## Quick Start

### 1. Install Integration Dependencies

```bash
pip install -e ".[integration]"
```

Or install manually:
```bash
pip install confluent-kafka
```

### 2. Start Docker Services

```bash
# Using Make
make docker-up

# Or manually
docker compose up -d
```

Wait for services to be ready (about 30-60 seconds):
```bash
docker compose ps
```

All services should show as "healthy" or "running".

### 3. Run Integration Tests

```bash
# Using the script (recommended)
./run_integration_tests.sh

# Or using Make
make test-integration

# Or directly with pytest
pytest tests/test_kafka_integration.py -v -m integration
```

### 4. Stop Services (when done)

```bash
make docker-down
# or
docker compose down
```

## Test Coverage

The integration tests (`tests/test_kafka_integration.py`) cover:

### Kafka Tools
- ✅ `kafka_offsets` - Get topic offsets from real Kafka
- ✅ `kafka_consume_batch` - Consume messages from real topics

### Schema Registry
- ✅ `schema_registry_get` - Fetch schemas by ID
- ✅ `schema_registry_get` - Fetch schemas by subject

### Avro Decoding
- ✅ `avro_decode` with Confluent format (magic byte + schema ID from registry)
- ✅ `avro_decode` with provided schema

### Combined Workflows
- ✅ Filtering Kafka records using JMESPath
- ✅ Grouping Kafka records by extracted keys

## Docker Services

The `docker-compose.yml` includes:

- **Zookeeper** (port 2181) - Kafka coordination service
- **Kafka** (port 9092) - Message broker
- **Schema Registry** (port 8081) - Schema management for Avro/Protobuf

All services use Confluent Platform images (version 7.5.0).

## Running Tests

### Run All Tests (Unit + Integration)

```bash
pytest -v
```

### Run Only Unit Tests (No Docker Required)

```bash
pytest -v -m "not integration"
# or
make test-unit
```

### Run Only Integration Tests

```bash
pytest -v -m integration
# or
make test-integration
```

## Troubleshooting

### Services Not Starting

```bash
# Check logs
docker compose logs

# Check service status
docker compose ps

# Restart services
docker compose restart
```

### Port Conflicts

If ports 2181, 9092, or 8081 are already in use, modify `docker-compose.yml`:

```yaml
ports:
  - "9093:9092"  # Use different host port
```

### Tests Timing Out

- Ensure services are fully healthy: `docker compose ps`
- Increase wait times in test fixtures
- Check Docker has enough resources allocated

### Connection Errors

- Verify services are running: `docker compose ps`
- Check firewall settings
- Ensure `localhost:9092` and `localhost:8081` are accessible

## Manual Testing

You can test the services manually:

```bash
# Check Kafka
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check Schema Registry
curl http://localhost:8081/subjects

# List topics (requires kafka tools)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## CI/CD Integration

For CI/CD pipelines, you can use the integration tests:

```yaml
# Example GitHub Actions
- name: Start Kafka services
  run: docker compose up -d

- name: Wait for services
  run: sleep 30

- name: Run integration tests
  run: pytest -v -m integration
```

## Files

- `docker-compose.yml` - Docker services configuration
- `tests/test_kafka_integration.py` - Integration test suite
- `tests/conftest_integration.py` - Pytest fixtures for integration tests
- `run_integration_tests.sh` - Helper script to run tests
- `Makefile` - Convenience commands

## Notes

- Integration tests are marked with `@pytest.mark.integration`
- Tests use real Kafka and Schema Registry (not mocks)
- Each test creates its own topics to avoid conflicts
- Services can be left running between test runs for faster execution

