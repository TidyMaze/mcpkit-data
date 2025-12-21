# Integration Tests with Dockerized Kafka

This directory contains integration tests that use a dockerized Kafka platform.

## Prerequisites

- Docker and Docker Compose installed
- Integration test dependencies: `pip install -e ".[integration]"`

## Running Integration Tests

### 1. Start Docker Services

```bash
docker-compose up -d
```

Wait for services to be ready (about 30-60 seconds):
```bash
docker-compose ps
```

All services should show as "healthy" or "running".

### 2. Run Integration Tests

```bash
# Run all integration tests
pytest tests/test_kafka_integration.py -v

# Run with markers
pytest -m integration -v

# Run specific test
pytest tests/test_kafka_integration.py::test_kafka_consume_batch_integration -v
```

### 3. Stop Docker Services (when done)

```bash
docker-compose down
```

Or keep them running for faster subsequent test runs.

## Test Coverage

The integration tests cover:

1. **Kafka Tools:**
   - `kafka_offsets` - Get topic offsets
   - `kafka_consume_batch` - Consume messages from topics

2. **Schema Registry:**
   - `schema_registry_get` - Fetch schemas by ID and subject

3. **Avro Decoding:**
   - `avro_decode` with Confluent format (magic byte + schema ID)
   - `avro_decode` with provided schema

4. **Kafka + JSON Tools:**
   - Filtering Kafka records with JMESPath
   - Grouping Kafka records by key

## Services

The docker-compose setup includes:

- **Zookeeper** (port 2181) - Kafka coordination
- **Kafka** (port 9092) - Message broker
- **Schema Registry** (port 8081) - Schema management

## Troubleshooting

### Services not starting

```bash
# Check logs
docker-compose logs

# Restart services
docker-compose restart
```

### Port conflicts

If ports 2181, 9092, or 8081 are already in use, modify `docker-compose.yml` to use different ports.

### Tests timing out

Increase wait times in `conftest_integration.py` or ensure services are fully healthy before running tests.

## Manual Testing

You can also test manually:

```bash
# Check Kafka is running
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check Schema Registry
curl http://localhost:8081/subjects
```

