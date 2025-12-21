"""Integration tests for Kafka, Schema Registry, and Avro tools using Docker."""

import base64
import json
import os
import time
from typing import Optional

import pytest
import requests

from mcpkit.core.decode import avro_decode
from mcpkit.core.guards import GuardError
from mcpkit.core.kafka_client import kafka_consume_batch, kafka_offsets
from mcpkit.core.schema_registry import schema_registry_get

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False
    # Fallback to kafka-python for basic operations
    from kafka import KafkaProducer, KafkaAdminClient
    from kafka.admin import NewTopic as KafkaNewTopic

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def kafka_setup():
    """Setup Kafka environment variables for tests."""
    os.environ["MCPKIT_KAFKA_BOOTSTRAP"] = "localhost:9092"
    os.environ["MCPKIT_SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    
    # Wait for services to be ready
    time.sleep(5)
    
    yield
    
    # Cleanup
    if "MCPKIT_KAFKA_BOOTSTRAP" in os.environ:
        del os.environ["MCPKIT_KAFKA_BOOTSTRAP"]
    if "MCPKIT_SCHEMA_REGISTRY_URL" in os.environ:
        del os.environ["MCPKIT_SCHEMA_REGISTRY_URL"]


@pytest.fixture
def kafka_topic(kafka_setup):
    """Create a test topic and yield its name."""
    topic_name = f"test_topic_{int(time.time())}"
    admin_client = None
    
    # Create topic
    if CONFLUENT_KAFKA_AVAILABLE:
        admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
        topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
    else:
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
        topic = KafkaNewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
    
    # Wait for topic creation and ensure it's ready
    time.sleep(3)
    
    yield topic_name
    
    # Cleanup - delete topic
    try:
        if admin_client:
            admin_client.delete_topics([topic_name])
    except Exception:
        pass


@pytest.fixture
def kafka_producer():
    """Create a Kafka producer."""
    if CONFLUENT_KAFKA_AVAILABLE:
        return Producer({"bootstrap.servers": "localhost:9092"})
    else:
        return KafkaProducer(bootstrap_servers="localhost:9092")


def test_kafka_offsets_integration(kafka_setup, kafka_topic, kafka_producer):
    """Test kafka_offsets with real Kafka."""
    # Produce some messages
    if CONFLUENT_KAFKA_AVAILABLE:
        for i in range(5):
            kafka_producer.produce(kafka_topic, key=f"key_{i}", value=f"value_{i}")
        # Flush with timeout - confluent-kafka flush() can hang if messages can't be delivered
        kafka_producer.flush(timeout=10)
    else:
        for i in range(5):
            kafka_producer.send(kafka_topic, key=f"key_{i}".encode(), value=f"value_{i}".encode())
        kafka_producer.flush()
    time.sleep(1)
    
    # Get offsets
    result = kafka_offsets(kafka_topic)
    
    assert "topic" in result
    assert result["topic"] == kafka_topic
    assert "partitions" in result
    assert len(result["partitions"]) > 0


def test_kafka_consume_batch_integration(kafka_setup, kafka_topic, kafka_producer):
    """Test kafka_consume_batch with real Kafka."""
    # Produce messages
    messages = [f"message_{i}" for i in range(10)]
    if CONFLUENT_KAFKA_AVAILABLE:
        for msg in messages:
            kafka_producer.produce(kafka_topic, value=msg)
        kafka_producer.flush()
    else:
        for msg in messages:
            kafka_producer.send(kafka_topic, value=msg.encode())
        kafka_producer.flush()
    time.sleep(1)
    
    # Consume batch
    result = kafka_consume_batch(kafka_topic, max_records=10, timeout_secs=5)
    
    assert "records" in result
    assert len(result["records"]) == 10
    
    # Verify we got the messages
    consumed_values = [
        base64.b64decode(r["value_base64"]).decode("utf-8")
        for r in result["records"]
        if r["value_base64"]
    ]
    assert len(consumed_values) == 10


def test_schema_registry_get_integration(kafka_setup):
    """Test schema_registry_get with real Schema Registry."""
    
    # Register a schema
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ]
    }
    
    subject = f"test-subject-{int(time.time())}"
    url = "http://localhost:8081/subjects/{}/versions".format(subject)
    response = requests.post(url, json={"schema": json.dumps(schema)})
    response.raise_for_status()
    schema_data = response.json()
    schema_id = schema_data["id"]
    
    # Get schema by ID
    result = schema_registry_get(schema_id=schema_id)
    assert "schema_id" in result
    assert result["schema_id"] == schema_id
    
    # Get schema by subject
    result2 = schema_registry_get(subject=subject)
    assert "subject" in result2
    assert result2["subject"] == subject


def test_avro_decode_with_schema_registry(kafka_setup):
    """Test avro_decode with Confluent format using Schema Registry."""
    import fastavro
    import io
    
    # Register schema
    schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"}
        ]
    }
    
    subject = f"test-user-{int(time.time())}"
    url = "http://localhost:8081/subjects/{}/versions".format(subject)
    response = requests.post(url, json={"schema": json.dumps(schema)})
    response.raise_for_status()
    schema_data = response.json()
    schema_id = schema_data["id"]
    
    # Create Avro-encoded value in Confluent format
    # Format: magic byte (0) + schema_id (4 bytes) + avro payload
    record = {"id": 1, "name": "Alice", "email": "alice@example.com"}
    
    # Encode with Avro
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    
    # Create Confluent format
    magic_byte = b"\x00"
    schema_id_bytes = schema_id.to_bytes(4, byteorder="big")
    confluent_value = magic_byte + schema_id_bytes + avro_payload
    
    # Encode to base64
    value_base64 = base64.b64encode(confluent_value).decode("utf-8")
    
    # Decode using our tool (should fetch schema from registry)
    result = avro_decode(value_base64)
    
    assert "decoded" in result
    decoded = result["decoded"]
    assert decoded["id"] == 1
    assert decoded["name"] == "Alice"
    assert decoded["email"] == "alice@example.com"


def test_avro_decode_with_provided_schema(kafka_setup):
    """Test avro_decode with provided schema."""
    import fastavro
    import io
    
    schema = {
        "type": "record",
        "name": "Product",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "price", "type": "double"}
        ]
    }
    
    record = {"id": 100, "name": "Widget", "price": 25.50}
    
    # Encode
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    
    # Encode to base64
    value_base64 = base64.b64encode(avro_payload).decode("utf-8")
    
    # Decode with provided schema
    result = avro_decode(value_base64, schema_json=schema)
    
    assert "decoded" in result
    decoded = result["decoded"]
    assert decoded["id"] == 100
    assert decoded["name"] == "Widget"
    assert decoded["price"] == 25.50


def test_kafka_filter_integration(kafka_setup, kafka_topic, kafka_producer):
    """Test kafka_filter with real Kafka records."""
    from mcpkit.core.json_tools import jq_transform
    
    # Produce JSON messages
    messages = [
        {"user_id": 1, "action": "login", "timestamp": 100},
        {"user_id": 2, "action": "click", "timestamp": 200},
        {"user_id": 1, "action": "logout", "timestamp": 300},
    ]
    
    if CONFLUENT_KAFKA_AVAILABLE:
        for msg in messages:
            kafka_producer.produce(kafka_topic, value=json.dumps(msg))
        kafka_producer.flush()
    else:
        for msg in messages:
            kafka_producer.send(kafka_topic, value=json.dumps(msg).encode())
        kafka_producer.flush()
    time.sleep(1)
    
    # Consume and filter
    result = kafka_consume_batch(kafka_topic, max_records=10, timeout_secs=5)
    records = result["records"]
    
    # Filter records where user_id == 1
    filtered = []
    for record in records:
        if record["value_base64"]:
            value_bytes = base64.b64decode(record["value_base64"])
            value_json = json.loads(value_bytes.decode("utf-8"))
            jmes_result = jq_transform(value_json, "user_id")
            if jmes_result.get("result") == 1:
                filtered.append(record)
    
    assert len(filtered) == 2  # Two records with user_id == 1


def test_kafka_groupby_key_integration(kafka_setup, kafka_topic, kafka_producer):
    """Test kafka_groupby_key with real Kafka records."""
    from mcpkit.core.json_tools import jq_transform
    
    # Produce messages with different keys
    messages = [
        {"order_id": 1, "item": "A", "quantity": 10},
        {"order_id": 2, "item": "B", "quantity": 5},
        {"order_id": 1, "item": "C", "quantity": 3},
    ]
    
    if CONFLUENT_KAFKA_AVAILABLE:
        for msg in messages:
            kafka_producer.produce(kafka_topic, value=json.dumps(msg))
        kafka_producer.flush()
    else:
        for msg in messages:
            kafka_producer.send(kafka_topic, value=json.dumps(msg).encode())
        kafka_producer.flush()
    time.sleep(1)
    
    # Consume
    result = kafka_consume_batch(kafka_topic, max_records=10, timeout_secs=5)
    records = result["records"]
    
    # Group by order_id
    groups = {}
    for record in records:
        if record["value_base64"]:
            value_bytes = base64.b64decode(record["value_base64"])
            value_json = json.loads(value_bytes.decode("utf-8"))
            key = jq_transform(value_json, "order_id").get("result")
            if key not in groups:
                groups[key] = []
            groups[key].append(value_json)
    
    assert 1 in groups
    assert 2 in groups
    assert len(groups[1]) == 2  # Two records for order_id 1
    assert len(groups[2]) == 1  # One record for order_id 2

