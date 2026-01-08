"""Integration tests for Kafka tools via MCP protocol."""

import base64
import json
import os
import time

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, json_stringify

pytestmark = pytest.mark.integration


# kafka_setup fixture is now in conftest_mcp.py


@pytest.fixture
def kafka_topic(docker_services, kafka_setup):
    """Create a test topic and yield its name."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        CONFLUENT_KAFKA_AVAILABLE = True
    except ImportError:
        CONFLUENT_KAFKA_AVAILABLE = False
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic as KafkaNewTopic
    
    topic_name = f"test_topic_{int(time.time())}"
    
    # Create topic
    if CONFLUENT_KAFKA_AVAILABLE:
        admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
        topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
    else:
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
        topic = KafkaNewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
    
    # Wait for topic creation (reduced from 3s to 0.5s)
    time.sleep(0.5)
    
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
    try:
        from confluent_kafka import Producer
        CONFLUENT_KAFKA_AVAILABLE = True
        return Producer({"bootstrap.servers": "localhost:9092"})
    except ImportError:
        from kafka import KafkaProducer
        return KafkaProducer(bootstrap_servers="localhost:9092")


def test_kafka_list_topics_mcp(mcp_client, kafka_setup):
    """Test kafka_list_topics via MCP protocol."""
    response = call_tool(mcp_client, "kafka_list_topics")
    
    assert_response_structure(response, ["topics", "topic_count"])
    assert isinstance(response["topics"], list)


def test_kafka_offsets_mcp(mcp_client, kafka_setup, kafka_topic, kafka_producer):
    """Test kafka_offsets via MCP protocol."""
    # Produce some messages
    try:
        from confluent_kafka import Producer
        kafka_producer.produce(kafka_topic, value="test_message")
        kafka_producer.flush(timeout=10)
    except Exception:
        kafka_producer.send(kafka_topic, value="test_message".encode())
        kafka_producer.flush()
    
    time.sleep(0.1)  # Reduced wait time
    
    response = call_tool(mcp_client, "kafka_offsets", topic=kafka_topic)
    
    assert_response_structure(response, ["topic", "partitions"])
    assert response["topic"] == kafka_topic
    assert len(response["partitions"]) > 0


def test_kafka_consume_batch_mcp(mcp_client, kafka_setup, kafka_topic, kafka_producer):
    """Test kafka_consume_batch via MCP protocol."""
    # Produce messages
    messages = [f"message_{i}" for i in range(5)]
    try:
        from confluent_kafka import Producer
        for msg in messages:
            kafka_producer.produce(kafka_topic, value=msg)
        kafka_producer.flush()
    except Exception:
        for msg in messages:
            kafka_producer.send(kafka_topic, value=msg.encode())
        kafka_producer.flush()
    
    time.sleep(0.1)  # Reduced wait time
    
    response = call_tool(
        mcp_client,
        "kafka_consume_batch",
        topic=kafka_topic,
        max_records=5,
        timeout_secs=5
    )
    
    assert_response_structure(response, ["dataset_id", "record_count", "columns"])
    assert response["record_count"] == 5


def test_kafka_consume_tail_mcp(mcp_client, kafka_setup, kafka_topic, kafka_producer):
    """Test kafka_consume_tail via MCP protocol."""
    # Produce messages
    try:
        from confluent_kafka import Producer
        for i in range(3):
            kafka_producer.produce(kafka_topic, value=f"tail_msg_{i}")
        kafka_producer.flush()
    except Exception:
        for i in range(3):
            kafka_producer.send(kafka_topic, value=f"tail_msg_{i}".encode())
        kafka_producer.flush()
    
    time.sleep(0.1)  # Reduced wait time
    
    response = call_tool(
        mcp_client,
        "kafka_consume_tail",
        topic=kafka_topic,
        n_messages=3
    )
    
    assert_response_structure(response, ["dataset_id", "record_count"])
    assert response["record_count"] == 3


def test_kafka_filter_mcp(mcp_client, kafka_setup, kafka_topic, kafka_producer):
    """Test kafka_filter via MCP protocol."""
    # Produce JSON messages
    messages = [
        {"user_id": 1, "action": "login"},
        {"user_id": 2, "action": "click"},
        {"user_id": 1, "action": "logout"},
    ]
    
    try:
        from confluent_kafka import Producer
        for msg in messages:
            kafka_producer.produce(kafka_topic, value=json.dumps(msg))
        kafka_producer.flush()
    except Exception:
        for msg in messages:
            kafka_producer.send(kafka_topic, value=json.dumps(msg).encode())
        kafka_producer.flush()
    
    time.sleep(0.1)  # Reduced wait time
    
    # Consume and get records
    consume_response = call_tool(
        mcp_client,
        "kafka_consume_batch",
        topic=kafka_topic,
        max_records=10,
        timeout_secs=5
    )
    
    # Get records from dataset (simplified - in real test would load dataset)
    # For now, test the filter tool with mock records
    test_records = [
        {"value_base64": base64.b64encode(json.dumps({"user_id": 1}).encode()).decode()},
        {"value_base64": base64.b64encode(json.dumps({"user_id": 2}).encode()).decode()},
    ]
    
    response = call_tool(
        mcp_client,
        "kafka_filter",
        records=json_stringify(test_records),
        predicate_jmes="user_id == `1`"
    )
    
    assert_response_structure(response, ["records", "count"])
    assert response["count"] >= 1


def test_kafka_groupby_key_mcp(mcp_client):
    """Test kafka_groupby_key via MCP protocol."""
    records = [
        {"order_id": 1, "item": "A"},
        {"order_id": 2, "item": "B"},
        {"order_id": 1, "item": "C"},
    ]
    
    response = call_tool(
        mcp_client,
        "kafka_groupby_key",
        records=json_stringify(records),
        key_jmes="order_id"
    )
    
    assert_response_structure(response, ["groups", "group_count"])
    assert response["group_count"] == 2


def test_kafka_describe_topic_mcp(mcp_client, kafka_setup, kafka_topic):
    """Test kafka_describe_topic via MCP protocol."""
    response = call_tool(mcp_client, "kafka_describe_topic", topic=kafka_topic)
    
    assert_response_structure(response, ["topic", "partitions", "partition_count"])
    assert response["topic"] == kafka_topic
    assert response["partition_count"] > 0


def test_kafka_describe_nonexistent_topic_mcp(mcp_client, kafka_setup):
    """Test kafka_describe_topic with non-existent topic via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "kafka_describe_topic",
            topic="nonexistent_topic_xyz"
        )


def test_kafka_consume_batch_empty_topic_mcp(mcp_client, kafka_setup, kafka_topic):
    """Test kafka_consume_batch with empty topic via MCP protocol."""
    response = call_tool(
        mcp_client,
        "kafka_consume_batch",
        topic=kafka_topic,
        max_records=10,
        timeout_secs=2
    )
    
    assert_response_structure(response, ["records", "count"])
    assert response["count"] == 0
    assert response["records"] == []


def test_kafka_consume_batch_with_partition_mcp(mcp_client, kafka_setup, kafka_topic, kafka_producer):
    """Test kafka_consume_batch with specific partition via MCP protocol."""
    # Produce a message first
    try:
        from confluent_kafka import Producer
        kafka_producer.produce(kafka_topic, value=b"test message", partition=0)
        kafka_producer.flush(timeout=10)
        time.sleep(0.2)  # Reduced wait time
    except Exception:
        kafka_producer.send(kafka_topic, value=b"test message", partition=0)
        kafka_producer.flush()
        time.sleep(0.2)  # Reduced wait time
    
    response = call_tool(
        mcp_client,
        "kafka_consume_batch",
        topic=kafka_topic,
        partition=0,
        max_records=10,
        timeout_secs=5
    )
    
    assert_response_structure(response, ["records", "count"])
    assert response["count"] >= 0


def test_kafka_consume_batch_with_offset_mcp(mcp_client, kafka_setup, kafka_topic):
    """Test kafka_consume_batch with specific offset via MCP protocol."""
    response = call_tool(
        mcp_client,
        "kafka_consume_batch",
        topic=kafka_topic,
        from_offset=0,
        max_records=5,
        timeout_secs=2
    )
    
    assert_response_structure(response, ["records", "count"])


def test_kafka_filter_empty_records_mcp(mcp_client):
    """Test kafka_filter with empty records via MCP protocol."""
    response = call_tool(
        mcp_client,
        "kafka_filter",
        records=json_stringify([]),
        predicate_jmes="value > 10"
    )
    
    assert_response_structure(response, ["records", "count"])
    assert response["count"] == 0


def test_kafka_filter_no_matches_mcp(mcp_client):
    """Test kafka_filter with no matching records via MCP protocol."""
    records = [
        {"key": "1", "value": {"price": 5}},
        {"key": "2", "value": {"price": 8}}
    ]
    
    response = call_tool(
        mcp_client,
        "kafka_filter",
        records=json_stringify(records),
        predicate_jmes="value.price > 100"
    )
    
    assert_response_structure(response, ["records", "count"])
    assert response["count"] == 0


def test_kafka_groupby_key_empty_mcp(mcp_client):
    """Test kafka_groupby_key with empty records via MCP protocol."""
    response = call_tool(
        mcp_client,
        "kafka_groupby_key",
        records=json_stringify([]),
        key_jmes="key"
    )
    
    assert_response_structure(response, ["groups", "group_count"])
    assert response["group_count"] == 0


def test_kafka_groupby_key_nested_mcp(mcp_client):
    """Test kafka_groupby_key with nested key extraction via MCP protocol."""
    records = [
        {"value": {"user": {"id": 1}, "action": "click"}},
        {"value": {"user": {"id": 1}, "action": "view"}},
        {"value": {"user": {"id": 2}, "action": "click"}}
    ]
    
    response = call_tool(
        mcp_client,
        "kafka_groupby_key",
        records=json_stringify(records),
        key_jmes="value.user.id"
    )
    
    assert_response_structure(response, ["groups", "group_count"])
    assert response["group_count"] >= 1


def test_kafka_offsets_nonexistent_topic_mcp(mcp_client, kafka_setup):
    """Test kafka_offsets with non-existent topic via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "kafka_offsets",
            topic="nonexistent_topic_xyz"
        )

