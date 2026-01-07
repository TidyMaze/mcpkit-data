"""Tests for Kafka tools."""

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.kafka_client import get_kafka_config, kafka_consume_batch, kafka_offsets


def test_get_kafka_config_missing(monkeypatch):
    """Test missing Kafka bootstrap."""
    monkeypatch.delenv("MCPKIT_KAFKA_BOOTSTRAP", raising=False)
    with pytest.raises(GuardError, match="MCPKIT_KAFKA_BOOTSTRAP"):
        get_kafka_config()


def test_get_kafka_config_basic(monkeypatch):
    """Test basic Kafka config."""
    monkeypatch.setenv("MCPKIT_KAFKA_BOOTSTRAP", "localhost:9092")
    config = get_kafka_config()
    assert "bootstrap_servers" in config
    assert config["bootstrap_servers"] == ["localhost:9092"]


def test_get_kafka_config_sasl(monkeypatch):
    """Test Kafka config with SASL."""
    monkeypatch.setenv("MCPKIT_KAFKA_BOOTSTRAP", "localhost:9092")
    monkeypatch.setenv("MCPKIT_KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("MCPKIT_KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("MCPKIT_KAFKA_SASL_USERNAME", "user")
    monkeypatch.setenv("MCPKIT_KAFKA_SASL_PASSWORD", "pass")
    
    config = get_kafka_config()
    assert config["security_protocol"] == "SASL_SSL"
    assert config["sasl_plain_username"] == "user"


def test_kafka_offsets_real(monkeypatch):
    """Test kafka_offsets with real Kafka (if available)."""
    monkeypatch.setenv("MCPKIT_KAFKA_BOOTSTRAP", "localhost:9092")
    
    # Skip if Kafka is not available
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"], consumer_timeout_ms=1000)
        consumer.close()
    except Exception:
        pytest.skip("Kafka not available")
    
    # Try to get offsets from a test topic
    result = kafka_offsets("test_topic")
    assert "partitions" in result
    assert result["topic"] == "test_topic"


def test_kafka_consume_batch_real(monkeypatch):
    """Test kafka_consume_batch with real Kafka (if available)."""
    monkeypatch.setenv("MCPKIT_KAFKA_BOOTSTRAP", "localhost:9092")
    
    # Skip if Kafka is not available
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"], consumer_timeout_ms=1000)
        consumer.close()
    except Exception:
        pytest.skip("Kafka not available")
    
    # Try to consume from a test topic (may be empty, that's OK)
    result = kafka_consume_batch("test_topic", max_records=1, timeout_secs=1)
    # Should return dataset_id, record_count, columns
    assert "dataset_id" in result
    assert "record_count" in result
    assert "columns" in result

