"""Tests for Kafka tools."""

from unittest.mock import MagicMock, patch

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


@patch("mcpkit.core.kafka_client.KafkaConsumer")
def test_kafka_offsets_mock(mock_consumer_class, monkeypatch):
    """Test kafka_offsets with mocked consumer."""
    monkeypatch.setenv("MCPKIT_KAFKA_BOOTSTRAP", "localhost:9092")
    
    mock_consumer = MagicMock()
    mock_consumer.partitions_for_topic.return_value = {0, 1}
    mock_consumer.highwater.return_value = 100
    mock_consumer.seek_to_beginning.return_value = 0
    mock_consumer.committed.return_value = 50
    mock_consumer._coordinator._partition_metadata = {}
    mock_consumer_class.return_value = mock_consumer
    
    result = kafka_offsets("test_topic")
    assert "partitions" in result
    assert result["topic"] == "test_topic"


@patch("mcpkit.core.kafka_client.KafkaConsumer")
def test_kafka_consume_batch_mock(mock_consumer_class, monkeypatch):
    """Test kafka_consume_batch with mocked consumer."""
    monkeypatch.setenv("MCPKIT_KAFKA_BOOTSTRAP", "localhost:9092")
    
    mock_consumer = MagicMock()
    mock_message = MagicMock()
    mock_message.partition = 0
    mock_message.offset = 100
    mock_message.timestamp = 1234567890
    mock_message.key = b"key"
    mock_message.value = b"value"
    mock_message.headers = []
    
    mock_consumer.__iter__ = lambda self: iter([mock_message])
    mock_consumer._coordinator._partition_metadata = {}
    mock_consumer_class.return_value = mock_consumer
    
    result = kafka_consume_batch("test_topic", max_records=1)
    assert "records" in result
    assert len(result["records"]) == 1
    assert result["records"][0]["partition"] == 0

