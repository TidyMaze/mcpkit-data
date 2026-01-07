"""Unit tests for kafka_client.py helper functions."""

import base64
import json
import os

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.kafka_client import (
    _decode_kafka_value,
    _flatten_dict,
    kafka_flatten_records,
    kafka_flatten_dataset,
)
from mcpkit.core.registry import dataset_put_rows, load_dataset


def test_decode_kafka_value_empty():
    """Test _decode_kafka_value returns None for empty bytes."""
    assert _decode_kafka_value(b"") is None
    assert _decode_kafka_value(None) is None


def test_decode_kafka_value_string_input():
    """Test _decode_kafka_value handles string input."""
    # String input (shouldn't happen but handled)
    result = _decode_kafka_value("test string")
    assert result == "test string"


def test_decode_kafka_value_string_with_null_bytes():
    """Test _decode_kafka_value handles string with null bytes."""
    # String with null bytes (binary data incorrectly decoded)
    result = _decode_kafka_value("\x00\x00\x00\x00test")
    assert result == "\x00\x00\x00\x00test"


def test_decode_kafka_value_utf8_text():
    """Test _decode_kafka_value decodes UTF-8 text."""
    text = "Hello, world! 你好"
    result = _decode_kafka_value(text.encode("utf-8"))
    assert result == text


def test_decode_kafka_value_binary_data():
    """Test _decode_kafka_value base64 encodes binary data."""
    # Use binary data that doesn't start with 0 (to avoid Avro detection)
    binary = b"\x01\x02\x03\xff\xfe\xfd"
    result = _decode_kafka_value(binary)
    assert isinstance(result, str)
    # Binary data should be base64 encoded (UTF-8 decode will fail)
    # The function tries UTF-8 first, then base64 encodes on failure
    try:
        binary.decode("utf-8", errors="strict")
        # If UTF-8 decode succeeds, it might return the string
        assert isinstance(result, str)
    except UnicodeDecodeError:
        # UTF-8 decode fails, so it should be base64 encoded
        assert result == base64.b64encode(binary).decode("utf-8")


def test_decode_kafka_value_control_chars():
    """Test _decode_kafka_value base64 encodes data with many control chars."""
    # String with >10% control characters
    data = b"normal\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f" + b"text"
    result = _decode_kafka_value(data)
    # Should be base64 encoded due to control chars
    assert isinstance(result, str)
    decoded = base64.b64decode(result)
    assert decoded == data


def test_decode_kafka_value_avro_no_registry(monkeypatch):
    """Test _decode_kafka_value raises error for Avro without registry URL."""
    monkeypatch.delenv("MCPKIT_SCHEMA_REGISTRY_URL", raising=False)
    
    # Confluent Avro format: magic byte 0 + schema_id (4 bytes) + payload
    avro_data = bytes([0]) + b"\x00\x00\x00\x01" + b"payload"
    
    with pytest.raises(GuardError, match="MCPKIT_SCHEMA_REGISTRY_URL"):
        _decode_kafka_value(avro_data)


def test_decode_kafka_value_avro_with_registry(monkeypatch):
    """Test _decode_kafka_value decodes Avro with registry URL."""
    monkeypatch.setenv("MCPKIT_SCHEMA_REGISTRY_URL", "http://localhost:8081")
    
    # This will fail because schema registry doesn't exist, but tests the path
    avro_data = bytes([0]) + b"\x00\x00\x00\x01" + b"payload"
    
    # Should attempt to decode (will fail at schema fetch, but tests the logic)
    with pytest.raises(Exception):  # Will fail at avro_decode or schema fetch
        _decode_kafka_value(avro_data, schema_registry_url="http://localhost:8081")


def test_decode_kafka_value_unicode_decode_error():
    """Test _decode_kafka_value handles UnicodeDecodeError."""
    # Invalid UTF-8 sequence
    invalid_utf8 = b"\xff\xfe\xfd"
    result = _decode_kafka_value(invalid_utf8)
    assert isinstance(result, str)
    assert result == base64.b64encode(invalid_utf8).decode("utf-8")


def test_flatten_dict_simple():
    """Test _flatten_dict with simple dict."""
    d = {"a": 1, "b": 2}
    result = _flatten_dict(d)
    assert result == {"a": 1, "b": 2}


def test_flatten_dict_nested():
    """Test _flatten_dict with nested dict."""
    d = {
        "a": 1,
        "b": {
            "c": 2,
            "d": {
                "e": 3
            }
        }
    }
    result = _flatten_dict(d)
    assert result == {
        "a": 1,
        "b_c": 2,
        "b_d_e": 3
    }


def test_flatten_dict_custom_separator():
    """Test _flatten_dict with custom separator."""
    d = {
        "a": 1,
        "b": {
            "c": 2
        }
    }
    result = _flatten_dict(d, sep=".")
    assert result == {
        "a": 1,
        "b.c": 2
    }


def test_flatten_dict_with_list():
    """Test _flatten_dict converts list to JSON string."""
    d = {
        "a": 1,
        "b": [1, 2, 3]
    }
    result = _flatten_dict(d)
    assert result["a"] == 1
    assert isinstance(result["b"], str)
    assert json.loads(result["b"]) == [1, 2, 3]


def test_flatten_dict_empty_list():
    """Test _flatten_dict handles empty list."""
    d = {
        "a": 1,
        "b": []
    }
    result = _flatten_dict(d)
    assert result["b"] is None


def test_flatten_dict_nested_list():
    """Test _flatten_dict handles nested list."""
    d = {
        "a": [{"x": 1}, {"y": 2}]
    }
    result = _flatten_dict(d)
    assert isinstance(result["a"], str)
    assert json.loads(result["a"]) == [{"x": 1}, {"y": 2}]


def test_kafka_flatten_records_empty():
    """Test kafka_flatten_records with empty list."""
    result = kafka_flatten_records([])
    assert result == {
        "columns": [],
        "rows": [],
        "record_count": 0
    }


def test_kafka_flatten_records_simple():
    """Test kafka_flatten_records with simple records."""
    records = [
        {
            "partition": 0,
            "offset": 100,
            "timestamp": 1234567890,
            "key": "key1",
            "value": json.dumps({"id": 1, "name": "test"}),
            "headers": {}
        }
    ]
    result = kafka_flatten_records(records)
    assert result["record_count"] == 1
    assert "partition" in result["columns"]
    assert "offset" in result["columns"]
    assert "id" in result["columns"]
    assert "name" in result["columns"]
    assert len(result["rows"]) == 1
    assert result["rows"][0][result["columns"].index("id")] == 1


def test_kafka_flatten_records_nested_json():
    """Test kafka_flatten_records with nested JSON value."""
    records = [
        {
            "partition": 0,
            "offset": 100,
            "timestamp": 1234567890,
            "key": "key1",
            "value": json.dumps({
                "user": {
                    "id": 1,
                    "profile": {
                        "name": "test"
                    }
                }
            }),
            "headers": {}
        }
    ]
    result = kafka_flatten_records(records)
    assert "user_id" in result["columns"]
    assert "user_profile_name" in result["columns"]


def test_kafka_flatten_records_array_value():
    """Test kafka_flatten_records with array value."""
    records = [
        {
            "partition": 0,
            "offset": 100,
            "timestamp": 1234567890,
            "key": "key1",
            "value": json.dumps([1, 2, 3]),
            "headers": {}
        }
    ]
    result = kafka_flatten_records(records)
    assert "value" in result["columns"]
    value_idx = result["columns"].index("value")
    assert json.loads(result["rows"][0][value_idx]) == [1, 2, 3]


def test_kafka_flatten_records_non_json_value():
    """Test kafka_flatten_records with non-JSON value."""
    records = [
        {
            "partition": 0,
            "offset": 100,
            "timestamp": 1234567890,
            "key": "key1",
            "value": "plain text",
            "headers": {}
        }
    ]
    result = kafka_flatten_records(records)
    assert "value" in result["columns"]
    value_idx = result["columns"].index("value")
    assert result["rows"][0][value_idx] == "plain text"


def test_kafka_flatten_records_none_value():
    """Test kafka_flatten_records with None value."""
    records = [
        {
            "partition": 0,
            "offset": 100,
            "timestamp": 1234567890,
            "key": "key1",
            "value": None,
            "headers": {}
        }
    ]
    result = kafka_flatten_records(records)
    assert "value" in result["columns"]
    value_idx = result["columns"].index("value")
    assert result["rows"][0][value_idx] is None


def test_kafka_flatten_records_with_headers():
    """Test kafka_flatten_records with headers."""
    records = [
        {
            "partition": 0,
            "offset": 100,
            "timestamp": 1234567890,
            "key": "key1",
            "value": json.dumps({"id": 1}),
            "headers": {
                "header1": "value1",
                "header2": "value2"
            }
        }
    ]
    result = kafka_flatten_records(records)
    assert "header_header1" in result["columns"]
    assert "header_header2" in result["columns"]


def test_kafka_flatten_records_headers_with_control_chars():
    """Test kafka_flatten_records base64 encodes headers with control chars."""
    records = [
        {
            "partition": 0,
            "offset": 100,
            "timestamp": 1234567890,
            "key": "key1",
            "value": json.dumps({"id": 1}),
            "headers": {
                "normal": "value1",
                "binary": "\x00\x01\x02\x03",
                "newline": "line1\nline2"
            }
        }
    ]
    result = kafka_flatten_records(records)
    # Binary headers should be base64 encoded
    assert "header_binary_base64" in result["columns"]
    assert "header_newline_base64" in result["columns"]


def test_kafka_flatten_records_multiple_records():
    """Test kafka_flatten_records with multiple records."""
    records = [
        {
            "partition": 0,
            "offset": 100,
            "timestamp": 1234567890,
            "key": "key1",
            "value": json.dumps({"id": 1, "field1": "a"}),
            "headers": {}
        },
        {
            "partition": 0,
            "offset": 101,
            "timestamp": 1234567891,
            "key": "key2",
            "value": json.dumps({"id": 2, "field2": "b"}),
            "headers": {}
        }
    ]
    result = kafka_flatten_records(records)
    assert result["record_count"] == 2
    assert len(result["rows"]) == 2
    # Should have all columns from both records
    assert "field1" in result["columns"]
    assert "field2" in result["columns"]


def test_kafka_flatten_records_column_order():
    """Test kafka_flatten_records maintains metadata columns first."""
    records = [
        {
            "partition": 0,
            "offset": 100,
            "timestamp": 1234567890,
            "key": "key1",
            "value": json.dumps({"id": 1}),
            "headers": {}
        }
    ]
    result = kafka_flatten_records(records)
    # Metadata columns should come first
    metadata_cols = ["partition", "offset", "timestamp", "key"]
    for i, col in enumerate(metadata_cols):
        if col in result["columns"]:
            assert result["columns"].index(col) == i


def test_kafka_flatten_dataset_empty(clean_registry):
    """Test kafka_flatten_dataset with empty dataset."""
    # Create empty dataset
    dataset_id = "test_empty_kafka"
    dataset_put_rows(
        columns=["partition", "offset", "timestamp", "key", "value", "headers"],
        rows=[],
        dataset_id=dataset_id
    )
    
    result = kafka_flatten_dataset(dataset_id)
    
    assert "dataset_id" in result
    assert result["columns"] == []
    assert result["rows"] == 0
    assert result["record_count"] == 0


def test_kafka_flatten_dataset_simple(clean_registry):
    """Test kafka_flatten_dataset with simple records."""
    # Create dataset with Kafka records
    dataset_id = "test_kafka_simple"
    dataset_put_rows(
        columns=["partition", "offset", "timestamp", "key", "value", "headers"],
        rows=[[0, 100, 1234567890, "key1", json.dumps({"id": 1, "name": "test"}), "{}"]],
        dataset_id=dataset_id
    )
    
    result = kafka_flatten_dataset(dataset_id)
    
    assert "dataset_id" in result
    assert "id" in result["columns"]
    assert "name" in result["columns"]
    assert result["rows"] == 1
    assert result["record_count"] == 1


def test_kafka_flatten_dataset_headers_dict(clean_registry):
    """Test kafka_flatten_dataset with headers as dict."""
    # Headers stored as dict in DataFrame (from Parquet)
    dataset_id = "test_kafka_headers_dict"
    dataset_put_rows(
        columns=["partition", "offset", "timestamp", "key", "value", "headers"],
        rows=[[0, 100, 1234567890, "key1", json.dumps({"id": 1}), json.dumps({"header1": "value1"})]],
        dataset_id=dataset_id
    )
    
    result = kafka_flatten_dataset(dataset_id)
    
    assert "dataset_id" in result
    assert result["rows"] == 1


def test_kafka_flatten_dataset_headers_string(clean_registry):
    """Test kafka_flatten_dataset with headers as JSON string."""
    dataset_id = "test_kafka_headers_string"
    dataset_put_rows(
        columns=["partition", "offset", "timestamp", "key", "value", "headers"],
        rows=[[0, 100, 1234567890, "key1", json.dumps({"id": 1}), json.dumps({"header1": "value1"})]],
        dataset_id=dataset_id
    )
    
    result = kafka_flatten_dataset(dataset_id)
    
    assert "dataset_id" in result
    assert result["rows"] == 1


def test_kafka_flatten_dataset_headers_none(clean_registry):
    """Test kafka_flatten_dataset with None headers."""
    dataset_id = "test_kafka_headers_none"
    dataset_put_rows(
        columns=["partition", "offset", "timestamp", "key", "value", "headers"],
        rows=[[0, 100, 1234567890, "key1", json.dumps({"id": 1}), None]],
        dataset_id=dataset_id
    )
    
    result = kafka_flatten_dataset(dataset_id)
    
    assert "dataset_id" in result
    assert result["rows"] == 1


def test_kafka_flatten_dataset_invalid_headers_json(clean_registry):
    """Test kafka_flatten_dataset with invalid JSON headers."""
    dataset_id = "test_kafka_invalid_headers"
    dataset_put_rows(
        columns=["partition", "offset", "timestamp", "key", "value", "headers"],
        rows=[[0, 100, 1234567890, "key1", json.dumps({"id": 1}), "not valid json"]],
        dataset_id=dataset_id
    )
    
    result = kafka_flatten_dataset(dataset_id)
    
    assert "dataset_id" in result
    assert result["rows"] == 1


def test_kafka_flatten_dataset_nan_values(clean_registry):
    """Test kafka_flatten_dataset handles NaN values."""
    dataset_id = "test_kafka_nan_values"
    dataset_put_rows(
        columns=["partition", "offset", "timestamp", "key", "value", "headers"],
        rows=[[0, 100, 1234567890, None, None, "{}"]],
        dataset_id=dataset_id
    )
    
    result = kafka_flatten_dataset(dataset_id)
    
    assert "dataset_id" in result
    assert result["rows"] == 1


def test_kafka_flatten_dataset_custom_output_id(clean_registry):
    """Test kafka_flatten_dataset with custom output dataset ID."""
    dataset_id = "test_kafka_custom_output"
    dataset_put_rows(
        columns=["partition", "offset", "timestamp", "key", "value", "headers"],
        rows=[[0, 100, 1234567890, "key1", json.dumps({"id": 1}), "{}"]],
        dataset_id=dataset_id
    )
    
    result = kafka_flatten_dataset(dataset_id, out_dataset_id="custom_output")
    
    assert result["dataset_id"] == "custom_output"
    # Verify the output dataset exists
    df = load_dataset("custom_output")
    assert len(df) == 1

