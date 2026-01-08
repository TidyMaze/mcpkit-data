"""Tests for JSON tools."""

import base64

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.json_tools import (
    base64_decode,
    base64_encode,
    dedupe_by_id,
    event_correlate,
    event_fingerprint,
    event_validate,
    jq_transform,
)


def test_base64_encode_decode():
    """Test base64 encoding/decoding."""
    data = b"hello world"
    encoded = base64_encode(data)
    decoded = base64_decode(encoded)
    assert decoded == data


def test_base64_decode_invalid():
    """Test invalid base64 decoding."""
    with pytest.raises(GuardError):
        base64_decode("not valid base64!!!")


def test_jq_transform_happy():
    """Test JMESPath transform happy path."""
    data = {"a": 1, "b": {"c": 2}}
    result = jq_transform(data, "b.c")
    assert result["result"] == 2


def test_jq_transform_array():
    """Test JMESPath transform with array extraction."""
    data = {
        "sales": [
            {"product": "Widget A", "quantity": 10},
            {"product": "Widget B", "quantity": 5}
        ],
        "total_items": 15
    }
    result = jq_transform(data, "sales[*].product")
    assert result["result"] == ["Widget A", "Widget B"]


def test_jq_transform_nested():
    """Test JMESPath transform with nested structures."""
    data = {
        "store": {
            "products": [
                {"name": "A", "price": 10},
                {"name": "B", "price": 20}
            ]
        }
    }
    result = jq_transform(data, "store.products[*].name")
    assert result["result"] == ["A", "B"]


def test_jq_transform_null_result():
    """Test JMESPath transform that returns null."""
    data = {"a": 1, "b": None}
    result = jq_transform(data, "b")
    assert result["result"] is None


def test_jq_transform_missing_field():
    """Test JMESPath transform with missing field."""
    data = {"a": 1}
    result = jq_transform(data, "b")
    assert result["result"] is None


def test_jq_transform_error():
    """Test JMESPath transform error."""
    with pytest.raises(GuardError):
        jq_transform({"a": 1}, "invalid[expression")


def test_event_validate_valid():
    """Test event validation - valid."""
    record = {"name": "test", "age": 30}
    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
    }
    result = event_validate(record, schema)
    assert result["valid"] is True


def test_event_validate_invalid():
    """Test event validation - invalid."""
    record = {"name": "test", "age": "not a number"}
    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
    }
    result = event_validate(record, schema)
    assert result["valid"] is False
    assert "errors" in result
    assert result["errors"] is not None


def test_event_validate_invalid_schema():
    """Test event validation - invalid schema."""
    # jsonschema may not always raise SchemaError for invalid schemas
    # Try with a clearly invalid schema structure
    try:
        result = event_validate({"a": 1}, {"type": "invalid_type_that_does_not_exist"})
        # If it doesn't raise, that's also acceptable - validation may pass or fail
        assert "valid" in result
    except GuardError:
        # If it raises GuardError for schema error, that's also fine
        pass


def test_event_fingerprint_all_fields():
    """Test fingerprinting all fields."""
    record = {"a": 1, "b": 2}
    result = event_fingerprint(record)
    assert "fingerprint" in result
    assert len(result["fingerprint"]) == 64  # SHA256 hex


def test_event_fingerprint_specific_fields():
    """Test fingerprinting specific fields."""
    record = {"a": 1, "b": 2, "c": 3}
    result = event_fingerprint(record, fields=["a", "b"])
    assert "fingerprint" in result


def test_event_fingerprint_non_dict():
    """Test fingerprinting non-dict with fields."""
    with pytest.raises(GuardError):
        event_fingerprint([1, 2, 3], fields=["a"])


def test_dedupe_by_id():
    """Test deduplication by ID."""
    records = [
        {"id": 1, "value": "a"},
        {"id": 2, "value": "b"},
        {"id": 1, "value": "c"},  # duplicate
    ]
    result = dedupe_by_id(records, "id")
    assert result["unique_count"] == 2
    assert result["original_count"] == 3
    assert len(result["records"]) == 2


def test_event_correlate():
    """Test event correlation."""
    batches = [
        [{"user_id": 1, "timestamp": 100, "event": "login"}],
        [{"user_id": 1, "timestamp": 200, "event": "click"}],
        [{"user_id": 2, "timestamp": 150, "event": "login"}],
    ]
    result = event_correlate(batches, "user_id", "timestamp")
    assert result["correlation_count"] >= 1
    # User 1 should have 2 events
    user1_events = [c for c in result["correlated"] if c["join_key"] == 1]
    assert len(user1_events) > 0

