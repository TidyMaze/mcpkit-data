"""Additional tests for json_tools module."""

import json

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.json_tools import dedupe_by_id, event_correlate, jq_transform


def test_jq_transform_json_string():
    """Test jq_transform with JSON string input."""
    data = '{"a": 1, "b": {"c": 2}}'
    result = jq_transform(data, "b.c")
    assert result["result"] == 2


def test_jq_transform_plain_string():
    """Test jq_transform with plain string (not JSON)."""
    data = "plain string"
    # JMESPath doesn't work on plain strings with "."
    # Use a valid expression that works on strings
    with pytest.raises(GuardError):
        jq_transform(data, ".")


def test_jq_transform_json_array_string():
    """Test jq_transform with JSON array string."""
    data = '[{"id": 1}, {"id": 2}]'
    result = jq_transform(data, "[*].id")
    assert result["result"] == [1, 2]


def test_jq_transform_invalid_json_string():
    """Test jq_transform with invalid JSON string (treated as plain string)."""
    data = "not valid json {"
    # Invalid JSON string is treated as plain string
    # JMESPath doesn't work on plain strings
    with pytest.raises(GuardError):
        jq_transform(data, ".")


def test_dedupe_by_id_nested():
    """Test dedupe_by_id with nested ID path."""
    records = [
        {"user": {"id": 1}, "value": "a"},
        {"user": {"id": 2}, "value": "b"},
        {"user": {"id": 1}, "value": "c"},  # duplicate
    ]
    result = dedupe_by_id(records, "user.id")
    assert result["unique_count"] == 2
    assert result["original_count"] == 3
    assert len(result["records"]) == 2


def test_dedupe_by_id_dict_id():
    """Test dedupe_by_id with dict as ID (converted to JSON)."""
    records = [
        {"id": {"key": "a"}, "value": "x"},
        {"id": {"key": "b"}, "value": "y"},
        {"id": {"key": "a"}, "value": "z"},  # duplicate
    ]
    result = dedupe_by_id(records, "id")
    assert result["unique_count"] == 2
    assert result["original_count"] == 3


def test_dedupe_by_id_list_id():
    """Test dedupe_by_id with list as ID (converted to JSON)."""
    records = [
        {"id": [1, 2], "value": "a"},
        {"id": [3, 4], "value": "b"},
        {"id": [1, 2], "value": "c"},  # duplicate
    ]
    result = dedupe_by_id(records, "id")
    assert result["unique_count"] == 2
    assert result["original_count"] == 3


def test_dedupe_by_id_none_id():
    """Test dedupe_by_id skips records with None ID."""
    records = [
        {"id": 1, "value": "a"},
        {"id": None, "value": "b"},  # skipped
        {"id": 2, "value": "c"},
    ]
    result = dedupe_by_id(records, "id")
    assert result["unique_count"] == 2
    assert result["original_count"] == 3
    assert result["skipped_count"] == 1


def test_dedupe_by_id_invalid_jmespath():
    """Test dedupe_by_id skips records where JMESPath fails."""
    records = [
        {"id": 1, "value": "a"},
        {"invalid": "structure"},  # JMESPath "id" fails
        {"id": 2, "value": "c"},
    ]
    result = dedupe_by_id(records, "id")
    assert result["unique_count"] == 2
    assert result["original_count"] == 3
    assert result["skipped_count"] == 1


def test_dedupe_by_id_empty():
    """Test dedupe_by_id with empty list."""
    result = dedupe_by_id([], "id")
    assert result["unique_count"] == 0
    assert result["original_count"] == 0
    assert result["records"] == []
    # skipped_count may be None or 0 when empty
    assert result.get("skipped_count") is None or result.get("skipped_count") == 0


def test_dedupe_by_id_all_skipped():
    """Test dedupe_by_id when all records are skipped."""
    records = [
        {"id": None, "value": "a"},
        {"id": None, "value": "b"},
    ]
    result = dedupe_by_id(records, "id")
    assert result["unique_count"] == 0
    assert result["original_count"] == 2
    assert result["skipped_count"] == 2


def test_event_correlate_empty_batches():
    """Test event_correlate with empty batches."""
    result = event_correlate([], "user_id", "timestamp")
    assert result["correlation_count"] == 0
    assert result["correlated"] == []


def test_event_correlate_single_batch():
    """Test event_correlate with single batch (no correlation)."""
    batches = [
        [{"user_id": 1, "timestamp": 100}],
        [{"user_id": 2, "timestamp": 200}],
    ]
    result = event_correlate(batches, "user_id", "timestamp")
    assert result["correlation_count"] == 0


def test_event_correlate_missing_timestamp():
    """Test event_correlate skips records with missing timestamp."""
    batches = [
        [{"user_id": 1, "timestamp": 100}],
        [{"user_id": 1}],  # missing timestamp
    ]
    result = event_correlate(batches, "user_id", "timestamp")
    # Should only correlate if both have timestamps
    assert result["correlation_count"] == 0


def test_event_correlate_missing_join_key():
    """Test event_correlate skips records with missing join key."""
    batches = [
        [{"user_id": 1, "timestamp": 100}],
        [{"timestamp": 200}],  # missing user_id
    ]
    result = event_correlate(batches, "user_id", "timestamp")
    assert result["correlation_count"] == 0


def test_event_correlate_custom_timestamp_field():
    """Test event_correlate with custom timestamp field."""
    batches = [
        [{"user_id": 1, "event_time": 100}],
        [{"user_id": 1, "event_time": 200}],
    ]
    result = event_correlate(batches, "user_id", "event_time")
    assert result["correlation_count"] == 1
    assert len(result["correlated"][0]["events"]) == 2


def test_event_correlate_sorted_by_timestamp():
    """Test event_correlate sorts events by timestamp."""
    batches = [
        [{"user_id": 1, "timestamp": 300}],
        [{"user_id": 1, "timestamp": 100}],
        [{"user_id": 1, "timestamp": 200}],
    ]
    result = event_correlate(batches, "user_id", "timestamp")
    assert result["correlation_count"] == 1
    events = result["correlated"][0]["events"]
    assert events[0]["timestamp"] == 100
    assert events[1]["timestamp"] == 200
    assert events[2]["timestamp"] == 300


def test_event_correlate_multiple_keys():
    """Test event_correlate with multiple join keys."""
    batches = [
        [{"user_id": 1, "timestamp": 100}],
        [{"user_id": 1, "timestamp": 200}],
        [{"user_id": 2, "timestamp": 150}],
        [{"user_id": 2, "timestamp": 250}],
    ]
    result = event_correlate(batches, "user_id", "timestamp")
    assert result["correlation_count"] == 2
    assert all(c["event_count"] == 2 for c in result["correlated"])


def test_event_correlate_jmespath_error():
    """Test event_correlate handles JMESPath errors gracefully."""
    batches = [
        [{"user_id": 1, "timestamp": 100}],
        [{"invalid": "structure"}],  # JMESPath "user_id" fails
    ]
    result = event_correlate(batches, "user_id", "timestamp")
    # Should skip invalid records
    assert result["correlation_count"] == 0


def test_event_correlate_single_event_per_key():
    """Test event_correlate excludes keys with only one event."""
    batches = [
        [{"user_id": 1, "timestamp": 100}],  # Only one event for user 1
        [{"user_id": 2, "timestamp": 200}],
        [{"user_id": 2, "timestamp": 300}],  # Two events for user 2
    ]
    result = event_correlate(batches, "user_id", "timestamp")
    assert result["correlation_count"] == 1
    assert result["correlated"][0]["join_key"] == 2
    assert result["correlated"][0]["event_count"] == 2

