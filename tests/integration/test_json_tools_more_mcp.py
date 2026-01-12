"""More integration tests for JSON tools via MCP protocol.

Covers missing paths in json_tools.py.
"""

import json
import pytest

from tests.utils_mcp import assert_response_structure, call_tool, json_stringify

pytestmark = pytest.mark.integration


def test_jq_transform_plain_string_mcp(mcp_client):
    """Test jq_transform with plain string (not JSON) via MCP protocol."""
    # Plain string that's not JSON should be used as-is
    response = call_tool(
        mcp_client,
        "jq_transform",
        data="plain string value",
        expression="."
    )
    
    assert_response_structure(response, ["result"])
    assert response["result"] == "plain string value"


def test_jq_transform_array_projection_mcp(mcp_client):
    """Test jq_transform with array projection via MCP protocol."""
    data = {"items": [{"price": 50}, {"price": 150}, {"price": 75}]}
    
    response = call_tool(
        mcp_client,
        "jq_transform",
        data=data,
        expression="items[*].price"
    )
    
    assert_response_structure(response, ["result"])
    assert response["result"] == [50, 150, 75]


def test_jq_transform_filter_mcp(mcp_client):
    """Test jq_transform with filter expression via MCP protocol."""
    data = {"items": [{"price": 50}, {"price": 150}, {"price": 75}]}
    
    response = call_tool(
        mcp_client,
        "jq_transform",
        data=data,
        expression="items[?price > `100`]"
    )
    
    assert_response_structure(response, ["result"])
    assert len(response["result"]) == 1
    assert response["result"][0]["price"] == 150


def test_jq_transform_invalid_expression_mcp(mcp_client):
    """Test jq_transform with invalid JMESPath expression via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "jq_transform",
            data={"id": 1},
            expression="invalid[jmespath[syntax"
        )


def test_event_validate_schema_error_mcp(mcp_client):
    """Test event_validate with invalid schema via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "event_validate",
            record={"id": 1},
            schema={"type": "invalid_type_xyz"}  # Invalid schema
        )


def test_event_fingerprint_non_dict_record_mcp(mcp_client):
    """Test event_fingerprint with non-dict record via MCP protocol."""
    # Non-dict should work (no fields specified)
    response = call_tool(
        mcp_client,
        "event_fingerprint",
        record=[1, 2, 3]  # List, not dict
    )
    
    assert_response_structure(response, ["fingerprint"])
    assert len(response["fingerprint"]) == 64


def test_event_fingerprint_fields_with_non_dict_mcp(mcp_client):
    """Test event_fingerprint with fields on non-dict record via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "event_fingerprint",
            record=[1, 2, 3],  # List, not dict
            fields=["id"]  # Fields only work with dict
        )


def test_dedupe_by_id_empty_records_mcp(mcp_client):
    """Test dedupe_by_id with empty records list via MCP protocol."""
    response = call_tool(
        mcp_client,
        "dedupe_by_id",
        records=json_stringify([]),
        id_jmes="id"
    )
    
    assert_response_structure(response, ["unique_count", "original_count", "records"])
    assert response["unique_count"] == 0
    assert response["original_count"] == 0
    assert response["records"] == []


def test_dedupe_by_id_nested_id_mcp(mcp_client):
    """Test dedupe_by_id with nested ID via MCP protocol."""
    records = [
        {"product": {"id": 1}, "value": "A"},
        {"product": {"id": 2}, "value": "B"},
        {"product": {"id": 1}, "value": "A"},  # Duplicate
    ]
    
    response = call_tool(
        mcp_client,
        "dedupe_by_id",
        records=json_stringify(records),
        id_jmes="product.id"
    )
    
    assert_response_structure(response, ["unique_count", "original_count", "records"])
    assert response["unique_count"] == 2


def test_dedupe_by_id_skipped_records_mcp(mcp_client):
    """Test dedupe_by_id with records where ID extraction fails via MCP protocol."""
    records = [
        {"id": 1, "value": "A"},
        {"value": "B"},  # Missing id
        {"id": 2, "value": "C"},
    ]
    
    response = call_tool(
        mcp_client,
        "dedupe_by_id",
        records=json_stringify(records),
        id_jmes="id"
    )
    
    assert_response_structure(response, ["unique_count", "original_count", "records"])
    assert response["original_count"] == 3
    assert response["unique_count"] == 2  # One skipped
    if "skipped_count" in response and response["skipped_count"] is not None:
        assert response["skipped_count"] == 1


def test_dedupe_by_id_dict_id_mcp(mcp_client):
    """Test dedupe_by_id with dict as ID (should be converted to JSON string) via MCP protocol."""
    records = [
        {"id": {"user": 1, "session": "a"}, "value": "A"},
        {"id": {"user": 2, "session": "b"}, "value": "B"},
        {"id": {"user": 1, "session": "a"}, "value": "A"},  # Duplicate
    ]
    
    response = call_tool(
        mcp_client,
        "dedupe_by_id",
        records=json_stringify(records),
        id_jmes="id"
    )
    
    assert_response_structure(response, ["unique_count", "original_count", "records"])
    assert response["unique_count"] == 2


def test_event_correlate_no_correlations_mcp(mcp_client):
    """Test event_correlate with no correlated events via MCP protocol."""
    batches = [
        [{"user_id": 1, "timestamp": 100}],
        [{"user_id": 2, "timestamp": 200}],
        [{"user_id": 3, "timestamp": 300}]
    ]
    
    response = call_tool(
        mcp_client,
        "event_correlate",
        batches=json_stringify(batches),
        join_key_jmes="user_id"
    )
    
    assert_response_structure(response, ["correlated", "correlation_count"])
    assert response["correlation_count"] == 0  # No user appears in multiple batches


def test_event_correlate_missing_timestamp_mcp(mcp_client):
    """Test event_correlate with records missing timestamp via MCP protocol."""
    batches = [
        [{"user_id": 1, "timestamp": 100}],
        [{"user_id": 1}],  # Missing timestamp
        [{"user_id": 1, "timestamp": 300}]
    ]
    
    response = call_tool(
        mcp_client,
        "event_correlate",
        batches=json_stringify(batches),
        join_key_jmes="user_id"
    )
    
    assert_response_structure(response, ["correlated", "correlation_count"])
    # Should correlate user_id=1 from batches 0 and 2 (batch 1 skipped due to missing timestamp)


def test_event_correlate_custom_timestamp_field_mcp(mcp_client):
    """Test event_correlate with custom timestamp field via MCP protocol."""
    batches = [
        [{"user_id": 1, "event_time": 100}],
        [{"user_id": 1, "event_time": 200}]
    ]
    
    response = call_tool(
        mcp_client,
        "event_correlate",
        batches=json_stringify(batches),
        join_key_jmes="user_id",
        timestamp_field="event_time"
    )
    
    assert_response_structure(response, ["correlated", "correlation_count"])
    assert response["correlation_count"] >= 1

