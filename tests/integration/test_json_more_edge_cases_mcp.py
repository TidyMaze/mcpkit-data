"""More edge case tests for JSON/QA tools via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, json_stringify

pytestmark = pytest.mark.integration


def test_jq_transform_array_projection_mcp(mcp_client):
    """Test jq_transform with array projection via MCP protocol."""
    data = {
        "products": [
            {"id": 1, "name": "A", "price": 10},
            {"id": 2, "name": "B", "price": 20}
        ]
    }
    
    response = call_tool(
        mcp_client,
        "jq_transform",
        data=data,
        expression="products[*].id"
    )
    
    assert_response_structure(response, ["result"])
    assert isinstance(response["result"], list)
    assert len(response["result"]) == 2


def test_jq_transform_filter_array_mcp(mcp_client):
    """Test jq_transform with array filtering via MCP protocol."""
    data = {
        "items": [
            {"id": 1, "price": 100},
            {"id": 2, "price": 50},
            {"id": 3, "price": 150}
        ]
    }
    
    response = call_tool(
        mcp_client,
        "jq_transform",
        data=data,
        expression="items[?price > `100`]"
    )
    
    assert_response_structure(response, ["result"])
    assert isinstance(response["result"], list)
    assert len(response["result"]) == 1  # Only id=3


def test_jq_transform_object_projection_mcp(mcp_client):
    """Test jq_transform with object projection via MCP protocol."""
    data = {
        "id": 1,
        "name": "test",
        "value": 100,
        "extra": "ignored"
    }
    
    response = call_tool(
        mcp_client,
        "jq_transform",
        data=data,
        expression="{id: id, name: name}"
    )
    
    assert_response_structure(response, ["result"])
    assert isinstance(response["result"], dict)
    assert "id" in response["result"]
    assert "name" in response["result"]
    assert "extra" not in response["result"]


def test_event_fingerprint_specific_fields_mcp(mcp_client):
    """Test event_fingerprint with specific fields via MCP protocol."""
    record = {
        "id": 1,
        "name": "test",
        "timestamp": "2024-01-01",
        "extra": "ignored"
    }
    
    response = call_tool(
        mcp_client,
        "event_fingerprint",
        record=record,
        fields=["id", "name"]
    )
    
    assert_response_structure(response, ["fingerprint"])
    assert isinstance(response["fingerprint"], str)


def test_event_fingerprint_all_fields_mcp(mcp_client):
    """Test event_fingerprint with all fields (default) via MCP protocol."""
    record = {
        "id": 1,
        "name": "test",
        "value": 100
    }
    
    response = call_tool(
        mcp_client,
        "event_fingerprint",
        record=record
    )
    
    assert_response_structure(response, ["fingerprint"])
    assert isinstance(response["fingerprint"], str)


def test_dedupe_by_id_nested_id_mcp(mcp_client):
    """Test dedupe_by_id with nested ID extraction via MCP protocol."""
    records = [
        {"data": {"user": {"id": 1}, "action": "click"}},
        {"data": {"user": {"id": 1}, "action": "view"}},  # duplicate id
        {"data": {"user": {"id": 2}, "action": "click"}}
    ]
    
    response = call_tool(
        mcp_client,
        "dedupe_by_id",
        records=json_stringify(records),
        id_jmes="data.user.id"
    )
    
    assert_response_structure(response, ["records", "unique_count", "original_count"])
    assert response["unique_count"] == 2  # Two unique IDs


def test_dedupe_by_id_no_duplicates_mcp(mcp_client):
    """Test dedupe_by_id with no duplicates via MCP protocol."""
    records = [
        {"id": 1, "value": "A"},
        {"id": 2, "value": "B"},
        {"id": 3, "value": "C"}
    ]
    
    response = call_tool(
        mcp_client,
        "dedupe_by_id",
        records=json_stringify(records),
        id_jmes="id"
    )
    
    assert_response_structure(response, ["records", "unique_count", "original_count"])
    assert response["unique_count"] == 3
    assert response["original_count"] == 3


def test_event_correlate_multiple_batches_mcp(mcp_client):
    """Test event_correlate with multiple batches via MCP protocol."""
    batch1 = [
        {"user_id": 1, "timestamp": "2024-01-01T10:00:00", "action": "login"},
        {"user_id": 2, "timestamp": "2024-01-01T10:05:00", "action": "login"}
    ]
    
    batch2 = [
        {"user_id": 1, "timestamp": "2024-01-01T10:10:00", "action": "purchase"},
        {"user_id": 3, "timestamp": "2024-01-01T10:15:00", "action": "purchase"}
    ]
    
    batches = [batch1, batch2]
    
    response = call_tool(
        mcp_client,
        "event_correlate",
        batches=json_stringify(batches),
        join_key_jmes="user_id"
    )
    
    assert_response_structure(response, ["correlated_count", "correlated_events"])
    assert response["correlated_count"] >= 1  # user_id=1 appears in both batches


def test_event_validate_complex_schema_mcp(mcp_client):
    """Test event_validate with complex nested schema via MCP protocol."""
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "user": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"}
                },
                "required": ["name"]
            }
        },
        "required": ["id", "user"]
    }
    
    record = {
        "id": 1,
        "user": {
            "name": "Alice",
            "age": 30
        }
    }
    
    response = call_tool(
        mcp_client,
        "event_validate",
        record=record,
        schema=schema
    )
    
    assert_response_structure(response, ["valid", "errors"])
    assert response["valid"] is True


def test_event_validate_nested_validation_error_mcp(mcp_client):
    """Test event_validate with nested validation error via MCP protocol."""
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "user": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"}
                },
                "required": ["name"]
            }
        },
        "required": ["id", "user"]
    }
    
    record = {
        "id": 1,
        "user": {}  # Missing required "name"
    }
    
    response = call_tool(
        mcp_client,
        "event_validate",
        record=record,
        schema=schema
    )
    
    assert_response_structure(response, ["valid", "errors"])
    assert response["valid"] is False
    assert len(response["errors"]) > 0

