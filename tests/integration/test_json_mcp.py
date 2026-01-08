"""Integration tests for JSON/QA tools via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, json_stringify

pytestmark = pytest.mark.integration


def test_jq_transform_mcp(mcp_client):
    """Test jq_transform via MCP protocol."""
    data = {"products": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]}
    
    response = call_tool(
        mcp_client,
        "jq_transform",
        data=data,
        expression="products[*].id"
    )
    
    assert_response_structure(response, ["result"])
    assert response["result"] == [1, 2]


def test_jq_transform_nested_mcp(mcp_client):
    """Test jq_transform with nested field via MCP protocol."""
    data = {"price": {"value": 100, "currency": "EUR"}}
    
    response = call_tool(
        mcp_client,
        "jq_transform",
        data=data,
        expression="price.value"
    )
    
    assert_response_structure(response, ["result"])
    assert response["result"] == 100


def test_jq_transform_json_string_mcp(mcp_client):
    """Test jq_transform with JSON string input via MCP protocol."""
    data = '{"id": "123", "title": "Product"}'
    
    response = call_tool(
        mcp_client,
        "jq_transform",
        data=data,
        expression="id"
    )
    
    assert_response_structure(response, ["result"])
    assert response["result"] == "123"


def test_event_validate_mcp(mcp_client):
    """Test event_validate via MCP protocol."""
    record = {"id": 1, "name": "Test", "age": 25}
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "age": {"type": "integer"}
        },
        "required": ["id", "name"]
    }
    
    response = call_tool(
        mcp_client,
        "event_validate",
        record=record,
        schema=schema
    )
    
    assert_response_structure(response, ["valid"])
    assert response["valid"] is True


def test_event_validate_invalid_mcp(mcp_client):
    """Test event_validate with invalid record via MCP protocol."""
    record = {"id": "not_a_number", "name": "Test"}
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"}
        },
        "required": ["id", "name"]
    }
    
    response = call_tool(
        mcp_client,
        "event_validate",
        record=record,
        schema=schema
    )
    
    assert_response_structure(response, ["valid"])
    assert response["valid"] is False
    assert "errors" in response


def test_event_fingerprint_mcp(mcp_client):
    """Test event_fingerprint via MCP protocol."""
    record = {"id": 1, "name": "Alice", "age": 25}
    
    response = call_tool(
        mcp_client,
        "event_fingerprint",
        record=record
    )
    
    assert_response_structure(response, ["fingerprint"])
    assert len(response["fingerprint"]) == 64  # SHA256 hex


def test_event_fingerprint_with_fields_mcp(mcp_client):
    """Test event_fingerprint with specific fields via MCP protocol."""
    record = {"id": 1, "name": "Alice", "age": 25}
    
    response = call_tool(
        mcp_client,
        "event_fingerprint",
        record=record,
        fields=["id", "name"]
    )
    
    assert_response_structure(response, ["fingerprint"])
    assert len(response["fingerprint"]) == 64


def test_dedupe_by_id_mcp(mcp_client):
    """Test dedupe_by_id via MCP protocol."""
    import json
    records = [
        {"id": 1, "value": "A"},
        {"id": 2, "value": "B"},
        {"id": 1, "value": "A"},  # Duplicate
        {"id": 3, "value": "C"}
    ]
    
    response = call_tool(
        mcp_client,
        "dedupe_by_id",
        records=json_stringify(records),
        id_jmes="id"
    )
    
    assert_response_structure(response, ["unique_count", "original_count", "records"])
    assert response["original_count"] == 4
    assert response["unique_count"] == 3
    assert len(response["records"]) == 3


def test_event_correlate_mcp(mcp_client):
    """Test event_correlate via MCP protocol."""
    import json
    batches = [
        [{"user_id": 1, "timestamp": 100, "action": "login"}],
        [{"user_id": 1, "timestamp": 200, "action": "click"}],
        [{"user_id": 2, "timestamp": 150, "action": "login"}]
    ]
    
    response = call_tool(
        mcp_client,
        "event_correlate",
        batches=json_stringify(batches),
        join_key_jmes="user_id",
        timestamp_field="timestamp"
    )
    
    assert_response_structure(response, ["correlated", "correlation_count"])
    assert response["correlation_count"] >= 1  # At least 1 correlation (user_id=1 has 2 events)

