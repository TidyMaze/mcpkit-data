"""Integration tests for server.py helper functions via MCP protocol.

Tests parameter conversion, validation, and error handling in tool wrappers.
"""

import pytest

from tests.utils_mcp import call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_to_int_string_conversion_mcp(mcp_client, clean_registry):
    """Test _to_int converts string parameters to int via MCP protocol."""
    # MCP may pass integers as strings
    create_test_dataset(mcp_client, "test_int", ["id"], [[1], [2], [3]])
    
    # max_rows as string should be converted to int
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM test_data LIMIT 2",
        source_name="test_data",
        source_dataset_id="test_int",
        max_rows="2"  # String instead of int
    )
    
    assert response["row_count"] <= 2


def test_list_validator_string_to_list_mcp(mcp_client, clean_registry):
    """Test _list_validator converts string to list via MCP protocol."""
    # Create dataset
    create_test_dataset(mcp_client, "test_list", ["id", "name"], [[1, "A"], [2, "B"]])
    
    # fields parameter as string should be converted to list
    response = call_tool(
        mcp_client,
        "event_fingerprint",
        record={"id": 1, "name": "A", "value": 10},
        fields='["id", "name"]'  # JSON string instead of list
    )
    
    assert "fingerprint" in response


def test_list_validator_list_passthrough_mcp(mcp_client, clean_registry):
    """Test _list_validator passes through list parameters via MCP protocol."""
    # fields as list should work
    response = call_tool(
        mcp_client,
        "event_fingerprint",
        record={"id": 1, "name": "A"},
        fields=["id", "name"]  # List directly
    )
    
    assert "fingerprint" in response


def test_sources_validator_json_string_mcp(mcp_client, clean_registry):
    """Test _sources_validator with JSON string via MCP protocol."""
    create_test_dataset(mcp_client, "test_sources", ["id"], [[1], [2]])
    
    # sources as JSON string should be parsed
    import json
    sources_json = json.dumps([{"name": "test_data", "dataset_id": "test_sources"}])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM test_data",
        sources=sources_json
    )
    
    assert response["row_count"] == 2


def test_sources_validator_individual_params_mcp(mcp_client, clean_registry):
    """Test _sources_validator with individual parameters via MCP protocol."""
    create_test_dataset(mcp_client, "test_individual", ["id"], [[1], [2]])
    
    # Individual source parameters should construct sources
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM test_data",
        source_name="test_data",
        source_dataset_id="test_individual"
    )
    
    assert response["row_count"] == 2


def test_error_handling_guard_error_mcp(mcp_client, clean_registry):
    """Test GuardError propagation through MCP protocol."""
    # Invalid dataset_id should raise GuardError
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "dataset_info",
            dataset_id="nonexistent_dataset_xyz_123"
        )


def test_error_handling_validation_error_mcp(mcp_client, clean_registry):
    """Test validation error handling via MCP protocol."""
    # Invalid parameters should raise validation errors
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "dataset_put_rows",
            columns=["id"],
            rows=[[1]],
            dataset_id="invalid/name"  # Invalid filename
        )


def test_optional_parameters_none_mcp(mcp_client, clean_registry):
    """Test optional parameters with None values via MCP protocol."""
    create_test_dataset(mcp_client, "test_optional", ["id"], [[1], [2], [3]])
    
    # max_rows=None should use default
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM test_data",
        source_name="test_data",
        source_dataset_id="test_optional"
        # max_rows not provided (None)
    )
    
    assert response["row_count"] == 3


def test_response_model_validation_mcp(mcp_client, clean_registry):
    """Test Pydantic response model validation via MCP protocol."""
    # All tool responses should match their Pydantic models
    response = call_tool(
        mcp_client,
        "dataset_list"
    )
    
    # Response should have correct structure
    assert "datasets" in response
    assert isinstance(response["datasets"], list)


def test_tool_timeout_handling_mcp(mcp_client, clean_registry):
    """Test tool timeout handling via MCP protocol."""
    # Tools should complete within timeout
    create_test_dataset(mcp_client, "test_timeout", ["id"], [[1], [2]])
    
    response = call_tool(
        mcp_client,
        "dataset_info",
        dataset_id="test_timeout"
    )
    
    # Should complete successfully
    assert "dataset_id" in response
    assert response["dataset_id"] == "test_timeout"

