"""Integration tests for server.py error handling via MCP protocol.

Tests error paths in tool wrappers.
"""

import pytest

from tests.utils_mcp import call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_great_expectations_not_installed_mcp(mcp_client, clean_registry):
    """Test great_expectations_check error when GE not installed via MCP protocol."""
    create_test_dataset(mcp_client, "ge_test", ["id"], [[1], [2]])
    
    # This will fail if GE is not installed, but we can't easily test that
    # without mocking. Just verify the tool exists and can be called.
    # If GE is installed, it should work; if not, it should raise RuntimeError
    try:
        response = call_tool(
            mcp_client,
            "great_expectations_check",
            dataset_id="ge_test",
            expectation_suite={"expectations": []}
        )
        # If we get here, GE is installed and it worked
        assert "success" in response or "results" in response
    except Exception as e:
        # If GE is not installed, should raise RuntimeError
        assert "not installed" in str(e).lower() or "great expectations" in str(e).lower()


def test_dataset_operations_error_handling_mcp(mcp_client, clean_registry):
    """Test dataset operations error handling via MCP protocol."""
    # Test invalid dataset_id format
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "dataset_info",
            dataset_id="invalid/name"  # Contains slash
        )


def test_tool_parameter_validation_mcp(mcp_client, clean_registry):
    """Test tool parameter validation via MCP protocol."""
    # Test missing required parameters
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "dataset_info"
            # Missing dataset_id
        )


def test_pandas_operations_error_handling_mcp(mcp_client, clean_registry):
    """Test pandas operations error handling via MCP protocol."""
    # Test with non-existent dataset
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "pandas_describe",
            dataset_id="nonexistent_dataset_xyz"
        )


def test_polars_operations_error_handling_mcp(mcp_client, clean_registry):
    """Test polars operations error handling via MCP protocol."""
    # Test with non-existent dataset
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "polars_groupby",
            dataset_id="nonexistent_dataset_xyz",
            group_cols=["id"],
            aggs={"value": ["sum"]}
        )


def test_evidence_operations_error_handling_mcp(mcp_client, clean_registry):
    """Test evidence operations error handling via MCP protocol."""
    # Test with non-existent dataset
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "evidence_bundle_plus",
            dataset_id="nonexistent_dataset_xyz",
            base_filename="test"
        )


def test_json_tools_error_handling_mcp(mcp_client):
    """Test JSON tools error handling via MCP protocol."""
    # Test invalid JMESPath expression
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "jq_transform",
            data={"id": 1, "name": "test"},
            expression="invalid[jmespath[syntax"
        )


def test_http_ops_error_handling_mcp(mcp_client):
    """Test HTTP operations error handling via MCP protocol."""
    # Test invalid URL
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "http_request",
            url="not-a-valid-url"
        )


def test_chart_ops_error_handling_mcp(mcp_client, clean_registry):
    """Test chart operations error handling via MCP protocol."""
    # Test with non-existent dataset
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "dataset_to_chart",
            dataset_id="nonexistent_dataset_xyz"
        )


def test_reconcile_error_handling_mcp(mcp_client, clean_registry):
    """Test reconcile operations error handling via MCP protocol."""
    # Test with non-existent dataset
    create_test_dataset(mcp_client, "left_ds", ["id"], [[1]])
    
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "reconcile_counts",
            left_dataset_id="left_ds",
            right_dataset_id="nonexistent_dataset_xyz",
            key_cols=["id"]
        )


def test_duckdb_error_handling_mcp(mcp_client, clean_registry):
    """Test DuckDB operations error handling via MCP protocol."""
    # Test invalid sources JSON
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "duckdb_query_local",
            sql="SELECT * FROM test_data",
            sources="invalid json {"
        )


def test_sources_validator_object_object_error_mcp(mcp_client, clean_registry):
    """Test _sources_validator with [object Object] error via MCP protocol."""
    # This tests the FastMCP serialization error detection
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "duckdb_query_local",
            sql="SELECT * FROM test_data",
            sources="[object Object]"  # FastMCP serialization failure
        )


def test_sources_validator_empty_string_mcp(mcp_client, clean_registry):
    """Test _sources_validator with empty string via MCP protocol."""
    # Empty string should be treated as None
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT 1 as x",
        sources=""  # Empty string
    )
    
    assert response["row_count"] == 1


def test_sources_validator_null_string_mcp(mcp_client, clean_registry):
    """Test _sources_validator with 'null' string via MCP protocol."""
    # "null" string should be treated as None
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT 1 as x",
        sources="null"  # "null" string
    )
    
    assert response["row_count"] == 1

