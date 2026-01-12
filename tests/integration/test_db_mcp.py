"""Integration tests for database tools via MCP protocol.

Uses PostgreSQL Docker container - automatically started via docker_services fixture.
"""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


def test_db_query_ro_mcp(mcp_client, db_setup):
    """Test db_query_ro via MCP protocol."""
    response = call_tool(
        mcp_client,
        "db_query_ro",
        query="SELECT 1 as test_column"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert "test_column" in response["columns"]
    assert response["row_count"] == 1


def test_db_query_ro_with_params_mcp(mcp_client, db_setup):
    """Test db_query_ro with parameters via MCP protocol."""
    response = call_tool(
        mcp_client,
        "db_query_ro",
        query="SELECT ? as param_value",
        params=[42]
    )
    
    assert_response_structure(response, ["columns", "rows"])
    assert response["row_count"] == 1


def test_db_introspect_mcp(mcp_client, db_setup):
    """Test db_introspect via MCP protocol."""
    response = call_tool(mcp_client, "db_introspect", max_tables=10)
    
    assert_response_structure(response, ["tables", "table_count"])
    assert isinstance(response["tables"], list)


def test_db_query_ro_readonly_enforcement_mcp(mcp_client, db_setup):
    """Test that db_query_ro rejects non-SELECT queries."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "db_query_ro",
            query="DROP TABLE test_table"
        )


def test_db_query_ro_with_explain_mcp(mcp_client, db_setup):
    """Test db_query_ro with EXPLAIN SELECT via MCP protocol."""
    response = call_tool(
        mcp_client,
        "db_query_ro",
        query="EXPLAIN SELECT 1"
    )
    
    assert_response_structure(response, ["columns", "rows"])


def test_db_query_ro_with_with_mcp(mcp_client, db_setup):
    """Test db_query_ro with WITH clause via MCP protocol."""
    response = call_tool(
        mcp_client,
        "db_query_ro",
        query="WITH test_cte AS (SELECT 1 as x) SELECT * FROM test_cte"
    )
    
    assert_response_structure(response, ["columns", "rows"])


def test_db_introspect_with_filters_mcp(mcp_client, db_setup):
    """Test db_introspect with schema/table filters via MCP protocol."""
    response = call_tool(
        mcp_client,
        "db_introspect",
        schema_like="public",
        table_like="test%",
        max_tables=5
    )
    
    assert_response_structure(response, ["tables", "table_count"])


def test_db_query_ro_rejects_insert_mcp(mcp_client, db_setup):
    """Test that db_query_ro rejects INSERT queries."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "db_query_ro",
            query="INSERT INTO test VALUES (1)"
        )


def test_db_query_ro_rejects_update_mcp(mcp_client, db_setup):
    """Test that db_query_ro rejects UPDATE queries."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "db_query_ro",
            query="UPDATE test SET x = 1"
        )


def test_db_query_ro_rejects_delete_mcp(mcp_client, db_setup):
    """Test that db_query_ro rejects DELETE queries."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "db_query_ro",
            query="DELETE FROM test"
        )


def test_db_query_ro_rejects_create_mcp(mcp_client, db_setup):
    """Test that db_query_ro rejects CREATE queries."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "db_query_ro",
            query="CREATE TABLE test (id INT)"
        )


def test_db_query_ro_invalid_query_mcp(mcp_client, db_setup):
    """Test db_query_ro with invalid SQL via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError or database error
        call_tool(
            mcp_client,
            "db_query_ro",
            query="SELECT * FROM nonexistent_table_xyz"
        )


def test_db_query_ro_with_multiple_params_mcp(mcp_client, db_setup):
    """Test db_query_ro with multiple parameters via MCP protocol."""
    response = call_tool(
        mcp_client,
        "db_query_ro",
        query="SELECT ? as a, ? as b, ? as c",
        params=[1, "test", 3.14]
    )
    
    assert_response_structure(response, ["columns", "rows"])
    assert response["row_count"] == 1
    assert len(response["rows"][0]) == 3


def test_db_introspect_empty_result_mcp(mcp_client, db_setup):
    """Test db_introspect with filter that matches nothing via MCP protocol."""
    response = call_tool(
        mcp_client,
        "db_introspect",
        schema_like="nonexistent_schema_xyz",
        max_tables=10
    )
    
    assert_response_structure(response, ["tables", "table_count"])
    assert response["table_count"] == 0

