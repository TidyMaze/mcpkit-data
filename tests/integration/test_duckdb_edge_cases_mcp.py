"""Edge case tests for DuckDB operations via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_duckdb_query_local_with_aggregation_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with aggregation functions via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_agg",
        ["category", "value"],
        [
            ["A", 10],
            ["A", 20],
            ["B", 30],
            ["B", 40]
        ]
    )
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT category, SUM(value) as total FROM test_agg GROUP BY category",
        source_name="test_agg",
        source_dataset_id="test_agg"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 2  # Two categories


def test_duckdb_query_local_with_window_function_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with window functions via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_window",
        ["id", "value"],
        [[1, 10], [2, 20], [3, 30]]
    )
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT id, value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM test_window",
        source_name="test_window",
        source_dataset_id="test_window"
    )
    
    assert_response_structure(response, ["columns", "rows"])
    assert "rn" in response["columns"]


def test_duckdb_query_local_with_cte_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with Common Table Expression via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_cte",
        ["id", "value"],
        [[1, 10], [2, 20]]
    )
    
    sql = """
    WITH doubled AS (
        SELECT id, value * 2 as doubled_value
        FROM test_cte
    )
    SELECT * FROM doubled WHERE doubled_value > 15
    """
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql=sql,
        source_name="test_cte",
        source_dataset_id="test_cte"
    )
    
    assert_response_structure(response, ["columns", "rows"])
    assert response["row_count"] == 1  # Only id=2 has doubled_value > 15


def test_duckdb_query_local_with_join_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with JOIN via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_left",
        ["id", "name"],
        [[1, "A"], [2, "B"]]
    )
    
    create_test_dataset(
        mcp_client,
        "test_right",
        ["id", "value"],
        [[1, 10], [3, 30]]
    )
    
    sql = """
    SELECT l.id, l.name, r.value
    FROM test_left l
    LEFT JOIN test_right r ON l.id = r.id
    """
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql=sql,
        sources='[{"name": "test_left", "dataset_id": "test_left"}, {"name": "test_right", "dataset_id": "test_right"}]'
    )
    
    assert_response_structure(response, ["columns", "rows"])
    assert response["row_count"] == 2


def test_duckdb_query_local_with_subquery_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with subquery via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_subquery",
        ["id", "value"],
        [[1, 10], [2, 20], [3, 30]]
    )
    
    sql = """
    SELECT * FROM test_subquery
    WHERE value > (SELECT AVG(value) FROM test_subquery)
    """
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql=sql,
        source_name="test_subquery",
        source_dataset_id="test_subquery"
    )
    
    assert_response_structure(response, ["columns", "rows"])
    # Should return rows with value > 20 (average is 20)


def test_duckdb_query_local_with_string_functions_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with string functions via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_string",
        ["id", "text"],
        [[1, "hello"], [2, "world"]]
    )
    
    sql = """
    SELECT id, UPPER(text) as upper_text, LENGTH(text) as len
    FROM test_string
    """
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql=sql,
        source_name="test_string",
        source_dataset_id="test_string"
    )
    
    assert_response_structure(response, ["columns", "rows"])
    assert "upper_text" in response["columns"]
    assert "len" in response["columns"]

