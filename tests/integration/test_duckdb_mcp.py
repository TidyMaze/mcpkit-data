"""Integration tests for DuckDB tools via MCP protocol."""

from pathlib import Path

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_duckdb_query_local_simple_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with simple query via MCP protocol."""
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT 1 as a, 'hello' as b"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["columns"] == ["a", "b"]
    assert response["row_count"] == 1
    assert response["rows"][0] == [1, "hello"]


def test_duckdb_query_local_with_dataset_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with dataset source via MCP protocol."""
    import json
    create_test_dataset(mcp_client, "test_duckdb", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    
    sources = json.dumps([{"name": "test_data", "dataset_id": "test_duckdb"}])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM test_data WHERE value > 15",
        sources=sources
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 2  # Two rows with value > 15


def test_duckdb_query_local_with_individual_params_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with individual source parameters via MCP protocol."""
    create_test_dataset(mcp_client, "test_duckdb2", ["x"], [[1], [2], [3]])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT SUM(x) as total FROM test_data",
        source_name="test_data",
        source_dataset_id="test_duckdb2"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 1
    assert response["rows"][0][0] == 6  # Sum of 1+2+3


def test_duckdb_query_local_with_file_source_mcp(mcp_client, clean_registry, clean_roots):
    """Test duckdb_query_local with file source via MCP protocol."""
    import pandas as pd
    # Create a parquet file
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    parquet_path = Path(clean_roots) / "test_file.parquet"
    df.to_parquet(parquet_path)
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM test_file WHERE value > 15",
        source_name="test_file",
        source_path=str(parquet_path),
        source_format="parquet"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 2  # Two rows with value > 15


def test_duckdb_query_local_with_max_rows_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with max_rows limit via MCP protocol."""
    import json
    # Create dataset with many rows
    rows = [[i, i*10] for i in range(100)]
    create_test_dataset(mcp_client, "test_large", ["id", "value"], rows)
    
    sources = json.dumps([{"name": "large_data", "dataset_id": "test_large"}])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM large_data ORDER BY id",
        sources=sources,
        max_rows=10
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] <= 10  # Should be capped


def test_duckdb_query_local_explain_mcp(mcp_client):
    """Test duckdb_query_local with EXPLAIN query via MCP protocol."""
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="EXPLAIN SELECT 1"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])


def test_duckdb_query_local_with_clause_mcp(mcp_client):
    """Test duckdb_query_local with WITH clause via MCP protocol."""
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="WITH t AS (SELECT 1 as x) SELECT * FROM t"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 1

