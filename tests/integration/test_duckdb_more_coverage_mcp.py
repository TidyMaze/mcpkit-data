"""More integration tests for DuckDB operations via MCP protocol.

Covers missing paths in SQL query execution.
"""

import json
import pytest
from pathlib import Path

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_duckdb_query_local_error_handling_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local error handling with invalid SQL via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "duckdb_query_local",
            sql="INVALID SQL SYNTAX XYZ"
        )


def test_duckdb_query_local_table_not_found_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with non-existent table via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "duckdb_query_local",
            sql="SELECT * FROM nonexistent_table_xyz"
        )


def test_duckdb_query_local_multiple_sources_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with multiple dataset sources via MCP protocol."""
    create_test_dataset(mcp_client, "left_ds", ["id", "name"], [[1, "A"], [2, "B"]])
    create_test_dataset(mcp_client, "right_ds", ["id", "value"], [[1, 10], [2, 20]])
    
    sources = json.dumps([
        {"name": "left_table", "dataset_id": "left_ds"},
        {"name": "right_table", "dataset_id": "right_ds"}
    ])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT l.id, l.name, r.value FROM left_table l JOIN right_table r ON l.id = r.id",
        sources=sources
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 2


def test_duckdb_query_local_file_source_mcp(mcp_client, clean_registry, clean_roots):
    """Test duckdb_query_local with file source via MCP protocol."""
    import pandas as pd
    
    # Create a parquet file
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    parquet_path = Path(clean_roots) / "test_file.parquet"
    df.to_parquet(parquet_path)
    
    sources = json.dumps([
        {"name": "file_data", "path": str(parquet_path), "format": "parquet"}
    ])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM file_data WHERE value > 15",
        sources=sources
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 2


def test_duckdb_query_local_csv_source_mcp(mcp_client, clean_registry, clean_roots):
    """Test duckdb_query_local with CSV file source via MCP protocol."""
    csv_path = Path(clean_roots) / "test.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
    
    sources = json.dumps([
        {"name": "csv_data", "path": str(csv_path), "format": "csv"}
    ])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM csv_data WHERE id > 1",
        sources=sources
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 2


def test_duckdb_query_local_jsonl_source_mcp(mcp_client, clean_registry, clean_roots):
    """Test duckdb_query_local with JSONL file source via MCP protocol."""
    jsonl_path = Path(clean_roots) / "test.jsonl"
    jsonl_path.write_text('{"id": 1, "value": 10}\n{"id": 2, "value": 20}\n')
    
    sources = json.dumps([
        {"name": "jsonl_data", "path": str(jsonl_path), "format": "jsonl"}
    ])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM jsonl_data",
        sources=sources
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 2


def test_duckdb_query_local_max_rows_capping_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local max_rows capping via MCP protocol."""
    # Create large dataset
    rows = [[i] for i in range(500)]
    create_test_dataset(mcp_client, "large_ds", ["id"], rows)
    
    # Query with max_rows limit
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM test_data",
        source_name="test_data",
        source_dataset_id="large_ds",
        max_rows=100
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] <= 100


def test_duckdb_query_local_aggregation_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with aggregation functions via MCP protocol."""
    create_test_dataset(mcp_client, "agg_ds", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT COUNT(*) as count, SUM(value) as total, AVG(value) as avg FROM test_data",
        source_name="test_data",
        source_dataset_id="agg_ds"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 1
    assert "count" in response["columns"] or "COUNT" in [c.upper() for c in response["columns"]]


def test_duckdb_query_local_window_function_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with window functions via MCP protocol."""
    create_test_dataset(mcp_client, "window_ds", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT id, value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM test_data",
        source_name="test_data",
        source_dataset_id="window_ds"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 3


def test_duckdb_query_local_subquery_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local with subquery via MCP protocol."""
    create_test_dataset(mcp_client, "subq_ds", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM (SELECT id, value FROM test_data WHERE value > 15) t",
        source_name="test_data",
        source_dataset_id="subq_ds"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 2


def test_duckdb_query_local_individual_params_file_mcp(mcp_client, clean_registry, clean_roots):
    """Test duckdb_query_local with individual file source parameters via MCP protocol."""
    import pandas as pd
    
    parquet_path = Path(clean_roots) / "individual.parquet"
    df = pd.DataFrame({"x": [1, 2], "y": [10, 20]})
    df.to_parquet(parquet_path)
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM file_data",
        source_name="file_data",
        source_path=str(parquet_path),
        source_format="parquet"
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])
    assert response["row_count"] == 2


def test_duckdb_query_local_error_missing_source_params_mcp(mcp_client, clean_registry):
    """Test duckdb_query_local error when source params are incomplete via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "duckdb_query_local",
            sql="SELECT * FROM test_data",
            source_name="test_data"
            # Missing source_dataset_id or source_path
        )

