"""Tests for DuckDB tools."""

import pytest

from mcpkit.core.duckdb_ops import duckdb_query_local


def test_duckdb_query_local_simple():
    """Test simple DuckDB query."""
    result = duckdb_query_local("SELECT 1 as a, 2 as b")
    assert "columns" in result
    assert "rows" in result
    assert result["columns"] == ["a", "b"]
    assert result["rows"][0] == [1, 2]


def test_duckdb_query_local_with_dataset(clean_registry):
    """Test DuckDB query with dataset source."""
    from mcpkit.core.registry import dataset_put_rows
    
    dataset_put_rows(columns=["a", "b"], rows=[[1, 2], [3, 4]], dataset_id="ds1")
    
    result = duckdb_query_local(
        "SELECT * FROM ds1",
        sources=[{"name": "ds1", "dataset_id": "ds1"}],
    )
    assert len(result["rows"]) == 2


def test_duckdb_query_local_max_rows():
    """Test DuckDB query with max_rows limit."""
    # Create query that returns many rows
    result = duckdb_query_local(
        "SELECT * FROM generate_series(1, 1000) as x",
        max_rows=10,
    )
    assert len(result["rows"]) <= 10


def test_duckdb_query_local_none_sources():
    """Test DuckDB query with None sources."""
    result = duckdb_query_local(
        "SELECT 1 as a, 'hello' as b",
        sources=None,
    )
    assert "columns" in result
    assert result["columns"] == ["a", "b"]
    assert result["rows"][0] == [1, "hello"]


def test_duckdb_query_local_empty_sources():
    """Test DuckDB query with empty sources list."""
    result = duckdb_query_local(
        "SELECT 2 as x, 3 as y",
        sources=[],
    )
    assert "columns" in result
    assert result["columns"] == ["x", "y"]
    assert result["rows"][0] == [2, 3]


def test_duckdb_query_local_with_file_source(clean_registry, tmp_path):
    """Test DuckDB query with file source."""
    import pandas as pd
    
    # Create a CSV file
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)
    
    result = duckdb_query_local(
        "SELECT * FROM test_csv",
        sources=[{"name": "test_csv", "path": str(csv_file), "format": "csv"}],
    )
    assert len(result["rows"]) == 3
    assert result["columns"] == ["id", "value"]


def test_duckdb_query_local_complex_query():
    """Test DuckDB query with complex SQL."""
    # Use VALUES to create a simple table for testing
    result = duckdb_query_local(
        """
        SELECT 
            x,
            x * 2 as doubled,
            CASE WHEN x > 5 THEN 'high' ELSE 'low' END as category
        FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS t(x)
        WHERE MOD(x, 2) = 0
        ORDER BY x
        """,
        max_rows=5,
    )
    assert "columns" in result
    assert len(result["rows"]) == 5
    assert result["rows"][0][0] == 2  # First even number
    assert result["rows"][0][2] == "low"  # Category for 2
    assert result["rows"][-1][2] == "high"  # Category for 10

