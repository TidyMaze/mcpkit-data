"""Tests for DuckDB tools."""

import json
import pytest

from mcpkit.core.duckdb_ops import duckdb_query_local
from mcpkit.core.guards import GuardError


def test_duckdb_query_local_simple():
    """Test simple DuckDB query."""
    result = duckdb_query_local("SELECT 1 as a, 2 as b")
    assert "columns" in result
    assert "rows" in result
    assert "row_count" in result
    assert result["columns"] == ["a", "b"]
    assert result["rows"][0] == [1, 2]
    assert result["row_count"] == 1


def test_duckdb_query_local_with_dataset(clean_registry):
    """Test DuckDB query with dataset source."""
    from mcpkit.core.registry import dataset_put_rows

    dataset_put_rows(columns=["a", "b"], rows=[[1, 2], [3, 4]], dataset_id="ds1")

    result = duckdb_query_local(
        "SELECT * FROM ds1",
        sources=[{"name": "ds1", "dataset_id": "ds1"}],
    )
    assert len(result["rows"]) == 2
    assert result["columns"] == ["a", "b"]
    assert result["row_count"] == 2


def test_duckdb_query_local_max_rows():
    """Test DuckDB query with max_rows limit."""
    # Create query that returns many rows
    result = duckdb_query_local(
        "SELECT * FROM generate_series(1, 1000) as x",
        max_rows=10,
    )
    assert len(result["rows"]) <= 10
    assert result["row_count"] <= 10


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


def test_duckdb_query_local_multiple_sources(clean_registry):
    """Test DuckDB query with multiple dataset sources."""
    from mcpkit.core.registry import dataset_put_rows

    dataset_put_rows(columns=["id", "name"], rows=[[1, "a"], [2, "b"]], dataset_id="ds1")
    dataset_put_rows(columns=["id", "value"], rows=[[1, 10], [2, 20]], dataset_id="ds2")

    result = duckdb_query_local(
        "SELECT a.id, a.name, b.value FROM ds1 a JOIN ds2 b ON a.id = b.id",
        sources=[
            {"name": "ds1", "dataset_id": "ds1"},
            {"name": "ds2", "dataset_id": "ds2"},
        ],
    )
    assert len(result["rows"]) == 2
    assert result["columns"] == ["id", "name", "value"]


def test_duckdb_query_local_aggregation(clean_registry):
    """Test DuckDB query with aggregation."""
    from mcpkit.core.registry import dataset_put_rows

    dataset_put_rows(
        columns=["category", "value"],
        rows=[["A", 10], ["A", 20], ["B", 30], ["B", 40]],
        dataset_id="agg_test",
    )

    result = duckdb_query_local(
        "SELECT category, SUM(value) as total FROM agg_test GROUP BY category",
        sources=[{"name": "agg_test", "dataset_id": "agg_test"}],
    )
    assert len(result["rows"]) == 2
    assert result["columns"] == ["category", "total"]


def test_duckdb_query_local_missing_table_error():
    """Test DuckDB query error when table doesn't exist."""
    with pytest.raises(GuardError, match="does not exist"):
        duckdb_query_local("SELECT * FROM nonexistent_table")


def test_duckdb_query_local_invalid_sources_type():
    """Test DuckDB query with invalid sources type."""
    with pytest.raises(GuardError, match="must be a list"):
        duckdb_query_local("SELECT 1", sources="not a list")


def test_duckdb_query_local_invalid_source_missing_name():
    """Test DuckDB query with source missing name field."""
    with pytest.raises(GuardError, match="missing required field 'name'"):
        duckdb_query_local("SELECT 1", sources=[{"dataset_id": "test"}])


def test_duckdb_query_local_invalid_source_missing_fields():
    """Test DuckDB query with source missing required fields."""
    with pytest.raises(GuardError, match="must have either 'dataset_id' or both 'path' and 'format'"):
        duckdb_query_local("SELECT 1", sources=[{"name": "test"}])


def test_duckdb_query_local_nonexistent_dataset(clean_registry):
    """Test DuckDB query with nonexistent dataset_id."""
    with pytest.raises(GuardError, match="not found"):
        duckdb_query_local(
            "SELECT * FROM test",
            sources=[{"name": "test", "dataset_id": "nonexistent"}],
        )


def test_duckdb_query_local_parquet_file(tmp_path):
    """Test DuckDB query with parquet file source."""
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file, index=False)

    result = duckdb_query_local(
        "SELECT * FROM test_parquet WHERE x > 1",
        sources=[{"name": "test_parquet", "path": str(parquet_file), "format": "parquet"}],
    )
    assert len(result["rows"]) == 2
    assert result["columns"] == ["x", "y"]


def test_duckdb_query_local_json_file(tmp_path):
    """Test DuckDB query with JSON file source."""
    import json

    data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    json_file = tmp_path / "test.json"
    with open(json_file, "w") as f:
        json.dump(data, f)

    result = duckdb_query_local(
        "SELECT * FROM test_json",
        sources=[{"name": "test_json", "path": str(json_file), "format": "json"}],
    )
    assert len(result["rows"]) == 2
    assert "id" in result["columns"]
    assert "name" in result["columns"]


def test_duckdb_query_local_with_clause():
    """Test DuckDB query with WITH clause."""
    result = duckdb_query_local(
        """
        WITH numbers AS (
            SELECT * FROM generate_series(1, 5) as x
        )
        SELECT x, x * 2 as doubled FROM numbers WHERE x > 2
        """
    )
    assert len(result["rows"]) == 3
    assert result["columns"] == ["x", "doubled"]


def test_duckdb_query_local_order_by():
    """Test DuckDB query with ORDER BY."""
    result = duckdb_query_local(
        """
        SELECT * FROM (VALUES (3), (1), (2)) AS t(x)
        ORDER BY x DESC
        """
    )
    assert result["rows"][0][0] == 3
    assert result["rows"][-1][0] == 1


def test_duckdb_query_local_response_structure():
    """Test that response has correct structure."""
    result = duckdb_query_local("SELECT 1 as a")

    assert isinstance(result, dict)
    assert "columns" in result
    assert "rows" in result
    assert "row_count" in result
    assert isinstance(result["columns"], list)
    assert isinstance(result["rows"], list)
    assert isinstance(result["row_count"], int)
    assert result["row_count"] == len(result["rows"])

