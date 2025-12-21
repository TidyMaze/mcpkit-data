"""Tests for polars tools."""

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.polars_ops import polars_from_rows, polars_groupby


def test_polars_from_rows(clean_registry):
    """Test creating polars dataset from rows."""
    result = polars_from_rows(
        columns=["a", "b"],
        rows=[[1, 2], [3, 4]],
    )
    assert "dataset_id" in result
    assert result["rows"] == 2
    assert result["columns"] == ["a", "b"]


def test_polars_groupby(clean_registry):
    """Test polars groupby."""
    polars_from_rows(
        columns=["group", "value"],
        rows=[["A", 10], ["A", 20], ["B", 30]],
        dataset_id="ds1",
    )
    
    result = polars_groupby(
        "ds1",
        group_cols=["group"],
        aggs={"value": ["sum", "count"]},
    )
    assert "dataset_id" in result


def test_polars_groupby_invalid_agg(clean_registry):
    """Test polars groupby with invalid aggregation."""
    polars_from_rows(columns=["a"], rows=[[1]], dataset_id="ds1")
    
    with pytest.raises(GuardError, match="Invalid aggregation"):
        polars_groupby("ds1", group_cols=[], aggs={"a": ["invalid"]})

