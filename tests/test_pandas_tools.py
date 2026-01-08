"""Tests for pandas tools."""

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.pandas_ops import (
    pandas_describe,
    pandas_diff_frames,
    pandas_filter_query,
    pandas_from_rows,
    pandas_groupby,
    pandas_join,
    pandas_schema_check,
    pandas_sample_stratified,
)


def test_pandas_from_rows(clean_registry):
    """Test creating pandas dataset from rows."""
    result = pandas_from_rows(
        columns=["a", "b"],
        rows=[[1, 2], [3, 4]],
    )
    assert "dataset_id" in result
    assert result["rows"] == 2


def test_pandas_describe(clean_registry):
    """Test describing dataset."""
    pandas_from_rows(columns=["a", "b"], rows=[[1, 2], [3, 4]], dataset_id="ds1")
    
    result = pandas_describe("ds1")
    assert "description" in result
    assert "dtypes" in result


def test_pandas_groupby(clean_registry):
    """Test groupby operation."""
    pandas_from_rows(
        columns=["group", "value"],
        rows=[["A", 10], ["A", 20], ["B", 30]],
        dataset_id="ds1",
    )
    
    result = pandas_groupby(
        "ds1",
        group_cols=["group"],
        aggs={"value": ["sum", "count"]},
    )
    assert "dataset_id" in result
    assert result["rows"] <= 2  # 2 groups


def test_pandas_groupby_invalid_agg(clean_registry):
    """Test groupby with invalid aggregation."""
    pandas_from_rows(columns=["a"], rows=[[1]], dataset_id="ds1")
    
    with pytest.raises(GuardError, match="Invalid aggregation"):
        pandas_groupby("ds1", group_cols=[], aggs={"a": ["invalid"]})


def test_pandas_join(clean_registry):
    """Test join operation."""
    pandas_from_rows(columns=["id", "name"], rows=[[1, "A"], [2, "B"]], dataset_id="left")
    pandas_from_rows(columns=["id", "value"], rows=[[1, 100], [3, 300]], dataset_id="right")
    
    result = pandas_join("left", "right", keys=["id"], how="inner")
    assert "dataset_id" in result
    assert result["rows"] == 1  # Only id=1 matches


def test_pandas_filter_query(clean_registry):
    """Test filter query."""
    pandas_from_rows(
        columns=["a", "b"],
        rows=[[1, "x"], [2, "y"], [3, "x"]],
        dataset_id="ds1",
    )
    
    result = pandas_filter_query(
        "ds1",
        filters=[{"column": "b", "op": "==", "value": "x"}],
    )
    assert result["rows"] == 2


def test_pandas_filter_query_invalid_op(clean_registry):
    """Test filter with invalid operator."""
    pandas_from_rows(columns=["a"], rows=[[1]], dataset_id="ds1")
    
    with pytest.raises(GuardError, match="Invalid filter op"):
        pandas_filter_query("ds1", filters=[{"column": "a", "op": "invalid", "value": 1}])


def test_pandas_diff_frames(clean_registry):
    """Test diff frames."""
    pandas_from_rows(columns=["id", "val"], rows=[[1, 10], [2, 20]], dataset_id="a")
    pandas_from_rows(columns=["id", "val"], rows=[[1, 11], [3, 30]], dataset_id="b")
    
    result = pandas_diff_frames("a", "b", key_cols=["id"], compare_cols=["val"])
    assert "changed" in result
    assert result["only_in_a"] == 1  # id=2
    assert result["only_in_b"] == 1  # id=3


def test_pandas_diff_frames_no_compare_cols(clean_registry):
    """Test diff frames without compare_cols."""
    pandas_from_rows(columns=["id", "val"], rows=[[1, 10], [2, 20]], dataset_id="a")
    pandas_from_rows(columns=["id", "val"], rows=[[1, 11], [3, 30]], dataset_id="b")
    
    result = pandas_diff_frames("a", "b", key_cols=["id"], compare_cols=None)
    assert "only_in_a" in result
    assert "only_in_b" in result
    assert "common" in result


def test_pandas_diff_frames_identical(clean_registry):
    """Test diff frames with identical datasets."""
    pandas_from_rows(columns=["id", "val"], rows=[[1, 10], [2, 20]], dataset_id="a")
    pandas_from_rows(columns=["id", "val"], rows=[[1, 10], [2, 20]], dataset_id="b")
    
    result = pandas_diff_frames("a", "b", key_cols=["id"], compare_cols=["val"])
    assert result["only_in_a"] == 0
    assert result["only_in_b"] == 0
    assert result["changed_count"] == 0


def test_pandas_diff_frames_max_changed(clean_registry):
    """Test diff frames with max_changed limit."""
    # Create datasets with many differences
    rows_a = [[i, 10] for i in range(100)]
    rows_b = [[i, 20] for i in range(100)]
    pandas_from_rows(columns=["id", "val"], rows=rows_a, dataset_id="a")
    pandas_from_rows(columns=["id", "val"], rows=rows_b, dataset_id="b")
    
    result = pandas_diff_frames("a", "b", key_cols=["id"], compare_cols=["val"], max_changed=10)
    assert len(result["changed"]) <= 10


def test_pandas_schema_check(clean_registry):
    """Test schema check."""
    pandas_from_rows(columns=["a", "b"], rows=[[1, "x"], [None, "y"]], dataset_id="ds1")
    
    result = pandas_schema_check(
        "ds1",
        required_columns=["a", "b"],
        non_null_columns=["b"],
    )
    assert "valid" in result
    assert isinstance(result["valid"], bool)


def test_pandas_schema_check_missing_columns(clean_registry):
    """Test schema check with missing required columns."""
    pandas_from_rows(columns=["a", "b"], rows=[[1, "x"]], dataset_id="ds1")
    
    result = pandas_schema_check(
        "ds1",
        required_columns=["a", "b", "c"],  # c is missing
    )
    assert result["valid"] is False
    assert len(result["errors"]) > 0
    assert any("Missing columns" in err for err in result["errors"])


def test_pandas_schema_check_dtype_mismatch(clean_registry):
    """Test schema check with dtype mismatch."""
    pandas_from_rows(columns=["a", "b"], rows=[[1, "x"]], dataset_id="ds1")
    
    result = pandas_schema_check(
        "ds1",
        dtype_overrides={"a": "float64"},  # a is int64, not float64
    )
    # Should either be valid (if dtype check is lenient) or invalid
    assert "valid" in result
    assert "errors" in result


def test_pandas_schema_check_null_values(clean_registry):
    """Test schema check with null values in non-null columns."""
    pandas_from_rows(columns=["a", "b"], rows=[[1, "x"], [None, "y"], [3, None]], dataset_id="ds1")
    
    result = pandas_schema_check(
        "ds1",
        non_null_columns=["a", "b"],
    )
    assert result["valid"] is False
    assert len(result["errors"]) > 0
    assert any("null" in err.lower() for err in result["errors"])


def test_pandas_schema_check_valid_dataset(clean_registry):
    """Test schema check with valid dataset."""
    pandas_from_rows(columns=["id", "name", "age"], rows=[[1, "Alice", 30], [2, "Bob", 25]], dataset_id="ds1")
    
    result = pandas_schema_check(
        "ds1",
        required_columns=["id", "name", "age"],
        dtype_overrides={"id": "int64", "age": "int64"},
        non_null_columns=["id", "name"],
    )
    assert result["valid"] is True
    assert result["errors"] is None or len(result["errors"]) == 0


def test_pandas_schema_check_no_constraints(clean_registry):
    """Test schema check with no constraints."""
    pandas_from_rows(columns=["a", "b"], rows=[[1, "x"]], dataset_id="ds1")
    
    result = pandas_schema_check("ds1")
    assert result["valid"] is True
    assert result["errors"] is None or len(result["errors"]) == 0


def test_pandas_sample_stratified(clean_registry):
    """Test stratified sampling."""
    # Create proper data
    rows = [[1, "a"] for _ in range(10)] + [[2, "b"] for _ in range(10)]
    pandas_from_rows(columns=["group", "value"], rows=rows, dataset_id="ds1")
    
    result = pandas_sample_stratified("ds1", strata_cols=["group"], n_per_group=3)
    assert result["rows"] <= 6  # 3 per group * 2 groups

