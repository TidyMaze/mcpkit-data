"""Tests for dataset registry tools."""

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.registry import (
    dataset_delete,
    dataset_info,
    dataset_list,
    dataset_put_rows,
)


def test_dataset_put_rows_happy(clean_registry):
    """Test creating dataset from rows."""
    result = dataset_put_rows(
        columns=["a", "b"],
        rows=[[1, 2], [3, 4]],
    )
    assert "dataset_id" in result
    assert result["rows"] == 2
    assert result["columns"] == ["a", "b"]


def test_dataset_put_rows_with_id(clean_registry):
    """Test creating dataset with specific ID."""
    result = dataset_put_rows(
        columns=["x"],
        rows=[[1], [2]],
        dataset_id="test_dataset",
    )
    assert result["dataset_id"] == "test_dataset"
    assert result["rows"] == 2


def test_dataset_list(clean_registry):
    """Test listing datasets."""
    dataset_put_rows(columns=["a"], rows=[[1]], dataset_id="ds1")
    dataset_put_rows(columns=["b"], rows=[[2]], dataset_id="ds2")
    
    result = dataset_list()
    assert len(result["datasets"]) == 2
    ids = [d["dataset_id"] for d in result["datasets"]]
    assert "ds1" in ids
    assert "ds2" in ids


def test_dataset_info(clean_registry):
    """Test getting dataset info."""
    dataset_put_rows(columns=["a", "b"], rows=[[1, 2]], dataset_id="ds1")
    
    result = dataset_info("ds1")
    assert result["dataset_id"] == "ds1"
    assert result["rows"] == 1
    assert result["columns"] == ["a", "b"]


def test_dataset_info_not_found(clean_registry):
    """Test getting info for non-existent dataset."""
    with pytest.raises(GuardError, match="not found"):
        dataset_info("nonexistent")


def test_dataset_delete(clean_registry):
    """Test deleting dataset."""
    dataset_put_rows(columns=["a"], rows=[[1]], dataset_id="ds1")
    
    result = dataset_delete("ds1")
    assert result["deleted"] is True
    assert result["dataset_id"] == "ds1"
    
    # Should be gone
    with pytest.raises(GuardError):
        dataset_info("ds1")


def test_dataset_delete_not_found(clean_registry):
    """Test deleting non-existent dataset."""
    with pytest.raises(GuardError, match="not found"):
        dataset_delete("nonexistent")


def test_dataset_unsafe_filename(clean_registry):
    """Test unsafe filename rejection."""
    with pytest.raises(GuardError):
        dataset_put_rows(columns=["a"], rows=[[1]], dataset_id="../hack")

