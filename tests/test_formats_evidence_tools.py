"""Tests for formats and evidence tools."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from mcpkit.core.evidence import evidence_bundle_plus, reconcile_counts
from mcpkit.core.formats import arrow_convert, parquet_inspect


def test_parquet_inspect(clean_registry, monkeypatch, tmp_path):
    """Test parquet inspection."""
    # Create test parquet file
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = parquet_inspect(str(parquet_file))
    assert "schema" in result
    assert "num_rows" in result
    assert result["num_rows"] == 3


def test_parquet_inspect_path_not_allowed(monkeypatch, tmp_path):
    """Test parquet inspect with disallowed path."""
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path / "allowed"))
    (tmp_path / "allowed").mkdir()
    
    other_file = tmp_path / "other.parquet"
    with pytest.raises(Exception):  # GuardError or file not found
        parquet_inspect(str(other_file))


def test_arrow_convert(clean_registry, clean_artifacts, monkeypatch, tmp_path):
    """Test arrow conversion."""
    # Create test CSV
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = arrow_convert(str(csv_file), "CSV", "PARQUET", "output.parquet")
    assert "output_path" in result
    assert Path(result["output_path"]).exists()


def test_reconcile_counts(clean_registry):
    """Test reconcile counts."""
    from mcpkit.core.registry import dataset_put_rows
    
    dataset_put_rows(
        columns=["id", "count"],
        rows=[[1, 10], [2, 20], [3, 30]],
        dataset_id="left",
    )
    dataset_put_rows(
        columns=["id", "count"],
        rows=[[1, 10], [2, 21], [4, 40]],  # id=2 differs, id=4 new
        dataset_id="right",
    )
    
    result = reconcile_counts("left", "right", key_cols=["id"])
    assert "mismatch_count" in result
    assert result["mismatch_count"] > 0


def test_evidence_bundle_plus(clean_registry, clean_artifacts):
    """Test evidence bundle generation."""
    from mcpkit.core.registry import dataset_put_rows
    
    dataset_put_rows(
        columns=["a", "b"],
        rows=[[1, "x"], [2, "y"]],
        dataset_id="ds1",
    )
    
    result = evidence_bundle_plus("ds1", "test_bundle")
    assert "exported_files" in result
    assert len(result["exported_files"]) >= 3  # parquet, csv, json

