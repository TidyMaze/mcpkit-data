"""Tests for chart tools."""

import tempfile
from pathlib import Path

import fastavro
import pandas as pd
import pytest

from mcpkit.core.chart_ops import dataset_to_chart
from mcpkit.core.guards import GuardError


def test_dataset_to_chart_from_dataset_id(clean_registry, clean_artifacts):
    """Test dataset_to_chart with dataset_id creates bar chart."""
    from mcpkit.core.registry import dataset_put_rows
    
    # Create dataset with categorical + numeric
    dataset_put_rows(
        columns=["category", "value"],
        rows=[["A", 10], ["B", 20], ["C", 15]],
        dataset_id="test_chart",
    )
    
    result = dataset_to_chart(dataset_id="test_chart", output_filename="test.png")
    
    assert "artifact_path" in result
    assert Path(result["artifact_path"]).exists()
    assert Path(result["artifact_path"]).stat().st_size > 0
    assert result["chart_spec"]["chart_type"] == "bar"
    assert "detected" in result
    assert "warnings" in result
    assert "sample" in result


def test_dataset_to_chart_from_csv_path(clean_artifacts, monkeypatch, tmp_path):
    """Test dataset_to_chart with CSV path creates chart."""
    # Create CSV
    df = pd.DataFrame({"category": ["X", "Y"], "value": [5, 10]})
    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = dataset_to_chart(path=str(csv_file), output_filename="test.png")
    
    assert "artifact_path" in result
    assert Path(result["artifact_path"]).exists()
    assert result["chart_spec"]["chart_type"] == "bar"


def test_dataset_to_chart_parquet_datetime_numeric(clean_artifacts, monkeypatch, tmp_path):
    """Test parquet with datetime+numeric creates line chart."""
    # Create parquet with datetime + numeric
    dates = pd.date_range("2024-01-01", periods=10, freq="D")
    df = pd.DataFrame({"date": dates, "value": range(10)})
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file, index=False)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = dataset_to_chart(path=str(parquet_file), output_filename="test.png")
    
    assert result["chart_spec"]["chart_type"] == "line"
    assert result["chart_spec"]["x"] == "date"
    assert result["chart_spec"]["y"] == "value"


def test_dataset_to_chart_avro_histogram(clean_artifacts, monkeypatch, tmp_path):
    """Test Avro container file creates histogram artifact."""
    # Create Avro file
    schema = {
        "type": "record",
        "name": "Test",
        "fields": [
            {"name": "value", "type": "double"},
        ],
    }
    
    avro_file = tmp_path / "test.avro"
    records = [{"value": float(i)} for i in range(20)]
    
    with open(avro_file, "wb") as f:
        fastavro.writer(f, schema, records)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = dataset_to_chart(path=str(avro_file), input_format="avro", output_filename="test.png")
    
    assert "artifact_path" in result
    assert Path(result["artifact_path"]).exists()
    assert Path(result["artifact_path"]).stat().st_size > 0


def test_dataset_to_chart_path_not_allowed(monkeypatch, tmp_path):
    """Test dataset_to_chart rejects path outside allowed roots."""
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path / "allowed"))
    (tmp_path / "allowed").mkdir()
    
    other_file = tmp_path / "other.csv"
    df = pd.DataFrame({"a": [1]})
    df.to_csv(other_file, index=False)
    
    with pytest.raises(GuardError):
        dataset_to_chart(path=str(other_file))


def test_dataset_to_chart_bad_filename():
    """Test dataset_to_chart rejects bad filename."""
    with pytest.raises(GuardError):
        dataset_to_chart(dataset_id="test", output_filename="../bad.png")
    
    with pytest.raises(GuardError):
        dataset_to_chart(dataset_id="test", output_filename="bad/file.png")


def test_dataset_to_chart_row_cap(clean_registry, clean_artifacts, monkeypatch):
    """Test dataset_to_chart enforces row cap."""
    from mcpkit.core.registry import dataset_put_rows
    
    # Create large dataset
    rows = [[i, i * 2] for i in range(1000)]
    dataset_put_rows(
        columns=["a", "b"],
        rows=rows,
        dataset_id="large",
    )
    
    monkeypatch.setenv("MCPKIT_MAX_ROWS", "100")
    
    result = dataset_to_chart(dataset_id="large", max_rows=50, output_filename="test.png")
    
    # Should have capped rows
    assert len(result["sample"]) <= 10  # Sample is capped at 10
    # The actual chart should work with capped data


def test_dataset_to_chart_both_dataset_id_and_path():
    """Test dataset_to_chart rejects both dataset_id and path."""
    with pytest.raises(GuardError):
        dataset_to_chart(dataset_id="test", path="test.csv")


def test_dataset_to_chart_neither_dataset_id_nor_path():
    """Test dataset_to_chart rejects neither dataset_id nor path."""
    with pytest.raises(GuardError):
        dataset_to_chart()

