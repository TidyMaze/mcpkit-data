"""Additional tests for formats module."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from mcpkit.core.formats import arrow_convert, parquet_inspect
from mcpkit.core.guards import GuardError


def test_parquet_inspect_empty_file(monkeypatch, tmp_path):
    """Test parquet_inspect with empty file."""
    df = pd.DataFrame()
    parquet_file = tmp_path / "empty.parquet"
    df.to_parquet(parquet_file)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = parquet_inspect(str(parquet_file))
    assert result["num_rows"] == 0
    assert result["sample_rows"] == []


def test_parquet_inspect_large_file(monkeypatch, tmp_path):
    """Test parquet_inspect with large file (capped sample)."""
    df = pd.DataFrame({"a": range(1000)})
    parquet_file = tmp_path / "large.parquet"
    df.to_parquet(parquet_file)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = parquet_inspect(str(parquet_file), sample_rows=10)
    assert result["num_rows"] == 1000
    assert len(result["sample_rows"]) == 10


def test_parquet_inspect_custom_sample_rows(monkeypatch, tmp_path):
    """Test parquet_inspect with custom sample_rows."""
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = parquet_inspect(str(parquet_file), sample_rows=2)
    assert len(result["sample_rows"]) == 2


def test_parquet_inspect_schema_fields(monkeypatch, tmp_path):
    """Test parquet_inspect returns correct schema fields."""
    df = pd.DataFrame({
        "int_col": [1, 2, 3],
        "str_col": ["a", "b", "c"],
        "float_col": [1.1, 2.2, 3.3],
        "bool_col": [True, False, True],
    })
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = parquet_inspect(str(parquet_file))
    assert "schema" in result
    assert "fields" in result["schema"]
    field_names = [f["name"] for f in result["schema"]["fields"]]
    assert "int_col" in field_names
    assert "str_col" in field_names
    assert "float_col" in field_names
    assert "bool_col" in field_names


def test_parquet_inspect_nonexistent_file(monkeypatch, tmp_path):
    """Test parquet_inspect with nonexistent file."""
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    with pytest.raises(GuardError):
        parquet_inspect(str(tmp_path / "nonexistent.parquet"))


def test_arrow_convert_csv_to_parquet(monkeypatch, tmp_path):
    """Test arrow_convert CSV to Parquet."""
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = arrow_convert(str(csv_file), "CSV", "PARQUET", "output.parquet")
    assert result["input_format"] == "CSV"
    assert result["output_format"] == "PARQUET"
    assert result["rows"] == 2
    assert Path(result["output_path"]).exists()


def test_arrow_convert_parquet_to_csv(monkeypatch, tmp_path):
    """Test arrow_convert Parquet to CSV."""
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = arrow_convert(str(parquet_file), "PARQUET", "CSV", "output.csv")
    assert result["input_format"] == "PARQUET"
    assert result["output_format"] == "CSV"
    assert result["rows"] == 2
    assert Path(result["output_path"]).exists()


def test_arrow_convert_json_to_parquet(monkeypatch, tmp_path):
    """Test arrow_convert JSON to Parquet."""
    import json
    
    # PyArrow JSON reader expects newline-delimited JSON (JSONL) or array of objects
    # Create JSONL format
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    json_file = tmp_path / "test.json"
    with open(json_file, "w") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = arrow_convert(str(json_file), "JSON", "PARQUET", "output.parquet")
    assert result["input_format"] == "JSON"
    assert result["output_format"] == "PARQUET"
    assert result["rows"] == 2


def test_arrow_convert_unsupported_input_format(monkeypatch, tmp_path):
    """Test arrow_convert with unsupported input format."""
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    # Create a dummy file
    test_file = tmp_path / "test.txt"
    test_file.write_text("test")
    
    with pytest.raises(GuardError):
        arrow_convert(str(test_file), "TXT", "PARQUET", "output.parquet")


def test_arrow_convert_unsupported_output_format(monkeypatch, tmp_path):
    """Test arrow_convert with unsupported output format."""
    df = pd.DataFrame({"a": [1, 2]})
    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    with pytest.raises(GuardError, match="Unsupported output format"):
        arrow_convert(str(csv_file), "CSV", "TXT", "output.txt")


def test_arrow_convert_case_insensitive_format(monkeypatch, tmp_path):
    """Test arrow_convert handles case-insensitive formats."""
    df = pd.DataFrame({"a": [1, 2]})
    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = arrow_convert(str(csv_file), "csv", "parquet", "output.parquet")
    assert result["rows"] == 2


def test_arrow_convert_nonexistent_input(monkeypatch, tmp_path):
    """Test arrow_convert with nonexistent input file."""
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    with pytest.raises(GuardError):
        arrow_convert(str(tmp_path / "nonexistent.csv"), "CSV", "PARQUET", "output.parquet")


def test_arrow_convert_invalid_filename(monkeypatch, tmp_path):
    """Test arrow_convert rejects invalid output filename."""
    df = pd.DataFrame({"a": [1, 2]})
    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    with pytest.raises(GuardError):
        arrow_convert(str(csv_file), "CSV", "PARQUET", "../invalid.parquet")


def test_arrow_convert_empty_file(monkeypatch, tmp_path):
    """Test arrow_convert with empty input file."""
    # Create empty CSV file (just headers)
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("col1,col2\n")
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = arrow_convert(str(csv_file), "CSV", "PARQUET", "output.parquet")
    assert result["rows"] == 0

