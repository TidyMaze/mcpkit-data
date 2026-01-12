"""More integration tests for formats tools to improve coverage via MCP protocol.

Covers missing paths in parquet_inspect and arrow_convert.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


def test_parquet_inspect_error_handling_mcp(mcp_client, clean_roots):
    """Test parquet_inspect error handling with invalid file via MCP protocol."""
    # Create invalid parquet file (just a text file)
    invalid_path = Path(clean_roots) / "invalid.parquet"
    invalid_path.write_text("not a parquet file")
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "parquet_inspect",
            path=str(invalid_path)
        )


def test_parquet_inspect_empty_file_mcp(mcp_client, clean_roots):
    """Test parquet_inspect with empty parquet file via MCP protocol."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    empty_path = Path(clean_roots) / "empty.parquet"
    # Create empty parquet file with schema
    empty_df = pd.DataFrame({"col": []})
    empty_df.to_parquet(empty_path)
    
    response = call_tool(
        mcp_client,
        "parquet_inspect",
        path=str(empty_path),
        sample_rows=5
    )
    
    assert_response_structure(response, ["path", "num_rows", "schema", "sample_rows"])
    assert response["num_rows"] == 0
    assert isinstance(response["sample_rows"], list)


def test_parquet_inspect_with_sample_rows_capping_mcp(mcp_client, clean_roots):
    """Test parquet_inspect with sample_rows larger than file via MCP protocol."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    parquet_path = Path(clean_roots) / "small.parquet"
    df = pd.DataFrame({"id": [1, 2, 3]})
    df.to_parquet(parquet_path)
    
    response = call_tool(
        mcp_client,
        "parquet_inspect",
        path=str(parquet_path),
        sample_rows=100  # More than actual rows
    )
    
    assert_response_structure(response, ["path", "num_rows", "schema", "sample_rows"])
    assert response["num_rows"] == 3
    # sample_rows should be capped to actual number of rows (3)
    assert isinstance(response["sample_rows"], list)
    assert len(response["sample_rows"]) <= 3


def test_arrow_convert_csv_to_parquet_mcp(mcp_client, clean_roots, clean_artifacts):
    """Test arrow_convert from CSV to Parquet via MCP protocol."""
    csv_path = Path(clean_roots) / "test_csv.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n")
    
    response = call_tool(
        mcp_client,
        "arrow_convert",
        input_path=str(csv_path),
        input_format="CSV",
        output_format="PARQUET",
        output_filename="converted.parquet"
    )
    
    assert_response_structure(response, ["input_path", "output_path", "rows"])
    assert response["output_path"].endswith(".parquet")
    assert response["rows"] == 2


@pytest.mark.skip(reason="pyarrow.json.write_json not available in this pyarrow version")
def test_arrow_convert_parquet_to_json_mcp(mcp_client, clean_roots, clean_artifacts):
    """Test arrow_convert from Parquet to JSON via MCP protocol.
    
    Note: Skipped because pyarrow.json.write_json may not be available in all pyarrow versions.
    """
    pass


def test_arrow_convert_json_to_parquet_mcp(mcp_client, clean_roots, clean_artifacts):
    """Test arrow_convert from JSON to Parquet via MCP protocol."""
    # Create JSONL file (one JSON object per line)
    json_path = Path(clean_roots) / "test.json"
    json_data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}, {"id": 3, "value": 30}]
    with json_path.open("w") as f:
        for item in json_data:
            f.write(json.dumps(item) + "\n")
    
    # This should work (JSON to Parquet)
    response = call_tool(
        mcp_client,
        "arrow_convert",
        input_path=str(json_path),
        input_format="JSON",
        output_format="PARQUET",
        output_filename="converted.parquet"
    )
    
    assert_response_structure(response, ["input_path", "output_path", "rows"])
    assert response["output_path"].endswith(".parquet")
    assert response["rows"] == 3


def test_arrow_convert_unsupported_input_format_mcp(mcp_client, clean_roots):
    """Test arrow_convert with unsupported input format via MCP protocol."""
    test_file = Path(clean_roots) / "test.txt"
    test_file.write_text("some content")
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "arrow_convert",
            input_path=str(test_file),
            input_format="TXT",  # Unsupported
            output_format="CSV",
            output_filename="output.csv"
        )


def test_arrow_convert_unsupported_output_format_mcp(mcp_client, clean_roots):
    """Test arrow_convert with unsupported output format via MCP protocol."""
    csv_path = Path(clean_roots) / "test.csv"
    csv_path.write_text("id,name\n1,Alice\n")
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "arrow_convert",
            input_path=str(csv_path),
            input_format="CSV",
            output_format="XML",  # Unsupported
            output_filename="output.xml"
        )


def test_arrow_convert_error_handling_mcp(mcp_client, clean_roots):
    """Test arrow_convert error handling with invalid input file via MCP protocol."""
    invalid_path = Path(clean_roots) / "invalid.csv"
    invalid_path.write_text("invalid,csv,content\nbroken,line")
    
    # Try to convert invalid CSV - should handle error gracefully
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "arrow_convert",
            input_path=str(invalid_path),
            input_format="CSV",
            output_format="PARQUET",
            output_filename="output.parquet"
        )

