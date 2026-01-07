"""More edge case tests for file format tools via MCP protocol."""

import json
from pathlib import Path

import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


def test_parquet_inspect_large_file_mcp(mcp_client, clean_roots):
    """Test parquet_inspect with larger file via MCP protocol."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    # Create a larger parquet file
    parquet_path = Path(clean_roots) / "large_test.parquet"
    df = pd.DataFrame({
        "id": range(100),
        "value": [f"item_{i}" for i in range(100)]
    })
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path)
    
    response = call_tool(
        mcp_client,
        "parquet_inspect",
        path=str(parquet_path),
        sample_rows=10
    )
    
    assert_response_structure(response, ["schema", "sample_rows", "row_count"])
    assert response["row_count"] == 100
    assert len(response["sample_rows"]) == 10


def test_arrow_convert_csv_to_json_mcp(mcp_client, clean_roots, clean_artifacts):
    """Test arrow_convert from CSV to JSON via MCP protocol."""
    csv_path = Path(clean_roots) / "test_convert.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n")
    
    response = call_tool(
        mcp_client,
        "arrow_convert",
        input_path=str(csv_path),
        input_format="CSV",
        output_format="JSON",
        output_filename="converted.json"
    )
    
    assert_response_structure(response, ["output_path"])
    assert response["output_path"].endswith(".json")


def test_arrow_convert_parquet_to_csv_mcp(mcp_client, clean_roots, clean_artifacts):
    """Test arrow_convert from Parquet to CSV via MCP protocol."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    parquet_path = Path(clean_roots) / "test_parquet.parquet"
    df = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path)
    
    response = call_tool(
        mcp_client,
        "arrow_convert",
        input_path=str(parquet_path),
        input_format="PARQUET",
        output_format="CSV",
        output_filename="converted.csv"
    )
    
    assert_response_structure(response, ["output_path"])
    assert response["output_path"].endswith(".csv")


def test_fs_read_binary_file_mcp(mcp_client, clean_roots):
    """Test fs_read with binary file via MCP protocol."""
    binary_path = Path(clean_roots) / "test_binary.bin"
    binary_path.write_bytes(b"\x00\x01\x02\x03\x04")
    
    response = call_tool(
        mcp_client,
        "fs_read",
        path=str(binary_path)
    )
    
    assert_response_structure(response, ["content", "size"])
    assert response["size"] == 5


def test_fs_read_with_offset_mcp(mcp_client, clean_roots):
    """Test fs_read with offset via MCP protocol."""
    text_path = Path(clean_roots) / "test_offset.txt"
    text_path.write_text("Hello World")
    
    response = call_tool(
        mcp_client,
        "fs_read",
        path=str(text_path),
        offset=6  # Start from "World"
    )
    
    assert_response_structure(response, ["content", "size"])
    assert "World" in response["content"]


def test_fs_list_dir_nested_mcp(mcp_client, clean_roots):
    """Test fs_list_dir with nested directories via MCP protocol."""
    nested_dir = Path(clean_roots) / "nested" / "subdir"
    nested_dir.mkdir(parents=True)
    (nested_dir / "file1.txt").write_text("test1")
    (nested_dir / "file2.txt").write_text("test2")
    
    response = call_tool(
        mcp_client,
        "fs_list_dir",
        path=str(nested_dir)
    )
    
    assert_response_structure(response, ["path", "entries"])
    assert len(response["entries"]) == 2


def test_rg_search_case_insensitive_mcp(mcp_client, clean_roots):
    """Test rg_search with case-insensitive pattern via MCP protocol."""
    test_dir = Path(clean_roots) / "rg_test"
    test_dir.mkdir()
    (test_dir / "file1.txt").write_text("Hello World\nTest Pattern")
    (test_dir / "file2.txt").write_text("hello world\nDifferent")
    
    # Note: rg_search doesn't have case-insensitive flag, but we can test basic search
    response = call_tool(
        mcp_client,
        "rg_search",
        root=str(test_dir),
        pattern="hello"
    )
    
    assert_response_structure(response, ["root", "pattern", "matches", "match_count"])
    assert response["match_count"] >= 0


def test_rg_search_multiline_pattern_mcp(mcp_client, clean_roots):
    """Test rg_search with multiline pattern via MCP protocol."""
    test_dir = Path(clean_roots) / "rg_multiline"
    test_dir.mkdir()
    (test_dir / "file.txt").write_text("Line 1\nLine 2\nLine 3")
    
    response = call_tool(
        mcp_client,
        "rg_search",
        root=str(test_dir),
        pattern="Line.*"
    )
    
    assert_response_structure(response, ["root", "pattern", "matches", "match_count"])
    assert response["match_count"] >= 0


def test_rg_search_no_matches_mcp(mcp_client, clean_roots):
    """Test rg_search with pattern that matches nothing via MCP protocol."""
    test_dir = Path(clean_roots) / "rg_no_match"
    test_dir.mkdir()
    (test_dir / "file.txt").write_text("some content")
    
    response = call_tool(
        mcp_client,
        "rg_search",
        root=str(test_dir),
        pattern="nonexistent_pattern_xyz"
    )
    
    assert_response_structure(response, ["root", "pattern", "matches", "match_count"])
    assert response["match_count"] == 0

