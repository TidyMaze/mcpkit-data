"""Integration tests for file format tools via MCP protocol."""

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_parquet_inspect_mcp(mcp_client, clean_registry, clean_roots):
    """Test parquet_inspect via MCP protocol."""
    # Create a parquet file
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
    parquet_path = Path(clean_roots) / "test.parquet"
    df.to_parquet(parquet_path)
    
    response = call_tool(
        mcp_client,
        "parquet_inspect",
        path=str(parquet_path),
        sample_rows=5
    )
    
    assert_response_structure(response, ["path", "num_rows", "schema", "sample_rows"])
    assert response["num_rows"] == 3
    assert len(response["schema"]["fields"]) == 2


def test_arrow_convert_mcp(mcp_client, clean_registry, clean_roots):
    """Test arrow_convert via MCP protocol."""
    # Create a CSV file
    csv_path = Path(clean_roots) / "test.csv"
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    df.to_csv(csv_path, index=False)
    
    response = call_tool(
        mcp_client,
        "arrow_convert",
        input_path=str(csv_path),
        input_format="CSV",
        output_format="PARQUET",
        output_filename="test_output.parquet"
    )
    
    assert_response_structure(response, ["input_path", "output_path", "rows"])
    assert response["rows"] == 3


def test_fs_read_mcp(mcp_client, clean_roots):
    """Test fs_read via MCP protocol."""
    # Create a test file
    test_file = Path(clean_roots) / "test.txt"
    test_file.write_text("Hello, World!\nThis is a test file.")
    
    response = call_tool(
        mcp_client,
        "fs_read",
        path=str(test_file)
    )
    
    assert_response_structure(response, ["path", "content", "size"])
    assert "Hello, World!" in response["content"]
    assert response["size"] > 0


def test_fs_read_with_offset_mcp(mcp_client, clean_roots):
    """Test fs_read with offset via MCP protocol."""
    test_file = Path(clean_roots) / "test_offset.txt"
    test_file.write_text("Hello, World!")
    
    response = call_tool(
        mcp_client,
        "fs_read",
        path=str(test_file),
        offset=7
    )
    
    assert_response_structure(response, ["content"])
    assert response["content"].startswith("World")


def test_fs_read_file_not_found_mcp(mcp_client, clean_roots):
    """Test fs_read with non-existent file via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "fs_read",
            path=str(Path(clean_roots) / "nonexistent.txt")
        )


def test_fs_read_not_a_file_mcp(mcp_client, clean_roots):
    """Test fs_read with directory path via MCP protocol."""
    test_dir = Path(clean_roots) / "test_dir"
    test_dir.mkdir()
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "fs_read",
            path=str(test_dir)
        )


def test_fs_read_truncated_mcp(mcp_client, clean_roots):
    """Test fs_read with large file that gets truncated via MCP protocol."""
    # Create a large file (larger than max_output_bytes)
    test_file = Path(clean_roots) / "large_file.txt"
    large_content = "x" * 2000000  # 2MB
    test_file.write_text(large_content)
    
    response = call_tool(
        mcp_client,
        "fs_read",
        path=str(test_file)
    )
    
    assert_response_structure(response, ["content", "size", "truncated"])
    assert response["truncated"] is True
    assert len(response["content"]) < len(large_content)


def test_fs_list_dir_not_found_mcp(mcp_client, clean_roots):
    """Test fs_list_dir with non-existent directory via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "fs_list_dir",
            path=str(Path(clean_roots) / "nonexistent_dir")
        )


def test_fs_list_dir_not_a_directory_mcp(mcp_client, clean_roots):
    """Test fs_list_dir with file path via MCP protocol."""
    test_file = Path(clean_roots) / "test_file.txt"
    test_file.write_text("content")
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "fs_list_dir",
            path=str(test_file)
        )


def test_rg_search_not_found_mcp(mcp_client, clean_roots):
    """Test rg_search with non-existent root via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "rg_search",
            root=str(Path(clean_roots) / "nonexistent"),
            pattern="test"
        )


def test_rg_search_no_matches_mcp(mcp_client, clean_roots):
    """Test rg_search with no matches via MCP protocol."""
    test_dir = Path(clean_roots) / "search_test_empty"
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
    assert response["matches"] == []


def test_fs_list_dir_mcp(mcp_client, clean_roots):
    """Test fs_list_dir via MCP protocol."""
    # Create some test files
    test_dir = Path(clean_roots) / "test_dir"
    test_dir.mkdir()
    (test_dir / "file1.txt").write_text("content1")
    (test_dir / "file2.txt").write_text("content2")
    (test_dir / "subdir").mkdir()
    
    response = call_tool(
        mcp_client,
        "fs_list_dir",
        path=str(test_dir)
    )
    
    assert_response_structure(response, ["path", "files", "directories", "file_count", "directory_count"])
    assert response["file_count"] == 2
    assert response["directory_count"] == 1


def test_rg_search_mcp(mcp_client, clean_roots):
    """Test rg_search via MCP protocol."""
    # Create test files with content
    test_dir = Path(clean_roots) / "search_test"
    test_dir.mkdir()
    (test_dir / "file1.txt").write_text("Hello World\nTest pattern\nAnother line")
    (test_dir / "file2.txt").write_text("No match here")
    
    response = call_tool(
        mcp_client,
        "rg_search",
        root=str(test_dir),
        pattern="pattern"
    )
    
    assert_response_structure(response, ["root", "pattern", "matches", "match_count"])
    assert response["match_count"] >= 1
    assert any("pattern" in m["text"].lower() for m in response["matches"])


def test_arrow_convert_json_mcp(mcp_client, clean_registry, clean_roots, clean_artifacts):
    """Test arrow_convert with JSON format via MCP protocol."""
    # Create a JSON file (JSONL format - one JSON object per line)
    json_path = Path(clean_roots) / "test.json"
    json_data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}]
    # Write as JSONL (one object per line) which pyarrow.json can read
    with json_path.open("w") as f:
        for item in json_data:
            f.write(json.dumps(item) + "\n")
    
    response = call_tool(
        mcp_client,
        "arrow_convert",
        input_path=str(json_path),
        input_format="JSON",
        output_format="CSV",
        output_filename="test_output.csv"
    )
    
    assert_response_structure(response, ["input_path", "output_path", "rows"])
    assert response["rows"] == 2


def test_arrow_convert_unsupported_format_mcp(mcp_client, clean_registry, clean_roots):
    """Test arrow_convert with unsupported format via MCP protocol."""
    csv_path = Path(clean_roots) / "test.csv"
    df = pd.DataFrame({"id": [1, 2]})
    df.to_csv(csv_path, index=False)
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "arrow_convert",
            input_path=str(csv_path),
            input_format="CSV",
            output_format="XML",  # Unsupported
            output_filename="test.xml"
        )

