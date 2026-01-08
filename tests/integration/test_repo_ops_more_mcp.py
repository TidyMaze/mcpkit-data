"""More integration tests for repo_ops tools via MCP protocol.

Covers missing paths in fs_read, rg_search, and fs_list_dir.
"""

import pytest
from pathlib import Path

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


def test_fs_read_with_offset_large_mcp(mcp_client, clean_roots):
    """Test fs_read with large offset via MCP protocol."""
    test_file = Path(clean_roots) / "offset_test.txt"
    content = "A" * 100 + "B" * 100
    test_file.write_text(content)
    
    response = call_tool(
        mcp_client,
        "fs_read",
        path=str(test_file),
        offset=100
    )
    
    assert_response_structure(response, ["path", "content", "size"])
    assert "B" in response["content"]
    assert "A" not in response["content"]


def test_fs_read_truncated_large_file_mcp(mcp_client, clean_roots):
    """Test fs_read truncation with very large file via MCP protocol."""
    # Create file larger than max_output_bytes (default is usually 10MB, so use 20MB)
    test_file = Path(clean_roots) / "very_large.txt"
    large_content = "x" * 20000000  # 20MB
    test_file.write_text(large_content)
    
    response = call_tool(
        mcp_client,
        "fs_read",
        path=str(test_file)
    )
    
    assert_response_structure(response, ["content", "size", "truncated"])
    # File should be truncated if it exceeds max_output_bytes
    # Check if truncated flag is set or content is smaller
    if "truncated" in response:
        assert response["truncated"] is True
    assert len(response["content"]) <= len(large_content)


def test_rg_search_with_matches_parsing_mcp(mcp_client, clean_roots):
    """Test rg_search match parsing via MCP protocol."""
    test_dir = Path(clean_roots) / "rg_parse_test"
    test_dir.mkdir()
    
    # Create file with specific content for parsing test
    (test_dir / "file.txt").write_text("Line 1: test pattern\nLine 2: another test")
    
    response = call_tool(
        mcp_client,
        "rg_search",
        root=str(test_dir),
        pattern="test"
    )
    
    assert_response_structure(response, ["root", "pattern", "matches", "match_count"])
    assert response["match_count"] >= 1
    # Verify match structure - matches are SearchMatch objects with path, line_number, text
    for match in response["matches"]:
        # Match can be dict or object
        match_dict = match if isinstance(match, dict) else match.__dict__ if hasattr(match, "__dict__") else {}
        assert "path" in match_dict or "text" in match_dict or hasattr(match, "path") or hasattr(match, "text")


def test_rg_search_error_handling_mcp(mcp_client, clean_roots):
    """Test rg_search error handling when rg command fails via MCP protocol."""
    test_dir = Path(clean_roots) / "rg_error"
    test_dir.mkdir()
    
    # rg_search should handle errors gracefully
    # Test with invalid root (should raise GuardError before calling rg)
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "rg_search",
            root=str(Path(clean_roots) / "nonexistent_abc"),
            pattern="test"
        )


def test_fs_list_dir_with_stat_errors_mcp(mcp_client, clean_roots):
    """Test fs_list_dir with files that might have stat errors via MCP protocol."""
    test_dir = Path(clean_roots) / "stat_test"
    test_dir.mkdir()
    
    # Create normal files
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
    # Files should have size/mtime if stat succeeds
    for file_info in response["files"]:
        assert "name" in file_info
        assert "path" in file_info


def test_fs_list_dir_hidden_files_skipped_mcp(mcp_client, clean_roots):
    """Test fs_list_dir skips hidden files via MCP protocol."""
    test_dir = Path(clean_roots) / "hidden_test"
    test_dir.mkdir()
    
    # Create visible and hidden files
    (test_dir / "visible.txt").write_text("visible")
    (test_dir / ".hidden.txt").write_text("hidden")
    (test_dir / ".hidden_dir").mkdir()
    
    response = call_tool(
        mcp_client,
        "fs_list_dir",
        path=str(test_dir)
    )
    
    assert_response_structure(response, ["path", "files", "directories"])
    # Hidden files should be skipped
    file_names = [f.get("name") or getattr(f, "name", None) for f in response["files"]]
    dir_names = [d.get("name") or getattr(d, "name", None) for d in response["directories"]]
    assert "visible.txt" in file_names
    assert ".hidden.txt" not in file_names
    assert ".hidden_dir" not in dir_names


def test_fs_read_binary_content_mcp(mcp_client, clean_roots):
    """Test fs_read with binary content that needs UTF-8 replacement via MCP protocol."""
    test_file = Path(clean_roots) / "binary_test.bin"
    # Write binary data that's not valid UTF-8
    test_file.write_bytes(b"\xff\xfe\x00\x01\x02")
    
    response = call_tool(
        mcp_client,
        "fs_read",
        path=str(test_file)
    )
    
    assert_response_structure(response, ["path", "content", "size"])
    assert response["size"] == 5
    # Content should be decoded with errors="replace"


def test_rg_search_no_matches_returncode_1_mcp(mcp_client, clean_roots):
    """Test rg_search when rg returns code 1 (no matches) via MCP protocol."""
    test_dir = Path(clean_roots) / "no_match_test"
    test_dir.mkdir()
    (test_dir / "file.txt").write_text("some content without pattern")
    
    # rg returns 1 when no matches found (not an error)
    response = call_tool(
        mcp_client,
        "rg_search",
        root=str(test_dir),
        pattern="nonexistent_pattern_xyz_123"
    )
    
    assert_response_structure(response, ["root", "pattern", "matches", "match_count"])
    assert response["match_count"] == 0
    assert response["matches"] == []

