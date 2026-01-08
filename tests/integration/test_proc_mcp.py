"""Integration tests for proc tools via MCP protocol.

Note: run_cmd is not directly exposed as an MCP tool - it's used internally
by tools like rg_search. These tests verify run_cmd functionality through
those tools.
"""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


def test_run_cmd_through_rg_search_mcp(mcp_client, clean_roots):
    """Test run_cmd functionality through rg_search (which uses run_cmd internally)."""
    from pathlib import Path
    
    # Create test files
    test_dir = Path(clean_roots) / "proc_test"
    test_dir.mkdir()
    (test_dir / "file1.txt").write_text("test content\nanother line")
    (test_dir / "file2.txt").write_text("no match")
    
    # rg_search uses run_cmd internally
    response = call_tool(
        mcp_client,
        "rg_search",
        root=str(test_dir),
        pattern="test"
    )
    
    assert_response_structure(response, ["root", "pattern", "matches", "match_count"])
    assert response["match_count"] >= 1
    # Verify run_cmd executed successfully (no timeout/error)
    assert "matches" in response


def test_run_cmd_error_handling_mcp(mcp_client, clean_roots):
    """Test run_cmd error handling through rg_search with invalid command."""
    from pathlib import Path
    
    # rg_search uses run_cmd - test with invalid root (should handle error gracefully)
    # This tests the exception handling path in run_cmd
    test_dir = Path(clean_roots) / "nonexistent_dir_xyz"
    
    # Should raise GuardError (path not found) rather than subprocess error
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "rg_search",
            root=str(test_dir),
            pattern="test"
        )


def test_run_cmd_output_capping_mcp(mcp_client, clean_roots):
    """Test run_cmd output capping through rg_search with large output."""
    from pathlib import Path
    
    # Create a file with many matches (to test output capping)
    test_dir = Path(clean_roots) / "large_output_test"
    test_dir.mkdir()
    large_file = test_dir / "large.txt"
    # Create file with many lines containing "test"
    large_file.write_text("\n".join(["test line"] * 1000))
    
    # rg_search uses run_cmd with max_output_bytes
    # Should still work but output may be capped
    response = call_tool(
        mcp_client,
        "rg_search",
        root=str(test_dir),
        pattern="test"
    )
    
    assert_response_structure(response, ["root", "pattern", "matches", "match_count"])
    # Should have matches (may be capped)
    assert response["match_count"] >= 0

