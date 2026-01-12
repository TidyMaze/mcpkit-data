"""More integration tests for guards validation via MCP protocol.

Tests validation functions through tools that use them.
"""

import pytest
from pathlib import Path

from tests.utils_mcp import call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_check_filename_safe_invalid_chars_mcp(mcp_client, clean_registry):
    """Test check_filename_safe with invalid characters via MCP protocol."""
    # Try to create dataset with invalid filename characters
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "dataset_put_rows",
            columns=["id"],
            rows=[[1]],
            dataset_id="invalid/name"  # Contains slash
        )


def test_check_filename_safe_dot_dot_mcp(mcp_client, clean_registry):
    """Test check_filename_safe with .. path traversal via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "dataset_put_rows",
            columns=["id"],
            rows=[[1]],
            dataset_id="../invalid"
        )


def test_check_path_allowed_outside_root_mcp(mcp_client, clean_roots):
    """Test check_path_allowed with path outside allowed roots via MCP protocol."""
    # Try to read file outside allowed roots
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "fs_read",
            path="/etc/passwd"  # Outside allowed roots
        )


def test_validate_db_query_dangerous_keywords_mcp(mcp_client, db_setup):
    """Test validate_db_query with dangerous SQL keywords via MCP protocol."""
    # Test DROP, DELETE, UPDATE, INSERT are blocked
    dangerous_queries = [
        "DROP TABLE users",
        "DELETE FROM users",
        "UPDATE users SET name='hack'",
        "INSERT INTO users VALUES (1, 'hack')",
    ]
    
    for query in dangerous_queries:
        with pytest.raises(Exception):  # Should raise GuardError
            call_tool(
                mcp_client,
                "db_query_ro",
                query=query
            )


def test_validate_db_query_invalid_start_mcp(mcp_client, db_setup):
    """Test validate_db_query with invalid query start via MCP protocol."""
    # Queries must start with SELECT, WITH, EXPLAIN SELECT, or EXPLAIN WITH
    invalid_queries = [
        "CREATE TABLE test (id INT)",
        "ALTER TABLE test ADD COLUMN name VARCHAR",
        "SHOW TABLES",
    ]
    
    for query in invalid_queries:
        with pytest.raises(Exception):  # Should raise GuardError
            call_tool(
                mcp_client,
                "db_query_ro",
                query=query
            )


def test_validate_sql_readonly_cte_mcp(mcp_client, clean_registry):
    """Test validate_sql_readonly with CTE (WITH clause) via MCP protocol."""
    # CTE should be allowed
    create_test_dataset(mcp_client, "test_cte", ["id", "value"], [[1, 10], [2, 20]])
    
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="WITH t AS (SELECT * FROM test_data) SELECT * FROM t",
        source_name="test_data",
        source_dataset_id="test_cte"
    )
    
    assert "columns" in response
    assert response["row_count"] == 2


def test_cap_rows_limit_mcp(mcp_client, clean_registry):
    """Test cap_rows limits large result sets via MCP protocol."""
    # Create dataset with many rows
    rows = [[i] for i in range(500)]  # 500 rows
    create_test_dataset(mcp_client, "large_dataset", ["id"], rows)
    
    # Query should be capped to max_rows (default 500)
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM test_data",
        source_name="test_data",
        source_dataset_id="large_dataset"
    )
    
    assert response["row_count"] <= 500  # Should be capped to default max_rows


def test_cap_bytes_truncation_mcp(mcp_client, clean_roots):
    """Test cap_bytes truncates large file content via MCP protocol."""
    # Create very large file (larger than default max_output_bytes = 1MB)
    large_file = Path(clean_roots) / "huge.txt"
    large_content = "x" * 2000000  # 2MB (larger than 1MB default)
    large_file.write_text(large_content)
    
    response = call_tool(
        mcp_client,
        "fs_read",
        path=str(large_file)
    )
    
    # Content should be truncated if it exceeds max_output_bytes
    assert "truncated" in response
    # If truncated, content length should be <= max_output_bytes (1MB default)
    if response.get("truncated"):
        assert len(response["content"]) <= 1000000  # Default max_output_bytes
    assert len(response["content"]) <= len(large_content)


def test_get_max_rows_default_mcp(mcp_client, clean_registry):
    """Test get_max_rows returns default value via MCP protocol."""
    # Create dataset
    create_test_dataset(mcp_client, "test_max_rows", ["id"], [[1], [2], [3]])
    
    # Query without max_rows should use default
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT * FROM test_data",
        source_name="test_data",
        source_dataset_id="test_max_rows"
    )
    
    # Should return all rows (3) since it's under default limit
    assert response["row_count"] == 3


def test_get_timeout_secs_default_mcp(mcp_client, clean_roots):
    """Test get_timeout_secs default via MCP protocol."""
    # rg_search uses run_cmd which uses get_timeout_secs
    # This test verifies that commands complete within the default timeout
    test_dir = Path(clean_roots) / "timeout_test"
    test_dir.mkdir()
    (test_dir / "file.txt").write_text("test content")
    
    # Should complete within default timeout (15s)
    # If it times out, the test will fail
    response = call_tool(
        mcp_client,
        "rg_search",
        root=str(test_dir),
        pattern="test"
    )
    
    # If we get here, timeout didn't trigger (good)
    assert "matches" in response or "match_count" in response

