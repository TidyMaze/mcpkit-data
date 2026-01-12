"""Integration tests for guards via MCP protocol."""

import pytest

from tests.utils_mcp import call_tool

pytestmark = pytest.mark.integration


def test_path_traversal_guard_mcp(mcp_client, clean_roots):
    """Test path traversal guard via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "fs_read",
            path="../outside_file.txt"
        )


def test_filename_safety_guard_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test filename safety guard via MCP protocol."""
    from tests.utils_mcp import create_test_dataset
    create_test_dataset(mcp_client, "test_export", ["id"], [[1]])
    
    # Try to export with unsafe filename
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "pandas_export",
            dataset_id="test_export",
            format="csv",
            filename="../unsafe_file.csv"
        )


def test_sql_readonly_guard_mcp(mcp_client):
    """Test SQL readonly guard via MCP protocol."""
    # Try to execute dangerous SQL
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "duckdb_query_local",
            sql="DROP TABLE test"
        )


def test_sql_multiple_statements_guard_mcp(mcp_client):
    """Test SQL multiple statements guard via MCP protocol."""
    # Test that DuckDB works with single statement
    from tests.utils_mcp import assert_response_structure
    response = call_tool(
        mcp_client,
        "duckdb_query_local",
        sql="SELECT 1 as x"
    )
    assert_response_structure(response, ["columns", "rows"])
    
    # Test JDBC guard (if configured) rejects semicolons
    import os
    if os.getenv("MCPKIT_JDBC_URL"):
        with pytest.raises(Exception):  # Should raise GuardError
            call_tool(
                mcp_client,
                "db_query_ro",
                query="SELECT 1; SELECT 2"
            )
    else:
        # Test with Athena (uses same guard)
        with pytest.raises(Exception):  # Should raise GuardError
            call_tool(
                mcp_client,
                "athena_start_query",
                sql="SELECT 1; SELECT 2",
                database="default"
            )


def test_path_outside_roots_mcp(mcp_client, clean_roots):
    """Test path outside allowed roots guard via MCP protocol."""
    import tempfile
    import os
    
    # Create a file outside allowed roots
    with tempfile.NamedTemporaryFile(delete=False) as f:
        outside_path = f.name
    
    try:
        with pytest.raises(Exception):  # Should raise GuardError
            call_tool(
                mcp_client,
                "fs_read",
                path=outside_path
            )
    finally:
        os.unlink(outside_path)


def test_sql_dangerous_keywords_guard_mcp(mcp_client):
    """Test SQL dangerous keywords guard via MCP protocol."""
    import os
    # DuckDB doesn't validate SQL - it allows DDL. Test with JDBC/Athena instead
    if os.getenv("MCPKIT_JDBC_URL"):
        dangerous_queries = [
            "DROP TABLE test",
            "INSERT INTO test VALUES (1)",
            "UPDATE test SET x=1",
            "DELETE FROM test",
            "ALTER TABLE test ADD COLUMN x",
            "CREATE TABLE test (x INT)",
            "TRUNCATE TABLE test",
        ]
        
        for query in dangerous_queries:
            with pytest.raises(Exception):  # Should raise GuardError
                call_tool(
                    mcp_client,
                    "db_query_ro",
                    query=query
                )
    else:
        # Test with Athena (uses same guard)
        dangerous_queries = ["DROP TABLE test", "INSERT INTO test VALUES (1)"]
        for query in dangerous_queries:
            with pytest.raises(Exception):  # Should raise GuardError
                call_tool(
                    mcp_client,
                    "athena_start_query",
                    sql=query,
                    database="default"
                )


def test_sql_invalid_start_guard_mcp(mcp_client):
    """Test SQL invalid start guard via MCP protocol."""
    import os
    # DuckDB doesn't validate SQL start - it allows SHOW. Test with JDBC/Athena instead
    if os.getenv("MCPKIT_JDBC_URL"):
        # Query that doesn't start with SELECT/WITH/EXPLAIN
        with pytest.raises(Exception):  # Should raise GuardError
            call_tool(
                mcp_client,
                "db_query_ro",
                query="SHOW TABLES"
            )
    else:
        # Test with Athena (uses same guard)
        with pytest.raises(Exception):  # Should raise GuardError
            call_tool(
                mcp_client,
                "athena_start_query",
                sql="SHOW TABLES",
                database="default"
            )

