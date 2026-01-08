"""Tests for database tools."""

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.db import db_introspect, db_query_ro


@pytest.fixture
def setup_test_db(monkeypatch):
    """Set up test database with tables and data."""
    monkeypatch.setenv("MCPKIT_DB_URL", "postgresql://testuser:testpass@localhost:5432/testdb")
    
    # Use direct psycopg2 connection for setup (bypasses validation)
    try:
        import psycopg2
        conn = psycopg2.connect("postgresql://testuser:testpass@localhost:5432/testdb")
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER, col2 VARCHAR(10))")
        cursor.execute("DELETE FROM test_table")
        cursor.execute("INSERT INTO test_table VALUES (1, 'a'), (2, 'b')")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pytest.skip("PostgreSQL not available or setup failed")


def test_db_query_ro_happy(setup_test_db):
    """Test read-only query happy path with real PostgreSQL."""
    result = db_query_ro("SELECT * FROM test_table")
    assert "columns" in result
    assert "rows" in result
    assert len(result["rows"]) == 2
    # Check column names
    assert "col1" in result["columns"] or len(result["columns"]) == 2


def test_db_query_ro_invalid_query():
    """Test read-only query with invalid query."""
    with pytest.raises(GuardError, match="DROP"):
        db_query_ro("DROP TABLE test")


def test_db_query_ro_semicolon():
    """Test read-only query with semicolon."""
    with pytest.raises(GuardError, match="semicolon"):
        db_query_ro("SELECT 1; SELECT 2")


def test_db_introspect_real(setup_test_db):
    """Test database introspect with real PostgreSQL."""
    result = db_introspect()
    assert "tables" in result
    assert len(result["tables"]) > 0
    # Should find our test table
    table_names = [t["table"] for t in result["tables"]]
    assert "test_table" in table_names

