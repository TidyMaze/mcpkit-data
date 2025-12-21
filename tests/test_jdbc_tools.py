"""Tests for JDBC tools."""

from unittest.mock import MagicMock, patch

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.jdbc import get_jdbc_config, jdbc_introspect, jdbc_query_ro


def test_get_jdbc_config_missing(monkeypatch):
    """Test missing JDBC config."""
    monkeypatch.delenv("MCPKIT_JDBC_DRIVER_CLASS", raising=False)
    with pytest.raises(GuardError):
        get_jdbc_config()


def test_get_jdbc_config_complete(monkeypatch):
    """Test complete JDBC config."""
    monkeypatch.setenv("MCPKIT_JDBC_DRIVER_CLASS", "org.postgresql.Driver")
    monkeypatch.setenv("MCPKIT_JDBC_URL", "jdbc:postgresql://localhost/test")
    monkeypatch.setenv("MCPKIT_JDBC_JARS", "/path/to/jar")
    monkeypatch.setenv("MCPKIT_JDBC_USER", "user")
    monkeypatch.setenv("MCPKIT_JDBC_PASSWORD", "pass")
    
    config = get_jdbc_config()
    assert config["driver"] == "org.postgresql.Driver"
    assert config["user"] == "user"


@patch("mcpkit.core.jdbc.jaydebeapi.connect")
def test_jdbc_query_ro_happy(mock_connect, monkeypatch):
    """Test read-only query happy path."""
    monkeypatch.setenv("MCPKIT_JDBC_DRIVER_CLASS", "org.postgresql.Driver")
    monkeypatch.setenv("MCPKIT_JDBC_URL", "jdbc:postgresql://localhost/test")
    monkeypatch.setenv("MCPKIT_JDBC_JARS", "/path/to/jar")
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("col1",), ("col2",)]
    mock_cursor.fetchall.return_value = [(1, "a"), (2, "b")]
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    result = jdbc_query_ro("SELECT * FROM test")
    assert "columns" in result
    assert "rows" in result
    assert result["columns"] == ["col1", "col2"]
    assert len(result["rows"]) == 2


def test_jdbc_query_ro_invalid_query():
    """Test read-only query with invalid query."""
    with pytest.raises(GuardError, match="DROP"):
        jdbc_query_ro("DROP TABLE test")


def test_jdbc_query_ro_semicolon():
    """Test read-only query with semicolon."""
    with pytest.raises(GuardError, match="semicolon"):
        jdbc_query_ro("SELECT 1; SELECT 2")


@patch("mcpkit.core.jdbc.jaydebeapi.connect")
def test_jdbc_introspect_mock(mock_connect, monkeypatch):
    """Test JDBC introspect with mock."""
    monkeypatch.setenv("MCPKIT_JDBC_DRIVER_CLASS", "org.postgresql.Driver")
    monkeypatch.setenv("MCPKIT_JDBC_URL", "jdbc:postgresql://localhost/test")
    monkeypatch.setenv("MCPKIT_JDBC_JARS", "/path/to/jar")
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("public", "table1", "col1", "integer", "YES"),
        ("public", "table1", "col2", "varchar", "NO"),
    ]
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    result = jdbc_introspect()
    assert "tables" in result
    assert len(result["tables"]) > 0

