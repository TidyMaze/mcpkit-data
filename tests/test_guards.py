"""Tests for guards module."""

import os
from pathlib import Path

import pytest

from mcpkit.core.guards import (
    GuardError,
    check_filename_safe,
    check_path_allowed,
    get_allowed_roots,
    get_max_output_bytes,
    get_max_records,
    get_max_rows,
    get_timeout_secs,
    validate_jdbc_query,
)


def test_get_timeout_secs_default():
    """Test default timeout."""
    assert get_timeout_secs() == 15


def test_get_timeout_secs_env(monkeypatch):
    """Test timeout from env."""
    monkeypatch.setenv("MCPKIT_TIMEOUT_SECS", "30")
    assert get_timeout_secs() == 30


def test_get_max_output_bytes_default():
    """Test default max output bytes."""
    assert get_max_output_bytes() == 1000000


def test_get_max_rows_default():
    """Test default max rows."""
    assert get_max_rows() == 500


def test_get_max_records_default():
    """Test default max records."""
    assert get_max_records() == 500


def test_get_allowed_roots_default(monkeypatch):
    """Test default allowed roots."""
    monkeypatch.delenv("MCPKIT_ROOTS", raising=False)
    roots = get_allowed_roots()
    assert len(roots) == 1
    assert roots[0] == Path.cwd().resolve()


def test_get_allowed_roots_env(monkeypatch, tmp_path):
    """Test allowed roots from env."""
    root1 = tmp_path / "root1"
    root2 = tmp_path / "root2"
    root1.mkdir()
    root2.mkdir()
    
    sep = os.pathsep
    monkeypatch.setenv("MCPKIT_ROOTS", f"{root1}{sep}{root2}")
    roots = get_allowed_roots()
    assert len(roots) == 2
    assert root1.resolve() in roots
    assert root2.resolve() in roots


def test_check_path_allowed_success(tmp_path, monkeypatch):
    """Test path check success."""
    allowed_file = tmp_path / "test.txt"
    allowed_file.write_text("test")
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    resolved = check_path_allowed(str(allowed_file))
    assert resolved == allowed_file.resolve()


def test_check_path_allowed_failure(tmp_path, monkeypatch):
    """Test path check failure."""
    other_dir = tmp_path.parent / "other"
    other_file = other_dir / "test.txt"
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    with pytest.raises(GuardError, match="outside allowed roots"):
        check_path_allowed(str(other_file))


def test_check_path_allowed_traversal(tmp_path, monkeypatch):
    """Test path traversal rejection."""
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    with pytest.raises(GuardError, match="traversal"):
        check_path_allowed("../other")


def test_check_filename_safe_success():
    """Test safe filename."""
    assert check_filename_safe("test.txt") == "test.txt"
    assert check_filename_safe("dataset_123") == "dataset_123"


def test_check_filename_safe_failure():
    """Test unsafe filename."""
    with pytest.raises(GuardError):
        check_filename_safe("test/file.txt")
    with pytest.raises(GuardError):
        check_filename_safe("../test.txt")
    with pytest.raises(GuardError):
        check_filename_safe("test\\file.txt")


def test_validate_jdbc_query_select():
    """Test valid SELECT query."""
    query = validate_jdbc_query("SELECT * FROM table")
    assert query == "SELECT * FROM table"


def test_validate_jdbc_query_with():
    """Test valid WITH query."""
    query = validate_jdbc_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
    assert "WITH" in query


def test_validate_jdbc_query_explain():
    """Test valid EXPLAIN query."""
    query = validate_jdbc_query("EXPLAIN SELECT * FROM table")
    assert query.startswith("EXPLAIN")


def test_validate_jdbc_query_semicolon():
    """Test semicolon rejection."""
    with pytest.raises(GuardError, match="semicolon"):
        validate_jdbc_query("SELECT 1; SELECT 2")


def test_validate_jdbc_query_drop():
    """Test DROP rejection."""
    with pytest.raises(GuardError, match="DROP"):
        validate_jdbc_query("DROP TABLE test")


def test_validate_jdbc_query_insert():
    """Test INSERT rejection."""
    with pytest.raises(GuardError, match="INSERT"):
        validate_jdbc_query("INSERT INTO test VALUES (1)")


def test_validate_jdbc_query_update():
    """Test UPDATE rejection."""
    with pytest.raises(GuardError, match="UPDATE"):
        validate_jdbc_query("UPDATE test SET col=1")


def test_validate_jdbc_query_delete():
    """Test DELETE rejection."""
    with pytest.raises(GuardError, match="DELETE"):
        validate_jdbc_query("DELETE FROM test")


def test_validate_jdbc_query_invalid_start():
    """Test invalid query start."""
    with pytest.raises(GuardError):
        validate_jdbc_query("SHOW TABLES")

