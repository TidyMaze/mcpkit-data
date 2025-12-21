"""Tests for repo tools."""

from pathlib import Path
from unittest.mock import patch

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.repo_ops import fs_read, rg_search


def test_fs_read_happy(monkeypatch, tmp_path):
    """Test file read happy path."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello world")
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    result = fs_read(str(test_file))
    assert "content" in result
    assert "hello world" in result["content"]


def test_fs_read_not_found(monkeypatch, tmp_path):
    """Test file read with non-existent file."""
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    with pytest.raises(GuardError, match="not found"):
        fs_read(str(tmp_path / "nonexistent.txt"))


def test_fs_read_path_not_allowed(monkeypatch, tmp_path):
    """Test file read with disallowed path."""
    allowed_dir = tmp_path / "allowed"
    allowed_dir.mkdir()
    other_file = tmp_path / "other.txt"
    other_file.write_text("test")
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(allowed_dir))
    
    with pytest.raises(GuardError, match="outside allowed roots"):
        fs_read(str(other_file))


@patch("mcpkit.core.repo_ops.run_cmd")
def test_rg_search_mock(mock_run_cmd, monkeypatch, tmp_path):
    """Test rg search with mocked subprocess."""
    monkeypatch.setenv("MCPKIT_ROOTS", str(tmp_path))
    
    mock_run_cmd.return_value = {
        "stdout": '{"type":"match","data":{"path":{"text":"test.txt"},"line_number":1,"lines":{"text":"hello"}}}\n',
        "stderr": "",
        "returncode": 0,
    }
    
    result = rg_search(str(tmp_path), "hello")
    assert "matches" in result
    assert result["match_count"] >= 0


def test_rg_search_path_not_allowed(monkeypatch, tmp_path):
    """Test rg search with disallowed path."""
    allowed_dir = tmp_path / "allowed"
    allowed_dir.mkdir()
    
    monkeypatch.setenv("MCPKIT_ROOTS", str(allowed_dir))
    
    with pytest.raises(GuardError, match="outside allowed roots"):
        rg_search(str(tmp_path.parent), "pattern")

