"""Unit tests for proc.py."""

from unittest.mock import MagicMock, patch

import pytest

from mcpkit.core.guards import GuardError
from mcpkit.core.proc import run_cmd


def test_run_cmd_success():
    """Test run_cmd with successful command."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        mock_result = MagicMock()
        mock_result.stdout = b"output"
        mock_result.stderr = b""
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        
        result = run_cmd(["echo", "test"])
        
        assert result["stdout"] == "output"
        assert result["stderr"] == ""
        assert result["returncode"] == 0


def test_run_cmd_with_error():
    """Test run_cmd with command that returns error code."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        mock_result = MagicMock()
        mock_result.stdout = b""
        mock_result.stderr = b"error message"
        mock_result.returncode = 1
        mock_run.return_value = mock_result
        
        result = run_cmd(["false"])
        
        assert result["stdout"] == ""
        assert result["stderr"] == "error message"
        assert result["returncode"] == 1


def test_run_cmd_timeout():
    """Test run_cmd with timeout."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired("cmd", 5)
        
        with pytest.raises(GuardError, match="timed out"):
            run_cmd(["sleep", "10"], timeout_secs=5)


def test_run_cmd_custom_timeout():
    """Test run_cmd with custom timeout."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        mock_result = MagicMock()
        mock_result.stdout = b"output"
        mock_result.stderr = b""
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        
        result = run_cmd(["echo", "test"], timeout_secs=30)
        
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["timeout"] == 30


def test_run_cmd_max_output_bytes():
    """Test run_cmd caps output bytes."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        mock_result = MagicMock()
        mock_result.stdout = b"x" * 2000
        mock_result.stderr = b"y" * 2000
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        
        result = run_cmd(["echo", "test"], max_output_bytes=100)
        
        # Output should be capped
        assert len(result["stdout"].encode("utf-8")) <= 100
        assert len(result["stderr"].encode("utf-8")) <= 100


def test_run_cmd_utf8_decode_error():
    """Test run_cmd handles UTF-8 decode errors."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        mock_result = MagicMock()
        # Invalid UTF-8 sequence
        mock_result.stdout = b"\xff\xfe\xfd"
        mock_result.stderr = b""
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        
        result = run_cmd(["echo", "test"])
        
        # Should decode with errors="replace"
        assert isinstance(result["stdout"], str)
        assert result["returncode"] == 0


def test_run_cmd_exception():
    """Test run_cmd handles exceptions."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        mock_run.side_effect = Exception("Command not found")
        
        with pytest.raises(GuardError, match="Command failed"):
            run_cmd(["nonexistent_command"])


def test_run_cmd_default_timeout():
    """Test run_cmd uses default timeout from guards."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        with patch("mcpkit.core.proc.get_timeout_secs", return_value=15):
            mock_result = MagicMock()
            mock_result.stdout = b"output"
            mock_result.stderr = b""
            mock_result.returncode = 0
            mock_run.return_value = mock_result
            
            result = run_cmd(["echo", "test"])
            
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs["timeout"] == 15


def test_run_cmd_empty_output():
    """Test run_cmd with empty output."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        mock_result = MagicMock()
        mock_result.stdout = b""
        mock_result.stderr = b""
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        
        result = run_cmd(["true"])
        
        assert result["stdout"] == ""
        assert result["stderr"] == ""
        assert result["returncode"] == 0


def test_run_cmd_multiline_output():
    """Test run_cmd with multiline output."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        mock_result = MagicMock()
        mock_result.stdout = b"line1\nline2\nline3"
        mock_result.stderr = b""
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        
        result = run_cmd(["echo", "-e", "line1\nline2\nline3"])
        
        assert "line1" in result["stdout"]
        assert "line2" in result["stdout"]
        assert "line3" in result["stdout"]


def test_run_cmd_capture_output():
    """Test run_cmd captures both stdout and stderr."""
    with patch("mcpkit.core.proc.subprocess.run") as mock_run:
        mock_result = MagicMock()
        mock_result.stdout = b"stdout message"
        mock_result.stderr = b"stderr message"
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        
        result = run_cmd(["command"])
        
        # Verify subprocess.run was called with capture_output=True
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["capture_output"] is True
        assert call_kwargs["text"] is False  # Should capture as bytes
        
        assert result["stdout"] == "stdout message"
        assert result["stderr"] == "stderr message"

