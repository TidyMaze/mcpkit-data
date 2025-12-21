"""Safe subprocess runner."""

import json
import subprocess
from typing import Optional

from .guards import GuardError, cap_bytes, get_timeout_secs


def run_cmd(
    cmd: list[str],
    timeout_secs: Optional[int] = None,
    max_output_bytes: Optional[int] = None
) -> dict:
    """
    Run command safely with timeout and output limits.
    Returns dict with stdout, stderr, returncode.
    """
    if timeout_secs is None:
        timeout_secs = get_timeout_secs()
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=timeout_secs,
            text=False,  # Capture as bytes for size control
        )
        
        stdout = result.stdout
        stderr = result.stderr
        
        if max_output_bytes:
            stdout = cap_bytes(stdout, max_output_bytes)
            stderr = cap_bytes(stderr, max_output_bytes)
        
        return {
            "stdout": stdout.decode("utf-8", errors="replace"),
            "stderr": stderr.decode("utf-8", errors="replace"),
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        raise GuardError(f"Command timed out after {timeout_secs}s")
    except Exception as e:
        raise GuardError(f"Command failed: {e}")

