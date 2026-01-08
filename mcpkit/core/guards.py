"""Security guards and limits enforcement."""

import os
from pathlib import Path
from typing import Optional


class GuardError(ValueError):
    """Raised when guardrails are violated."""


def get_timeout_secs() -> int:
    """Get timeout limit from env, default 15."""
    return int(os.getenv("MCPKIT_TIMEOUT_SECS", "15"))


def get_max_output_bytes() -> int:
    """Get max output bytes from env, default 1_000_000."""
    return int(os.getenv("MCPKIT_MAX_OUTPUT_BYTES", "1000000"))


def get_max_rows() -> int:
    """Get max rows limit from env, default 500."""
    return int(os.getenv("MCPKIT_MAX_ROWS", "500"))


def get_max_records() -> int:
    """Get max records limit from env, default 500."""
    return int(os.getenv("MCPKIT_MAX_RECORDS", "500"))


def get_allowed_roots() -> list[Path]:
    """Get allowed filesystem roots from env."""
    roots_str = os.getenv("MCPKIT_ROOTS")
    if roots_str:
        return [Path(r).resolve() for r in roots_str.split(os.pathsep)]
    # Default to current working directory
    return [Path.cwd().resolve()]


def check_path_allowed(path: str) -> Path:
    """
    Check if path is within allowed roots.
    Raises GuardError if not allowed.
    Returns resolved Path.
    """
    roots = get_allowed_roots()
    # Resolve relative paths to absolute
    resolved = Path(path).resolve()
    
    # Check for traversal attempts in the original path string
    if ".." in path:
        raise GuardError(f"Path contains traversal: {path}")
    
    # Check if within any allowed root
    for root in roots:
        try:
            resolved.relative_to(root)
            return resolved
        except ValueError:
            continue
    
    raise GuardError(f"Path {path} is outside allowed roots: {[str(r) for r in roots]}")


def check_filename_safe(filename: str) -> str:
    """
    Check filename is safe (no slashes, no traversal).
    Raises GuardError if unsafe.
    Returns filename.
    """
    if "/" in filename or "\\" in filename or ".." in filename:
        raise GuardError(f"Filename contains invalid characters: {filename}")
    return filename


def cap_rows(rows: list, max_rows: Optional[int] = None) -> list:
    """Cap rows to max_rows limit."""
    if max_rows is None:
        max_rows = get_max_rows()
    return rows[:max_rows]


def cap_records(records: list, max_records: Optional[int] = None) -> list:
    """Cap records to max_records limit."""
    if max_records is None:
        max_records = get_max_records()
    return records[:max_records]


def cap_bytes(content: bytes, max_bytes: Optional[int] = None) -> bytes:
    """Cap bytes to max_bytes limit."""
    if max_bytes is None:
        max_bytes = get_max_output_bytes()
    if len(content) > max_bytes:
        return content[:max_bytes]
    return content


def validate_sql_readonly(query: str) -> str:
    """
    Validate SQL query is read-only (for JDBC, Athena, etc.).
    Raises GuardError if not read-only.
    Returns normalized query.
    """
    query = query.strip()
    
    # Reject multiple statements
    if ";" in query:
        raise GuardError("Multiple statements not allowed (semicolon detected)")
    
    # Check for dangerous keywords (case-insensitive)
    upper_query = query.upper()
    dangerous = ["DROP", "INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE"]
    for keyword in dangerous:
        # Check for keyword as standalone word (not part of another word)
        if f" {keyword} " in upper_query or upper_query.startswith(f"{keyword} "):
            raise GuardError(f"Dangerous SQL keyword detected: {keyword}")
    
    # Allow SELECT and WITH, and EXPLAIN SELECT/WITH
    if upper_query.startswith("SELECT ") or upper_query.startswith("WITH ") or \
       upper_query.startswith("EXPLAIN SELECT ") or upper_query.startswith("EXPLAIN WITH "):
        return query
    
    raise GuardError("Query must start with SELECT, WITH, EXPLAIN SELECT, or EXPLAIN WITH")


def validate_db_query(query: str) -> str:
    """
    Validate database query is read-only.
    Raises GuardError if not read-only.
    Returns normalized query.
    """
    return validate_sql_readonly(query)

