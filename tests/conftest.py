"""Pytest configuration and fixtures."""

import atexit

# Import fixtures from conftest_mcp.py
from tests.conftest_mcp import *  # noqa: F403, F401


def pytest_configure(config):
    """Configure pytest to combine subprocess coverage."""
    # Register coverage combine to run at exit
    def combine_coverage():
        try:
            import coverage
            cov = coverage.Coverage()
            cov.combine()
            cov.save()
        except Exception:
            pass

    atexit.register(combine_coverage)
