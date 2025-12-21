"""Pytest configuration and fixtures."""

import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_env(monkeypatch):
    """Mock environment variables for testing."""
    def _set_env(**kwargs):
        for key, value in kwargs.items():
            monkeypatch.setenv(key, value)
    return _set_env


@pytest.fixture
def clean_registry(monkeypatch, temp_dir):
    """Clean dataset registry for testing."""
    dataset_dir = temp_dir / "datasets"
    dataset_dir.mkdir()
    monkeypatch.setenv("MCPKIT_DATASET_DIR", str(dataset_dir))
    yield dataset_dir


@pytest.fixture
def clean_artifacts(monkeypatch, temp_dir):
    """Clean artifact directory for testing."""
    artifact_dir = temp_dir / "artifacts"
    artifact_dir.mkdir()
    monkeypatch.setenv("MCPKIT_ARTIFACT_DIR", str(artifact_dir))
    yield artifact_dir

