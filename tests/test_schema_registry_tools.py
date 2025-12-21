"""Tests for Schema Registry and decode tools."""

from unittest.mock import patch

import pytest
import responses

from mcpkit.core.guards import GuardError
from mcpkit.core.schema_registry import get_schema_registry_url, schema_registry_get


def test_get_schema_registry_url_missing(monkeypatch):
    """Test missing Schema Registry URL."""
    monkeypatch.delenv("MCPKIT_SCHEMA_REGISTRY_URL", raising=False)
    with pytest.raises(GuardError, match="MCPKIT_SCHEMA_REGISTRY_URL"):
        get_schema_registry_url()


@responses.activate
def test_schema_registry_get_by_id(monkeypatch):
    """Test getting schema by ID."""
    monkeypatch.setenv("MCPKIT_SCHEMA_REGISTRY_URL", "http://localhost:8081")
    
    responses.add(
        responses.GET,
        "http://localhost:8081/schemas/ids/1",
        json={"schema": '{"type": "string"}'},
        status=200,
    )
    
    result = schema_registry_get(schema_id=1)
    assert "schema_id" in result
    assert result["schema_id"] == 1


@responses.activate
def test_schema_registry_get_by_subject(monkeypatch):
    """Test getting schema by subject."""
    monkeypatch.setenv("MCPKIT_SCHEMA_REGISTRY_URL", "http://localhost:8081")
    
    responses.add(
        responses.GET,
        "http://localhost:8081/subjects/test-subject/versions/latest",
        json={
            "id": 1,
            "subject": "test-subject",
            "version": 1,
            "schema": '{"type": "string"}',
        },
        status=200,
    )
    
    result = schema_registry_get(subject="test-subject")
    assert "subject" in result
    assert result["subject"] == "test-subject"


def test_schema_registry_get_no_args():
    """Test schema_registry_get with no args."""
    with pytest.raises(GuardError):
        schema_registry_get()


def test_schema_registry_get_both_args():
    """Test schema_registry_get with both args."""
    with pytest.raises(GuardError):
        schema_registry_get(schema_id=1, subject="test")

