"""Test parameter validation for list[dict] parameters."""

import pytest

from mcpkit.server import _parse_list_param


def test_parse_list_param_with_list():
    """Test parsing when input is already a list."""
    result = _parse_list_param([{"key": "value"}], None)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["key"] == "value"


def test_parse_list_param_with_json_string():
    """Test parsing JSON string."""
    result = _parse_list_param('[{"key": "value"}]', None)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["key"] == "value"


def test_parse_list_param_with_none():
    """Test parsing None."""
    result = _parse_list_param(None, [])
    assert result == []


def test_parse_list_param_with_empty_string():
    """Test parsing empty string."""
    result = _parse_list_param("", None)
    assert result is None


def test_parse_list_param_with_null_string():
    """Test parsing 'null' string."""
    result = _parse_list_param("null", None)
    assert result is None


def test_parse_list_param_with_invalid_json():
    """Test parsing invalid JSON."""
    from mcpkit.core.guards import GuardError
    # Invalid JSON should raise GuardError
    with pytest.raises(GuardError):
        _parse_list_param("not json", None)


def test_parse_list_param_with_nested_lists():
    """Test parsing nested lists (for batches)."""
    result = _parse_list_param('[[{"id": 1}], [{"id": 2}]]', None)
    assert isinstance(result, list)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert result[0][0]["id"] == 1


def test_parse_list_param_with_complex_dicts():
    """Test parsing complex dicts (for filters)."""
    filters_json = '[{"column": "age", "op": ">", "value": 18}, {"column": "name", "op": "==", "value": "John"}]'
    result = _parse_list_param(filters_json, None)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["column"] == "age"
    assert result[0]["op"] == ">"
    assert result[0]["value"] == 18

