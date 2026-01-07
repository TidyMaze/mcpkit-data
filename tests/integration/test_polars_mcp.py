"""Integration tests for polars tools via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


def test_polars_from_rows_mcp(mcp_client, clean_registry):
    """Test polars_from_rows via MCP protocol."""
    response = call_tool(
        mcp_client,
        "polars_from_rows",
        columns=["id", "value"],
        rows=[[1, 10.5], [2, 20.3], [3, 30.1]],
        dataset_id="test_polars"
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["dataset_id"] == "test_polars"
    assert response["rows"] == 3
    assert response["columns"] == ["id", "value"]


def test_polars_groupby_mcp(mcp_client, clean_registry):
    """Test polars_groupby via MCP protocol."""
    from tests.utils_mcp import create_test_dataset
    create_test_dataset(
        mcp_client,
        "test_polars_groupby",
        ["category", "value"],
        [["A", 10], ["A", 20], ["B", 30], ["B", 40]]
    )
    
    response = call_tool(
        mcp_client,
        "polars_groupby",
        dataset_id="test_polars_groupby",
        group_cols=["category"],
        aggs={"value": ["sum", "mean"]}
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 2  # Two categories


def test_polars_export_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test polars_export via MCP protocol."""
    from tests.utils_mcp import create_test_dataset
    create_test_dataset(mcp_client, "test_polars_export", ["id", "value"], [[1, 10], [2, 20]])
    
    response = call_tool(
        mcp_client,
        "polars_export",
        dataset_id="test_polars_export",
        format="csv",
        filename="test_polars_export.csv"
    )
    
    assert_response_structure(response, ["dataset_id", "format", "filename", "path"])
    assert response["format"] == "csv"
    assert response["filename"] == "test_polars_export.csv"

