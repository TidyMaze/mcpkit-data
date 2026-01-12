"""More integration tests for Polars operations via MCP protocol.

Covers missing paths in polars_ops.py.
"""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_polars_from_rows_empty_rows_mcp(mcp_client, clean_registry):
    """Test polars_from_rows with empty rows via MCP protocol."""
    response = call_tool(
        mcp_client,
        "polars_from_rows",
        columns=["id", "name"],
        rows=[],
        dataset_id="polars_empty"
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 0
    assert response["columns"] == ["id", "name"]


def test_polars_from_rows_auto_id_mcp(mcp_client, clean_registry):
    """Test polars_from_rows with auto-generated ID via MCP protocol."""
    response = call_tool(
        mcp_client,
        "polars_from_rows",
        columns=["x"],
        rows=[[1], [2], [3]]
        # No dataset_id - should auto-generate
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["dataset_id"] is not None
    assert response["rows"] == 3


def test_polars_groupby_invalid_agg_mcp(mcp_client, clean_registry):
    """Test polars_groupby with invalid aggregation via MCP protocol."""
    create_test_dataset(mcp_client, "polars_invalid", ["id", "value"], [[1, 10], [2, 20]])
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "polars_groupby",
            dataset_id="polars_invalid",
            group_cols=["id"],
            aggs={"value": ["invalid_agg"]}
        )


def test_polars_groupby_all_aggs_mcp(mcp_client, clean_registry):
    """Test polars_groupby with all aggregation types via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "polars_all_aggs",
        ["category", "value"],
        [["A", 10], ["A", 20], ["B", 30], ["B", 40]]
    )
    
    response = call_tool(
        mcp_client,
        "polars_groupby",
        dataset_id="polars_all_aggs",
        group_cols=["category"],
        aggs={"value": ["count", "sum", "min", "max", "mean", "nunique"]}
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 2  # Two categories


def test_polars_groupby_multiple_group_cols_mcp(mcp_client, clean_registry):
    """Test polars_groupby with multiple group columns via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "polars_multi_group",
        ["cat1", "cat2", "value"],
        [["A", "X", 10], ["A", "Y", 20], ["B", "X", 30]]
    )
    
    response = call_tool(
        mcp_client,
        "polars_groupby",
        dataset_id="polars_multi_group",
        group_cols=["cat1", "cat2"],
        aggs={"value": ["sum"]}
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 3  # Three unique combinations


def test_polars_export_json_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test polars_export to JSON format via MCP protocol."""
    create_test_dataset(mcp_client, "polars_json", ["id", "name"], [[1, "A"], [2, "B"]])
    
    response = call_tool(
        mcp_client,
        "polars_export",
        dataset_id="polars_json",
        format="json",
        filename="polars_export.json"
    )
    
    assert_response_structure(response, ["dataset_id", "format", "filename", "path"])
    assert response["format"].upper() == "JSON"
    assert response["path"].endswith(".json")


def test_polars_export_parquet_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test polars_export to Parquet format via MCP protocol."""
    create_test_dataset(mcp_client, "polars_parquet", ["id"], [[1], [2], [3]])
    
    response = call_tool(
        mcp_client,
        "polars_export",
        dataset_id="polars_parquet",
        format="parquet",
        filename="polars_export.parquet"
    )
    
    assert_response_structure(response, ["dataset_id", "format", "filename", "path"])
    assert response["format"].upper() == "PARQUET"
    assert response["path"].endswith(".parquet")


def test_polars_export_unsupported_format_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test polars_export with unsupported format via MCP protocol."""
    create_test_dataset(mcp_client, "polars_unsupported", ["id"], [[1]])
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "polars_export",
            dataset_id="polars_unsupported",
            format="xml",  # Unsupported
            filename="test.xml"
        )


def test_polars_groupby_not_found_mcp(mcp_client, clean_registry):
    """Test polars_groupby with non-existent dataset via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "polars_groupby",
            dataset_id="nonexistent_polars_xyz",
            group_cols=["id"],
            aggs={"value": ["sum"]}
        )


def test_polars_export_not_found_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test polars_export with non-existent dataset via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "polars_export",
            dataset_id="nonexistent_polars_xyz",
            format="csv",
            filename="test.csv"
        )

