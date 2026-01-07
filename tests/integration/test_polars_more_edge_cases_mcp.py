"""More edge case tests for Polars operations via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_polars_groupby_multiple_group_cols_mcp(mcp_client, clean_registry):
    """Test polars_groupby with multiple group columns via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "polars_multi_group",
        ["category", "subcategory", "value"],
        [
            ["A", "X", 10],
            ["A", "X", 20],
            ["A", "Y", 30],
            ["B", "X", 40]
        ]
    )
    
    response = call_tool(
        mcp_client,
        "polars_groupby",
        dataset_id="polars_multi_group",
        group_cols=["category", "subcategory"],
        aggs={"value": ["sum", "mean"]}
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 3  # Three unique combinations


def test_polars_groupby_nunique_mcp(mcp_client, clean_registry):
    """Test polars_groupby with nunique aggregation via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "polars_nunique",
        ["category", "value"],
        [
            ["A", 10],
            ["A", 10],  # duplicate
            ["A", 20],
            ["B", 30]
        ]
    )
    
    response = call_tool(
        mcp_client,
        "polars_groupby",
        dataset_id="polars_nunique",
        group_cols=["category"],
        aggs={"value": ["nunique", "count"]}
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 2  # Two categories


def test_polars_export_csv_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test polars_export to CSV format via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "polars_export_csv",
        ["id", "name"],
        [[1, "A"], [2, "B"]]
    )
    
    response = call_tool(
        mcp_client,
        "polars_export",
        dataset_id="polars_export_csv",
        format="csv",
        filename="polars_export.csv"
    )
    
    assert_response_structure(response, ["artifact_path"])
    assert response["artifact_path"].endswith(".csv")


def test_polars_export_json_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test polars_export to JSON format via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "polars_export_json",
        ["x", "y"],
        [[1, 10], [2, 20]]
    )
    
    response = call_tool(
        mcp_client,
        "polars_export",
        dataset_id="polars_export_json",
        format="json",
        filename="polars_export.json"
    )
    
    assert_response_structure(response, ["artifact_path"])
    assert response["artifact_path"].endswith(".json")


def test_polars_export_parquet_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test polars_export to Parquet format via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "polars_export_parquet",
        ["id", "value"],
        [[1, 10.5], [2, 20.3]]
    )
    
    response = call_tool(
        mcp_client,
        "polars_export",
        dataset_id="polars_export_parquet",
        format="parquet",
        filename="polars_export.parquet"
    )
    
    assert_response_structure(response, ["artifact_path"])
    assert response["artifact_path"].endswith(".parquet")

