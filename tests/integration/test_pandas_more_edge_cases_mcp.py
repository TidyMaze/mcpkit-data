"""More edge case tests for Pandas operations via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_pandas_join_left_join_mcp(mcp_client, clean_registry):
    """Test pandas_join with left join via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "left_join",
        ["id", "name"],
        [[1, "A"], [2, "B"], [3, "C"]]
    )
    
    create_test_dataset(
        mcp_client,
        "right_join",
        ["id", "value"],
        [[1, 10], [4, 40]]  # id=4 not in left
    )
    
    response = call_tool(
        mcp_client,
        "pandas_join",
        left_dataset_id="left_join",
        right_dataset_id="right_join",
        keys=["id"],
        how="left"
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 3  # All from left


def test_pandas_join_outer_join_mcp(mcp_client, clean_registry):
    """Test pandas_join with outer join via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "outer_left",
        ["id", "left_val"],
        [[1, "A"], [2, "B"]]
    )
    
    create_test_dataset(
        mcp_client,
        "outer_right",
        ["id", "right_val"],
        [[2, "X"], [3, "Y"]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_join",
        left_dataset_id="outer_left",
        right_dataset_id="outer_right",
        keys=["id"],
        how="outer"
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 3  # id=1, id=2, id=3


def test_pandas_filter_time_range_with_start_mcp(mcp_client, clean_registry):
    """Test pandas_filter_time_range with start time only via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "time_range_start",
        ["timestamp", "value"],
        [
            ["2024-01-01T00:00:00", 10],
            ["2024-01-15T00:00:00", 20],
            ["2024-02-01T00:00:00", 30]
        ]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_filter_time_range",
        dataset_id="time_range_start",
        timestamp_column="timestamp",
        start_time="2024-01-10T00:00:00"
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 2  # Only last two rows


def test_pandas_filter_time_range_with_end_mcp(mcp_client, clean_registry):
    """Test pandas_filter_time_range with end time only via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "time_range_end",
        ["timestamp", "value"],
        [
            ["2024-01-01T00:00:00", 10],
            ["2024-01-15T00:00:00", 20],
            ["2024-02-01T00:00:00", 30]
        ]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_filter_time_range",
        dataset_id="time_range_end",
        timestamp_column="timestamp",
        end_time="2024-01-20T00:00:00"
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 2  # First two rows


def test_pandas_schema_check_required_columns_mcp(mcp_client, clean_registry):
    """Test pandas_schema_check with required columns via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "schema_check",
        ["id", "name", "value"],
        [[1, "A", 10], [2, "B", 20]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_schema_check",
        dataset_id="schema_check",
        required_columns=["id", "name"]
    )
    
    assert_response_structure(response, ["valid", "checks"])
    assert response["valid"] is True


def test_pandas_schema_check_missing_required_column_mcp(mcp_client, clean_registry):
    """Test pandas_schema_check with missing required column via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "schema_check_missing",
        ["id", "name"],
        [[1, "A"]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_schema_check",
        dataset_id="schema_check_missing",
        required_columns=["id", "name", "missing_col"]
    )
    
    assert_response_structure(response, ["valid", "checks"])
    assert response["valid"] is False


def test_pandas_schema_check_non_null_columns_mcp(mcp_client, clean_registry):
    """Test pandas_schema_check with non-null columns via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "schema_check_null",
        ["id", "name"],
        [[1, "A"], [2, None]]  # name has null
    )
    
    response = call_tool(
        mcp_client,
        "pandas_schema_check",
        dataset_id="schema_check_null",
        non_null_columns=["id"]
    )
    
    assert_response_structure(response, ["valid", "checks"])
    # id column should be valid (no nulls), name column has nulls but not checked
    assert response["valid"] is True


def test_pandas_groupby_multiple_group_cols_mcp(mcp_client, clean_registry):
    """Test pandas_groupby with multiple group columns via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "multi_groupby",
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
        "pandas_groupby",
        dataset_id="multi_groupby",
        group_cols=["category", "subcategory"],
        aggs={"value": ["sum", "mean"]}
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 3  # Three unique combinations


def test_pandas_groupby_nunique_mcp(mcp_client, clean_registry):
    """Test pandas_groupby with nunique aggregation via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "groupby_nunique",
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
        "pandas_groupby",
        dataset_id="groupby_nunique",
        group_cols=["category"],
        aggs={"value": ["nunique", "count"]}
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 2  # Two categories


def test_pandas_parse_json_column_nested_fields_mcp(mcp_client, clean_registry):
    """Test pandas_parse_json_column with nested JSON fields via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "json_nested",
        ["id", "metadata"],
        [
            [1, '{"user": {"id": 1, "name": "Alice"}, "score": 100}']
        ]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_parse_json_column",
        dataset_id="json_nested",
        column="metadata",
        target_columns=["user.id", "user.name", "score"]
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 1


def test_pandas_export_csv_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test pandas_export to CSV format via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "export_csv",
        ["id", "name"],
        [[1, "A"], [2, "B"]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_export",
        dataset_id="export_csv",
        format="csv",
        filename="test_export.csv"
    )
    
    assert_response_structure(response, ["artifact_path"])
    assert response["artifact_path"].endswith(".csv")


def test_pandas_export_json_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test pandas_export to JSON format via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "export_json",
        ["x", "y"],
        [[1, 10], [2, 20]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_export",
        dataset_id="export_json",
        format="json",
        filename="test_export.json"
    )
    
    assert_response_structure(response, ["artifact_path"])
    assert response["artifact_path"].endswith(".json")


def test_pandas_export_parquet_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test pandas_export to Parquet format via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "export_parquet",
        ["id", "value"],
        [[1, 10.5], [2, 20.3]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_export",
        dataset_id="export_parquet",
        format="parquet",
        filename="test_export.parquet"
    )
    
    assert_response_structure(response, ["artifact_path"])
    assert response["artifact_path"].endswith(".parquet")

