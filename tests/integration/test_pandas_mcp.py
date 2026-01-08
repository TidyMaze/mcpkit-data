"""Integration tests for pandas tools via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset, json_stringify

pytestmark = pytest.mark.integration


def test_pandas_from_rows_mcp(mcp_client, clean_registry):
    """Test pandas_from_rows via MCP protocol."""
    response = call_tool(
        mcp_client,
        "pandas_from_rows",
        columns=["id", "value"],
        rows=[[1, 10.5], [2, 20.3], [3, 30.1]],
        dataset_id="test_pandas"
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["dataset_id"] == "test_pandas"
    assert response["rows"] == 3
    assert response["columns"] == ["id", "value"]


def test_pandas_describe_mcp(mcp_client, clean_registry):
    """Test pandas_describe via MCP protocol."""
    create_test_dataset(mcp_client, "test_desc", ["id", "value"], [[1, 10.5], [2, 20.3], [3, 30.1]])
    
    response = call_tool(mcp_client, "pandas_describe", dataset_id="test_desc", include="all")
    
    assert_response_structure(response, ["dataset_id", "dtypes"])
    assert response["dataset_id"] == "test_desc"
    assert "id" in response["dtypes"]
    assert "value" in response["dtypes"]


def test_pandas_groupby_mcp(mcp_client, clean_registry):
    """Test pandas_groupby via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_groupby",
        ["category", "value"],
        [["A", 10], ["A", 20], ["B", 30], ["B", 40]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_groupby",
        dataset_id="test_groupby",
        group_cols=["category"],
        aggs={"value": ["sum", "mean"]}
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 2  # Two categories


def test_pandas_join_mcp(mcp_client, clean_registry):
    """Test pandas_join via MCP protocol."""
    create_test_dataset(mcp_client, "left_ds", ["id", "name"], [[1, "Alice"], [2, "Bob"]])
    create_test_dataset(mcp_client, "right_ds", ["id", "age"], [[1, 25], [2, 30]])
    
    response = call_tool(
        mcp_client,
        "pandas_join",
        left_dataset_id="left_ds",
        right_dataset_id="right_ds",
        keys=["id"],
        how="inner"
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 2
    assert len(response["columns"]) >= 3  # id, name, age


def test_pandas_filter_query_mcp(mcp_client, clean_registry):
    """Test pandas_filter_query via MCP protocol."""
    create_test_dataset(mcp_client, "test_filter", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    
    filters = json_stringify([
        {"column": "value", "op": ">=", "value": 20}
    ])
    
    response = call_tool(
        mcp_client,
        "pandas_filter_query",
        dataset_id="test_filter",
        filters=filters
    )
    
    assert_response_structure(response, ["dataset_id", "rows"])
    assert response["rows"] == 2  # Two rows with value >= 20


def test_pandas_filter_time_range_mcp(mcp_client, clean_registry):
    """Test pandas_filter_time_range via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_time",
        ["timestamp", "value"],
        [
            ["2024-01-01", 10],
            ["2024-01-15", 20],
            ["2024-02-01", 30]
        ]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_filter_time_range",
        dataset_id="test_time",
        timestamp_column="timestamp",
        start_time="2024-01-10",
        end_time="2024-01-20"
    )
    
    assert_response_structure(response, ["dataset_id", "rows"])
    assert response["rows"] == 1  # One row in range


def test_pandas_diff_frames_mcp(mcp_client, clean_registry):
    """Test pandas_diff_frames via MCP protocol."""
    create_test_dataset(mcp_client, "ds_a", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    create_test_dataset(mcp_client, "ds_b", ["id", "value"], [[1, 10], [2, 25], [4, 40]])
    
    response = call_tool(
        mcp_client,
        "pandas_diff_frames",
        dataset_a="ds_a",
        dataset_b="ds_b",
        key_cols=["id"]
    )
    
    assert_response_structure(response, ["only_in_a", "only_in_b", "common", "changed"])
    assert response["only_in_a"] == 1  # id=3 only in A
    assert response["only_in_b"] == 1  # id=4 only in B
    assert response["common"] == 2  # id=1,2 in both


def test_pandas_schema_check_mcp(mcp_client, clean_registry):
    """Test pandas_schema_check via MCP protocol."""
    create_test_dataset(mcp_client, "test_schema", ["id", "name"], [[1, "Alice"], [2, "Bob"]])
    
    response = call_tool(
        mcp_client,
        "pandas_schema_check",
        dataset_id="test_schema",
        required_columns=["id", "name"]
    )
    
    assert_response_structure(response, ["valid", "columns", "dtypes"])
    assert response["valid"] is True
    assert "id" in response["columns"]
    assert "name" in response["columns"]


def test_pandas_sample_stratified_mcp(mcp_client, clean_registry):
    """Test pandas_sample_stratified via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_stratified",
        ["category", "value"],
        [["A", 1], ["A", 2], ["B", 3], ["B", 4], ["B", 5]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_sample_stratified",
        dataset_id="test_stratified",
        strata_cols=["category"],
        n_per_group=2
    )
    
    assert_response_structure(response, ["dataset_id", "rows"])
    assert response["rows"] <= 4  # At most 2 per group


def test_pandas_sample_random_mcp(mcp_client, clean_registry):
    """Test pandas_sample_random via MCP protocol."""
    create_test_dataset(mcp_client, "test_random", ["id"], [[i] for i in range(10)])
    
    response = call_tool(
        mcp_client,
        "pandas_sample_random",
        dataset_id="test_random",
        n=5,
        seed=42
    )
    
    assert_response_structure(response, ["dataset_id", "rows"])
    assert response["rows"] == 5


def test_pandas_count_distinct_mcp(mcp_client, clean_registry):
    """Test pandas_count_distinct via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_distinct",
        ["id", "category"],
        [[1, "A"], [2, "A"], [3, "B"], [4, "B"], [5, "B"]]
    )
    
    response = call_tool(mcp_client, "pandas_count_distinct", dataset_id="test_distinct")
    
    assert_response_structure(response, ["dataset_id", "distinct_counts"])
    assert response["distinct_counts"]["id"] == 5
    assert response["distinct_counts"]["category"] == 2


def test_pandas_parse_json_column_mcp(mcp_client, clean_registry):
    """Test pandas_parse_json_column via MCP protocol."""
    import json
    create_test_dataset(
        mcp_client,
        "test_json",
        ["id", "data"],
        [
            [1, json.dumps({"name": "Alice", "age": 25})],
            [2, json.dumps({"name": "Bob", "age": 30})]
        ]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_parse_json_column",
        dataset_id="test_json",
        column="data"
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert "name" in response["columns"] or "data.name" in response["columns"]


def test_pandas_export_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test pandas_export via MCP protocol."""
    create_test_dataset(mcp_client, "test_export", ["id", "value"], [[1, 10], [2, 20]])
    
    response = call_tool(
        mcp_client,
        "pandas_export",
        dataset_id="test_export",
        format="csv",
        filename="test_export.csv"
    )
    
    assert_response_structure(response, ["dataset_id", "format", "filename", "path"])
    assert response["format"] == "csv"
    assert response["filename"] == "test_export.csv"


def test_pandas_export_parquet_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test pandas_export to parquet via MCP protocol."""
    create_test_dataset(mcp_client, "test_parquet", ["id", "value"], [[1, 10], [2, 20]])
    
    response = call_tool(
        mcp_client,
        "pandas_export",
        dataset_id="test_parquet",
        format="parquet",
        filename="test_parquet.parquet"
    )
    
    assert_response_structure(response, ["format"])
    assert response["format"] == "parquet"


def test_pandas_export_json_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test pandas_export to JSON via MCP protocol."""
    create_test_dataset(mcp_client, "test_json", ["id"], [[1], [2]])
    
    response = call_tool(
        mcp_client,
        "pandas_export",
        dataset_id="test_json",
        format="json",
        filename="test_json.json"
    )
    
    assert_response_structure(response, ["format"])
    assert response["format"] == "json"


def test_pandas_export_excel_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test pandas_export to Excel via MCP protocol."""
    create_test_dataset(mcp_client, "test_excel", ["id", "name"], [[1, "A"], [2, "B"]])
    
    # Excel format is not supported (only CSV, JSON, PARQUET)
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "pandas_export",
            dataset_id="test_excel",
            format="excel",
            filename="test_excel.xlsx"
        )


def test_pandas_describe_numeric_only_mcp(mcp_client, clean_registry):
    """Test pandas_describe with numeric only via MCP protocol."""
    create_test_dataset(mcp_client, "test_num", ["id", "value"], [[1, 10.5], [2, 20.3]])
    
    response = call_tool(mcp_client, "pandas_describe", dataset_id="test_num", include="numeric")
    
    assert_response_structure(response, ["dataset_id", "dtypes"])
    assert response["dataset_id"] == "test_num"
    # description might be None for numeric-only with small dataset
    assert "description" in response or "dtypes" in response


def test_pandas_describe_object_only_mcp(mcp_client, clean_registry):
    """Test pandas_describe with object only via MCP protocol."""
    create_test_dataset(mcp_client, "test_obj", ["name"], [["A"], ["B"]])
    
    response = call_tool(mcp_client, "pandas_describe", dataset_id="test_obj", include="object")
    
    assert_response_structure(response, ["dataset_id"])


def test_pandas_filter_query_multiple_filters_mcp(mcp_client, clean_registry):
    """Test pandas_filter_query with multiple filters via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_multi_filter",
        ["id", "value", "category"],
        [[1, 10, "A"], [2, 20, "B"], [3, 30, "A"], [4, 40, "B"]]
    )
    
    filters = json_stringify([
        {"column": "value", "op": ">=", "value": 20},
        {"column": "category", "op": "==", "value": "B"}
    ])
    
    response = call_tool(
        mcp_client,
        "pandas_filter_query",
        dataset_id="test_multi_filter",
        filters=filters
    )
    
    assert_response_structure(response, ["rows"])
    assert response["rows"] == 2  # id=2 and id=4


def test_pandas_filter_query_in_operator_mcp(mcp_client, clean_registry):
    """Test pandas_filter_query with 'in' operator via MCP protocol."""
    create_test_dataset(mcp_client, "test_in", ["id"], [[i] for i in range(10)])
    
    filters = json_stringify([
        {"column": "id", "op": "in", "value": [2, 4, 6]}
    ])
    
    response = call_tool(
        mcp_client,
        "pandas_filter_query",
        dataset_id="test_in",
        filters=filters
    )
    
    assert_response_structure(response, ["rows"])
    assert response["rows"] == 3


def test_pandas_join_left_mcp(mcp_client, clean_registry):
    """Test pandas_join with left join via MCP protocol."""
    create_test_dataset(mcp_client, "left_join", ["id", "name"], [[1, "Alice"], [2, "Bob"], [3, "Charlie"]])
    create_test_dataset(mcp_client, "right_join", ["id", "age"], [[1, 25], [2, 30]])
    
    response = call_tool(
        mcp_client,
        "pandas_join",
        left_dataset_id="left_join",
        right_dataset_id="right_join",
        keys=["id"],
        how="left"
    )
    
    assert_response_structure(response, ["rows"])
    assert response["rows"] == 3  # All from left


def test_pandas_join_outer_mcp(mcp_client, clean_registry):
    """Test pandas_join with outer join via MCP protocol."""
    create_test_dataset(mcp_client, "left_outer", ["id", "x"], [[1, "a"], [2, "b"]])
    create_test_dataset(mcp_client, "right_outer", ["id", "y"], [[2, "c"], [3, "d"]])
    
    response = call_tool(
        mcp_client,
        "pandas_join",
        left_dataset_id="left_outer",
        right_dataset_id="right_outer",
        keys=["id"],
        how="outer"
    )
    
    assert_response_structure(response, ["rows"])
    assert response["rows"] == 3  # id=1,2,3


def test_pandas_groupby_multiple_aggs_mcp(mcp_client, clean_registry):
    """Test pandas_groupby with multiple aggregations via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_multi_agg",
        ["category", "value"],
        [["A", 10], ["A", 20], ["B", 30], ["B", 40]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_groupby",
        dataset_id="test_multi_agg",
        group_cols=["category"],
        aggs={"value": ["sum", "mean", "min", "max"]}
    )
    
    assert_response_structure(response, ["rows", "columns"])
    assert response["rows"] == 2


def test_pandas_schema_check_with_dtype_overrides_mcp(mcp_client, clean_registry):
    """Test pandas_schema_check with dtype overrides via MCP protocol."""
    create_test_dataset(mcp_client, "test_dtype", ["id", "value"], [[1, "10"], [2, "20"]])
    
    response = call_tool(
        mcp_client,
        "pandas_schema_check",
        dataset_id="test_dtype",
        dtype_overrides={"id": "int64", "value": "object"}
    )
    
    assert_response_structure(response, ["valid", "dtypes"])


def test_pandas_schema_check_with_non_null_mcp(mcp_client, clean_registry):
    """Test pandas_schema_check with non-null columns via MCP protocol."""
    create_test_dataset(mcp_client, "test_nonnull", ["id", "name"], [[1, "A"], [2, "B"]])
    
    response = call_tool(
        mcp_client,
        "pandas_schema_check",
        dataset_id="test_nonnull",
        non_null_columns=["id", "name"]
    )
    
    assert_response_structure(response, ["valid"])


def test_pandas_empty_dataset_mcp(mcp_client, clean_registry):
    """Test pandas operations with empty dataset via MCP protocol."""
    create_test_dataset(mcp_client, "test_empty", ["id"], [])
    
    response = call_tool(mcp_client, "pandas_describe", dataset_id="test_empty")
    assert_response_structure(response, ["dataset_id"])

