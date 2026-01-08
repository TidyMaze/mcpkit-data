"""Edge case tests for Pandas operations via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_pandas_filter_query_multiple_filters_mcp(mcp_client, clean_registry):
    """Test pandas_filter_query with multiple filters via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_multi_filter",
        ["category", "price", "stock"],
        [
            ["A", 10, 5],
            ["A", 20, 10],
            ["B", 15, 0],
            ["B", 25, 8]
        ]
    )
    
    filters = [
        {"column": "category", "op": "==", "value": "A"},
        {"column": "price", "op": ">=", "value": 15}
    ]
    
    response = call_tool(
        mcp_client,
        "pandas_filter_query",
        dataset_id="test_multi_filter",
        filters=filters
    )
    
    assert_response_structure(response, ["dataset_id"])
    # Verify filtered dataset
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 1  # Only A with price >= 15


def test_pandas_filter_query_in_operator_mcp(mcp_client, clean_registry):
    """Test pandas_filter_query with 'in' operator via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_in_filter",
        ["id", "status"],
        [
            [1, "active"],
            [2, "pending"],
            [3, "active"],
            [4, "inactive"]
        ]
    )
    
    filters = [
        {"column": "status", "op": "in", "value": ["active", "pending"]}
    ]
    
    response = call_tool(
        mcp_client,
        "pandas_filter_query",
        dataset_id="test_in_filter",
        filters=filters
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 3  # active, pending, active


def test_pandas_filter_query_contains_mcp(mcp_client, clean_registry):
    """Test pandas_filter_query with 'contains' operator via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_contains",
        ["id", "description"],
        [
            [1, "test product"],
            [2, "sample item"],
            [3, "test case"]
        ]
    )
    
    filters = [
        {"column": "description", "op": "contains", "value": "test"}
    ]
    
    response = call_tool(
        mcp_client,
        "pandas_filter_query",
        dataset_id="test_contains",
        filters=filters
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 2  # "test product", "test case"


def test_pandas_diff_frames_no_differences_mcp(mcp_client, clean_registry):
    """Test pandas_diff_frames with identical datasets via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "diff_a",
        ["id", "value"],
        [[1, 10], [2, 20]]
    )
    
    create_test_dataset(
        mcp_client,
        "diff_b",
        ["id", "value"],
        [[1, 10], [2, 20]]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_diff_frames",
        dataset_a="diff_a",
        dataset_b="diff_b",
        key_cols=["id"]
    )
    
    assert_response_structure(response, ["only_in_left", "only_in_right", "in_both", "mismatch_count"])
    assert response["mismatch_count"] == 0
    assert response["only_in_left"] == 0
    assert response["only_in_right"] == 0


def test_pandas_diff_frames_with_compare_cols_mcp(mcp_client, clean_registry):
    """Test pandas_diff_frames with specific compare columns via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "diff_c",
        ["id", "name", "value"],
        [[1, "A", 10], [2, "B", 20]]
    )
    
    create_test_dataset(
        mcp_client,
        "diff_d",
        ["id", "name", "value"],
        [[1, "A", 15], [2, "B", 20]]  # value changed for id=1
    )
    
    response = call_tool(
        mcp_client,
        "pandas_diff_frames",
        dataset_a="diff_c",
        dataset_b="diff_d",
        key_cols=["id"],
        compare_cols=["value"]
    )
    
    assert_response_structure(response, ["mismatch_count", "examples"])
    assert response["mismatch_count"] == 1  # id=1 has different value


def test_pandas_sample_stratified_multiple_strata_mcp(mcp_client, clean_registry):
    """Test pandas_sample_stratified with multiple strata columns via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_stratified",
        ["category", "subcategory", "value"],
        [
            ["A", "X", 10],
            ["A", "X", 20],
            ["A", "Y", 30],
            ["B", "X", 40],
            ["B", "Y", 50]
        ]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_sample_stratified",
        dataset_id="test_stratified",
        strata_cols=["category", "subcategory"],
        n_per_group=1
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] >= 1


def test_pandas_parse_json_column_expand_arrays_mcp(mcp_client, clean_registry):
    """Test pandas_parse_json_column with expand_arrays=True via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_json_array",
        ["id", "items"],
        [
            [1, '["item1", "item2"]'],
            [2, '["item3"]']
        ]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_parse_json_column",
        dataset_id="test_json_array",
        column="items",
        expand_arrays=True
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    # Should have expanded rows: 2 + 1 = 3 rows
    assert info["row_count"] >= 2


def test_pandas_parse_json_column_target_columns_mcp(mcp_client, clean_registry):
    """Test pandas_parse_json_column with specific target columns via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_json_target",
        ["id", "metadata"],
        [
            [1, '{"name": "test", "value": 10, "extra": "ignored"}']
        ]
    )
    
    response = call_tool(
        mcp_client,
        "pandas_parse_json_column",
        dataset_id="test_json_target",
        column="metadata",
        target_columns=["name", "value"]
    )
    
    assert_response_structure(response, ["dataset_id"])
    info = call_tool(mcp_client, "dataset_info", dataset_id=response["dataset_id"])
    assert info["row_count"] == 1

