"""More integration tests for registry operations via MCP protocol.

Covers missing paths in dataset operations.
"""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_dataset_list_empty_registry_mcp(mcp_client, clean_registry):
    """Test dataset_list with empty registry via MCP protocol."""
    response = call_tool(mcp_client, "dataset_list")
    
    assert_response_structure(response, ["datasets", "dataset_count"])
    assert response["dataset_count"] == 0
    assert response["datasets"] == []


def test_dataset_info_not_found_error_mcp(mcp_client, clean_registry):
    """Test dataset_info with non-existent dataset via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "dataset_info",
            dataset_id="nonexistent_dataset_xyz_123"
        )


def test_dataset_put_rows_large_dataset_mcp(mcp_client, clean_registry):
    """Test dataset_put_rows with large dataset via MCP protocol."""
    # Create dataset with many rows (tests capping)
    rows = [[i, f"item_{i}"] for i in range(1000)]
    
    response = call_tool(
        mcp_client,
        "dataset_put_rows",
        columns=["id", "name"],
        rows=rows,
        dataset_id="large_dataset"
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 1000
    assert response["columns"] == ["id", "name"]


def test_dataset_put_rows_empty_dataset_mcp(mcp_client, clean_registry):
    """Test dataset_put_rows with empty rows via MCP protocol."""
    response = call_tool(
        mcp_client,
        "dataset_put_rows",
        columns=["id", "name"],
        rows=[],
        dataset_id="empty_dataset"
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 0
    assert response["columns"] == ["id", "name"]


def test_dataset_delete_not_found_error_mcp(mcp_client, clean_registry):
    """Test dataset_delete with non-existent dataset via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "dataset_delete",
            dataset_id="nonexistent_dataset_xyz_123"
        )


def test_dataset_delete_success_mcp(mcp_client, clean_registry):
    """Test dataset_delete successfully removes dataset via MCP protocol."""
    # Create dataset first
    create_test_dataset(mcp_client, "to_delete", ["id"], [[1], [2], [3]])
    
    # Verify it exists
    response = call_tool(mcp_client, "dataset_info", dataset_id="to_delete")
    assert response["dataset_id"] == "to_delete"
    
    # Delete it
    response = call_tool(mcp_client, "dataset_delete", dataset_id="to_delete")
    assert_response_structure(response, ["dataset_id", "deleted"])
    assert response["deleted"] is True
    
    # Verify it's gone
    with pytest.raises(Exception):
        call_tool(mcp_client, "dataset_info", dataset_id="to_delete")


def test_dataset_info_path_resolution_mcp(mcp_client, clean_registry):
    """Test dataset_info path resolution via MCP protocol."""
    create_test_dataset(mcp_client, "path_test", ["id"], [[1], [2]])
    
    response = call_tool(mcp_client, "dataset_info", dataset_id="path_test")
    
    assert_response_structure(response, ["dataset_id", "path", "rows", "columns"])
    assert "path" in response
    assert response["path"].endswith(".parquet")


def test_dataset_list_multiple_datasets_mcp(mcp_client, clean_registry):
    """Test dataset_list with multiple datasets via MCP protocol."""
    # Create multiple datasets
    create_test_dataset(mcp_client, "ds1", ["id"], [[1]])
    create_test_dataset(mcp_client, "ds2", ["id"], [[2]])
    create_test_dataset(mcp_client, "ds3", ["id"], [[3]])
    
    response = call_tool(mcp_client, "dataset_list")
    
    assert_response_structure(response, ["datasets", "dataset_count"])
    assert response["dataset_count"] >= 3
    dataset_ids = [ds["dataset_id"] for ds in response["datasets"]]
    assert "ds1" in dataset_ids
    assert "ds2" in dataset_ids
    assert "ds3" in dataset_ids


def test_dataset_put_rows_with_nulls_mcp(mcp_client, clean_registry):
    """Test dataset_put_rows with null values via MCP protocol."""
    response = call_tool(
        mcp_client,
        "dataset_put_rows",
        columns=["id", "name", "value"],
        rows=[[1, "A", 10], [2, None, None], [3, "C", 30]],
        dataset_id="with_nulls"
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 3


def test_dataset_put_rows_special_characters_mcp(mcp_client, clean_registry):
    """Test dataset_put_rows with special characters in data via MCP protocol."""
    response = call_tool(
        mcp_client,
        "dataset_put_rows",
        columns=["id", "text"],
        rows=[[1, "Hello, World!"], [2, "Line 1\nLine 2"], [3, "Quote: \"test\""]],
        dataset_id="special_chars"
    )
    
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["rows"] == 3

