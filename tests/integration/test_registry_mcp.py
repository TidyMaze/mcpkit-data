"""Integration tests for dataset registry tools via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool
from tests.utils_mcp import create_test_dataset

pytestmark = pytest.mark.integration


def test_dataset_list_mcp(mcp_client, clean_registry):
    """Test dataset_list via MCP protocol."""
    # Create some test datasets
    create_test_dataset(mcp_client, "test_ds1", ["a", "b"], [[1, 2], [3, 4]])
    create_test_dataset(mcp_client, "test_ds2", ["x"], [[10], [20]])
    
    # Call via MCP
    response = call_tool(mcp_client, "dataset_list")
    
    # Verify response structure
    assert_response_structure(response, ["datasets"])
    assert len(response["datasets"]) >= 2
    
    # Verify dataset IDs
    dataset_ids = [d["dataset_id"] for d in response["datasets"]]
    assert "test_ds1" in dataset_ids
    assert "test_ds2" in dataset_ids


def test_dataset_info_mcp(mcp_client, clean_registry):
    """Test dataset_info via MCP protocol."""
    # Create test dataset
    create_test_dataset(mcp_client, "test_info", ["col1", "col2"], [[1, "a"], [2, "b"]])
    
    # Call via MCP
    response = call_tool(mcp_client, "dataset_info", dataset_id="test_info")
    
    # Verify response structure
    assert_response_structure(response, ["dataset_id", "rows", "columns", "path"])
    assert response["dataset_id"] == "test_info"
    assert response["rows"] == 2
    assert response["columns"] == ["col1", "col2"]


def test_dataset_put_rows_mcp(mcp_client, clean_registry):
    """Test dataset_put_rows via MCP protocol."""
    # Call via MCP
    response = call_tool(
        mcp_client,
        "dataset_put_rows",
        columns=["id", "name"],
        rows=[[1, "Alice"], [2, "Bob"]],
        dataset_id="test_put"
    )
    
    # Verify response structure
    assert_response_structure(response, ["dataset_id", "rows", "columns", "path"])
    assert response["dataset_id"] == "test_put"
    assert response["rows"] == 2
    assert response["columns"] == ["id", "name"]


def test_dataset_put_rows_auto_id_mcp(mcp_client, clean_registry):
    """Test dataset_put_rows with auto-generated ID via MCP protocol."""
    # Call via MCP without dataset_id
    response = call_tool(
        mcp_client,
        "dataset_put_rows",
        columns=["x"],
        rows=[[1], [2], [3]]
    )
    
    # Verify response structure
    assert_response_structure(response, ["dataset_id", "rows", "columns"])
    assert response["dataset_id"] is not None
    assert response["rows"] == 3


def test_dataset_delete_mcp(mcp_client, clean_registry):
    """Test dataset_delete via MCP protocol."""
    # Create test dataset
    create_test_dataset(mcp_client, "test_delete", ["a"], [[1]])
    
    # Verify it exists
    list_response = call_tool(mcp_client, "dataset_list")
    dataset_ids = [d["dataset_id"] for d in list_response["datasets"]]
    assert "test_delete" in dataset_ids
    
    # Delete via MCP
    response = call_tool(mcp_client, "dataset_delete", dataset_id="test_delete")
    
    # Verify response structure
    assert_response_structure(response, ["dataset_id", "deleted"])
    assert response["dataset_id"] == "test_delete"
    assert response["deleted"] is True
    
    # Verify it's gone
    list_response = call_tool(mcp_client, "dataset_list")
    dataset_ids = [d["dataset_id"] for d in list_response["datasets"]]
    assert "test_delete" not in dataset_ids


def test_dataset_info_not_found_mcp(mcp_client, clean_registry):
    """Test dataset_info with non-existent dataset via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError or similar
        call_tool(mcp_client, "dataset_info", dataset_id="nonexistent_dataset")


def test_dataset_put_rows_empty_rows_mcp(mcp_client, clean_registry):
    """Test dataset_put_rows with empty rows via MCP protocol."""
    response = call_tool(
        mcp_client,
        "dataset_put_rows",
        columns=["id", "name"],
        rows=[],
        dataset_id="test_empty"
    )
    
    assert_response_structure(response, ["dataset_id", "rows"])
    assert response["rows"] == 0


def test_dataset_put_rows_single_row_mcp(mcp_client, clean_registry):
    """Test dataset_put_rows with single row via MCP protocol."""
    response = call_tool(
        mcp_client,
        "dataset_put_rows",
        columns=["id"],
        rows=[[1]],
        dataset_id="test_single"
    )
    
    assert_response_structure(response, ["rows"])
    assert response["rows"] == 1


def test_dataset_list_empty_mcp(mcp_client, clean_registry):
    """Test dataset_list with empty registry via MCP protocol."""
    response = call_tool(mcp_client, "dataset_list")
    
    assert_response_structure(response, ["datasets"])
    assert isinstance(response["datasets"], list)


def test_dataset_delete_nonexistent_mcp(mcp_client, clean_registry):
    """Test dataset_delete with non-existent dataset via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(mcp_client, "dataset_delete", dataset_id="nonexistent")

