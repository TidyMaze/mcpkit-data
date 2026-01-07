"""Integration tests for dataset operations via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_dataset_head_tail_mcp(mcp_client, clean_registry):
    """Test dataset_head_tail via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_head_tail",
        ["id"],
        [[i] for i in range(10)]
    )
    
    response = call_tool(
        mcp_client,
        "dataset_head_tail",
        dataset_id="test_head_tail",
        head=3,
        tail=2
    )
    
    assert_response_structure(response, ["dataset_id", "total_rows", "head_rows", "tail_rows"])
    assert response["total_rows"] == 10
    assert len(response["head_rows"]) == 3
    assert len(response["tail_rows"]) == 2


def test_dataset_head_tail_head_only_mcp(mcp_client, clean_registry):
    """Test dataset_head_tail with head only via MCP protocol."""
    create_test_dataset(mcp_client, "test_head", ["x"], [[i] for i in range(5)])
    
    response = call_tool(
        mcp_client,
        "dataset_head_tail",
        dataset_id="test_head",
        head=2
    )
    
    assert_response_structure(response, ["head_rows"])
    assert len(response["head_rows"]) == 2
    assert response.get("tail_rows") is None


def test_dataset_head_tail_tail_only_mcp(mcp_client, clean_registry):
    """Test dataset_head_tail with tail only via MCP protocol."""
    create_test_dataset(mcp_client, "test_tail", ["x"], [[i] for i in range(5)])
    
    response = call_tool(
        mcp_client,
        "dataset_head_tail",
        dataset_id="test_tail",
        tail=2
    )
    
    assert_response_structure(response, ["tail_rows"])
    assert len(response["tail_rows"]) == 2
    assert response.get("head_rows") is None

