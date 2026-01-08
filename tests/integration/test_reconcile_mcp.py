"""Integration tests for reconciliation tools via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_reconcile_counts_mcp(mcp_client, clean_registry):
    """Test reconcile_counts via MCP protocol."""
    create_test_dataset(mcp_client, "left_reconcile", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    create_test_dataset(mcp_client, "right_reconcile", ["id", "value"], [[1, 10], [2, 25], [4, 40]])
    
    response = call_tool(
        mcp_client,
        "reconcile_counts",
        left_dataset_id="left_reconcile",
        right_dataset_id="right_reconcile",
        key_cols=["id"]
    )
    
    assert_response_structure(response, ["only_in_left", "only_in_right", "in_both", "examples"])
    assert response["only_in_left"] == 1  # id=3 only in left
    assert response["only_in_right"] == 1  # id=4 only in right
    assert response["in_both"] == 2  # id=1,2 in both

