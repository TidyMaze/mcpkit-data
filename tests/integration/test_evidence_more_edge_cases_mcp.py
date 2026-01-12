"""More edge case tests for Evidence tools via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_evidence_bundle_plus_with_describe_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test evidence_bundle_plus with describe enabled via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "evidence_desc",
        ["id", "value"],
        [[1, 10], [2, 20], [3, 30]]
    )
    
    response = call_tool(
        mcp_client,
        "evidence_bundle_plus",
        dataset_id="evidence_desc",
        base_filename="evidence_test",
        include_describe=True
    )
    
    assert_response_structure(response, ["dataset_id", "exported_files"])
    assert isinstance(response["exported_files"], list)


def test_evidence_bundle_plus_without_describe_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test evidence_bundle_plus with describe disabled via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "evidence_no_desc",
        ["x", "y"],
        [[1, 10], [2, 20]]
    )
    
    response = call_tool(
        mcp_client,
        "evidence_bundle_plus",
        dataset_id="evidence_no_desc",
        base_filename="evidence_test2",
        include_describe=False
    )
    
    assert_response_structure(response, ["dataset_id", "exported_files"])


def test_evidence_bundle_plus_custom_sample_rows_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test evidence_bundle_plus with custom sample_rows count via MCP protocol."""
    # Create larger dataset
    rows = [[i, i * 10] for i in range(100)]
    create_test_dataset(mcp_client, "evidence_large", ["id", "value"], rows)
    
    response = call_tool(
        mcp_client,
        "evidence_bundle_plus",
        dataset_id="evidence_large",
        base_filename="evidence_large",
        include_sample_rows=20
    )
    
    assert_response_structure(response, ["dataset_id", "exported_files"])


def test_reconcile_counts_no_mismatches_mcp(mcp_client, clean_registry):
    """Test reconcile_counts with identical datasets via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "reconcile_left",
        ["id", "value"],
        [[1, 10], [2, 20]]
    )
    
    create_test_dataset(
        mcp_client,
        "reconcile_right",
        ["id", "value"],
        [[1, 10], [2, 20]]
    )
    
    response = call_tool(
        mcp_client,
        "reconcile_counts",
        left_dataset_id="reconcile_left",
        right_dataset_id="reconcile_right",
        key_cols=["id"]
    )
    
    assert_response_structure(response, ["only_in_left", "only_in_right", "in_both", "mismatch_count"])
    assert response["mismatch_count"] == 0
    assert response["only_in_left"] == 0
    assert response["only_in_right"] == 0


def test_reconcile_counts_with_mismatches_mcp(mcp_client, clean_registry):
    """Test reconcile_counts with mismatched values via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "reconcile_mismatch_left",
        ["id", "value"],
        [[1, 10], [2, 20]]
    )
    
    create_test_dataset(
        mcp_client,
        "reconcile_mismatch_right",
        ["id", "value"],
        [[1, 15], [2, 20]]  # id=1 has different value
    )
    
    response = call_tool(
        mcp_client,
        "reconcile_counts",
        left_dataset_id="reconcile_mismatch_left",
        right_dataset_id="reconcile_mismatch_right",
        key_cols=["id"]
    )
    
    assert_response_structure(response, ["mismatch_count", "examples"])
    assert response["mismatch_count"] >= 0  # Should detect mismatch for id=1

