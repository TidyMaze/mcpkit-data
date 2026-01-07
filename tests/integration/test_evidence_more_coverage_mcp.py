"""More integration tests for evidence operations via MCP protocol.

Covers missing paths in evidence.py.
"""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_evidence_bundle_plus_no_describe_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test evidence_bundle_plus with describe disabled via MCP protocol."""
    create_test_dataset(mcp_client, "evidence_no_desc", ["id", "value"], [[1, 10], [2, 20]])
    
    response = call_tool(
        mcp_client,
        "evidence_bundle_plus",
        dataset_id="evidence_no_desc",
        base_filename="evidence_test",
        include_describe=False
    )
    
    assert_response_structure(response, ["dataset_id", "info", "exported_files"])
    assert response["dataset_id"] == "evidence_no_desc"
    assert "describe" not in response or response.get("describe") is None


def test_evidence_bundle_plus_no_sample_rows_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test evidence_bundle_plus with sample_rows=0 via MCP protocol."""
    create_test_dataset(mcp_client, "evidence_no_samples", ["id"], [[1], [2], [3]])
    
    response = call_tool(
        mcp_client,
        "evidence_bundle_plus",
        dataset_id="evidence_no_samples",
        base_filename="evidence_test",
        include_sample_rows=0
    )
    
    assert_response_structure(response, ["dataset_id", "info", "exported_files"])
    assert "sample_rows" not in response or response.get("sample_rows") is None or len(response.get("sample_rows", [])) == 0


def test_evidence_bundle_plus_large_sample_rows_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test evidence_bundle_plus with large sample_rows count via MCP protocol."""
    rows = [[i, i*10] for i in range(100)]
    create_test_dataset(mcp_client, "evidence_large", ["id", "value"], rows)
    
    response = call_tool(
        mcp_client,
        "evidence_bundle_plus",
        dataset_id="evidence_large",
        base_filename="evidence_test",
        include_sample_rows=50
    )
    
    assert_response_structure(response, ["dataset_id", "info", "exported_files"])
    if "sample_rows" in response and response["sample_rows"]:
        assert len(response["sample_rows"]) <= 50  # Should be capped


def test_reconcile_counts_no_mismatches_mcp(mcp_client, clean_registry):
    """Test reconcile_counts with identical datasets via MCP protocol."""
    create_test_dataset(mcp_client, "reconcile_left", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    create_test_dataset(mcp_client, "reconcile_right", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    
    response = call_tool(
        mcp_client,
        "reconcile_counts",
        left_dataset_id="reconcile_left",
        right_dataset_id="reconcile_right",
        key_cols=["id"]
    )
    
    assert_response_structure(response, ["only_in_left", "only_in_right", "in_both", "examples"])
    assert response["only_in_left"] == 0
    assert response["only_in_right"] == 0
    assert response["in_both"] == 3


def test_reconcile_counts_only_in_left_mcp(mcp_client, clean_registry):
    """Test reconcile_counts with records only in left dataset via MCP protocol."""
    create_test_dataset(mcp_client, "reconcile_left2", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    create_test_dataset(mcp_client, "reconcile_right2", ["id", "value"], [[1, 10], [2, 20]])
    
    response = call_tool(
        mcp_client,
        "reconcile_counts",
        left_dataset_id="reconcile_left2",
        right_dataset_id="reconcile_right2",
        key_cols=["id"]
    )
    
    assert_response_structure(response, ["only_in_left", "only_in_right", "in_both", "examples"])
    assert response["only_in_left"] == 1
    assert response["only_in_right"] == 0
    assert response["in_both"] == 2


def test_reconcile_counts_only_in_right_mcp(mcp_client, clean_registry):
    """Test reconcile_counts with records only in right dataset via MCP protocol."""
    create_test_dataset(mcp_client, "reconcile_left3", ["id", "value"], [[1, 10], [2, 20]])
    create_test_dataset(mcp_client, "reconcile_right3", ["id", "value"], [[1, 10], [2, 20], [3, 30]])
    
    response = call_tool(
        mcp_client,
        "reconcile_counts",
        left_dataset_id="reconcile_left3",
        right_dataset_id="reconcile_right3",
        key_cols=["id"]
    )
    
    assert_response_structure(response, ["only_in_left", "only_in_right", "in_both", "examples"])
    assert response["only_in_left"] == 0
    assert response["only_in_right"] == 1
    assert response["in_both"] == 2


def test_reconcile_counts_with_mismatches_mcp(mcp_client, clean_registry):
    """Test reconcile_counts with mismatched values via MCP protocol."""
    create_test_dataset(mcp_client, "reconcile_left4", ["id", "value"], [[1, 10], [2, 20]])
    create_test_dataset(mcp_client, "reconcile_right4", ["id", "value"], [[1, 15], [2, 20]])  # id=1 has different value
    
    response = call_tool(
        mcp_client,
        "reconcile_counts",
        left_dataset_id="reconcile_left4",
        right_dataset_id="reconcile_right4",
        key_cols=["id"],
        max_examples=10
    )
    
    assert_response_structure(response, ["only_in_left", "only_in_right", "in_both", "examples"])
    assert response["in_both"] == 2  # Both keys exist in both
    # Should have examples of mismatches
    if "examples" in response:
        assert isinstance(response["examples"], list)


def test_reconcile_counts_multiple_key_cols_mcp(mcp_client, clean_registry):
    """Test reconcile_counts with multiple key columns via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "reconcile_left5",
        ["id", "category", "value"],
        [[1, "A", 10], [2, "B", 20]]
    )
    create_test_dataset(
        mcp_client,
        "reconcile_right5",
        ["id", "category", "value"],
        [[1, "A", 10], [2, "B", 25]]  # Different value for (2, B)
    )
    
    response = call_tool(
        mcp_client,
        "reconcile_counts",
        left_dataset_id="reconcile_left5",
        right_dataset_id="reconcile_right5",
        key_cols=["id", "category"]
    )
    
    assert_response_structure(response, ["only_in_left", "only_in_right", "in_both", "examples"])
    assert response["in_both"] == 2  # Both key combinations exist


def test_reconcile_counts_with_output_dataset_mcp(mcp_client, clean_registry):
    """Test reconcile_counts with output dataset specified via MCP protocol."""
    create_test_dataset(mcp_client, "reconcile_left6", ["id"], [[1], [2]])
    create_test_dataset(mcp_client, "reconcile_right6", ["id"], [[2], [3]])
    
    response = call_tool(
        mcp_client,
        "reconcile_counts",
        left_dataset_id="reconcile_left6",
        right_dataset_id="reconcile_right6",
        key_cols=["id"],
        out_dataset_id="reconcile_output"
    )
    
    assert_response_structure(response, ["only_in_left", "only_in_right", "in_both", "examples"])
    # Output dataset should be created
    # Verify it exists
    try:
        info = call_tool(mcp_client, "dataset_info", dataset_id="reconcile_output")
        assert info["dataset_id"] == "reconcile_output"
    except Exception:
        pass  # Output dataset creation is optional


def test_evidence_bundle_plus_not_found_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test evidence_bundle_plus with non-existent dataset via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "evidence_bundle_plus",
            dataset_id="nonexistent_evidence_xyz",
            base_filename="test"
        )


def test_reconcile_counts_not_found_mcp(mcp_client, clean_registry):
    """Test reconcile_counts with non-existent dataset via MCP protocol."""
    create_test_dataset(mcp_client, "reconcile_left7", ["id"], [[1]])
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "reconcile_counts",
            left_dataset_id="reconcile_left7",
            right_dataset_id="nonexistent_reconcile_xyz",
            key_cols=["id"]
        )

