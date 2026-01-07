"""Integration tests for Great Expectations tools via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def ge_setup():
    """Check if Great Expectations is available."""
    try:
        import great_expectations
    except ImportError:
        pytest.skip("Great Expectations not installed")


def test_great_expectations_check_mcp(mcp_client, clean_registry, ge_setup):
    """Test great_expectations_check via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_ge",
        ["id", "value"],
        [[1, 10], [2, 20], [3, 30]]
    )
    
    expectation_suite = {
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "id"}
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "value"}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "value", "min_value": 0, "max_value": 100}
            }
        ]
    }
    
    response = call_tool(
        mcp_client,
        "great_expectations_check",
        dataset_id="test_ge",
        expectation_suite=expectation_suite
    )
    
    assert_response_structure(response, ["dataset_id", "success", "statistics", "results"])
    assert response["dataset_id"] == "test_ge"
    assert isinstance(response["results"], list)

