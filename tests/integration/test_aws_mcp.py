"""Integration tests for AWS tools via MCP protocol.

Uses moto for AWS mocking - no real AWS credentials needed.
"""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
def aws_setup(monkeypatch):
    """Setup AWS mocks for tests.
    
    Uses moto to mock AWS services - no real credentials needed.
    """
    try:
        from moto import mock_aws
        
        # Set dummy AWS credentials for moto
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        
        # Start AWS mocks (moto 5.x uses mock_aws for all services)
        # Use decorator-style to ensure proper setup
        with mock_aws():
            yield
    except ImportError:
        pytest.skip("moto not installed, skipping AWS mock tests")


def test_athena_start_query_mcp(mcp_client, aws_setup):
    """Test athena_start_query via MCP protocol."""
    # Use a simple query that should work in any Athena database
    response = call_tool(
        mcp_client,
        "athena_start_query",
        sql="SELECT 1 as test_column",
        database="default"
    )
    
    assert_response_structure(response, ["query_execution_id", "status"])
    assert response["query_execution_id"] is not None


def test_athena_poll_query_mcp(mcp_client, aws_setup):
    """Test athena_poll_query via MCP protocol."""
    # First start a query
    start_response = call_tool(
        mcp_client,
        "athena_start_query",
        sql="SELECT 1 as test_column",
        database="default"
    )
    
    query_execution_id = start_response["query_execution_id"]
    
    # Poll the query
    response = call_tool(
        mcp_client,
        "athena_poll_query",
        query_execution_id=query_execution_id
    )
    
    assert_response_structure(response, ["query_execution_id", "status"])
    assert response["query_execution_id"] == query_execution_id


def test_athena_get_results_mcp(mcp_client, aws_setup):
    """Test athena_get_results via MCP protocol."""
    # Start and wait for query
    start_response = call_tool(
        mcp_client,
        "athena_start_query",
        sql="SELECT 1 as test_column",
        database="default"
    )
    
    query_execution_id = start_response["query_execution_id"]
    
    # Poll until complete (simplified - in real test would wait properly)
    import time
    time.sleep(2)
    
    # Get results
    response = call_tool(
        mcp_client,
        "athena_get_results",
        query_execution_id=query_execution_id
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])


def test_athena_explain_mcp(mcp_client, aws_setup):
    """Test athena_explain via MCP protocol."""
    response = call_tool(
        mcp_client,
        "athena_explain",
        sql="SELECT * FROM test_table",
        database="default"
    )
    
    assert_response_structure(response, ["query_execution_id", "status"])


def test_s3_list_prefix_mcp(mcp_client, aws_setup):
    """Test s3_list_prefix via MCP protocol."""
    # Create a test bucket using moto
    import boto3
    s3_client = boto3.client("s3", region_name="us-east-1")
    test_bucket = "test-bucket-mcp"
    s3_client.create_bucket(Bucket=test_bucket)
    s3_client.put_object(Bucket=test_bucket, Key="test/file.txt", Body=b"test content")
    
    response = call_tool(
        mcp_client,
        "s3_list_prefix",
        bucket=test_bucket,
        prefix="",
        max_keys=10
    )
    
    assert_response_structure(response, ["objects", "object_count"])
    assert isinstance(response["objects"], list)
    assert len(response["objects"]) >= 1


def test_athena_start_query_readonly_validation_mcp(mcp_client, aws_setup):
    """Test athena_start_query rejects non-SELECT queries via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "athena_start_query",
            sql="DROP TABLE test",
            database="default"
        )


def test_athena_start_query_with_workgroup_mcp(mcp_client, aws_setup):
    """Test athena_start_query with workgroup via MCP protocol."""
    response = call_tool(
        mcp_client,
        "athena_start_query",
        sql="SELECT 1",
        database="default",
        workgroup="primary"
    )
    
    assert_response_structure(response, ["query_execution_id"])


def test_athena_start_query_with_output_s3_mcp(mcp_client, aws_setup):
    """Test athena_start_query with output S3 path via MCP protocol."""
    # Create test bucket using moto
    import boto3
    s3_client = boto3.client("s3", region_name="us-east-1")
    test_bucket = "test-athena-output"
    s3_client.create_bucket(Bucket=test_bucket)
    
    response = call_tool(
        mcp_client,
        "athena_start_query",
        sql="SELECT 1",
        database="default",
        output_s3=f"s3://{test_bucket}/results/"
    )
    
    assert_response_structure(response, ["query_execution_id"])


def test_athena_partitions_list_mcp(mcp_client, aws_setup):
    """Test athena_partitions_list via MCP protocol."""
    response = call_tool(
        mcp_client,
        "athena_partitions_list",
        database="default",
        table="test_table",
        max_partitions=10
    )
    
    assert_response_structure(response, ["partitions", "partition_count"])


def test_athena_repair_table_mcp(mcp_client, aws_setup):
    """Test athena_repair_table via MCP protocol."""
    response = call_tool(
        mcp_client,
        "athena_repair_table",
        database="default",
        table="test_table"
    )
    
    assert_response_structure(response, ["query_execution_id", "status"])


def test_athena_ctas_export_mcp(mcp_client, aws_setup):
    """Test athena_ctas_export via MCP protocol."""
    # Create test bucket using moto
    import boto3
    s3_client = boto3.client("s3", region_name="us-east-1")
    test_bucket = "test-ctas-export"
    s3_client.create_bucket(Bucket=test_bucket)
    
    response = call_tool(
        mcp_client,
        "athena_ctas_export",
        database="default",
        dest_table="test_export_table",
        select_sql="SELECT 1 as x",
        output_s3=f"s3://{test_bucket}/exports/"
    )
    
    assert_response_structure(response, ["query_execution_id", "status"])

