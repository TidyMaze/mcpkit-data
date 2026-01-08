"""Integration tests for AWS tools via MCP protocol.

Uses LocalStack Docker container for AWS services - no real AWS credentials needed.
LocalStack provides S3, Athena, and Glue emulation.
"""

import os
import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


@pytest.fixture
def s3_bucket(aws_setup):
    """Create a test S3 bucket in LocalStack."""
    import boto3
    endpoint_url = aws_setup
    s3_client = boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")
    test_bucket = f"test-bucket-{int(__import__('time').time())}"
    s3_client.create_bucket(Bucket=test_bucket)
    yield test_bucket
    # Cleanup
    try:
        # Delete all objects first
        objects = s3_client.list_objects_v2(Bucket=test_bucket)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                s3_client.delete_object(Bucket=test_bucket, Key=obj["Key"])
        s3_client.delete_bucket(Bucket=test_bucket)
    except Exception:
        pass


def test_s3_list_prefix_mcp(mcp_client, aws_setup, s3_bucket):
    """Test s3_list_prefix via MCP protocol."""
    # Create test objects
    import boto3
    endpoint_url = aws_setup
    s3_client = boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")
    s3_client.put_object(Bucket=s3_bucket, Key="test/file.txt", Body=b"test content")
    s3_client.put_object(Bucket=s3_bucket, Key="test/file2.txt", Body=b"test content 2")
    
    response = call_tool(
        mcp_client,
        "s3_list_prefix",
        bucket=s3_bucket,
        prefix="test/",
        max_keys=10
    )
    
    assert_response_structure(response, ["objects", "object_count"])
    assert isinstance(response["objects"], list)
    assert len(response["objects"]) >= 2


def test_s3_list_prefix_empty_bucket_mcp(mcp_client, aws_setup, s3_bucket):
    """Test s3_list_prefix with empty bucket via MCP protocol."""
    response = call_tool(
        mcp_client,
        "s3_list_prefix",
        bucket=s3_bucket,
        prefix="",
        max_keys=10
    )
    
    assert_response_structure(response, ["objects", "object_count"])
    assert isinstance(response["objects"], list)
    assert response["object_count"] == 0


def test_s3_list_prefix_nonexistent_bucket_mcp(mcp_client, aws_setup):
    """Test s3_list_prefix with non-existent bucket via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError or AWS error
        call_tool(
            mcp_client,
            "s3_list_prefix",
            bucket="nonexistent-bucket-xyz",
            prefix="",
            max_keys=10
        )


# Note: LocalStack's Athena support is limited and may not work perfectly
# These tests may need to be skipped or adjusted based on LocalStack version
@pytest.mark.skip(reason="LocalStack Athena support is limited - requires proper setup")
def test_athena_start_query_mcp(mcp_client, aws_setup):
    """Test athena_start_query via MCP protocol."""
    # First create a database and table in Glue (via LocalStack)
    import boto3
    endpoint_url = aws_setup
    glue_client = boto3.client("glue", endpoint_url=endpoint_url, region_name="us-east-1")
    
    database_name = "test_db"
    try:
        glue_client.create_database(DatabaseInput={"Name": database_name})
    except Exception:
        pass  # Database might already exist
    
    response = call_tool(
        mcp_client,
        "athena_start_query",
        sql="SELECT 1 as test_column",
        database=database_name
    )
    
    assert_response_structure(response, ["query_execution_id", "status"])
    assert response["query_execution_id"] is not None


@pytest.mark.skip(reason="LocalStack Athena support is limited - requires proper setup")
def test_athena_poll_query_mcp(mcp_client, aws_setup):
    """Test athena_poll_query via MCP protocol."""
    import boto3
    endpoint_url = aws_setup
    glue_client = boto3.client("glue", endpoint_url=endpoint_url, region_name="us-east-1")
    
    database_name = "test_db"
    try:
        glue_client.create_database(DatabaseInput={"Name": database_name})
    except Exception:
        pass
    
    # First start a query
    start_response = call_tool(
        mcp_client,
        "athena_start_query",
        sql="SELECT 1 as test_column",
        database=database_name
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


@pytest.mark.skip(reason="LocalStack Athena support is limited - requires proper setup")
def test_athena_get_results_mcp(mcp_client, aws_setup):
    """Test athena_get_results via MCP protocol."""
    import boto3
    endpoint_url = aws_setup
    glue_client = boto3.client("glue", endpoint_url=endpoint_url, region_name="us-east-1")
    
    database_name = "test_db"
    try:
        glue_client.create_database(DatabaseInput={"Name": database_name})
    except Exception:
        pass
    
    # Start query
    start_response = call_tool(
        mcp_client,
        "athena_start_query",
        sql="SELECT 1 as test_column",
        database=database_name
    )
    
    query_execution_id = start_response["query_execution_id"]
    
    # Poll until complete
    import time
    max_wait = 30
    waited = 0
    while waited < max_wait:
        poll_response = call_tool(
            mcp_client,
            "athena_poll_query",
            query_execution_id=query_execution_id
        )
        if poll_response.get("status") in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)
        waited += 1
    
    # Get results
    response = call_tool(
        mcp_client,
        "athena_get_results",
        query_execution_id=query_execution_id
    )
    
    assert_response_structure(response, ["columns", "rows", "row_count"])


def test_athena_start_query_readonly_validation_mcp(mcp_client, aws_setup):
    """Test athena_start_query rejects non-SELECT queries via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "athena_start_query",
            sql="DROP TABLE test",
            database="default"
        )


def test_athena_start_query_rejects_insert_mcp(mcp_client, aws_setup):
    """Test athena_start_query rejects INSERT queries via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "athena_start_query",
            sql="INSERT INTO test VALUES (1)",
            database="default"
        )


def test_athena_start_query_rejects_update_mcp(mcp_client, aws_setup):
    """Test athena_start_query rejects UPDATE queries via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "athena_start_query",
            sql="UPDATE test SET x = 1",
            database="default"
        )


def test_s3_list_prefix_with_max_keys_mcp(mcp_client, aws_setup, s3_bucket):
    """Test s3_list_prefix respects max_keys limit via MCP protocol."""
    import boto3
    endpoint_url = aws_setup
    s3_client = boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")
    
    # Create more objects than max_keys
    for i in range(15):
        s3_client.put_object(Bucket=s3_bucket, Key=f"file_{i}.txt", Body=f"content {i}".encode())
    
    response = call_tool(
        mcp_client,
        "s3_list_prefix",
        bucket=s3_bucket,
        prefix="",
        max_keys=10
    )
    
    assert_response_structure(response, ["objects", "object_count"])
    assert len(response["objects"]) <= 10


def test_s3_list_prefix_nested_structure_mcp(mcp_client, aws_setup, s3_bucket):
    """Test s3_list_prefix with nested directory structure via MCP protocol."""
    import boto3
    endpoint_url = aws_setup
    s3_client = boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")
    
    # Create nested structure
    s3_client.put_object(Bucket=s3_bucket, Key="level1/level2/file1.txt", Body=b"content1")
    s3_client.put_object(Bucket=s3_bucket, Key="level1/level2/file2.txt", Body=b"content2")
    s3_client.put_object(Bucket=s3_bucket, Key="level1/file3.txt", Body=b"content3")
    
    # List level1 prefix
    response = call_tool(
        mcp_client,
        "s3_list_prefix",
        bucket=s3_bucket,
        prefix="level1/",
        max_keys=10
    )
    
    assert_response_structure(response, ["objects", "object_count"])
    assert response["object_count"] >= 3


def test_s3_list_prefix_partial_match_mcp(mcp_client, aws_setup, s3_bucket):
    """Test s3_list_prefix with partial prefix match via MCP protocol."""
    import boto3
    endpoint_url = aws_setup
    s3_client = boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")
    
    # Create objects with similar prefixes
    s3_client.put_object(Bucket=s3_bucket, Key="test_file1.txt", Body=b"content1")
    s3_client.put_object(Bucket=s3_bucket, Key="test_file2.txt", Body=b"content2")
    s3_client.put_object(Bucket=s3_bucket, Key="other_file.txt", Body=b"content3")
    
    # List with prefix "test"
    response = call_tool(
        mcp_client,
        "s3_list_prefix",
        bucket=s3_bucket,
        prefix="test",
        max_keys=10
    )
    
    assert_response_structure(response, ["objects", "object_count"])
    assert response["object_count"] >= 2
    # Verify all returned objects start with prefix
    for obj in response["objects"]:
        assert obj["key"].startswith("test")


def test_s3_list_prefix_empty_prefix_mcp(mcp_client, aws_setup, s3_bucket):
    """Test s3_list_prefix with empty prefix lists all objects via MCP protocol."""
    import boto3
    endpoint_url = aws_setup
    s3_client = boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")
    
    # Create objects in different "directories"
    s3_client.put_object(Bucket=s3_bucket, Key="a/file1.txt", Body=b"content1")
    s3_client.put_object(Bucket=s3_bucket, Key="b/file2.txt", Body=b"content2")
    s3_client.put_object(Bucket=s3_bucket, Key="file3.txt", Body=b"content3")
    
    response = call_tool(
        mcp_client,
        "s3_list_prefix",
        bucket=s3_bucket,
        prefix="",
        max_keys=10
    )
    
    assert_response_structure(response, ["objects", "object_count"])
    assert response["object_count"] >= 3


def test_s3_list_prefix_invalid_bucket_name_mcp(mcp_client, aws_setup):
    """Test s3_list_prefix with invalid bucket name via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError or AWS error
        call_tool(
            mcp_client,
            "s3_list_prefix",
            bucket="",  # Empty bucket name
            prefix="",
            max_keys=10
        )


def test_athena_start_query_rejects_delete_mcp(mcp_client, aws_setup):
    """Test athena_start_query rejects DELETE queries via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "athena_start_query",
            sql="DELETE FROM test WHERE id = 1",
            database="default"
        )


def test_athena_start_query_rejects_create_mcp(mcp_client, aws_setup):
    """Test athena_start_query rejects CREATE queries via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "athena_start_query",
            sql="CREATE TABLE test (id INT)",
            database="default"
        )


def test_athena_start_query_rejects_alter_mcp(mcp_client, aws_setup):
    """Test athena_start_query rejects ALTER queries via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "athena_start_query",
            sql="ALTER TABLE test ADD COLUMN x INT",
            database="default"
        )
