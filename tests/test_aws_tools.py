"""Tests for AWS tools."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.stub import Stubber

from mcpkit.core.aws_ops import (
    athena_get_results,
    athena_partitions_list,
    athena_poll_query,
    athena_start_query,
    s3_list_prefix,
)


@patch("mcpkit.core.aws_ops.get_athena_client")
def test_athena_start_query_mock(mock_get_client):
    """Test starting Athena query."""
    mock_client = MagicMock()
    mock_client.start_query_execution.return_value = {"QueryExecutionId": "query-123"}
    mock_get_client.return_value = mock_client
    
    result = athena_start_query("SELECT 1", "test_db")
    assert "query_execution_id" in result
    assert result["query_execution_id"] == "query-123"


@patch("mcpkit.core.aws_ops.get_athena_client")
def test_athena_poll_query_mock(mock_get_client):
    """Test polling Athena query."""
    mock_client = MagicMock()
    mock_client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "Statistics": {
                "DataScannedInBytes": 1000,
                "TotalExecutionTimeInMillis": 500,
            },
        }
    }
    mock_get_client.return_value = mock_client
    
    result = athena_poll_query("query-123")
    assert result["status"] == "SUCCEEDED"
    assert "data_scanned_bytes" in result


@patch("mcpkit.core.aws_ops.get_athena_client")
def test_athena_get_results_mock(mock_get_client):
    """Test getting Athena results."""
    mock_client = MagicMock()
    mock_client.get_query_results.return_value = {
        "ResultSet": {
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {"Name": "col1"},
                    {"Name": "col2"},
                ]
            },
            "Rows": [
                {"Data": [{"VarCharValue": "col1"}, {"VarCharValue": "col2"}]},  # header
                {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "a"}]},
            ],
        }
    }
    mock_get_client.return_value = mock_client
    
    result = athena_get_results("query-123", max_rows=10)
    assert "columns" in result
    assert "rows" in result
    assert result["columns"] == ["col1", "col2"]


@patch("mcpkit.core.aws_ops.get_glue_client")
def test_athena_partitions_list_mock(mock_get_client):
    """Test listing partitions."""
    mock_client = MagicMock()
    mock_client.get_partitions.return_value = {
        "Partitions": [
            {
                "Values": ["2024-01-01"],
                "StorageDescriptor": {"Location": "s3://bucket/table/2024-01-01"},
                "Parameters": {},
            }
        ]
    }
    mock_get_client.return_value = mock_client
    
    result = athena_partitions_list("test_db", "test_table")
    assert "partitions" in result
    assert len(result["partitions"]) == 1


@patch("mcpkit.core.aws_ops.get_s3_client")
def test_s3_list_prefix_mock(mock_get_client):
    """Test listing S3 objects."""
    mock_client = MagicMock()
    mock_client.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": "prefix/file1.txt",
                "Size": 100,
                "LastModified": "2024-01-01T00:00:00Z",
                "ETag": '"abc123"',
            }
        ]
    }
    mock_get_client.return_value = mock_client
    
    result = s3_list_prefix("bucket", "prefix/")
    assert "objects" in result
    assert len(result["objects"]) == 1
    assert result["objects"][0]["key"] == "prefix/file1.txt"

