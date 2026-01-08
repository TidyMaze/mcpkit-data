"""AWS tools for MCP server."""

from typing import Annotated, Optional

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import aws_ops
from mcpkit.server_models import (
    AthenaPartition,
    AthenaPartitionsResponse,
    AthenaPollResponse,
    AthenaQueryResponse,
    AthenaResultsResponse,
    S3ListResponse,
    S3Object,
)
from mcpkit.server_utils import _to_int


def register_aws_tools(mcp: FastMCP):
    """Register AWS tools with MCP server."""
    
    @mcp.tool()
    def athena_start_query(
        sql: Annotated[str, Field(description="SQL query string")],
        database: Annotated[str, Field(description="Athena database name")],
        workgroup: Annotated[Optional[str], Field(description="Optional Athena workgroup")] = None,
        output_s3: Annotated[Optional[str], Field(description="Optional S3 path for results")] = None,
        validate_readonly: Annotated[bool, Field(description="Validate query is read-only (default: True). Set to False to allow DDL/DML (not recommended).")] = True,
        region: Annotated[Optional[str], Field(description="Optional AWS region (default: from AWS_REGION env var or AWS config)")] = None,
    ) -> AthenaQueryResponse:
        """Start Athena query execution. Queries are validated as read-only by default (SELECT/WITH only)."""
        result = aws_ops.athena_start_query(sql, database, workgroup, output_s3, validate_readonly, region)
        return AthenaQueryResponse(**result)

    @mcp.tool()
    def athena_poll_query(
        query_execution_id: Annotated[str, Field(description="Query execution ID from athena_start_query")],
        region: Annotated[Optional[str], Field(description="Optional AWS region (default: from AWS_REGION env var or AWS config)")] = None,
    ) -> AthenaPollResponse:
        """Poll Athena query execution status."""
        result = aws_ops.athena_poll_query(query_execution_id, region)
        return AthenaPollResponse(**result)

    @mcp.tool()
    def athena_get_results(
        query_execution_id: Annotated[str, Field(description="Query execution ID from athena_start_query")],
        max_rows: Annotated[Optional[int], Field(description="Optional max rows to return. Default: MCPKIT_MAX_ROWS")] = None,
        region: Annotated[Optional[str], Field(description="Optional AWS region (default: from AWS_REGION env var or AWS config)")] = None,
    ) -> AthenaResultsResponse:
        """Get Athena query results."""
        result = aws_ops.athena_get_results(query_execution_id, max_rows, region)
        return AthenaResultsResponse(**result)

    @mcp.tool()
    def athena_explain(
        sql: Annotated[str, Field(description="SQL query string")],
        database: Annotated[str, Field(description="Athena database name")],
        workgroup: Annotated[Optional[str], Field(description="Optional Athena workgroup")] = None,
        output_s3: Annotated[Optional[str], Field(description="Optional S3 path for results")] = None,
    ) -> AthenaQueryResponse:
        """Explain Athena query plan."""
        result = aws_ops.athena_explain(sql, database, workgroup, output_s3)
        return AthenaQueryResponse(**result)

    @mcp.tool()
    def athena_partitions_list(
        database: Annotated[str, Field(description="Athena database name")],
        table: Annotated[str, Field(description="Table name")],
        max_partitions: Annotated[int, Field(description="Maximum partitions to return. Default: 200")] = 200,
    ) -> AthenaPartitionsResponse:
        """List Athena table partitions."""
        max_partitions = _to_int(max_partitions, 200)
        result = aws_ops.athena_partitions_list(database, table, max_partitions)
        partitions = [AthenaPartition(**p) for p in result["partitions"]]
        return AthenaPartitionsResponse(partitions=partitions, partition_count=result["partition_count"])

    @mcp.tool()
    def athena_repair_table(
        database: Annotated[str, Field(description="Athena database name")],
        table: Annotated[str, Field(description="Table name")],
        workgroup: Annotated[Optional[str], Field(description="Optional Athena workgroup")] = None,
        output_s3: Annotated[Optional[str], Field(description="Optional S3 path for results")] = None,
    ) -> AthenaQueryResponse:
        """Repair Athena table (MSCK REPAIR TABLE)."""
        result = aws_ops.athena_repair_table(database, table, workgroup, output_s3)
        return AthenaQueryResponse(**result)

    @mcp.tool()
    def athena_ctas_export(
        database: Annotated[str, Field(description="Athena database name")],
        dest_table: Annotated[str, Field(description="Destination table name")],
        select_sql: Annotated[str, Field(description="SELECT query")],
        output_s3: Annotated[str, Field(description="S3 path for output")],
        format: Annotated[str, Field(description="Output format: \"PARQUET\", \"ORC\", \"JSON\", \"TEXTFILE\" (default: \"PARQUET\")")] = "PARQUET",
        workgroup: Annotated[Optional[str], Field(description="Optional Athena workgroup")] = None,
    ) -> AthenaQueryResponse:
        """Create table as select (CTAS) for export."""
        result = aws_ops.athena_ctas_export(database, dest_table, select_sql, output_s3, format, workgroup)
        return AthenaQueryResponse(**result)

    @mcp.tool()
    def s3_list_prefix(
        bucket: Annotated[str, Field(description="S3 bucket name")],
        prefix: Annotated[str, Field(description="Object key prefix")],
        max_keys: Annotated[int, Field(description="Maximum keys to return. Default: 200")] = 200,
    ) -> S3ListResponse:
        """List S3 objects with prefix."""
        max_keys = _to_int(max_keys, 200)
        result = aws_ops.s3_list_prefix(bucket, prefix, max_keys)
        objects = [S3Object(**o) for o in result["objects"]]
        return S3ListResponse(objects=objects, object_count=result["object_count"])

