"""MCP server with all data engineering tools."""

from typing import Annotated, Any, Optional, Union

from fastmcp import FastMCP
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from mcpkit.core import (
    aws_ops,
    chart_ops,
    decode,
    duckdb_ops,
    evidence,
    formats,
    http_ops,
    jdbc,
    json_tools,
    kafka_client,
    nomad_consul,
    pandas_ops,
    polars_ops,
    registry,
    repo_ops,
    schema_registry,
)

mcp = FastMCP("mcpkit-all-ops")


# ============================================================================
# Response Models
# ============================================================================

class DatasetInfo(BaseModel):
    """Dataset information."""
    dataset_id: str = Field(description="Dataset identifier")
    created_at: str = Field(description="Creation timestamp (ISO format)")
    rows: int = Field(description="Number of rows")
    columns: list[str] = Field(description="List of column names")
    path: str = Field(description="Path to dataset file")


class DatasetListResponse(BaseModel):
    """Response for listing datasets."""
    datasets: list[DatasetInfo] = Field(description="List of datasets")


class DatasetInfoResponse(DatasetInfo):
    """Response for dataset info."""
    current_rows: int = Field(description="Current number of rows (from file)")
    current_columns: list[str] = Field(description="Current columns (from file)")


class DatasetCreateResponse(BaseModel):
    """Response for dataset creation."""
    dataset_id: str = Field(description="Dataset identifier")
    rows: int = Field(description="Number of rows")
    columns: list[str] = Field(description="List of column names")
    path: str = Field(description="Path to dataset file")


class DatasetDeleteResponse(BaseModel):
    """Response for dataset deletion."""
    dataset_id: str = Field(description="Dataset identifier")
    deleted: bool = Field(description="Whether deletion was successful")


class TopicListResponse(BaseModel):
    """Response for listing Kafka topics."""
    topics: list[str] = Field(description="List of topic names")
    topic_count: int = Field(description="Total number of topics")


class PartitionInfo(BaseModel):
    """Information about a Kafka partition."""
    high_water_mark: int = Field(description="Highest offset in partition")
    low_water_mark: int = Field(description="Lowest offset in partition")
    committed: Optional[int] = Field(description="Committed offset (if group_id provided)")


class KafkaOffsetsResponse(BaseModel):
    """Response for Kafka topic offsets."""
    topic: str = Field(description="Topic name")
    partitions: dict[int, PartitionInfo] = Field(description="Partition information by partition ID")


class KafkaConsumeResponse(BaseModel):
    """Response for consuming Kafka records."""
    dataset_id: str = Field(description="Dataset ID containing consumed records")
    record_count: int = Field(description="Number of records consumed")
    columns: list[str] = Field(description="Column names in the dataset")


class KafkaFilterResponse(BaseModel):
    """Response for filtering Kafka records."""
    records: list[dict] = Field(description="Filtered records")
    count: int = Field(description="Number of filtered records")


class KafkaGroupResponse(BaseModel):
    """Response for grouping Kafka records."""
    groups: dict[str, dict] = Field(description="Groups of records by key")
    group_count: int = Field(description="Number of groups")


class SchemaRegistryResponse(BaseModel):
    """Response from Schema Registry."""
    model_config = ConfigDict(populate_by_name=True)
    
    schema_id: int = Field(description="Schema ID")
    subject: Optional[str] = Field(description="Subject name (if subject provided)")
    version: Optional[int] = Field(description="Schema version (if subject provided)")
    schema_json: dict = Field(description="Schema as JSON")
    schema_string: str = Field(description="Schema as string")


class SchemaRegistryListResponse(BaseModel):
    """Response for listing Schema Registry subjects."""
    subjects: list[str] = Field(description="List of subject names")
    subject_count: int = Field(description="Total number of subjects")


class DecodeResponse(BaseModel):
    """Response for decoding messages."""
    decoded: dict = Field(description="Decoded message")


class QueryResultResponse(BaseModel):
    """Response for SQL queries."""
    columns: list[str] = Field(description="Column names")
    rows: list[list] = Field(description="Query result rows")
    row_count: int = Field(description="Number of rows returned")


class JDBCColumn(BaseModel):
    """JDBC column information."""
    name: str = Field(description="Column name")
    type: str = Field(description="Column type")
    nullable: bool = Field(description="Whether column is nullable")


class JDBCTable(BaseModel):
    """JDBC table information."""
    model_config = ConfigDict(populate_by_name=True)
    
    schema: str = Field(description="Schema name")
    table: str = Field(description="Table name")
    columns: list[JDBCColumn] = Field(description="Table columns")


class JDBCIntrospectResponse(BaseModel):
    """Response for JDBC introspection."""
    tables: list[JDBCTable] = Field(description="List of tables")
    table_count: int = Field(description="Number of tables")


class AthenaQueryResponse(BaseModel):
    """Response for Athena query operations."""
    query_execution_id: str = Field(description="Query execution ID")
    status: str = Field(description="Query status")


class AthenaPollResponse(BaseModel):
    """Response for polling Athena query status."""
    query_execution_id: str = Field(description="Query execution ID")
    status: str = Field(description="Query status (QUEUED|RUNNING|SUCCEEDED|FAILED|CANCELLED)")
    data_scanned_bytes: int = Field(description="Data scanned in bytes")
    execution_time_ms: int = Field(description="Execution time in milliseconds")
    error: Optional[str] = Field(default=None, description="Error message (if failed)")


class AthenaResultsResponse(BaseModel):
    """Response for Athena query results."""
    columns: list[str] = Field(description="Column names")
    rows: list[list] = Field(description="Query result rows")
    row_count: int = Field(description="Number of rows returned")


class AthenaPartition(BaseModel):
    """Athena partition information."""
    values: list[str] = Field(description="Partition values")
    location: str = Field(description="Partition location")
    parameters: dict = Field(description="Partition parameters")


class AthenaPartitionsResponse(BaseModel):
    """Response for listing Athena partitions."""
    partitions: list[AthenaPartition] = Field(description="List of partitions")
    partition_count: int = Field(description="Number of partitions")


class S3Object(BaseModel):
    """S3 object information."""
    key: str = Field(description="Object key")
    size: int = Field(description="Object size in bytes")
    last_modified: str = Field(description="Last modified timestamp (ISO format)")


class S3ListResponse(BaseModel):
    """Response for listing S3 objects."""
    objects: list[S3Object] = Field(description="List of S3 objects")
    object_count: int = Field(description="Number of objects")


class TransformResponse(BaseModel):
    """Response for data transformation."""
    result: dict | list | str | int | float | bool | None = Field(description="Transformed result")


class ValidateResponse(BaseModel):
    """Response for validation."""
    valid: bool = Field(description="Whether validation passed")
    errors: Optional[list[str]] = Field(description="Validation errors (if invalid)")


class FingerprintResponse(BaseModel):
    """Response for fingerprinting."""
    fingerprint: str = Field(description="SHA256 hash fingerprint")


class DedupeResponse(BaseModel):
    """Response for deduplication."""
    unique_count: int = Field(description="Number of unique records")
    original_count: int = Field(description="Original number of records")
    records: list[dict] = Field(description="Deduplicated records (keeps first occurrence)")


class CorrelatedEvent(BaseModel):
    """Correlated events."""
    join_key: dict | list | str | int | float | bool | None = Field(description="Join key value")
    events: list[dict] = Field(description="Events with same join_key")


class CorrelateResponse(BaseModel):
    """Response for event correlation."""
    correlated: list[CorrelatedEvent] = Field(description="Correlated events")
    correlation_count: int = Field(description="Number of correlations")


class DatasetStatsResponse(BaseModel):
    """Response for dataset statistics."""
    dataset_id: str = Field(description="Dataset identifier")
    description: Optional[dict] = Field(description="Statistical description (pandas describe output)")
    dtypes: dict[str, str] = Field(description="Column data types")


class DatasetOperationResponse(BaseModel):
    """Response for dataset operations (groupby, join, filter, etc.)."""
    dataset_id: str = Field(description="Dataset identifier")
    rows: int = Field(description="Number of rows")
    columns: list[str] = Field(description="List of column names")


class DatasetDiffResponse(BaseModel):
    """Response for dataset comparison."""
    only_in_a: int = Field(description="Records only in first dataset")
    only_in_b: int = Field(description="Records only in second dataset")
    common: int = Field(description="Records in both datasets")
    changed: list[dict] = Field(description="Changed rows with differences")
    changed_count: int = Field(description="Number of changed rows")


class SchemaCheckResponse(BaseModel):
    """Response for schema validation."""
    valid: bool = Field(description="Whether schema is valid")
    errors: Optional[list[str]] = Field(description="Validation errors (if invalid)")
    columns: list[str] = Field(description="Column names")
    dtypes: dict[str, str] = Field(description="Column data types")


class ExportResponse(BaseModel):
    """Response for dataset export."""
    dataset_id: str = Field(description="Dataset identifier")
    format: str = Field(description="Export format")
    filename: str = Field(description="Output filename")
    path: str = Field(description="Full path to exported file")


class ParquetField(BaseModel):
    """Parquet field information."""
    name: str = Field(description="Field name")
    type: str = Field(description="Field type")
    nullable: bool = Field(description="Whether field is nullable")


class ParquetSchema(BaseModel):
    """Parquet schema."""
    fields: list[ParquetField] = Field(description="Schema fields")


class ParquetInspectResponse(BaseModel):
    """Response for parquet inspection."""
    model_config = ConfigDict(populate_by_name=True)
    
    path: str = Field(description="File path")
    num_rows: int = Field(description="Number of rows")
    num_row_groups: int = Field(description="Number of row groups")
    schema: ParquetSchema = Field(description="Parquet schema")
    sample_rows: list[dict] = Field(description="Sample rows")


class ArrowConvertResponse(BaseModel):
    """Response for format conversion."""
    input_path: str = Field(description="Input file path")
    input_format: str = Field(description="Input format")
    output_path: str = Field(description="Output file path")
    output_format: str = Field(description="Output format")
    rows: int = Field(description="Number of rows converted")


class ReconcileResponse(BaseModel):
    """Response for reconciliation."""
    only_in_left: int = Field(description="Records only in left dataset")
    only_in_right: int = Field(description="Records only in right dataset")
    in_both: int = Field(description="Records in both datasets")
    examples: list[dict] = Field(description="Sample differences")


class EvidenceBundleResponse(BaseModel):
    """Response for evidence bundle."""
    dataset_id: str = Field(description="Dataset identifier")
    info: dict = Field(description="Dataset info")
    describe: Optional[dict] = Field(description="Statistical description (if include_describe)")
    sample_rows: Optional[list[dict]] = Field(description="Sample rows (if include_sample_rows > 0)")
    exported_files: list[str] = Field(description="Paths to exported files")


class FileReadResponse(BaseModel):
    """Response for file reading."""
    path: str = Field(description="Resolved file path")
    content: str = Field(description="File content")
    size: int = Field(description="File size in bytes")
    truncated: bool = Field(description="Whether content was truncated (if exceeds MCPKIT_MAX_OUTPUT_BYTES)")


class FileInfo(BaseModel):
    """File or directory information."""
    name: str = Field(description="File or directory name")
    path: str = Field(description="Full path")
    size: Optional[int] = Field(default=None, description="File size in bytes (for files only)")
    mtime: Optional[float] = Field(default=None, description="Modification time (for files only)")


class DirectoryListResponse(BaseModel):
    """Response for directory listing."""
    path: str = Field(description="Directory path")
    files: list[FileInfo] = Field(description="List of files")
    directories: list[FileInfo] = Field(description="List of subdirectories")
    file_count: int = Field(description="Number of files")
    directory_count: int = Field(description="Number of subdirectories")


class DistinctCountsResponse(BaseModel):
    """Response for distinct value counts."""
    dataset_id: str = Field(description="Dataset identifier")
    distinct_counts: dict[str, int] = Field(description="Distinct count per column")


class SearchMatch(BaseModel):
    """Search match information."""
    path: str = Field(description="File path")
    line_number: int = Field(description="Line number")
    text: str = Field(description="Matching text")


class SearchResponse(BaseModel):
    """Response for search operations."""
    root: str = Field(description="Root directory searched")
    pattern: str = Field(description="Search pattern")
    matches: list[SearchMatch] = Field(description="Search matches")
    match_count: int = Field(description="Number of matches")


class GEExpectationResult(BaseModel):
    """Great Expectations expectation result."""
    expectation_type: str = Field(description="Expectation type")
    success: bool = Field(description="Whether expectation passed")
    result: dict = Field(description="Expectation result details")


class GEValidationResponse(BaseModel):
    """Response for Great Expectations validation."""
    dataset_id: str = Field(description="Dataset identifier")
    success: bool = Field(description="Whether validation passed")
    statistics: dict = Field(description="Validation statistics")
    results: list[GEExpectationResult] = Field(description="Expectation results")


class ChartResponse(BaseModel):
    """Response for chart generation."""
    artifact_path: str = Field(description="Path to generated chart artifact")
    chart_spec: dict = Field(description="Final resolved chart specification used")
    detected: dict = Field(description="Detected schema/stats for columns")
    warnings: list[str] = Field(description="Warnings encountered during chart generation")
    sample: list[dict] = Field(description="Sample rows from dataset (capped)")


def _to_int(value, default=None):
    """Convert value to int if it's a string, otherwise return as-is or default."""
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    if isinstance(value, int):
        return value
    return default


def _parse_list_param(value, default=None):
    """Parse list parameter that might come as string (JSON array) or list."""
    if value is None:
        return default
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        if value.lower() == "null" or value == "":
            return default
        try:
            import json
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
    return default


def _list_validator(value):
    """Pydantic validator to convert string (JSON array) to list."""
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        if value.lower() == "null" or value == "":
            return None
        try:
            import json
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
    return value


def _int_validator(value):
    """Pydantic validator to convert string to int for Optional[int] parameters."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return int(value)
        except (ValueError, TypeError):
            raise ValueError(f"Cannot convert {value} to int")
    if isinstance(value, int):
        return value
    raise ValueError(f"Expected int or str, got {type(value).__name__}")




# ============================================================================
# Dataset Registry
# ============================================================================

@mcp.tool()
def dataset_list() -> DatasetListResponse:
    """List all datasets in registry."""
    result = registry.dataset_list()
    return DatasetListResponse(datasets=[DatasetInfo(**d) for d in result["datasets"]])


@mcp.tool()
def dataset_info(dataset_id: Annotated[str, Field(description="Dataset identifier (must be safe filename)")]) -> DatasetInfoResponse:
    """Get info about a dataset."""
    result = registry.dataset_info(dataset_id)
    return DatasetInfoResponse(**result)


@mcp.tool()
def dataset_put_rows(
    columns: Annotated[list[str], Field(description="List of column names")],
    rows: Annotated[list[list], Field(description="List of rows, each row is a list of values matching columns")],
    dataset_id: Annotated[Optional[str], Field(description="Optional dataset ID (auto-generated if None)")] = None,
) -> DatasetCreateResponse:
    """Store rows as parquet dataset in registry."""
    result = registry.dataset_put_rows(columns, rows, dataset_id)
    return DatasetCreateResponse(**result)


@mcp.tool()
def dataset_delete(dataset_id: Annotated[str, Field(description="Dataset ID to delete")]) -> DatasetDeleteResponse:
    """Delete a dataset from registry."""
    result = registry.dataset_delete(dataset_id)
    return DatasetDeleteResponse(**result)


# ============================================================================
# Kafka
# ============================================================================

@mcp.tool()
def kafka_list_topics(
    bootstrap_servers: Annotated[
        Optional[str],
        Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
    ] = None,
) -> TopicListResponse:
    """List all Kafka topics."""
    result = kafka_client.kafka_list_topics(bootstrap_servers)
    return TopicListResponse(**result)


@mcp.tool()
def kafka_offsets(
    topic: Annotated[str, Field(description="Topic name")],
    group_id: Annotated[Optional[str], Field(description="Optional consumer group ID (if provided, returns committed offsets)")] = None,
    bootstrap_servers: Annotated[
        Optional[str],
        Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
    ] = None,
) -> KafkaOffsetsResponse:
    """Get Kafka topic offsets."""
    result = kafka_client.kafka_offsets(topic, group_id, bootstrap_servers)
    partitions = {int(k): PartitionInfo(**v) for k, v in result["partitions"].items()}
    return KafkaOffsetsResponse(topic=result["topic"], partitions=partitions)


@mcp.tool()
def kafka_consume_batch(
    topic: Annotated[str, Field(description="Topic name")],
    partition: Annotated[Optional[Union[int, str]], Field(description="Optional partition number. If None, consumes from all partitions")] = None,
    from_offset: Annotated[Optional[Union[int, str]], Field(description="Optional starting offset. If None, starts from beginning")] = None,
    max_records: Annotated[Optional[Union[int, str]], Field(description="Optional max records to consume. Default: MCPKIT_MAX_RECORDS")] = None,
    timeout_secs: Annotated[Optional[Union[int, str]], Field(description="Optional timeout in seconds. Default: MCPKIT_TIMEOUT_SECS")] = None,
    bootstrap_servers: Annotated[
        Optional[str],
        Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
    ] = None,
) -> KafkaConsumeResponse:
    """Consume batch of records from Kafka topic and store as dataset."""
    # Convert string inputs to int (MCP client may pass strings)
    partition = _to_int(partition, None)
    from_offset = _to_int(from_offset, None)
    max_records = _to_int(max_records, None)
    timeout_secs = _to_int(timeout_secs, None)
    result = kafka_client.kafka_consume_batch(topic, partition, from_offset, max_records, timeout_secs, bootstrap_servers)
    return KafkaConsumeResponse(**result)


@mcp.tool()
def kafka_filter(
    records: Annotated[list[dict], Field(description="List of Kafka record dicts (from kafka_consume_batch)")],
    predicate_jmes: Annotated[str, Field(description="JMESPath expression (evaluates to truthy to include record)")],
) -> KafkaFilterResponse:
    """Filter Kafka records using JMESPath predicate."""
    filtered = []
    for record in records:
        try:
            result = json_tools.jq_transform(record, predicate_jmes)
            if result.get("result"):
                filtered.append(record)
        except Exception:
            continue
    return KafkaFilterResponse(records=filtered, count=len(filtered))


@mcp.tool()
def kafka_flatten(
    dataset_id: Annotated[str, Field(description="Input dataset ID containing Kafka records")],
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
) -> dict:
    """
    Flatten Kafka records from a dataset generically.
    
    Parses JSON values and flattens nested structures into columns.
    Works with any Kafka event structure. Input dataset should contain Kafka records
    with decoded key, value, and headers.
    
    Returns:
        dict with dataset_id, columns, rows, and record_count.
    """
    result = kafka_client.kafka_flatten_dataset(dataset_id, out_dataset_id)
    return result


@mcp.tool()
def kafka_groupby_key(
    records: Annotated[list[dict], Field(description="List of Kafka record dicts")],
    key_jmes: Annotated[str, Field(description="JMESPath expression to extract grouping key")],
    max_groups: Annotated[int, Field(description="Maximum number of groups to return. Default: 200")] = 200,
) -> KafkaGroupResponse:
    """Group Kafka records by key extracted via JMESPath."""
    groups = {}
    for record in records:
        try:
            key = json_tools.jq_transform(record, key_jmes).get("result")
            if key is not None:
                key_str = str(key)
                if key_str not in groups:
                    groups[key_str] = []
                groups[key_str].append(record)
                if len(groups) >= max_groups:
                    break
        except Exception:
            continue
    return KafkaGroupResponse(
        groups={k: {"count": len(v), "records": v[:10]} for k, v in groups.items()},
        group_count=len(groups)
    )


# ============================================================================
# Schema Registry + Decoding
# ============================================================================

@mcp.tool()
def schema_registry_get(
    schema_id: Annotated[Optional[int], Field(description="Optional schema ID. Must provide either schema_id or subject")] = None,
    subject: Annotated[Optional[str], Field(description="Optional subject name (returns latest version)")] = None,
    url: Annotated[
        Optional[str],
        Field(description="Optional Schema Registry URL. If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var")
    ] = None,
) -> SchemaRegistryResponse:
    """Get schema from Schema Registry by ID or subject (latest version)."""
    result = schema_registry.schema_registry_get(schema_id, subject, url)
    return SchemaRegistryResponse(**result)


@mcp.tool()
def schema_registry_list_subjects(
    url: Annotated[
        Optional[str],
        Field(description="Optional Schema Registry URL. If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var")
    ] = None,
) -> SchemaRegistryListResponse:
    """List all subjects in Schema Registry."""
    result = schema_registry.schema_registry_list_subjects(url)
    return SchemaRegistryListResponse(**result)


@mcp.tool()
def avro_decode(
    value_base64: Annotated[str, Field(description="Base64-encoded Avro value")],
    schema_json: Annotated[Optional[dict], Field(description="Optional schema dict (if None, expects Confluent format with schema ID)")] = None,
    schema_registry_url: Annotated[
        Optional[str],
        Field(description="Optional Schema Registry URL (for fetching schema by ID). If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var")
    ] = None,
) -> DecodeResponse:
    """Decode Avro-encoded value (base64)."""
    result = decode.avro_decode(value_base64, schema_json, schema_registry_url)
    return DecodeResponse(**result)


@mcp.tool()
def protobuf_decode(
    value_base64: Annotated[str, Field(description="Base64-encoded Protobuf message")],
    file_descriptor_set_base64: Annotated[str, Field(description="Base64-encoded FileDescriptorSet")],
    message_full_name: Annotated[str, Field(description="Full message type name (e.g., \"com.example.Message\")")],
) -> DecodeResponse:
    """Decode Protobuf-encoded value."""
    result = decode.protobuf_decode(value_base64, file_descriptor_set_base64, message_full_name)
    return DecodeResponse(**result)


# ============================================================================
# JDBC
# ============================================================================

@mcp.tool()
def jdbc_query_ro(
    query: Annotated[str, Field(description="SQL query (must start with SELECT, WITH, EXPLAIN SELECT, or EXPLAIN WITH)")],
    params: Annotated[Optional[list[str | int | float | bool | None]], Field(description="Optional list of query parameters")] = None,
) -> QueryResultResponse:
    """Execute read-only JDBC query (SELECT/WITH only, no mutations)."""
    result = jdbc.jdbc_query_ro(query, params)
    return QueryResultResponse(**result)


@mcp.tool()
def jdbc_introspect(
    schema_like: Annotated[Optional[str], Field(description="Optional schema name pattern (SQL LIKE)")] = None,
    table_like: Annotated[Optional[str], Field(description="Optional table name pattern (SQL LIKE)")] = None,
    max_tables: Annotated[int, Field(description="Maximum tables to return. Default: 200")] = 200,
) -> JDBCIntrospectResponse:
    """Introspect JDBC database schema."""
    max_tables = _to_int(max_tables, 200)
    result = jdbc.jdbc_introspect(schema_like, table_like, max_tables)
    tables = [JDBCTable(**t) for t in result["tables"]]
    return JDBCIntrospectResponse(tables=tables, table_count=result["table_count"])


# ============================================================================
# AWS (Athena, S3, Glue)
# ============================================================================

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


# ============================================================================
# JSON / QA / Correlation
# ============================================================================

@mcp.tool()
def jq_transform(
    data: Annotated[dict | list | str | int | float | bool | None, Field(description="Input data (dict, list, or primitive)")],
    expression: Annotated[str, Field(description="JMESPath expression")],
) -> TransformResponse:
    """Transform data using JMESPath expression."""
    result = json_tools.jq_transform(data, expression)
    return TransformResponse(**result)


@mcp.tool()
def event_validate(
    record: Annotated[dict, Field(description="Record dict to validate")],
    schema: Annotated[dict, Field(description="JSONSchema dict")],
) -> ValidateResponse:
    """Validate record against JSONSchema."""
    result = json_tools.event_validate(record, schema)
    return ValidateResponse(**result)


@mcp.tool()
def event_fingerprint(
    record: Annotated[dict, Field(description="Record dict or object")],
    fields: Annotated[Optional[Union[list[str], str]], BeforeValidator(_list_validator), Field(description="Optional list of field names to include (if None, uses all fields)")] = None,
) -> FingerprintResponse:
    """Generate fingerprint for record."""
    result = json_tools.event_fingerprint(record, fields)
    return FingerprintResponse(**result)


@mcp.tool()
def dedupe_by_id(
    records: Annotated[list[dict], Field(description="List of record dicts")],
    id_jmes: Annotated[str, Field(description="JMESPath expression to extract ID")],
) -> DedupeResponse:
    """Deduplicate records by extracting ID using JMESPath."""
    result = json_tools.dedupe_by_id(records, id_jmes)
    return DedupeResponse(**result)


@mcp.tool()
def event_correlate(
    batches: Annotated[list[list[dict]], Field(description="List of batches, each batch is a list of record dicts")],
    join_key_jmes: Annotated[str, Field(description="JMESPath expression to extract join key")],
    timestamp_field: Annotated[str, Field(description="Field name containing timestamp (default: \"timestamp\")")] = "timestamp",
) -> CorrelateResponse:
    """Correlate events across batches using join key and timestamp."""
    result = json_tools.event_correlate(batches, join_key_jmes, timestamp_field)
    correlated = [CorrelatedEvent(**c) for c in result["correlated"]]
    return CorrelateResponse(correlated=correlated, correlation_count=result["correlation_count"])


# ============================================================================
# Pandas
# ============================================================================

@mcp.tool()
def pandas_from_rows(
    columns: Annotated[list[str], Field(description="List of column names")],
    rows: Annotated[list[list], Field(description="List of rows, each row is a list of values matching columns")],
    dataset_id: Annotated[Optional[str], Field(description="Optional dataset ID (auto-generated if None)")] = None,
) -> DatasetCreateResponse:
    """Create pandas DataFrame from rows and save to registry."""
    result = pandas_ops.pandas_from_rows(columns, rows, dataset_id)
    return DatasetCreateResponse(**result)


@mcp.tool()
def pandas_describe(
    dataset_id: Annotated[str, Field(description="Dataset ID")],
    include: Annotated[str, Field(description="Statistics to include: \"all\", \"numeric\", or \"object\" (default: \"all\")")] = "all",
) -> DatasetStatsResponse:
    """Describe dataset statistics."""
    result = pandas_ops.pandas_describe(dataset_id, include)
    return DatasetStatsResponse(**result)


@mcp.tool()
def pandas_groupby(
    dataset_id: Annotated[str, Field(description="Input dataset ID")],
    group_cols: Annotated[list[str], Field(description="List of column names to group by")],
    aggs: Annotated[dict[str, list[str]], Field(description="Dict mapping column names to list of aggregation functions. Allowed: \"count\", \"sum\", \"min\", \"max\", \"mean\", \"nunique\"")],
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
) -> DatasetOperationResponse:
    """Group by columns with aggregations."""
    result = pandas_ops.pandas_groupby(dataset_id, group_cols, aggs, out_dataset_id)
    return DatasetOperationResponse(**result)


@mcp.tool()
def pandas_join(
    left_dataset_id: Annotated[str, Field(description="Left dataset ID")],
    right_dataset_id: Annotated[str, Field(description="Right dataset ID")],
    keys: Annotated[list[str], Field(description="List of column names to join on")],
    how: Annotated[str, Field(description="Join type: \"inner\", \"left\", \"right\", \"outer\" (default: \"inner\")")] = "inner",
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
) -> DatasetOperationResponse:
    """Join two datasets."""
    result = pandas_ops.pandas_join(left_dataset_id, right_dataset_id, keys, how, out_dataset_id)
    return DatasetOperationResponse(**result)


@mcp.tool()
def pandas_filter_query(
    dataset_id: Annotated[str, Field(description="Input dataset ID")],
    filters: Annotated[list[dict], Field(description="List of filter dicts, each with: \"column\" (str), \"op\" (str: ==, !=, <, <=, >, >=, in, contains, startswith, endswith), \"value\" (any)")],
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
) -> DatasetOperationResponse:
    """Filter dataset using query conditions."""
    result = pandas_ops.pandas_filter_query(dataset_id, filters, out_dataset_id)
    return DatasetOperationResponse(**result)


@mcp.tool()
def pandas_filter_time_range(
    dataset_id: Annotated[str, Field(description="Input dataset ID")],
    timestamp_column: Annotated[str, Field(description="Name of timestamp column")],
    start_time: Annotated[Optional[str], Field(description="Optional start time (ISO format string or pandas-parsable)")] = None,
    end_time: Annotated[Optional[str], Field(description="Optional end time (ISO format string or pandas-parsable)")] = None,
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
) -> DatasetOperationResponse:
    """Filter dataset by timestamp column."""
    result = pandas_ops.pandas_filter_time_range(dataset_id, timestamp_column, start_time, end_time, out_dataset_id)
    return DatasetOperationResponse(**result)


@mcp.tool()
def pandas_diff_frames(
    dataset_a: Annotated[str, Field(description="First dataset ID")],
    dataset_b: Annotated[str, Field(description="Second dataset ID")],
    key_cols: Annotated[list[str], Field(description="List of column names to use as keys")],
    compare_cols: Annotated[Optional[Union[list[str], str]], BeforeValidator(_list_validator), Field(description="Optional list of columns to compare (if None, compares all non-key columns)")] = None,
    max_changed: Annotated[int, Field(description="Maximum changed rows to return. Default: 200")] = 200,
) -> DatasetDiffResponse:
    """Compare two datasets and find differences."""
    max_changed = _to_int(max_changed, 200)
    result = pandas_ops.pandas_diff_frames(dataset_a, dataset_b, key_cols, compare_cols, max_changed)
    return DatasetDiffResponse(**result)


@mcp.tool()
def pandas_schema_check(
    dataset_id: Annotated[str, Field(description="Dataset ID")],
    required_columns: Annotated[Optional[Union[list[str], str]], BeforeValidator(_list_validator), Field(description="Optional list of required column names")] = None,
    dtype_overrides: Annotated[Optional[dict[str, str]], Field(description="Optional dict mapping column names to expected dtypes")] = None,
    non_null_columns: Annotated[Optional[Union[list[str], str]], BeforeValidator(_list_validator), Field(description="Optional list of columns that must not be null")] = None,
) -> SchemaCheckResponse:
    """Check dataset schema constraints."""
    result = pandas_ops.pandas_schema_check(dataset_id, required_columns, dtype_overrides, non_null_columns)
    return SchemaCheckResponse(**result)


@mcp.tool()
def pandas_sample_stratified(
    dataset_id: Annotated[str, Field(description="Input dataset ID")],
    strata_cols: Annotated[list[str], Field(description="List of column names to stratify by")],
    n_per_group: Annotated[int, Field(description="Number of samples per group. Default: 5")] = 5,
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
) -> DatasetOperationResponse:
    """Stratified sampling."""
    n_per_group = _to_int(n_per_group, 5)
    result = pandas_ops.pandas_sample_stratified(dataset_id, strata_cols, n_per_group, out_dataset_id)
    return DatasetOperationResponse(**result)


@mcp.tool()
def pandas_sample_random(
    dataset_id: Annotated[str, Field(description="Input dataset ID")],
    n: Annotated[Union[int, str], Field(description="Number of samples to take")],
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
    seed: Annotated[Optional[Union[int, str]], Field(description="Optional random seed for reproducibility")] = None,
) -> DatasetOperationResponse:
    """Random sampling from dataset."""
    n = _to_int(n, None)
    seed = _to_int(seed, None)
    result = pandas_ops.pandas_sample_random(dataset_id, n, out_dataset_id, seed)
    return DatasetOperationResponse(**result)


@mcp.tool()
def pandas_count_distinct(
    dataset_id: Annotated[str, Field(description="Dataset ID")],
    columns: Annotated[Optional[Union[list[str], str]], BeforeValidator(_list_validator), Field(description="Optional list of columns to count. If None, counts all columns.")] = None,
) -> DistinctCountsResponse:
    """Count distinct values per column."""
    result = pandas_ops.pandas_count_distinct(dataset_id, columns)
    return DistinctCountsResponse(**result)


@mcp.tool()
def pandas_export(
    dataset_id: Annotated[str, Field(description="Dataset ID")],
    format: Annotated[str, Field(description="Export format: \"csv\", \"json\", \"parquet\", \"excel\"")],
    filename: Annotated[str, Field(description="Output filename (must be safe, no slashes)")],
) -> ExportResponse:
    """Export dataset to file in artifact directory."""
    result = pandas_ops.pandas_export(dataset_id, format, filename)
    return ExportResponse(**result)


# ============================================================================
# Polars
# ============================================================================

@mcp.tool()
def polars_from_rows(
    columns: Annotated[list[str], Field(description="List of column names")],
    rows: Annotated[list[list], Field(description="List of rows, each row is a list of values matching columns")],
    dataset_id: Annotated[Optional[str], Field(description="Optional dataset ID (auto-generated if None)")] = None,
) -> DatasetCreateResponse:
    """Create polars DataFrame from rows and save to registry."""
    result = polars_ops.polars_from_rows(columns, rows, dataset_id)
    return DatasetCreateResponse(**result)


@mcp.tool()
def polars_groupby(
    dataset_id: Annotated[str, Field(description="Input dataset ID")],
    group_cols: Annotated[list[str], Field(description="List of column names to group by")],
    aggs: Annotated[dict[str, list[str]], Field(description="Dict mapping column names to list of aggregation functions. Allowed: \"count\", \"sum\", \"min\", \"max\", \"mean\", \"nunique\"")],
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
) -> DatasetOperationResponse:
    """Group by columns with aggregations (Polars)."""
    result = polars_ops.polars_groupby(dataset_id, group_cols, aggs, out_dataset_id)
    return DatasetOperationResponse(**result)


@mcp.tool()
def polars_export(
    dataset_id: Annotated[str, Field(description="Dataset ID")],
    format: Annotated[str, Field(description="Export format: \"csv\", \"json\", \"parquet\"")],
    filename: Annotated[str, Field(description="Output filename (must be safe, no slashes)")],
) -> ExportResponse:
    """Export dataset to file in artifact directory (Polars)."""
    result = polars_ops.polars_export(dataset_id, format, filename)
    return ExportResponse(**result)


# ============================================================================
# DuckDB
# ============================================================================

@mcp.tool()
def duckdb_query_local(
    sql: Annotated[str, Field(description="SQL query string")],
    sources: Annotated[Optional[Union[list[dict], str]], BeforeValidator(_list_validator), Field(description="Optional list of source definitions: {\"name\": str, \"dataset_id\": str} for registry datasets OR {\"name\": str, \"path\": str, \"format\": \"parquet\"|\"csv\"|\"json\"|\"jsonl\"} for files")] = None,
    max_rows: Annotated[Optional[int], Field(description="Optional max rows to return. Default: MCPKIT_MAX_ROWS")] = None,
) -> QueryResultResponse:
    """Execute SQL query on local sources (DuckDB)."""
    max_rows = _to_int(max_rows, None)
    result = duckdb_ops.duckdb_query_local(sql, sources, max_rows)
    return QueryResultResponse(**result)


# ============================================================================
# File Formats + Evidence
# ============================================================================

@mcp.tool()
def parquet_inspect(
    path: Annotated[str, Field(description="Path to parquet file (must be within allowed roots)")],
    sample_rows: Annotated[int, Field(description="Number of sample rows to return. Default: 20")] = 20,
) -> ParquetInspectResponse:
    """Inspect parquet file and return schema + sample rows."""
    sample_rows = _to_int(sample_rows, 20)
    result = formats.parquet_inspect(path, sample_rows)
    schema = ParquetSchema(fields=[ParquetField(**f) for f in result["schema"]["fields"]])
    return ParquetInspectResponse(
        path=result["path"],
        num_rows=result["num_rows"],
        num_row_groups=result["num_row_groups"],
        schema=schema,
        sample_rows=result["sample_rows"]
    )


@mcp.tool()
def arrow_convert(
    input_path: Annotated[str, Field(description="Input file path (must be within allowed roots)")],
    input_format: Annotated[str, Field(description="Input format: \"PARQUET\", \"CSV\", \"JSON\"")],
    output_format: Annotated[str, Field(description="Output format: \"PARQUET\", \"CSV\", \"JSON\"")],
    output_filename: Annotated[str, Field(description="Output filename (must be safe, no slashes)")],
) -> ArrowConvertResponse:
    """Convert between arrow-supported formats."""
    result = formats.arrow_convert(input_path, input_format, output_format, output_filename)
    return ArrowConvertResponse(**result)


@mcp.tool()
def reconcile_counts(
    left_dataset_id: Annotated[str, Field(description="First dataset ID")],
    right_dataset_id: Annotated[str, Field(description="Second dataset ID")],
    key_cols: Annotated[list[str], Field(description="List of column names to use as keys")],
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID for reconciliation results")] = None,
    max_examples: Annotated[int, Field(description="Maximum examples to return. Default: 200")] = 200,
) -> ReconcileResponse:
    """Reconcile record counts between two datasets."""
    max_examples = _to_int(max_examples, 200)
    result = evidence.reconcile_counts(left_dataset_id, right_dataset_id, key_cols, out_dataset_id, max_examples)
    return ReconcileResponse(**result)


@mcp.tool()
def evidence_bundle_plus(
    dataset_id: Annotated[str, Field(description="Dataset ID")],
    base_filename: Annotated[str, Field(description="Base filename for exports (must be safe, no slashes)")],
    include_describe: Annotated[bool, Field(description="Include statistical description (default: True)")] = True,
    include_sample_rows: Annotated[int, Field(description="Number of sample rows to include. Default: 50")] = 50,
) -> EvidenceBundleResponse:
    """Generate evidence bundle: dataset info, describe, sample rows, exported files."""
    include_sample_rows = _to_int(include_sample_rows, 50)
    result = evidence.evidence_bundle_plus(dataset_id, base_filename, include_describe, include_sample_rows)
    return EvidenceBundleResponse(**result)


# ============================================================================
# Repo Helpers
# ============================================================================

@mcp.tool()
def fs_read(
    path: Annotated[str, Field(description="File path (must be within allowed roots)")],
    offset: Annotated[int, Field(description="Byte offset to start reading from. Default: 0")] = 0,
) -> FileReadResponse:
    """Read file content (with offset support)."""
    offset = _to_int(offset, 0)
    result = repo_ops.fs_read(path, offset)
    return FileReadResponse(**result)


@mcp.tool()
def fs_list_dir(
    path: Annotated[str, Field(description="Directory path (must be within allowed roots)")],
) -> DirectoryListResponse:
    """List directory contents."""
    result = repo_ops.fs_list_dir(path)
    # Ensure size and mtime are set for files, None for directories
    files = [FileInfo(
        name=f["name"],
        path=f["path"],
        size=f.get("size"),
        mtime=f.get("mtime")
    ) for f in result["files"]]
    directories = [FileInfo(
        name=d["name"],
        path=d["path"],
        size=None,
        mtime=None
    ) for d in result["directories"]]
    return DirectoryListResponse(
        path=result["path"],
        files=files,
        directories=directories,
        file_count=result["file_count"],
        directory_count=result["directory_count"],
    )


@mcp.tool()
def rg_search(
    root: Annotated[str, Field(description="Root directory to search (must be within allowed roots)")],
    pattern: Annotated[str, Field(description="Regex pattern to search for")],
) -> SearchResponse:
    """Search using ripgrep (rg)."""
    result = repo_ops.rg_search(root, pattern)
    matches = [SearchMatch(**m) for m in result["matches"]]
    return SearchResponse(
        root=result["root"],
        pattern=result["pattern"],
        matches=matches,
        match_count=result["match_count"]
    )


# ============================================================================
# Charts
# ============================================================================

@mcp.tool()
def dataset_to_chart(
    dataset_id: Annotated[Optional[str], Field(description="Dataset identifier (must provide exactly one of dataset_id or path)")] = None,
    path: Annotated[Optional[str], Field(description="Path to dataset file (must be within allowed roots, must provide exactly one of dataset_id or path)")] = None,
    input_format: Annotated[str, Field(description="Input format: \"auto\", \"csv\", \"parquet\", \"avro\", \"jsonl\" (default: \"auto\")")] = "auto",
    goal: Annotated[Optional[str], Field(description="Optional goal description for chart selection")] = None,
    hints: Annotated[Optional[dict], Field(description="Optional hints dict with chart_type, x, y, agg, group_by, date_granularity, top_k, filters")] = None,
    max_rows: Annotated[Optional[int], Field(description="Optional max rows to load (capped by MCPKIT_MAX_ROWS)")] = None,
    output_filename: Annotated[str, Field(description="Output filename (must be safe, no slashes, default: \"chart.png\")")] = "chart.png",
    output_format: Annotated[str, Field(description="Output format: \"png\" or \"svg\" (default: \"png\")")] = "png",
) -> ChartResponse:
    """
    Load dataset, auto-detect schema, pick chart type, render to artifact.
    
    Exactly one of dataset_id or path must be provided.
    Supports csv, parquet, jsonl, and avro container files.
    Auto-detects column types and picks appropriate chart type.
    """
    max_rows = _to_int(max_rows, None)
    result = chart_ops.dataset_to_chart(
        dataset_id=dataset_id,
        path=path,
        input_format=input_format,
        goal=goal,
        hints=hints,
        max_rows=max_rows,
        output_filename=output_filename,
        output_format=output_format,
    )
    return ChartResponse(**result)


# ============================================================================
# Nomad & Consul
# ============================================================================

class NomadJobInfo(BaseModel):
    """Nomad job information."""
    ID: str = Field(description="Job ID", alias="ID")
    Name: str = Field(description="Job name", alias="Name")
    Status: str = Field(description="Job status", alias="Status")
    Type: str = Field(description="Job type", alias="Type")
    Priority: int = Field(description="Job priority", alias="Priority")
    Datacenters: list[str] = Field(description="Datacenters", alias="Datacenters")
    model_config = ConfigDict(populate_by_name=True)


class NomadListResponse(BaseModel):
    """Response for listing Nomad jobs."""
    jobs: list[NomadJobInfo] = Field(description="List of running jobs")
    total_jobs: int = Field(description="Total number of jobs")
    running_count: int = Field(description="Number of running jobs")


class ConsulServiceIP(BaseModel):
    """Consul service IP information."""
    ip: str = Field(description="IP address")
    port: Optional[int] = Field(description="Service port")
    node: Optional[str] = Field(description="Node name")
    service_id: Optional[str] = Field(description="Service ID")
    service_name: Optional[str] = Field(description="Service name (when multiple services match)")
    healthy: Optional[bool] = Field(description="Whether service is passing health checks")


class ConsulServiceResponse(BaseModel):
    """Response for Consul service IPs."""
    service_name: str = Field(description="Service name")
    ips: list[ConsulServiceIP] = Field(description="List of IP addresses")
    count: int = Field(description="Number of IPs found")


class ConsulListServicesResponse(BaseModel):
    """Response for listing Consul services."""
    services: list[str] = Field(description="List of service names")
    count: int = Field(description="Number of services")


class NomadNodeStatusResponse(BaseModel):
    """Response for Nomad node status."""
    node_id: str = Field(description="Node ID")
    node: dict = Field(description="Full node information including status, resources, and health")
    allocations: list[dict] = Field(description="List of allocations on the node")


class ConsulServiceHealthCheck(BaseModel):
    """Consul health check information."""
    check_id: str = Field(description="Check ID")
    name: str = Field(description="Check name")
    status: str = Field(description="Check status (passing, warning, critical)")
    output: Optional[str] = Field(description="Check output")
    service_id: Optional[str] = Field(description="Service ID")
    service_name: Optional[str] = Field(description="Service name")


class ConsulServiceHealthInstance(BaseModel):
    """Consul service health instance."""
    node: Optional[str] = Field(description="Node name")
    node_address: Optional[str] = Field(description="Node address")
    service_id: Optional[str] = Field(description="Service ID")
    service_name: Optional[str] = Field(description="Service name")
    service_address: Optional[str] = Field(description="Service address")
    service_port: Optional[int] = Field(description="Service port")
    health_status: str = Field(description="Overall health status")
    checks: list[dict] = Field(description="List of health checks")
    check_summary: dict = Field(description="Summary of check counts")


class ConsulServiceHealthResponse(BaseModel):
    """Response for Consul service health."""
    service_name: str = Field(description="Service name")
    instances: list[ConsulServiceHealthInstance] = Field(description="List of service instances with health")
    count: int = Field(description="Number of instances")


class KafkaPartitionInfo(BaseModel):
    """Kafka partition information."""
    partition: int = Field(description="Partition number")
    leader: Optional[int] = Field(description="Leader broker ID")
    replicas: list[int] = Field(description="List of replica broker IDs")
    isr: list[int] = Field(description="In-sync replica broker IDs")


class KafkaDescribeTopicResponse(BaseModel):
    """Response for Kafka topic description."""
    topic: str = Field(description="Topic name")
    partitions: list[KafkaPartitionInfo] = Field(description="List of partition information")
    partition_count: int = Field(description="Number of partitions")
    replication_factor: int = Field(description="Replication factor")
    config: dict[str, str] = Field(description="Topic configuration")


class HttpRequestResponse(BaseModel):
    """Response for HTTP request."""
    url: str = Field(description="Final URL (after redirects)")
    status_code: int = Field(description="HTTP status code")
    headers: dict[str, str] = Field(description="Response headers")
    body: dict | list | str = Field(description="Response body (JSON dict/list or text)")
    content_type: str = Field(description="Content type: 'json' or 'text'")
    size: int = Field(description="Response size in bytes")


class DatasetHeadTailResponse(BaseModel):
    """Response for dataset head/tail."""
    dataset_id: str = Field(description="Dataset ID")
    total_rows: int = Field(description="Total number of rows")
    head_rows: Optional[list[dict]] = Field(description="First N rows")
    tail_rows: Optional[list[dict]] = Field(description="Last M rows")
    head_count: int = Field(description="Number of head rows returned")
    tail_count: int = Field(description="Number of tail rows returned")


@mcp.tool()
def nomad_list_jobs(
    address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
    name_pattern: Annotated[Optional[str], Field(description="Optional name pattern (substring match, case-insensitive)")] = None,
) -> NomadListResponse:
    """List all running Nomad services, optionally filtered by name pattern."""
    result = nomad_consul.nomad_list_jobs(address, name_pattern)
    jobs = [NomadJobInfo(**job) for job in result["jobs"]]
    return NomadListResponse(
        jobs=jobs,
        total_jobs=result["total_jobs"],
        running_count=result["running_count"],
    )


@mcp.tool()
def consul_get_service_ips(
    service_name: Annotated[str, Field(description="Service name pattern (substring match, case-insensitive, e.g., stats-api matches coreai-stats-api)")],
    consul_address: Annotated[str, Field(description="Consul API address (e.g., consul.example.com:8500)")],
) -> ConsulServiceResponse:
    """Get IP addresses for Consul services matching name pattern (substring match)."""
    result = nomad_consul.consul_get_service_ips(service_name, consul_address)
    ips = [ConsulServiceIP(**ip) for ip in result["ips"]]
    return ConsulServiceResponse(
        service_name=result["service_name"],
        ips=ips,
        count=result["count"],
    )


@mcp.tool()
def nomad_get_job_status(
    address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
    job_id: Annotated[str, Field(description="Job ID (e.g., coreai-engagement-conversion-api)")],
) -> dict:
    """Get full status of a Nomad job including details, allocations, and summary."""
    return nomad_consul.nomad_get_job_status(address, job_id)


@mcp.tool()
def nomad_get_allocation_logs(
    address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
    allocation_id: Annotated[str, Field(description="Allocation ID")],
    task_name: Annotated[str, Field(description="Task name")],
    log_type: Annotated[str, Field(description="Log type: 'stdout' or 'stderr' (default: 'stdout')")] = "stdout",
    offset: Annotated[int, Field(description="Byte offset to start reading from (default: 0)")] = 0,
    limit: Annotated[int, Field(description="Maximum bytes to read (default: 10000)")] = 10000,
) -> dict:
    """Get logs from a Nomad allocation (read-only)."""
    return nomad_consul.nomad_get_allocation_logs(address, allocation_id, task_name, log_type, offset, limit)


@mcp.tool()
def nomad_get_allocation_events(
    address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
    allocation_id: Annotated[str, Field(description="Allocation ID")],
) -> dict:
    """Get allocation lifecycle events."""
    return nomad_consul.nomad_get_allocation_events(address, allocation_id)


@mcp.tool()
def consul_list_services(
    consul_address: Annotated[str, Field(description="Consul API address (e.g., consul.example.com:8500)")],
) -> ConsulListServicesResponse:
    """List all services in Consul catalog."""
    result = nomad_consul.consul_list_services(consul_address)
    return ConsulListServicesResponse(**result)


@mcp.tool()
def nomad_get_node_status(
    address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
    node_id: Annotated[str, Field(description="Node ID")],
) -> NomadNodeStatusResponse:
    """Get node status and health information from Nomad."""
    result = nomad_consul.nomad_get_node_status(address, node_id)
    return NomadNodeStatusResponse(**result)


@mcp.tool()
def consul_get_service_health(
    service_name: Annotated[str, Field(description="Service name (exact match)")],
    consul_address: Annotated[str, Field(description="Consul API address (e.g., consul.example.com:8500)")],
) -> ConsulServiceHealthResponse:
    """Get detailed health information for a Consul service."""
    result = nomad_consul.consul_get_service_health(service_name, consul_address)
    instances = [ConsulServiceHealthInstance(**inst) for inst in result["instances"]]
    return ConsulServiceHealthResponse(
        service_name=result["service_name"],
        instances=instances,
        count=result["count"],
    )


# ============================================================================
# HTTP Operations
# ============================================================================

@mcp.tool()
def http_request(
    url: Annotated[str, Field(description="Request URL")],
    method: Annotated[str, Field(description="HTTP method: 'GET' or 'POST' (default: 'GET'). POST requires allow_post=True.")] = "GET",
    headers: Annotated[Optional[dict], Field(description="Optional headers dict")] = None,
    params: Annotated[Optional[dict], Field(description="Optional query parameters")] = None,
    data: Annotated[Optional[str], Field(description="Optional request body (string, only for POST)")] = None,
    json_data: Annotated[Optional[dict], Field(description="Optional JSON body (dict, only for POST)")] = None,
    timeout: Annotated[Optional[int], Field(description="Optional timeout in seconds (default: MCPKIT_TIMEOUT_SECS)")] = None,
    allow_post: Annotated[bool, Field(description="Allow POST requests (default: False for safety). POST can mutate external systems.")] = False,
) -> HttpRequestResponse:
    """Make HTTP request (read-only, GET only by default). POST requires explicit allow_post=True."""
    result = http_ops.http_request(url, method, headers, params, data, json_data, timeout, allow_post)
    return HttpRequestResponse(**result)


# ============================================================================
# Dataset Operations
# ============================================================================

@mcp.tool()
def dataset_head_tail(
    dataset_id: Annotated[str, Field(description="Dataset ID")],
    head: Annotated[Optional[Union[int, str]], Field(description="Number of rows from start (default: None)")] = None,
    tail: Annotated[Optional[Union[int, str]], Field(description="Number of rows from end (default: None)")] = None,
) -> DatasetHeadTailResponse:
    """Get first N and/or last M rows from dataset (for debugging)."""
    head = _to_int(head, None)
    tail = _to_int(tail, None)
    result = pandas_ops.pandas_head_tail(dataset_id, head, tail)
    return DatasetHeadTailResponse(**result)


# ============================================================================
# Kafka Operations
# ============================================================================

@mcp.tool()
def kafka_consume_tail(
    topic: Annotated[str, Field(description="Topic name")],
    n_messages: Annotated[Union[int, str], Field(description="Number of messages to consume from tail (default: 10)")] = 10,
    partition: Annotated[Optional[Union[int, str]], Field(description="Optional partition number. If None, consumes from all partitions")] = None,
    bootstrap_servers: Annotated[
        Optional[str],
        Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
    ] = None,
) -> KafkaConsumeResponse:
    """Consume last N messages from a Kafka topic (for debugging)."""
    n_messages = _to_int(n_messages, 10)
    partition = _to_int(partition, None)
    result = kafka_client.kafka_consume_tail(topic, n_messages, partition, bootstrap_servers)
    return KafkaConsumeResponse(**result)


@mcp.tool()
def kafka_describe_topic(
    topic: Annotated[str, Field(description="Topic name")],
    bootstrap_servers: Annotated[
        Optional[str],
        Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
    ] = None,
) -> KafkaDescribeTopicResponse:
    """Get topic configuration, partitions, and replication information."""
    result = kafka_client.kafka_describe_topic(topic, bootstrap_servers)
    partitions = [KafkaPartitionInfo(**p) for p in result["partitions"]]
    return KafkaDescribeTopicResponse(
        topic=result["topic"],
        partitions=partitions,
        partition_count=result["partition_count"],
        replication_factor=result["replication_factor"],
        config=result["config"],
    )


# ============================================================================
# Great Expectations (optional)
# ============================================================================

@mcp.tool()
def great_expectations_check(
    dataset_id: Annotated[str, Field(description="Dataset ID")],
    expectation_suite: Annotated[dict, Field(description="Great Expectations expectation suite dict")],
) -> GEValidationResponse:
    """Run Great Expectations validation suite on dataset."""
    try:
        import great_expectations as ge
    except ImportError:
        raise RuntimeError("Great Expectations not installed. Install with: pip install great-expectations")
    
    from mcpkit.core.registry import load_dataset
    
    df = load_dataset(dataset_id)
    context = ge.get_context()
    
    try:
        # Use Fluent API with pandas datasource - create fresh for each validation
        datasource_name = f"pandas_{dataset_id}"
        
        # Remove existing datasource if it exists
        try:
            existing_ds = context.data_sources.get(datasource_name)
            context.data_sources.delete(datasource_name)
        except (KeyError, ValueError, AttributeError):
            pass
        
        # Create new datasource and asset
        pandas_ds = context.data_sources.add_pandas(name=datasource_name)
        asset = pandas_ds.add_dataframe_asset(name="dataframe_asset")
        
        # Build batch request with dataframe
        batch_request = asset.build_batch_request(options={"dataframe": df})
        
        # Create expectation suite directly
        from great_expectations.core import ExpectationSuite
        suite_name = f"suite_{dataset_id}"
        suite = ExpectationSuite(name=suite_name)
        
        # Add expectations from suite dict
        from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
        expectations = expectation_suite.get("expectations", [])
        for exp in expectations:
            exp_config = ExpectationConfiguration(
                type=exp["expectation_type"],
                kwargs=exp.get("kwargs", {})
            )
            suite.add_expectation_configuration(exp_config)
        
        # Get validator and validate
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )
        
        validation_result = validator.validate()
        
        # Build result object - convert numpy types to native Python types
        def convert_to_native(obj):
            """Convert numpy types to native Python types for serialization."""
            import numpy as np
            if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
                return float(obj)
            elif isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {k: convert_to_native(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert_to_native(item) for item in obj]
            elif hasattr(obj, 'item'):  # numpy scalar
                return obj.item()
            return obj
        
        results = []
        for r in validation_result.results:
            # Convert result dict
            result_dict = convert_to_native(dict(r.result)) if r.result else {}
            results.append(
                GEExpectationResult(
                    expectation_type=str(r.expectation_config.type),
                    success=bool(r.success),
                    result=result_dict,
                )
            )
        
        # Convert statistics
        stats = convert_to_native(dict(validation_result.statistics))
        
        return GEValidationResponse(
            dataset_id=dataset_id,
            success=bool(validation_result.success),
            statistics=stats,
            results=results
        )
    except Exception as e:
        raise RuntimeError(f"Great Expectations validation failed: {e}")


if __name__ == "__main__":
    mcp.run()

