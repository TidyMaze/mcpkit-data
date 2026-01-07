"""Pydantic response models for MCP server tools."""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


# ============================================================================
# Dataset Registry Models
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


# ============================================================================
# Kafka Models
# ============================================================================

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


# ============================================================================
# Schema Registry Models
# ============================================================================

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


# ============================================================================
# Database Models
# ============================================================================

class QueryResultResponse(BaseModel):
    """Response for SQL queries."""
    columns: list[str] = Field(description="Column names")
    rows: list[list] = Field(description="Query result rows")
    row_count: int = Field(description="Number of rows returned")


class DBColumn(BaseModel):
    """Database column information."""
    name: str = Field(description="Column name")
    type: str = Field(description="Column type")
    nullable: bool = Field(description="Whether column is nullable")


class DBTable(BaseModel):
    """Database table information."""
    model_config = ConfigDict(populate_by_name=True)

    schema: str = Field(description="Schema name")
    table: str = Field(description="Table name")
    columns: list[DBColumn] = Field(description="Table columns")


class DBIntrospectResponse(BaseModel):
    """Response for database introspection."""
    tables: list[DBTable] = Field(description="List of tables")
    table_count: int = Field(description="Number of tables")


# ============================================================================
# AWS Models
# ============================================================================

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


# ============================================================================
# JSON Tools Models
# ============================================================================

class TransformResponse(BaseModel):
    """Response for data transformation."""
    result: dict | list | str | int | float | bool | None = Field(description="Transformed result")


class ValidateResponse(BaseModel):
    """Response for validation."""
    valid: bool = Field(description="Whether validation passed")
    errors: Optional[list[str]] = Field(default=None, description="Validation errors (if invalid)")


class FingerprintResponse(BaseModel):
    """Response for fingerprinting."""
    fingerprint: str = Field(description="SHA256 hash fingerprint")


class DedupeResponse(BaseModel):
    """Response for deduplication."""
    unique_count: int = Field(description="Number of unique records")
    original_count: int = Field(description="Original number of records")
    records: list[dict] = Field(description="Deduplicated records (keeps first occurrence)")
    skipped_count: Optional[int] = Field(default=None, description="Number of records skipped due to ID extraction failure (if any)")


class CorrelatedEvent(BaseModel):
    """Correlated events."""
    join_key: dict | list | str | int | float | bool | None = Field(description="Join key value")
    events: list[dict] = Field(description="Events with same join_key")


class CorrelateResponse(BaseModel):
    """Response for event correlation."""
    correlated: list[CorrelatedEvent] = Field(description="Correlated events")
    correlation_count: int = Field(description="Number of correlations")


# ============================================================================
# Pandas Models
# ============================================================================

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


# ============================================================================
# Formats Models
# ============================================================================

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


# ============================================================================
# Evidence Models
# ============================================================================

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


# ============================================================================
# File System Models
# ============================================================================

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


# ============================================================================
# Other Models
# ============================================================================

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


# ============================================================================
# Nomad & Consul Models
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


# ============================================================================
# Kafka Additional Models
# ============================================================================

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


# ============================================================================
# HTTP Models
# ============================================================================

class HttpRequestResponse(BaseModel):
    """Response for HTTP request."""
    url: str = Field(description="Final URL (after redirects)")
    status_code: int = Field(description="HTTP status code")
    headers: dict[str, str] = Field(description="Response headers")
    body: dict | list | str = Field(description="Response body (JSON dict/list or text)")
    content_type: str = Field(description="Content type: 'json' or 'text'")
    size: int = Field(description="Response size in bytes")


# ============================================================================
# Dataset Operations Models
# ============================================================================

class DatasetHeadTailResponse(BaseModel):
    """Response for dataset head/tail."""
    dataset_id: str = Field(description="Dataset ID")
    total_rows: int = Field(description="Total number of rows")
    head_rows: Optional[list[dict]] = Field(description="First N rows")
    tail_rows: Optional[list[dict]] = Field(description="Last M rows")
    head_count: int = Field(description="Number of head rows returned")
    tail_count: int = Field(description="Number of tail rows returned")

