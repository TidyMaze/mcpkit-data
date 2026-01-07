"""Pandas tools for MCP server."""

from typing import Annotated, Optional, Union

from fastmcp import FastMCP
from pydantic import BeforeValidator, Field

from mcpkit.core import pandas_ops
from mcpkit.core.guards import GuardError
from mcpkit.server_models import (
    DatasetCreateResponse,
    DatasetDiffResponse,
    DatasetOperationResponse,
    DatasetStatsResponse,
    DistinctCountsResponse,
    ExportResponse,
    SchemaCheckResponse,
)
from mcpkit.server_utils import _list_validator, _parse_list_param, _to_int


def register_pandas_tools(mcp: FastMCP):
    """Register pandas tools with MCP server."""
    
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
        filters: Annotated[str, Field(description="JSON string of filter dicts, each with: {\"column\": str, \"op\": str (==, !=, <, <=, >, >=, in, contains, startswith, endswith), \"value\": any}")],
        out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
    ) -> DatasetOperationResponse:
        """Filter dataset using query conditions."""
        filters_list = _parse_list_param(filters, [])
        if not isinstance(filters_list, list):
            raise GuardError(f"filters must be a list, got {type(filters_list)}")
        result = pandas_ops.pandas_filter_query(dataset_id, filters_list, out_dataset_id)
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
    def pandas_parse_json_column(
        dataset_id: Annotated[str, Field(description="Input dataset ID")],
        column: Annotated[str, Field(description="Column name containing JSON strings to parse")],
        expand_arrays: Annotated[bool, Field(description="If True, expand JSON arrays into multiple rows (cross join). If False, parse as single object per row. Default: False")] = False,
        target_columns: Annotated[Optional[Union[list[str], str]], BeforeValidator(_list_validator), Field(description="Optional list of JSON keys to extract as columns. Use dot notation for nested fields: [\"id\", \"title\", \"price.value\", \"price.currency\"]. If None, extracts all top-level keys. Example: [\"id\", \"title\"]")] = None,
        out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
    ) -> DatasetOperationResponse:
        """
        Parse JSON strings from a column into structured columns.

        This tool parses JSON strings stored as text in a column and extracts
        their fields as new columns. Useful for working with nested JSON data
        from Kafka events, API responses, or database JSONB columns.

        **Input Format:**
        - Column contains JSON strings: `'{"id": "123", "title": "Product"}'`
        - Or JSON arrays: `'[{"id": "1"}, {"id": "2"}]'`
        - Or nested JSON strings in arrays: `'["{\\"id\\":\\"1\\"}"]'` (common in Kafka)

        **Behavior:**
        - If `expand_arrays=True` and column contains JSON arrays, each array
          element becomes a separate row (cross join behavior)
        - If `expand_arrays=False`, arrays are parsed but kept as single value
        - Null/empty strings are skipped
        - Invalid JSON strings are skipped with warning (doesn't fail entire operation)
        - Original columns are preserved (except the parsed JSON column)
        - New columns use dot notation for nested fields: `price.currency`, `price.value`

        **Examples:**

        1. Parse product records from Kafka events (with nested JSON strings in array):
        ```python
        # Input: column "productRecords" = '["{\\"id\\":\\"123\\",\\"title\\":\\"Product\\"}"]'
        # With expand_arrays=True: creates one row per product
        pandas_parse_json_column(
            dataset_id="kafka_events",
            column="productRecords",
            expand_arrays=True
        )
        ```

        2. Extract specific fields from JSON:
        ```python
        pandas_parse_json_column(
            dataset_id="events",
            column="metadata",
            target_columns=["userId", "timestamp", "action", "price.value", "price.currency"]
        )
        ```

        3. Parse single JSON object per row:
        ```python
        pandas_parse_json_column(
            dataset_id="api_responses",
            column="response",
            expand_arrays=False
        )
        ```

        **Returns:**
        - New dataset with parsed JSON fields as columns
        - Original columns are preserved (except parsed column)
        - New columns use dot notation for nested fields

        **Error Handling:**
        - Raises `GuardError` if column doesn't exist
        - Skips invalid JSON with warning (doesn't fail entire operation)
        - Returns error if all JSON is invalid (no valid rows found)
        """
        result = pandas_ops.pandas_parse_json_column(
            dataset_id, column, expand_arrays, target_columns, out_dataset_id
        )
        return DatasetOperationResponse(**result)

    @mcp.tool()
    def pandas_export(
        dataset_id: Annotated[str, Field(description="Dataset ID")],
        format: Annotated[str, Field(description="Export format: \"csv\", \"json\", \"parquet\", \"excel\"")],
        filename: Annotated[str, Field(description="Output filename (must be safe, no slashes)")],
    ) -> ExportResponse:
        """Export dataset to file in artifact directory."""
        result = pandas_ops.pandas_export(dataset_id, format, filename)
        return ExportResponse(**result)

