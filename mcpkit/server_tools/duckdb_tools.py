"""DuckDB tools for MCP server."""

from typing import Annotated, Optional, Union

from fastmcp import FastMCP
from pydantic import BeforeValidator, Field

from mcpkit.core import duckdb_ops
from mcpkit.core.guards import GuardError
from mcpkit.server_models import QueryResultResponse
from mcpkit.server_utils import _sources_validator, _to_int


def register_duckdb_tools(mcp: FastMCP):
    """Register DuckDB tools with MCP server."""
    
    @mcp.tool()
    def duckdb_query_local(
        sql: Annotated[str, Field(description="SQL query string (SELECT/WITH only, read-only queries)")],
        sources: Annotated[Optional[Union[str, list]], BeforeValidator(_sources_validator), Field(description="Source definitions as JSON string. Format: '[{\"name\": \"view_name\", \"dataset_id\": \"dataset_id\"}]' for registry datasets OR '[{\"name\": \"view_name\", \"path\": \"/path/to/file\", \"format\": \"parquet\"|\"csv\"|\"json\"|\"jsonl\"}]' for files. Example: '[{\"name\": \"events\", \"dataset_id\": \"kafka_events\"}]'. WORKAROUND: If you have a dict/list, convert to JSON first: json.dumps([{\"name\": \"view\", \"dataset_id\": \"id\"}])")] = None,
        # Alternative: single source parameters (workaround for MCP serialization)
        source_name: Annotated[Optional[str], Field(description="Alternative: single source view name (use with source_dataset_id or source_path+source_format)")] = None,
        source_dataset_id: Annotated[Optional[str], Field(description="Alternative: single source dataset_id (use with source_name)")] = None,
        source_path: Annotated[Optional[str], Field(description="Alternative: single source file path (use with source_name and source_format)")] = None,
        source_format: Annotated[Optional[str], Field(description="Alternative: single source file format: parquet, csv, json, jsonl (use with source_name and source_path)")] = None,
        max_rows: Annotated[Optional[int], Field(description="Optional maximum number of rows to return. Default: MCPKIT_MAX_ROWS environment variable (default: 200)")] = None,
    ) -> QueryResultResponse:
        """
        Execute SQL query on local sources using DuckDB.

        This tool allows you to run read-only SQL queries (SELECT, WITH) on datasets registered as views.
        Sources can be datasets from the registry or files from disk.

        **Parameters:**
        - `sql`: SQL query string (SELECT/WITH statements only, read-only)
        - `sources`: Optional JSON string array of source definitions to register as views
        - `max_rows`: Optional maximum rows to return (default: MCPKIT_MAX_ROWS env var, or 200)

        **Source Format:**
        Sources must be a JSON string array. Each source object can be one of:

        1. **Registry dataset**: `{"name": "view_name", "dataset_id": "dataset_id"}`
           - Registers a dataset from the registry as a view
           - Example: `{"name": "events", "dataset_id": "kafka_events"}`

        2. **File source**: `{"name": "view_name", "path": "/path/to/file", "format": "parquet"}`
           - Registers a file as a view
           - Supported formats: `parquet`, `csv`, `json`, `jsonl`
           - Example: `{"name": "data", "path": "/tmp/data.parquet", "format": "parquet"}`

        **Supported File Formats:**
        - `parquet`: Parquet files
        - `csv`: CSV files
        - `json`: JSON files
        - `jsonl`: JSON Lines files

        **Return Value:**
        Returns a `QueryResultResponse` with:
        - `columns`: List of column names (list[str])
        - `rows`: List of rows, each row is a list of values (list[list])
        - `row_count`: Number of rows returned (int)

        **Examples:**

        1. Simple query without sources:
        ```sql
        SELECT 1 as a, 'hello' as b
        ```

        2. Query a registered dataset:
        ```sql
        SELECT * FROM events WHERE projectId = 23 LIMIT 10
        ```
        With sources: `'[{"name": "events", "dataset_id": "kafka_events"}]'`

        3. Query multiple datasets with JOIN:
        ```sql
        SELECT a.id, b.name FROM dataset_a a JOIN dataset_b b ON a.id = b.id
        ```
        With sources: `'[{"name": "dataset_a", "dataset_id": "a"}, {"name": "dataset_b", "dataset_id": "b"}]'`

        4. Query a file:
        ```sql
        SELECT * FROM data WHERE value > 100
        ```
        With sources: `'[{"name": "data", "path": "/path/to/data.parquet", "format": "parquet"}]'`

        5. Query with aggregation:
        ```sql
        SELECT projectId, COUNT(*) as count FROM events GROUP BY projectId
        ```
        With sources: `'[{"name": "events", "dataset_id": "kafka_events"}]'`

        **Error Handling:**
        - If a table/view doesn't exist and no sources provided, returns helpful error with available views
        - If dataset_id not found, lists attempted paths
        - If JSON parsing fails, provides clear error message with expected format

        **Workarounds for MCP Serialization:**

        FastMCP cannot serialize dict/list parameters directly. Use one of these workarounds:

        1. **Individual parameters** (recommended for single source):
           - Use `source_name` + `source_dataset_id` for registry datasets
           - Use `source_name` + `source_path` + `source_format` for files
           - Example: `source_name="events", source_dataset_id="kafka_events"`

        2. **JSON string** (for multiple sources):
           - Pass `sources` as JSON string: `'[{"name": "view", "dataset_id": "id"}]'`
           - Convert dict to JSON: `json.dumps([{"name": "view", "dataset_id": "id"}])`

        3. **Direct Python calls** (not via MCP):
           - Can pass dict/list directly when calling the function in Python
        """
        import logging
        logger = logging.getLogger(__name__)
        
        max_rows = _to_int(max_rows, None)

        # WORKAROUND: If sources is None but individual source parameters are provided,
        # construct sources from individual parameters
        if sources is None and source_name:
            logger.debug(f"duckdb_query_local: constructing sources from individual parameters")
            if source_dataset_id:
                sources = f'[{{"name": "{source_name}", "dataset_id": "{source_dataset_id}"}}]'
                logger.debug(f"duckdb_query_local: constructed sources from dataset_id: {sources}")
            elif source_path and source_format:
                sources = f'[{{"name": "{source_name}", "path": "{source_path}", "format": "{source_format}"}}]'
                logger.debug(f"duckdb_query_local: constructed sources from file: {sources}")
            else:
                raise GuardError(
                    "When using individual source parameters, provide either: "
                    "(source_name + source_dataset_id) OR (source_name + source_path + source_format)"
                )

        # Log what we receive after validation
        logger.debug(
            f"duckdb_query_local called with: "
            f"sql length={len(sql)}, "
            f"sources type={type(sources).__name__ if sources is not None else 'None'}, "
            f"sources value={repr(sources)[:200] if sources is not None else 'None'}, "
            f"max_rows={max_rows}"
        )

        # If sources is a string (JSON), validate and convert to list
        # The validator should have done this, but if we constructed it from individual params,
        # we need to validate it ourselves
        # Note: After BeforeValidator, sources should already be a list, but FastMCP might pass it as string
        if sources is not None:
            if isinstance(sources, str):
                sources_list = _sources_validator(sources)
            elif isinstance(sources, list):
                sources_list = sources
            else:
                raise GuardError(f"sources must be a JSON string or list, got {type(sources)}")
        else:
            sources_list = None
        if sources_list is not None:
            logger.debug(f"duckdb_query_local: sources_list after validator: {sources_list}")
            if not isinstance(sources_list, list):
                raise GuardError(
                    f"sources must be a list, got {type(sources_list)}. "
                    f"This should have been handled by the validator."
                )

            # Validate each source is a dict with required fields
            for i, source in enumerate(sources_list):
                if not isinstance(source, dict):
                    raise GuardError(
                        f"Source {i} must be a dict, got {type(source)}: {source}"
                    )
                if "name" not in source:
                    raise GuardError(
                        f"Source {i} missing required field 'name'. Got: {source}"
                    )
                if "dataset_id" not in source and ("path" not in source or "format" not in source):
                    raise GuardError(
                        f"Source {i} must have either 'dataset_id' or both 'path' and 'format'. Got: {source}"
                    )

        result = duckdb_ops.duckdb_query_local(sql, sources_list, max_rows)
        return QueryResultResponse(**result)

