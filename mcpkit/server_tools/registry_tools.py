"""Dataset registry tools."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import registry
from mcpkit.server_models import (
    DatasetCreateResponse,
    DatasetDeleteResponse,
    DatasetInfo,
    DatasetInfoResponse,
    DatasetListResponse,
    DatasetPurgeResponse,
)


def register_registry_tools(mcp: FastMCP):
    """Register dataset registry tools with MCP server."""
    
    @mcp.tool()
    def dataset_list() -> DatasetListResponse:
        """List all datasets in registry."""
        result = registry.dataset_list()
        datasets = [DatasetInfo(**d) for d in result["datasets"]]
        return DatasetListResponse(datasets=datasets, dataset_count=len(datasets))

    @mcp.tool()
    def dataset_info(dataset_id: Annotated[str, Field(description="Dataset identifier (must be safe filename)")]) -> DatasetInfoResponse:
        """Get info about a dataset."""
        result = registry.dataset_info(dataset_id)
        return DatasetInfoResponse(**result)

    @mcp.tool()
    def dataset_put_rows(
        columns: Annotated[list[str], Field(description="List of column names")],
        rows: Annotated[list[list], Field(description="List of rows, each row is a list of values matching columns")],
        dataset_id: Annotated[str | None, Field(description="Optional dataset ID (auto-generated if None)")] = None,
    ) -> DatasetCreateResponse:
        """Store rows as parquet dataset in registry."""
        result = registry.dataset_put_rows(columns, rows, dataset_id)
        return DatasetCreateResponse(**result)

    @mcp.tool()
    def dataset_delete(dataset_id: Annotated[str, Field(description="Dataset ID to delete")]) -> DatasetDeleteResponse:
        """Delete a dataset from registry."""
        result = registry.dataset_delete(dataset_id)
        return DatasetDeleteResponse(**result)

    @mcp.tool()
    def dataset_purge(
        older_than_days: Annotated[int | None, Field(description="Delete datasets older than N days", default=None)] = None,
        pattern: Annotated[str | None, Field(description="Delete datasets matching pattern (substring in dataset_id)", default=None)] = None,
        max_delete: Annotated[int | None, Field(description="Maximum number of datasets to delete (safety limit)", default=None)] = None,
        purge_artifacts: Annotated[bool | None, Field(description="Also purge artifacts. If None, auto-purges when no filters (purging all)", default=None)] = None,
    ) -> DatasetPurgeResponse:
        """Purge datasets matching criteria (age or pattern). Also purges artifacts by default when purging all datasets."""
        result = registry.dataset_purge(
            older_than_days=older_than_days,
            pattern=pattern,
            max_delete=max_delete,
            purge_artifacts=purge_artifacts,
        )
        return DatasetPurgeResponse(**result)
