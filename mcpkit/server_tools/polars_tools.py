"""Polars tools for MCP server."""

from typing import Annotated, Optional

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import polars_ops
from mcpkit.server_models import DatasetCreateResponse, DatasetOperationResponse, ExportResponse


def register_polars_tools(mcp: FastMCP):
    """Register Polars tools with MCP server."""
    
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

