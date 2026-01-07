"""Chart tools for MCP server."""

from typing import Annotated, Optional

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import chart_ops
from mcpkit.server_models import ChartResponse
from mcpkit.server_utils import _to_int


def register_chart_tools(mcp: FastMCP):
    """Register chart tools with MCP server."""
    
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

