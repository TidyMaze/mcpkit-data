"""Dataset operations tools for MCP server."""

from typing import Annotated, Optional, Union

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import pandas_ops
from mcpkit.server_models import DatasetHeadTailResponse
from mcpkit.server_utils import _to_int


def register_dataset_ops_tools(mcp: FastMCP):
    """Register dataset operations tools with MCP server."""
    
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

