"""HTTP tools for MCP server."""

from typing import Annotated, Optional

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import http_ops
from mcpkit.server_models import HttpRequestResponse


def register_http_tools(mcp: FastMCP):
    """Register HTTP tools with MCP server."""
    
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

