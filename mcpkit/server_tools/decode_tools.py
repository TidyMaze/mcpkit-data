"""Decode tools for MCP server."""

from typing import Annotated, Optional

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import decode
from mcpkit.server_models import DecodeResponse


def register_decode_tools(mcp: FastMCP):
    """Register decode tools with MCP server."""
    
    @mcp.tool()
    def protobuf_decode(
        value_base64: Annotated[str, Field(description="Base64-encoded Protobuf message")],
        file_descriptor_set_base64: Annotated[str, Field(description="Base64-encoded FileDescriptorSet")],
        message_full_name: Annotated[str, Field(description="Full message type name (e.g., \"com.example.Message\")")],
    ) -> DecodeResponse:
        """Decode Protobuf-encoded value."""
        result = decode.protobuf_decode(value_base64, file_descriptor_set_base64, message_full_name)
        return DecodeResponse(**result)

