"""Schema Registry tools for MCP server."""

from typing import Annotated, Optional

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import decode, schema_registry
from mcpkit.server_models import (
    DecodeResponse,
    SchemaRegistryListResponse,
    SchemaRegistryResponse,
)


def register_schema_registry_tools(mcp: FastMCP):
    """Register Schema Registry tools with MCP server."""
    
    @mcp.tool()
    def schema_registry_get(
        schema_id: Annotated[Optional[int], Field(description="Optional schema ID. Must provide either schema_id or subject")] = None,
        subject: Annotated[Optional[str], Field(description="Optional subject name (returns latest version)")] = None,
        url: Annotated[Optional[str], Field(description="Optional Schema Registry URL. If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var")] = None,
    ) -> SchemaRegistryResponse:
        """Get schema from Schema Registry by ID or subject (latest version)."""
        result = schema_registry.schema_registry_get(schema_id, subject, url)
        return SchemaRegistryResponse(**result)

    @mcp.tool()
    def schema_registry_list_subjects(
        url: Annotated[Optional[str], Field(description="Optional Schema Registry URL. If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var")] = None,
    ) -> SchemaRegistryListResponse:
        """List all subjects in Schema Registry."""
        result = schema_registry.schema_registry_list_subjects(url)
        return SchemaRegistryListResponse(**result)

    @mcp.tool()
    def avro_decode(
        value_base64: Annotated[str, Field(description="Base64-encoded Avro value")],
        schema_json: Annotated[Optional[dict], Field(description="Optional schema dict (if None, expects Confluent format with schema ID)")] = None,
        schema_registry_url: Annotated[Optional[str], Field(description="Optional Schema Registry URL (for fetching schema by ID). If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var")] = None,
    ) -> DecodeResponse:
        """Decode Avro-encoded value (base64)."""
        result = decode.avro_decode(value_base64, schema_json, schema_registry_url)
        return DecodeResponse(**result)

