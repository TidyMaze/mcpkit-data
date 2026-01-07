"""JSON tools for MCP server."""

from typing import Annotated, Optional, Union

from fastmcp import FastMCP
from pydantic import BeforeValidator, Field

from mcpkit.core import json_tools
from mcpkit.server_models import (
    CorrelateResponse,
    CorrelatedEvent,
    DedupeResponse,
    FingerprintResponse,
    TransformResponse,
    ValidateResponse,
)
from mcpkit.server_utils import _list_validator, _parse_list_param


def register_json_tools(mcp: FastMCP):
    """Register JSON tools with MCP server."""
    
    @mcp.tool()
    def jq_transform(
        data: Annotated[Union[dict, list, str, int, float, bool, None], Field(description="Input data (dict, list, JSON string, or primitive)")],
        expression: Annotated[str, Field(description="JMESPath expression (e.g., \"products[0].id\", \"items[*].name\", \"price.value\")")],
    ) -> TransformResponse:
        """Transform data using JMESPath expression."""
        result = json_tools.jq_transform(data, expression)
        return TransformResponse(**result)

    @mcp.tool()
    def event_validate(
        record: Annotated[dict, Field(description="Record dict to validate")],
        schema: Annotated[dict, Field(description="JSONSchema dict")],
    ) -> ValidateResponse:
        """Validate record against JSONSchema."""
        result = json_tools.event_validate(record, schema)
        return ValidateResponse(**result)

    @mcp.tool()
    def event_fingerprint(
        record: Annotated[dict, Field(description="Record dict or object")],
        fields: Annotated[Optional[Union[list[str], str]], BeforeValidator(_list_validator), Field(description="Optional list of field names to include (if None, uses all fields)")] = None,
    ) -> FingerprintResponse:
        """Generate fingerprint for record."""
        result = json_tools.event_fingerprint(record, fields)
        return FingerprintResponse(**result)

    @mcp.tool()
    def dedupe_by_id(
        records: Annotated[str, Field(description="JSON string of record dicts")],
        id_jmes: Annotated[str, Field(description="JMESPath expression to extract ID")],
    ) -> DedupeResponse:
        """Deduplicate records by extracting ID using JMESPath."""
        records_list = _parse_list_param(records, [])
        if not isinstance(records_list, list):
            raise ValueError(f"records must be a list, got {type(records_list)}")
        result = json_tools.dedupe_by_id(records_list, id_jmes)
        return DedupeResponse(**result)

    @mcp.tool()
    def event_correlate(
        batches: Annotated[str, Field(description="JSON string of batches, each batch is a list of record dicts")],
        join_key_jmes: Annotated[str, Field(description="JMESPath expression to extract join key")],
        timestamp_field: Annotated[str, Field(description="Field name containing timestamp (default: \"timestamp\")")] = "timestamp",
    ) -> CorrelateResponse:
        """Correlate events across batches using join key and timestamp."""
        batches_list = _parse_list_param(batches, [])
        if not isinstance(batches_list, list):
            raise ValueError(f"batches must be a list, got {type(batches_list)}")
        result = json_tools.event_correlate(batches_list, join_key_jmes, timestamp_field)
        correlated = [CorrelatedEvent(**c) for c in result["correlated"]]
        return CorrelateResponse(correlated=correlated, correlation_count=result["correlation_count"])

