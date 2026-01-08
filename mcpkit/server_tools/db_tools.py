"""Database tools for MCP server."""

from typing import Annotated, Optional, Union

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import db
from mcpkit.server_models import DBIntrospectResponse, DBTable, QueryResultResponse
from mcpkit.server_utils import _to_int


def register_db_tools(mcp: FastMCP):
    """Register database tools with MCP server."""
    
    @mcp.tool()
    def db_query_ro(
        query: Annotated[str, Field(description="SQL query (must start with SELECT, WITH, EXPLAIN SELECT, or EXPLAIN WITH)")],
        params: Annotated[Optional[list[str | int | float | bool | None]], Field(description="Optional list of query parameters")] = None,
    ) -> QueryResultResponse:
        """Execute read-only database query (SELECT/WITH only, no mutations)."""
        result = db.db_query_ro(query, params)
        return QueryResultResponse(**result)

    @mcp.tool()
    def db_introspect(
        schema_like: Annotated[Optional[str], Field(description="Optional schema name pattern (SQL LIKE)")] = None,
        table_like: Annotated[Optional[str], Field(description="Optional table name pattern (SQL LIKE)")] = None,
        max_tables: Annotated[Optional[Union[int, str]], Field(description="Maximum tables to return. Default: 200")] = 200,
    ) -> DBIntrospectResponse:
        """Introspect database schema."""
        max_tables_int: int = _to_int(max_tables, 200)  # type: ignore[assignment]
        result = db.db_introspect(schema_like, table_like, max_tables_int)
        tables = [DBTable(**t) for t in result["tables"]]
        return DBIntrospectResponse(tables=tables, table_count=result["table_count"])

