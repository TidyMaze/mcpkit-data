"""MCP server with all data engineering tools."""

import logging
from typing import Annotated, Any, Optional, Union

from fastmcp import FastMCP
from pydantic import BeforeValidator, Field

logger = logging.getLogger(__name__)

from mcpkit.core import (
    aws_ops,
    chart_ops,
    db,
    decode,
    duckdb_ops,
    evidence,
    formats,
    http_ops,
    json_tools,
    kafka_client,
    nomad_consul,
    pandas_ops,
    polars_ops,
    registry,
    repo_ops,
    schema_registry,
)
from mcpkit.core.guards import GuardError
from mcpkit.server_models import *  # noqa: F403, F401
from mcpkit.server_utils import (  # noqa: F401
    _int_validator,
    _list_validator,
    _parse_dict_param,
    _parse_list_param,
    _sources_validator,
    _to_int,
)

mcp = FastMCP("mcpkit-all-ops")

# Register all tools from modular tool files
from mcpkit.server_tools import (
    db_tools,
    decode_tools,
    json_tools as json_tools_module,
    kafka_tools,
    registry_tools,
    schema_registry_tools,
)

# Register tools
registry_tools.register_registry_tools(mcp)
kafka_tools.register_kafka_tools(mcp)
schema_registry_tools.register_schema_registry_tools(mcp)
decode_tools.register_decode_tools(mcp)
db_tools.register_db_tools(mcp)
json_tools_module.register_json_tools(mcp)

from mcpkit.server_tools import (
    aws_tools,
    chart_tools,
    dataset_ops_tools,
    duckdb_tools,
    formats_tools,
    ge_tools,
    http_tools,
    nomad_consul_tools,
    pandas_tools,
    polars_tools,
    repo_tools,
)

aws_tools.register_aws_tools(mcp)
pandas_tools.register_pandas_tools(mcp)
polars_tools.register_polars_tools(mcp)
duckdb_tools.register_duckdb_tools(mcp)
formats_tools.register_formats_tools(mcp)
repo_tools.register_repo_tools(mcp)
chart_tools.register_chart_tools(mcp)
nomad_consul_tools.register_nomad_consul_tools(mcp)
http_tools.register_http_tools(mcp)
dataset_ops_tools.register_dataset_ops_tools(mcp)
ge_tools.register_ge_tools(mcp)

if __name__ == "__main__":
    mcp.run()

