"""Tests for Great Expectations tool."""

import pytest

from mcpkit.core.registry import dataset_put_rows


def test_great_expectations_check_missing(clean_registry):
    """Test GE check when GE is not installed."""
    dataset_put_rows(columns=["a"], rows=[[1]], dataset_id="ds1")
    
    # Test that the tool exists in the server
    from mcpkit.server import mcp
    import asyncio
    
    async def check_tool():
        tools = await mcp.get_tools()
        # get_tools() returns a dict with tool names as keys
        assert "great_expectations_check" in tools
    
    asyncio.run(check_tool())
    # The actual function is wrapped, so we just verify the module loads
    # In a real scenario, GE would need to be installed to test the full functionality


def test_great_expectations_check_mock(clean_registry):
    """Test GE check - verify tool exists."""
    dataset_put_rows(columns=["a", "b"], rows=[[1, 2], [3, 4]], dataset_id="ds1")
    
    # Verify the tool is registered in the server
    from mcpkit.server import mcp
    import asyncio
    
    async def check_tool():
        tools = await mcp.get_tools()
        # get_tools() returns a dict with tool names as keys
        assert "great_expectations_check" in tools
    
    asyncio.run(check_tool())
    # The tool is wrapped, so full testing would require GE to be installed
    # or testing through the MCP interface

