"""Utilities for MCP protocol integration tests."""

import json
from typing import Any, Dict, List, Optional


def call_tool(client, tool_name: str, **kwargs) -> Dict[str, Any]:
    """Helper to call an MCP tool via client.
    
    Args:
        client: MCP test client fixture
        tool_name: Name of the tool to call
        **kwargs: Tool arguments
        
    Returns:
        Tool response as dict
    """
    # Filter out None values to match MCP protocol behavior
    arguments = {k: v for k, v in kwargs.items() if v is not None}
    return client.call_tool_sync(tool_name, arguments)


def assert_response_structure(response: Dict[str, Any], expected_fields: List[str]):
    """Assert that response contains expected fields.
    
    Args:
        response: Tool response dict
        expected_fields: List of field names that must be present
    """
    for field in expected_fields:
        assert field in response, f"Response missing field '{field}'. Got: {list(response.keys())}"


def create_test_dataset(client, dataset_id: str, columns: List[str], rows: List[List[Any]]) -> Dict[str, Any]:
    """Helper to create a test dataset.
    
    Args:
        client: MCP test client
        dataset_id: Dataset ID
        columns: Column names
        rows: Row data
        
    Returns:
        Dataset creation response
    """
    return call_tool(client, "dataset_put_rows", columns=columns, rows=rows, dataset_id=dataset_id)


def json_stringify(data: Any) -> str:
    """Convert data to JSON string (for tools that expect JSON strings).
    
    Args:
        data: Data to convert
        
    Returns:
        JSON string
    """
    return json.dumps(data)


def parse_json_string(data: str) -> Any:
    """Parse JSON string to Python object.
    
    Args:
        data: JSON string
        
    Returns:
        Parsed Python object
    """
    return json.loads(data)

