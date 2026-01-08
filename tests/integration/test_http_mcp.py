"""Integration tests for HTTP tools via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


def test_http_request_get_mcp(mcp_client):
    """Test http_request GET via MCP protocol."""
    # Use a public API that returns JSON
    response = call_tool(
        mcp_client,
        "http_request",
        url="https://httpbin.org/json",
        method="GET"
    )
    
    assert_response_structure(response, ["url", "status_code", "headers", "body"])
    assert response["status_code"] == 200
    assert response["content_type"] in ["json", "text"]


def test_http_request_with_params_mcp(mcp_client):
    """Test http_request with query parameters via MCP protocol."""
    response = call_tool(
        mcp_client,
        "http_request",
        url="https://httpbin.org/get",
        method="GET",
        params={"key": "value", "test": "123"}
    )
    
    assert_response_structure(response, ["status_code", "body"])
    assert response["status_code"] == 200


def test_http_request_post_disabled_mcp(mcp_client):
    """Test http_request POST is disabled by default via MCP protocol."""
    # POST should fail without allow_post=True
    with pytest.raises(Exception):
        call_tool(
            mcp_client,
            "http_request",
            url="https://httpbin.org/post",
            method="POST",
            json_data={"test": "data"}
        )


def test_http_request_invalid_method_mcp(mcp_client):
    """Test http_request rejects invalid methods via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "http_request",
            url="https://httpbin.org/get",
            method="PUT"
        )


def test_http_request_with_headers_mcp(mcp_client):
    """Test http_request with custom headers via MCP protocol."""
    response = call_tool(
        mcp_client,
        "http_request",
        url="https://httpbin.org/headers",
        method="GET",
        headers={"X-Test-Header": "test-value"}
    )
    
    assert_response_structure(response, ["status_code", "body"])
    assert response["status_code"] == 200

