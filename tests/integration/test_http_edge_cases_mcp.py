"""Edge case tests for HTTP operations via MCP protocol."""

import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


def test_http_request_with_timeout_mcp(mcp_client):
    """Test http_request with custom timeout via MCP protocol."""
    response = call_tool(
        mcp_client,
        "http_request",
        url="https://httpbin.org/delay/1",
        timeout=5
    )
    
    assert_response_structure(response, ["status_code", "url"])


def test_http_request_redirects_mcp(mcp_client):
    """Test http_request handles redirects via MCP protocol."""
    response = call_tool(
        mcp_client,
        "http_request",
        url="https://httpbin.org/redirect/1"
    )
    
    assert_response_structure(response, ["status_code", "url"])
    # Should follow redirect and return 200
    assert response["status_code"] == 200


def test_http_request_json_response_mcp(mcp_client):
    """Test http_request with JSON response via MCP protocol."""
    response = call_tool(
        mcp_client,
        "http_request",
        url="https://httpbin.org/json"
    )
    
    assert_response_structure(response, ["status_code", "body", "content_type"])
    assert response["content_type"] == "json"
    assert isinstance(response["body"], dict)


def test_http_request_text_response_mcp(mcp_client):
    """Test http_request with text response via MCP protocol."""
    response = call_tool(
        mcp_client,
        "http_request",
        url="https://httpbin.org/robots.txt"
    )
    
    assert_response_structure(response, ["status_code", "body", "content_type"])
    assert response["content_type"] == "text"
    assert isinstance(response["body"], str)


def test_http_request_404_error_mcp(mcp_client):
    """Test http_request with 404 error via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "http_request",
            url="https://httpbin.org/status/404"
        )


def test_http_request_500_error_mcp(mcp_client):
    """Test http_request with 500 error via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "http_request",
            url="https://httpbin.org/status/500"
        )

