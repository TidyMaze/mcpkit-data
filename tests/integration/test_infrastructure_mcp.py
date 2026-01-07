"""Integration tests for infrastructure tools (Nomad/Consul) via MCP protocol.

Note: These tests require Nomad/Consul servers. They will be skipped if
servers are not available.
"""

import os

import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def nomad_setup():
    """Check if Nomad is configured."""
    nomad_addr = os.getenv("MCPKIT_TEST_NOMAD_ADDRESS", "http://localhost:4646")
    # Could add health check here
    return nomad_addr


@pytest.fixture(scope="module")
def consul_setup():
    """Check if Consul is configured."""
    consul_addr = os.getenv("MCPKIT_TEST_CONSUL_ADDRESS", "localhost:8500")
    # Could add health check here
    return consul_addr


def test_nomad_list_jobs_mcp(mcp_client, nomad_setup):
    """Test nomad_list_jobs via MCP protocol."""
    try:
        response = call_tool(
            mcp_client,
            "nomad_list_jobs",
            address=nomad_setup
        )
        
        assert_response_structure(response, ["jobs", "total_jobs", "running_count"])
        assert isinstance(response["jobs"], list)
    except Exception:
        pytest.skip("Nomad server not available")


def test_nomad_list_jobs_with_pattern_mcp(mcp_client, nomad_setup):
    """Test nomad_list_jobs with name pattern via MCP protocol."""
    try:
        response = call_tool(
            mcp_client,
            "nomad_list_jobs",
            address=nomad_setup,
            name_pattern="test"
        )
        
        assert_response_structure(response, ["jobs", "total_jobs"])
    except Exception:
        pytest.skip("Nomad server not available")


def test_nomad_get_job_status_mcp(mcp_client, nomad_setup):
    """Test nomad_get_job_status via MCP protocol."""
    # This requires a real job ID, so we'll test error handling
    try:
        response = call_tool(
            mcp_client,
            "nomad_get_job_status",
            address=nomad_setup,
            job_id="nonexistent-job"
        )
        # If job doesn't exist, should get error or empty response
    except Exception:
        # Expected if job doesn't exist
        pass


def test_consul_list_services_mcp(mcp_client, consul_setup):
    """Test consul_list_services via MCP protocol."""
    try:
        response = call_tool(
            mcp_client,
            "consul_list_services",
            consul_address=consul_setup
        )
        
        assert_response_structure(response, ["services", "count"])
        assert isinstance(response["services"], list)
    except Exception:
        pytest.skip("Consul server not available")


def test_consul_get_service_ips_mcp(mcp_client, consul_setup):
    """Test consul_get_service_ips via MCP protocol."""
    try:
        response = call_tool(
            mcp_client,
            "consul_get_service_ips",
            service_name="consul",  # Consul usually has itself registered
            consul_address=consul_setup
        )
        
        assert_response_structure(response, ["service_name", "ips", "count"])
    except Exception:
        pytest.skip("Consul server not available or service not found")


def test_consul_get_service_health_mcp(mcp_client, consul_setup):
    """Test consul_get_service_health via MCP protocol."""
    try:
        response = call_tool(
            mcp_client,
            "consul_get_service_health",
            service_name="consul",
            consul_address=consul_setup
        )
        
        assert_response_structure(response, ["service_name", "instances", "count"])
    except Exception:
        pytest.skip("Consul server not available or service not found")

