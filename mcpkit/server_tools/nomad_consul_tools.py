"""Nomad and Consul tools for MCP server."""

from typing import Annotated, Optional

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import nomad_consul
from mcpkit.server_models import (
    ConsulListServicesResponse,
    ConsulServiceHealthInstance,
    ConsulServiceHealthResponse,
    ConsulServiceIP,
    ConsulServiceResponse,
    NomadJobInfo,
    NomadListResponse,
    NomadNodeStatusResponse,
)


def register_nomad_consul_tools(mcp: FastMCP):
    """Register Nomad and Consul tools with MCP server."""
    
    @mcp.tool()
    def nomad_list_jobs(
        address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
        name_pattern: Annotated[Optional[str], Field(description="Optional name pattern (substring match, case-insensitive)")] = None,
    ) -> NomadListResponse:
        """List all running Nomad services, optionally filtered by name pattern."""
        result = nomad_consul.nomad_list_jobs(address, name_pattern)
        jobs = [NomadJobInfo(**job) for job in result["jobs"]]
        return NomadListResponse(
            jobs=jobs,
            total_jobs=result["total_jobs"],
            running_count=result["running_count"],
        )

    @mcp.tool()
    def consul_get_service_ips(
        service_name: Annotated[str, Field(description="Service name pattern (substring match, case-insensitive, e.g., stats-api matches coreai-stats-api)")],
        consul_address: Annotated[str, Field(description="Consul API address (e.g., consul.example.com:8500)")],
    ) -> ConsulServiceResponse:
        """Get IP addresses for Consul services matching name pattern (substring match)."""
        result = nomad_consul.consul_get_service_ips(service_name, consul_address)
        ips = [ConsulServiceIP(**ip) for ip in result["ips"]]
        return ConsulServiceResponse(
            service_name=result["service_name"],
            ips=ips,
            count=result["count"],
        )

    @mcp.tool()
    def nomad_get_job_status(
        address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
        job_id: Annotated[str, Field(description="Job ID (e.g., coreai-engagement-conversion-api)")],
    ) -> dict:
        """Get full status of a Nomad job including details, allocations, and summary."""
        return nomad_consul.nomad_get_job_status(address, job_id)

    @mcp.tool()
    def nomad_get_allocation_logs(
        address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
        allocation_id: Annotated[str, Field(description="Allocation ID")],
        task_name: Annotated[str, Field(description="Task name")],
        log_type: Annotated[str, Field(description="Log type: 'stdout' or 'stderr' (default: 'stdout')")] = "stdout",
        offset: Annotated[int, Field(description="Byte offset to start reading from (default: 0)")] = 0,
        limit: Annotated[int, Field(description="Maximum bytes to read (default: 10000)")] = 10000,
    ) -> dict:
        """Get logs from a Nomad allocation (read-only)."""
        return nomad_consul.nomad_get_allocation_logs(address, allocation_id, task_name, log_type, offset, limit)

    @mcp.tool()
    def nomad_get_allocation_events(
        address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
        allocation_id: Annotated[str, Field(description="Allocation ID")],
    ) -> dict:
        """Get allocation lifecycle events."""
        return nomad_consul.nomad_get_allocation_events(address, allocation_id)

    @mcp.tool()
    def consul_list_services(
        consul_address: Annotated[str, Field(description="Consul API address (e.g., consul.example.com:8500)")],
    ) -> ConsulListServicesResponse:
        """List all services in Consul catalog."""
        result = nomad_consul.consul_list_services(consul_address)
        return ConsulListServicesResponse(**result)

    @mcp.tool()
    def nomad_get_node_status(
        address: Annotated[str, Field(description="Nomad API address (e.g., http://nomad.example.com:4646)")],
        node_id: Annotated[str, Field(description="Node ID")],
    ) -> NomadNodeStatusResponse:
        """Get node status and health information from Nomad."""
        result = nomad_consul.nomad_get_node_status(address, node_id)
        return NomadNodeStatusResponse(**result)

    @mcp.tool()
    def consul_get_service_health(
        service_name: Annotated[str, Field(description="Service name (exact match)")],
        consul_address: Annotated[str, Field(description="Consul API address (e.g., consul.example.com:8500)")],
    ) -> ConsulServiceHealthResponse:
        """Get detailed health information for a Consul service."""
        result = nomad_consul.consul_get_service_health(service_name, consul_address)
        instances = [ConsulServiceHealthInstance(**inst) for inst in result["instances"]]
        return ConsulServiceHealthResponse(
            service_name=result["service_name"],
            instances=instances,
            count=result["count"],
        )

