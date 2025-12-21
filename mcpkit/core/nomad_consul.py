"""Nomad and Consul API clients."""

import re
from typing import Optional

import requests

from .guards import GuardError


def nomad_list_jobs(address: str, name_pattern: Optional[str] = None) -> dict:
    """
    List all running Nomad jobs, optionally filtered by name pattern.
    
    Args:
        address: Nomad API address (e.g., http://nomad.example.com:4646)
        name_pattern: Optional name pattern (substring match, case-insensitive)
    
    Returns:
        dict with jobs list
    """
    url = f"{address.rstrip('/')}/v1/jobs"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        jobs = response.json()
        
        # Apply name filter first if provided (before status filter)
        filtered_jobs = jobs
        if name_pattern:
            pattern_lower = name_pattern.lower()
            filtered_jobs = [
                job for job in jobs
                if pattern_lower in job.get("Name", "").lower() or pattern_lower in job.get("ID", "").lower()
            ]
        
        # Filter to only running jobs
        running_jobs = [job for job in filtered_jobs if job.get("Status") == "running"]
        
        # If name pattern provided and no running jobs found, return all matching jobs (any status)
        if name_pattern and not running_jobs:
            return {
                "jobs": filtered_jobs,
                "total_jobs": len(jobs),
                "running_count": 0,
            }
        
        return {
            "jobs": running_jobs,
            "total_jobs": len(jobs),
            "running_count": len(running_jobs),
        }
    except requests.RequestException as e:
        raise GuardError(f"Nomad request failed: {e}")


def nomad_get_job_status(address: str, job_id: str) -> dict:
    """
    Get full status of a Nomad job including details, allocations, and summary.
    
    Args:
        address: Nomad API address (e.g., http://nomad.example.com:4646)
        job_id: Job ID (e.g., "coreai-engagement-conversion-api")
    
    Returns:
        dict with job details, allocations, and summary
    """
    base_url = address.rstrip('/')
    
    try:
        # Get job details
        job_url = f"{base_url}/v1/job/{job_id}"
        job_response = requests.get(job_url, timeout=15)
        job_response.raise_for_status()
        job_data = job_response.json()
        
        # Get job allocations
        alloc_url = f"{base_url}/v1/job/{job_id}/allocations"
        alloc_response = requests.get(alloc_url, timeout=15)
        alloc_response.raise_for_status()
        allocations = alloc_response.json()
        
        # Get job summary
        summary_url = f"{base_url}/v1/job/{job_id}/summary"
        summary_response = requests.get(summary_url, timeout=15)
        summary_response.raise_for_status()
        summary = summary_response.json()
        
        return {
            "job_id": job_id,
            "job": job_data,
            "allocations": allocations,
            "summary": summary,
        }
    except requests.RequestException as e:
        raise GuardError(f"Nomad request failed: {e}")


def nomad_get_allocation_logs(
    address: str,
    allocation_id: str,
    task_name: str,
    log_type: str = "stdout",
    offset: int = 0,
    limit: int = 10000
) -> dict:
    """
    Get logs from a Nomad allocation.
    
    Args:
        address: Nomad API address (e.g., http://nomad.example.com:4646)
        allocation_id: Allocation ID
        task_name: Task name
        log_type: Log type: "stdout" or "stderr" (default: "stdout")
        offset: Byte offset to start reading from (default: 0)
        limit: Maximum bytes to read (default: 10000)
    
    Returns:
        dict with log content and metadata
    """
    base_url = address.rstrip('/')
    
    if log_type not in ["stdout", "stderr"]:
        raise GuardError(f"log_type must be 'stdout' or 'stderr', got: {log_type}")
    
    try:
        # Use server's proxy endpoint to get logs from client node
        # The server will route the request to the appropriate client
        logs_url = f"{base_url}/v1/client/fs/logs/{allocation_id}"
        
        params = {
            "task": task_name,
            "type": log_type,
            "origin": "start",
            "offset": offset,
            "limit": limit,
            "plain": "true"  # Get plain text, not JSON
        }
        
        logs_response = requests.get(logs_url, params=params, timeout=30)
        logs_response.raise_for_status()
        
        # Logs are returned as plain text
        log_content = logs_response.text
        
        return {
            "allocation_id": allocation_id,
            "task_name": task_name,
            "log_type": log_type,
            "content": log_content,
            "size": len(log_content.encode('utf-8')),
            "offset": offset,
        }
    except requests.RequestException as e:
        raise GuardError(f"Nomad logs request failed: {e}")


def nomad_get_node_status(address: str, node_id: str) -> dict:
    """
    Get full status of a Nomad client node.
    
    Args:
        address: Nomad API address (e.g., http://nomad.example.com:4646)
        node_id: Node ID
    
    Returns:
        dict with node details, allocations, and health
    """
    base_url = address.rstrip('/')
    node_url = f"{base_url}/v1/node/{node_id}"
    node_allocations_url = f"{base_url}/v1/node/{node_id}/allocations"
    
    try:
        node_response = requests.get(node_url, timeout=15)
        node_response.raise_for_status()
        node_data = node_response.json()
        
        allocations_response = requests.get(node_allocations_url, timeout=15)
        allocations_response.raise_for_status()
        allocations_data = allocations_response.json()
        
        return {
            "node_id": node_id,
            "node": node_data,
            "allocations": allocations_data,
        }
    except requests.RequestException as e:
        raise GuardError(f"Nomad node status request failed: {e}")


def nomad_get_allocation_events(address: str, allocation_id: str) -> dict:
    """
    Get allocation lifecycle events.
    
    Args:
        address: Nomad API address (e.g., http://nomad.example.com:4646)
        allocation_id: Allocation ID
    
    Returns:
        dict with allocation events from all tasks
    """
    base_url = address.rstrip('/')
    
    try:
        alloc_url = f"{base_url}/v1/allocation/{allocation_id}"
        alloc_response = requests.get(alloc_url, timeout=15)
        alloc_response.raise_for_status()
        allocation = alloc_response.json()
        
        # Extract events from task states
        events_by_task = {}
        task_states = allocation.get("TaskStates", {})
        
        for task_name, task_state in task_states.items():
            events = task_state.get("Events", [])
            events_by_task[task_name] = events
        
        return {
            "allocation_id": allocation_id,
            "events_by_task": events_by_task,
            "total_events": sum(len(events) for events in events_by_task.values()),
        }
    except requests.RequestException as e:
        raise GuardError(f"Nomad request failed: {e}")


def consul_get_service_health(service_name: str, consul_address: str) -> dict:
    """
    Get detailed health information for a Consul service.
    
    Args:
        service_name: Service name (exact match)
        consul_address: Consul API address (e.g., consul.example.com:8500)
    
    Returns:
        dict with service health details including checks and status
    """
    # Ensure protocol is present
    if not consul_address.startswith("http://") and not consul_address.startswith("https://"):
        consul_address = f"http://{consul_address}"
    
    base_url = consul_address.rstrip('/')
    
    try:
        # Get health information for the service
        health_url = f"{base_url}/v1/health/service/{service_name}"
        health_response = requests.get(health_url, timeout=15)
        health_response.raise_for_status()
        services = health_response.json()
        
        # Process health information
        health_details = []
        for service in services:
            node = service.get("Node", {})
            service_info = service.get("Service", {})
            checks = service.get("Checks", [])
            
            # Determine overall health status
            passing_checks = [c for c in checks if c.get("Status") == "passing"]
            warning_checks = [c for c in checks if c.get("Status") == "warning"]
            critical_checks = [c for c in checks if c.get("Status") == "critical"]
            
            health_status = "passing"
            if critical_checks:
                health_status = "critical"
            elif warning_checks:
                health_status = "warning"
            
            health_details.append({
                "node": node.get("Node"),
                "node_address": node.get("Address"),
                "service_id": service_info.get("ID"),
                "service_name": service_info.get("Service"),
                "service_address": service_info.get("Address"),
                "service_port": service_info.get("Port"),
                "health_status": health_status,
                "checks": checks,
                "check_summary": {
                    "total": len(checks),
                    "passing": len(passing_checks),
                    "warning": len(warning_checks),
                    "critical": len(critical_checks),
                },
            })
        
        return {
            "service_name": service_name,
            "instances": health_details,
            "count": len(health_details),
        }
    except requests.RequestException as e:
        raise GuardError(f"Consul request failed: {e}")


def consul_list_services(consul_address: str) -> dict:
    """
    List all services in Consul catalog.
    
    Args:
        consul_address: Consul API address (e.g., consul.example.com:8500)
    
    Returns:
        dict with list of service names
    """
    # Ensure protocol is present
    if not consul_address.startswith("http://") and not consul_address.startswith("https://"):
        consul_address = f"http://{consul_address}"
    
    base_url = consul_address.rstrip('/')
    
    try:
        catalog_url = f"{base_url}/v1/catalog/services"
        catalog_response = requests.get(catalog_url, timeout=15)
        catalog_response.raise_for_status()
        all_services = catalog_response.json()
        
        # Extract service names (keys of the dict)
        service_names = sorted(all_services.keys())
        
        return {
            "services": service_names,
            "count": len(service_names),
        }
    except requests.RequestException as e:
        raise GuardError(f"Consul request failed: {e}")


def consul_get_service_ips(service_name: str, consul_address: str) -> dict:
    """
    Get IP addresses for Consul services matching name pattern (substring match).
    
    Args:
        service_name: Service name pattern (e.g., "stats-api" matches "coreai-stats-api")
        consul_address: Consul API address (e.g., consul.example.com:8500)
    
    Returns:
        dict with service info and IPs for all matching services
    """
    # Ensure protocol is present
    if not consul_address.startswith("http://") and not consul_address.startswith("https://"):
        consul_address = f"http://{consul_address}"
    
    base_url = consul_address.rstrip('/')
    pattern_lower = service_name.lower()
    
    try:
        # First, get all services from catalog
        catalog_url = f"{base_url}/v1/catalog/services"
        catalog_response = requests.get(catalog_url, timeout=15)
        catalog_response.raise_for_status()
        all_services = catalog_response.json()
        
        # Find matching service names (substring match, case-insensitive)
        matching_services = [
            svc_name for svc_name in all_services.keys()
            if pattern_lower in svc_name.lower()
        ]
        
        if not matching_services:
            return {
                "service_name": service_name,
                "ips": [],
                "count": 0,
            }
        
        # Get IPs for all matching services
        all_ips = []
        for svc_name in matching_services:
            # Try passing=true first (healthy services)
            health_url = f"{base_url}/v1/health/service/{svc_name}?passing=true"
            health_response = requests.get(health_url, timeout=15)
            health_response.raise_for_status()
            services = health_response.json()
            
            # If no passing services found, try without filter (includes non-passing)
            if not services:
                health_url = f"{base_url}/v1/health/service/{svc_name}"
                health_response = requests.get(health_url, timeout=15)
                health_response.raise_for_status()
                services = health_response.json()
            
            for service in services:
                node = service.get("Node", {})
                service_info = service.get("Service", {})
                address = node.get("Address") or service_info.get("Address")
                if address:
                    # Check health status
                    checks = service.get("Checks", [])
                    passing = any(check.get("Status") == "passing" for check in checks)
                    
                    all_ips.append({
                        "ip": address,
                        "port": service_info.get("Port"),
                        "node": node.get("Node"),
                        "service_id": service_info.get("ID"),
                        "service_name": svc_name,
                        "healthy": passing,
                    })
        
        return {
            "service_name": service_name,
            "ips": all_ips,
            "count": len(all_ips),
        }
    except requests.RequestException as e:
        raise GuardError(f"Consul request failed: {e}")

