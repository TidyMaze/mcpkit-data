"""Schema Registry client."""

import base64
import os
from typing import Optional

import requests

from .guards import GuardError


def get_schema_registry_url(url: Optional[str] = None) -> str:
    """
    Get Schema Registry URL from parameter or environment.
    
    Args:
        url: Optional Schema Registry URL.
            If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var.
    """
    registry_url = url or os.getenv("MCPKIT_SCHEMA_REGISTRY_URL")
    if not registry_url:
        raise GuardError("Schema Registry URL not provided. Set url parameter or MCPKIT_SCHEMA_REGISTRY_URL env var")
    return registry_url.rstrip("/")


def get_schema_registry_auth() -> Optional[tuple[str, str]]:
    """Get Schema Registry basic auth from env."""
    auth_str = os.getenv("MCPKIT_SCHEMA_REGISTRY_BASIC_AUTH")
    if auth_str:
        parts = auth_str.split(":", 1)
        if len(parts) == 2:
            return (parts[0], parts[1])
    return None


def schema_registry_get(schema_id: Optional[int] = None, subject: Optional[str] = None, url: Optional[str] = None) -> dict:
    """
    Get schema from Schema Registry by ID or subject (latest).
    Returns dict with schema_json and metadata.
    """
    if schema_id is None and subject is None:
        raise GuardError("Either schema_id or subject must be provided")
    
    if schema_id is not None and subject is not None:
        raise GuardError("Provide either schema_id or subject, not both")
    
    base_url = get_schema_registry_url(url)
    auth = get_schema_registry_auth()
    
    if schema_id is not None:
        url = f"{base_url}/schemas/ids/{schema_id}"
    else:
        url = f"{base_url}/subjects/{subject}/versions/latest"
    
    try:
        response = requests.get(url, auth=auth, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # Normalize response format
        if schema_id is not None:
            # Response: {"schema": "..."}
            schema_str = data.get("schema", "")
            # Parse schema if it's a string
            import json
            if isinstance(schema_str, str):
                try:
                    schema_json = json.loads(schema_str)
                except (json.JSONDecodeError, TypeError):
                    schema_json = {"schema": schema_str}
            else:
                schema_json = data
            return {
                "schema_id": schema_id,
                "subject": None,
                "version": None,
                "schema_json": schema_json,
                "schema_string": schema_str if isinstance(schema_str, str) else None,
            }
        else:
            # Response: {"id": 1, "schema": "...", "subject": "...", "version": 1}
            return {
                "schema_id": data.get("id"),
                "subject": data.get("subject"),
                "version": data.get("version"),
                "schema_json": data,
                "schema_string": data.get("schema"),
            }
    except requests.RequestException as e:
        raise GuardError(f"Schema Registry request failed: {e}")


def schema_registry_list_subjects(url: Optional[str] = None) -> dict:
    """
    List all subjects in Schema Registry.
    
    Args:
        url: Optional Schema Registry URL. If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var
    
    Returns:
        dict with subjects list
    """
    base_url = get_schema_registry_url(url)
    auth = get_schema_registry_auth()
    
    subjects_url = f"{base_url}/subjects"
    
    try:
        response = requests.get(subjects_url, auth=auth, timeout=15)
        response.raise_for_status()
        subjects = response.json()
        
        return {
            "subjects": subjects,
            "subject_count": len(subjects),
        }
    except requests.RequestException as e:
        raise GuardError(f"Schema Registry request failed: {e}")

