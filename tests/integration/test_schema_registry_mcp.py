"""Integration tests for Schema Registry tools via MCP protocol."""

import base64
import json
import os
import time

import pytest
import requests

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
def schema_registry_setup(docker_services, kafka_setup):
    """Setup Schema Registry environment for tests."""
    # kafka_setup already sets MCPKIT_SCHEMA_REGISTRY_URL
    yield


def test_schema_registry_get_by_id_mcp(mcp_client, schema_registry_setup):
    """Test schema_registry_get by ID via MCP protocol."""
    # Register a schema first
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ]
    }
    
    subject = f"test-subject-{int(time.time())}"
    url = f"http://localhost:8081/subjects/{subject}/versions"
    response = requests.post(url, json={"schema": json.dumps(schema)})
    response.raise_for_status()
    schema_data = response.json()
    schema_id = schema_data["id"]
    
    # Get schema by ID via MCP
    mcp_response = call_tool(mcp_client, "schema_registry_get", schema_id=schema_id)
    
    assert_response_structure(mcp_response, ["schema_id", "schema_json"])
    assert mcp_response["schema_id"] == schema_id


def test_schema_registry_get_by_subject_mcp(mcp_client, schema_registry_setup):
    """Test schema_registry_get by subject via MCP protocol."""
    # Register a schema
    schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "email", "type": "string"}
        ]
    }
    
    subject = f"test-user-{int(time.time())}"
    url = f"http://localhost:8081/subjects/{subject}/versions"
    response = requests.post(url, json={"schema": json.dumps(schema)})
    response.raise_for_status()
    
    # Get schema by subject via MCP
    mcp_response = call_tool(mcp_client, "schema_registry_get", subject=subject)
    
    assert_response_structure(mcp_response, ["subject", "schema_json"])
    assert mcp_response["subject"] == subject


def test_schema_registry_list_subjects_mcp(mcp_client, schema_registry_setup):
    """Test schema_registry_list_subjects via MCP protocol."""
    response = call_tool(mcp_client, "schema_registry_list_subjects")
    
    assert_response_structure(response, ["subjects", "subject_count"])
    assert isinstance(response["subjects"], list)


def test_avro_decode_with_schema_registry_mcp(mcp_client, schema_registry_setup):
    """Test avro_decode with Schema Registry via MCP protocol."""
    import fastavro
    import io
    
    # Register schema
    schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"}
        ]
    }
    
    subject = f"test-user-{int(time.time())}"
    url = f"http://localhost:8081/subjects/{subject}/versions"
    response = requests.post(url, json={"schema": json.dumps(schema)})
    response.raise_for_status()
    schema_data = response.json()
    schema_id = schema_data["id"]
    
    # Create Avro-encoded value in Confluent format
    record = {"id": 1, "name": "Alice", "email": "alice@example.com"}
    
    # Encode with Avro
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    
    # Create Confluent format
    magic_byte = b"\x00"
    schema_id_bytes = schema_id.to_bytes(4, byteorder="big")
    confluent_value = magic_byte + schema_id_bytes + avro_payload
    
    # Encode to base64
    value_base64 = base64.b64encode(confluent_value).decode("utf-8")
    
    # Decode using MCP tool
    mcp_response = call_tool(mcp_client, "avro_decode", value_base64=value_base64)
    
    assert_response_structure(mcp_response, ["decoded"])
    assert mcp_response["decoded"]["id"] == 1
    assert mcp_response["decoded"]["name"] == "Alice"


def test_avro_decode_with_provided_schema_mcp(mcp_client):
    """Test avro_decode with provided schema via MCP protocol."""
    import fastavro
    import io
    
    schema = {
        "type": "record",
        "name": "Product",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "price", "type": "double"}
        ]
    }
    
    record = {"id": 100, "name": "Widget", "price": 25.50}
    
    # Encode
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    
    # Encode to base64
    value_base64 = base64.b64encode(avro_payload).decode("utf-8")
    
    # Decode with provided schema via MCP
    mcp_response = call_tool(
        mcp_client,
        "avro_decode",
        value_base64=value_base64,
        schema_json=schema
    )
    
    assert_response_structure(mcp_response, ["decoded"])
    assert mcp_response["decoded"]["id"] == 100
    assert mcp_response["decoded"]["name"] == "Widget"
    assert mcp_response["decoded"]["price"] == 25.50


def test_schema_registry_get_nonexistent_id_mcp(mcp_client, schema_registry_setup):
    """Test schema_registry_get with non-existent schema ID via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "schema_registry_get",
            schema_id=999999
        )


def test_schema_registry_get_nonexistent_subject_mcp(mcp_client, schema_registry_setup):
    """Test schema_registry_get with non-existent subject via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "schema_registry_get",
            subject="nonexistent-subject-xyz"
        )

