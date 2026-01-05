"""Avro and Protobuf decoding helpers."""

import base64
import io
from typing import Optional

import fastavro
from google.protobuf import descriptor_pb2, message_factory

from .guards import GuardError
from .json_tools import base64_decode
from .schema_registry import schema_registry_get


def avro_decode(value_base64: str, schema_json: Optional[dict] = None, schema_registry_url: Optional[str] = None) -> dict:
    """
    Decode Avro-encoded value.
    If schema_json omitted, expect Confluent framing (magic byte 0 + schemaId + payload).
    """
    value_bytes = base64_decode(value_base64)
    payload = value_bytes
    
    if schema_json is None:
        # Confluent format: magic byte (0) + schema_id (4 bytes) + payload
        if len(value_bytes) < 5:
            raise GuardError("Invalid Confluent Avro format: too short")
        
        if value_bytes[0] != 0:
            raise GuardError(f"Invalid Confluent Avro magic byte: {value_bytes[0]}")
        
        schema_id = int.from_bytes(value_bytes[1:5], byteorder="big")
        payload = value_bytes[5:]
        
        # Fetch schema from registry
        schema_data = schema_registry_get(schema_id=schema_id, url=schema_registry_url)
        schema_str = schema_data.get("schema_string")
        if schema_str:
            import json
            schema_json = json.loads(schema_str)
        else:
            raise GuardError("Could not get schema from registry")
    
    # Decode with schema
    try:
        bytes_io = io.BytesIO(payload)
        decoded = fastavro.schemaless_reader(bytes_io, schema_json)
        
        # Convert UUID objects and other non-serializable types to strings for JSON compatibility
        decoded = _convert_avro_types(decoded)
        
        return {"decoded": decoded}
    except Exception as e:
        raise GuardError(f"Avro decode error: {e}")


def _convert_avro_types(obj):
    """Convert Avro-specific types (UUID, etc.) to JSON-serializable types."""
    import uuid
    
    if isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: _convert_avro_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_avro_types(item) for item in obj]
    elif isinstance(obj, bytes):
        # Convert bytes to base64 string for JSON compatibility
        return base64.b64encode(obj).decode("utf-8")
    else:
        return obj


def protobuf_decode(
    value_base64: str,
    file_descriptor_set_base64: str,
    message_full_name: str
) -> dict:
    """
    Decode Protobuf-encoded value using file descriptor set.
    message_full_name is the fully qualified message name (e.g., "com.example.Message").
    """
    value_bytes = base64_decode(value_base64)
    descriptor_bytes = base64_decode(file_descriptor_set_base64)
    
    try:
        # Parse file descriptor set
        file_desc_set = descriptor_pb2.FileDescriptorSet()
        file_desc_set.ParseFromString(descriptor_bytes)
        
        # Create message factory
        pool = descriptor_pb2.DescriptorPool()
        for file_desc in file_desc_set.file:
            pool.Add(file_desc)
        
        # Get message descriptor
        desc = pool.FindMessageTypeByName(message_full_name)
        if desc is None:
            raise GuardError(f"Message type not found: {message_full_name}")
        
        # Create message class and parse
        msg_class = message_factory.GetMessageClass(desc)
        msg = msg_class()
        msg.ParseFromString(value_bytes)
        
        # Convert to dict
        result = {}
        for field, value in msg.ListFields():
            result[field.name] = _protobuf_value_to_python(value)
        
        return {"decoded": result}
    except Exception as e:
        raise GuardError(f"Protobuf decode error: {e}")


def _protobuf_value_to_python(value):
    """Convert protobuf value to Python native type."""
    if isinstance(value, (str, int, float, bool)):
        return value
    elif isinstance(value, bytes):
        return base64.b64encode(value).decode("utf-8")
    elif hasattr(value, "ListFields"):
        # Nested message
        return {field.name: _protobuf_value_to_python(val) for field, val in value.ListFields()}
    elif isinstance(value, (list, tuple)):
        return [_protobuf_value_to_python(v) for v in value]
    else:
        return str(value)

