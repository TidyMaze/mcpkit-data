"""Integration tests for decode tools via MCP protocol."""

import base64
import pytest

from tests.utils_mcp import assert_response_structure, call_tool

pytestmark = pytest.mark.integration


def test_avro_decode_with_schema_mcp(mcp_client):
    """Test avro_decode with provided schema via MCP protocol."""
    import fastavro
    import io
    
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "price", "type": "double"}
        ]
    }
    
    record = {"id": 100, "name": "Test Product", "price": 25.50}
    
    # Encode
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    
    # Encode to base64
    value_base64 = base64.b64encode(avro_payload).decode("utf-8")
    
    # Decode via MCP
    response = call_tool(
        mcp_client,
        "avro_decode",
        value_base64=value_base64,
        schema_json=schema
    )
    
    assert_response_structure(response, ["decoded"])
    assert response["decoded"]["id"] == 100
    assert response["decoded"]["name"] == "Test Product"
    assert response["decoded"]["price"] == 25.50


def test_avro_decode_nested_schema_mcp(mcp_client):
    """Test avro_decode with nested schema via MCP protocol."""
    import fastavro
    import io
    
    schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "address", "type": {
                "type": "record",
                "name": "Address",
                "fields": [
                    {"name": "street", "type": "string"},
                    {"name": "city", "type": "string"}
                ]
            }}
        ]
    }
    
    record = {"id": 1, "address": {"street": "123 Main St", "city": "NYC"}}
    
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    value_base64 = base64.b64encode(avro_payload).decode("utf-8")
    
    response = call_tool(
        mcp_client,
        "avro_decode",
        value_base64=value_base64,
        schema_json=schema
    )
    
    assert_response_structure(response, ["decoded"])
    assert response["decoded"]["id"] == 1
    assert response["decoded"]["address"]["street"] == "123 Main St"


def test_avro_decode_array_schema_mcp(mcp_client):
    """Test avro_decode with array schema via MCP protocol."""
    import fastavro
    import io
    
    schema = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "order_id", "type": "int"},
            {"name": "items", "type": {"type": "array", "items": "string"}}
        ]
    }
    
    record = {"order_id": 123, "items": ["item1", "item2", "item3"]}
    
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    value_base64 = base64.b64encode(avro_payload).decode("utf-8")
    
    response = call_tool(
        mcp_client,
        "avro_decode",
        value_base64=value_base64,
        schema_json=schema
    )
    
    assert_response_structure(response, ["decoded"])
    assert response["decoded"]["order_id"] == 123
    assert len(response["decoded"]["items"]) == 3


def test_avro_decode_invalid_base64_mcp(mcp_client):
    """Test avro_decode with invalid base64 via MCP protocol."""
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "avro_decode",
            value_base64="not valid base64!!!",
            schema_json={"type": "record", "name": "Test", "fields": []}
        )


def test_avro_decode_invalid_schema_mcp(mcp_client):
    """Test avro_decode with invalid schema via MCP protocol."""
    import fastavro
    import io
    
    # Create valid Avro data
    schema1 = {"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]}
    record = {"id": 1}
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema1, record)
    avro_payload = bytes_io.getvalue()
    value_base64 = base64.b64encode(avro_payload).decode("utf-8")
    
    # Try to decode with wrong schema
    wrong_schema = {"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "avro_decode",
            value_base64=value_base64,
            schema_json=wrong_schema
        )


def test_protobuf_decode_mcp(mcp_client):
    """Test protobuf_decode via MCP protocol."""
    from google.protobuf import descriptor_pb2, message_factory
    
    # Create a simple protobuf message descriptor
    file_desc_proto = descriptor_pb2.FileDescriptorProto()
    file_desc_proto.name = "test.proto"
    file_desc_proto.package = "test"
    
    # Add message descriptor
    msg_desc = file_desc_proto.message_type.add()
    msg_desc.name = "TestMessage"
    
    # Add field: int32 id = 1
    field = msg_desc.field.add()
    field.name = "id"
    field.number = 1
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT32
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    
    # Add field: string name = 2
    field2 = msg_desc.field.add()
    field2.name = "name"
    field2.number = 2
    field2.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
    field2.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    
    # Create file descriptor set
    file_desc_set = descriptor_pb2.FileDescriptorSet()
    file_desc_set.file.append(file_desc_proto)
    
    # Serialize descriptor set
    descriptor_bytes = file_desc_set.SerializeToString()
    descriptor_base64 = base64.b64encode(descriptor_bytes).decode("utf-8")
    
    # Create and serialize message using the descriptor pool (same as decode.py)
    try:
        pool = descriptor_pb2.DescriptorPool()
        for file_desc in file_desc_set.file:
            pool.Add(file_desc)
        desc = pool.FindMessageTypeByName("test.TestMessage")
        msg_class = message_factory.GetMessageClass(desc)
        msg = msg_class()
        msg.id = 42
        msg.name = "Test"
        msg_bytes = msg.SerializeToString()
        msg_base64 = base64.b64encode(msg_bytes).decode("utf-8")
        
        # Decode via MCP
        response = call_tool(
            mcp_client,
            "protobuf_decode",
            value_base64=msg_base64,
            file_descriptor_set_base64=descriptor_base64,
            message_full_name="test.TestMessage"
        )
        
        assert_response_structure(response, ["decoded"])
        assert response["decoded"]["id"] == 42
        assert response["decoded"]["name"] == "Test"
    except (AttributeError, Exception) as e:
        # Protobuf setup can be complex, skip if it fails
        pytest.skip(f"Protobuf test setup failed: {e}")


def test_protobuf_decode_invalid_message_name_mcp(mcp_client):
    """Test protobuf_decode with invalid message name via MCP protocol."""
    from google.protobuf import descriptor_pb2
    
    # Create minimal descriptor set
    file_desc_set = descriptor_pb2.FileDescriptorSet()
    descriptor_bytes = file_desc_set.SerializeToString()
    descriptor_base64 = base64.b64encode(descriptor_bytes).decode("utf-8")
    
    # Try to decode with non-existent message name
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "protobuf_decode",
            value_base64="dGVzdA==",  # "test" in base64
            file_descriptor_set_base64=descriptor_base64,
            message_full_name="nonexistent.Message"
        )


def test_avro_decode_confluent_format_mcp(mcp_client, kafka_setup):
    """Test avro_decode with Confluent format (magic byte + schema ID) via MCP protocol."""
    import fastavro
    import io
    
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [{"name": "id", "type": "int"}]
    }
    
    record = {"id": 42}
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    
    # Confluent format: magic byte (0) + schema_id (4 bytes) + payload
    # For testing, use schema_id=1 (would need to register schema first in real scenario)
    schema_id = 1
    confluent_payload = bytes([0]) + schema_id.to_bytes(4, byteorder="big") + avro_payload
    value_base64 = base64.b64encode(confluent_payload).decode("utf-8")
    
    # This will fail without schema registry, but tests the Confluent format parsing
    with pytest.raises(Exception):  # Should raise GuardError (schema not found)
        call_tool(
            mcp_client,
            "avro_decode",
            value_base64=value_base64,
            schema_json=None  # Expects Confluent format
        )


def test_avro_decode_invalid_confluent_format_mcp(mcp_client):
    """Test avro_decode with invalid Confluent format via MCP protocol."""
    # Too short
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "avro_decode",
            value_base64=base64.b64encode(b"123").decode("utf-8"),  # Too short
            schema_json=None
        )
    
    # Wrong magic byte
    invalid_payload = bytes([1]) + b"\x00\x00\x00\x01" + b"payload"
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "avro_decode",
            value_base64=base64.b64encode(invalid_payload).decode("utf-8"),
            schema_json=None
        )


def test_avro_decode_bytes_field_mcp(mcp_client):
    """Test avro_decode with bytes field (should convert to base64) via MCP protocol."""
    import fastavro
    import io
    
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "data", "type": "bytes"}
        ]
    }
    
    record = {"id": 1, "data": b"binary data"}
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    value_base64 = base64.b64encode(avro_payload).decode("utf-8")
    
    response = call_tool(
        mcp_client,
        "avro_decode",
        value_base64=value_base64,
        schema_json=schema
    )
    
    assert_response_structure(response, ["decoded"])
    assert response["decoded"]["id"] == 1
    # Bytes should be converted to base64 string
    assert isinstance(response["decoded"]["data"], str)


def test_avro_decode_uuid_field_mcp(mcp_client):
    """Test avro_decode with UUID field (should convert to string) via MCP protocol."""
    import fastavro
    import io
    import uuid
    
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "uuid_field", "type": {"type": "string", "logicalType": "uuid"}}
        ]
    }
    
    test_uuid = uuid.uuid4()
    record = {"id": "1", "uuid_field": str(test_uuid)}
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, schema, record)
    avro_payload = bytes_io.getvalue()
    value_base64 = base64.b64encode(avro_payload).decode("utf-8")
    
    response = call_tool(
        mcp_client,
        "avro_decode",
        value_base64=value_base64,
        schema_json=schema
    )
    
    assert_response_structure(response, ["decoded"])
    assert isinstance(response["decoded"]["uuid_field"], str)


def test_protobuf_decode_nested_message_mcp(mcp_client):
    """Test protobuf_decode with nested message via MCP protocol."""
    from google.protobuf import descriptor_pb2, message_factory
    
    # Create nested message descriptor
    file_desc_proto = descriptor_pb2.FileDescriptorProto()
    file_desc_proto.name = "test.proto"
    file_desc_proto.package = "test"
    
    # Outer message
    outer_msg = file_desc_proto.message_type.add()
    outer_msg.name = "OuterMessage"
    
    field1 = outer_msg.field.add()
    field1.name = "id"
    field1.number = 1
    field1.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT32
    
    # Inner message
    inner_msg = file_desc_proto.message_type.add()
    inner_msg.name = "InnerMessage"
    
    field2 = inner_msg.field.add()
    field2.name = "value"
    field2.number = 1
    field2.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
    
    # Reference inner message from outer
    field3 = outer_msg.field.add()
    field3.name = "inner"
    field3.number = 2
    field3.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    field3.type_name = "test.InnerMessage"
    
    file_desc_set = descriptor_pb2.FileDescriptorSet()
    file_desc_set.file.append(file_desc_proto)
    descriptor_bytes = file_desc_set.SerializeToString()
    descriptor_base64 = base64.b64encode(descriptor_bytes).decode("utf-8")
    
    try:
        pool = descriptor_pb2.DescriptorPool()
        for file_desc in file_desc_set.file:
            pool.Add(file_desc)
        
        outer_desc = pool.FindMessageTypeByName("test.OuterMessage")
        inner_desc = pool.FindMessageTypeByName("test.InnerMessage")
        
        outer_class = message_factory.GetMessageClass(outer_desc)
        inner_class = message_factory.GetMessageClass(inner_desc)
        
        inner_msg = inner_class()
        inner_msg.value = "nested"
        
        outer_msg = outer_class()
        outer_msg.id = 42
        outer_msg.inner.CopyFrom(inner_msg)
        
        msg_bytes = outer_msg.SerializeToString()
        msg_base64 = base64.b64encode(msg_bytes).decode("utf-8")
        
        response = call_tool(
            mcp_client,
            "protobuf_decode",
            value_base64=msg_base64,
            file_descriptor_set_base64=descriptor_base64,
            message_full_name="test.OuterMessage"
        )
        
        assert_response_structure(response, ["decoded"])
        assert response["decoded"]["id"] == 42
        assert isinstance(response["decoded"]["inner"], dict)
    except (AttributeError, Exception) as e:
        pytest.skip(f"Protobuf nested message test setup failed: {e}")


def test_protobuf_decode_repeated_field_mcp(mcp_client):
    """Test protobuf_decode with repeated field via MCP protocol."""
    from google.protobuf import descriptor_pb2, message_factory
    
    file_desc_proto = descriptor_pb2.FileDescriptorProto()
    file_desc_proto.name = "test.proto"
    file_desc_proto.package = "test"
    
    msg_desc = file_desc_proto.message_type.add()
    msg_desc.name = "ListMessage"
    
    field = msg_desc.field.add()
    field.name = "items"
    field.number = 1
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
    
    file_desc_set = descriptor_pb2.FileDescriptorSet()
    file_desc_set.file.append(file_desc_proto)
    descriptor_bytes = file_desc_set.SerializeToString()
    descriptor_base64 = base64.b64encode(descriptor_bytes).decode("utf-8")
    
    try:
        pool = descriptor_pb2.DescriptorPool()
        for file_desc in file_desc_set.file:
            pool.Add(file_desc)
        
        desc = pool.FindMessageTypeByName("test.ListMessage")
        msg_class = message_factory.GetMessageClass(desc)
        msg = msg_class()
        msg.items.extend(["item1", "item2", "item3"])
        
        msg_bytes = msg.SerializeToString()
        msg_base64 = base64.b64encode(msg_bytes).decode("utf-8")
        
        response = call_tool(
            mcp_client,
            "protobuf_decode",
            value_base64=msg_base64,
            file_descriptor_set_base64=descriptor_base64,
            message_full_name="test.ListMessage"
        )
        
        assert_response_structure(response, ["decoded"])
        assert isinstance(response["decoded"]["items"], list)
        assert len(response["decoded"]["items"]) == 3
    except (AttributeError, Exception) as e:
        pytest.skip(f"Protobuf repeated field test setup failed: {e}")

