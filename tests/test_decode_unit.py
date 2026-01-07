"""Unit tests for decode.py helper functions."""

import base64
from datetime import datetime, date, time
import uuid

import pytest

from mcpkit.core.decode import _convert_avro_types, _protobuf_value_to_python


def test_convert_avro_types_uuid():
    """Test _convert_avro_types converts UUID to string."""
    test_uuid = uuid.uuid4()
    result = _convert_avro_types(test_uuid)
    assert isinstance(result, str)
    assert result == str(test_uuid)


def test_convert_avro_types_datetime():
    """Test _convert_avro_types converts datetime to ISO string."""
    dt = datetime(2024, 1, 15, 10, 30, 45)
    result = _convert_avro_types(dt)
    assert isinstance(result, str)
    assert result == "2024-01-15T10:30:45"


def test_convert_avro_types_date():
    """Test _convert_avro_types converts date to ISO string."""
    d = date(2024, 1, 15)
    result = _convert_avro_types(d)
    assert isinstance(result, str)
    assert result == "2024-01-15"


def test_convert_avro_types_time():
    """Test _convert_avro_types converts time to ISO string."""
    t = time(10, 30, 45)
    result = _convert_avro_types(t)
    assert isinstance(result, str)
    assert result == "10:30:45"


def test_convert_avro_types_bytes():
    """Test _convert_avro_types converts bytes to base64 string."""
    data = b"binary data"
    result = _convert_avro_types(data)
    assert isinstance(result, str)
    assert result == base64.b64encode(data).decode("utf-8")


def test_convert_avro_types_dict():
    """Test _convert_avro_types recursively converts dict."""
    test_uuid = uuid.uuid4()
    dt = datetime(2024, 1, 15, 10, 30, 45)
    data = {
        "id": 1,
        "uuid": test_uuid,
        "timestamp": dt,
        "nested": {
            "date": date(2024, 1, 15),
            "time": time(10, 30, 45),
        }
    }
    result = _convert_avro_types(data)
    assert isinstance(result, dict)
    assert result["id"] == 1
    assert isinstance(result["uuid"], str)
    assert result["uuid"] == str(test_uuid)
    assert isinstance(result["timestamp"], str)
    assert isinstance(result["nested"], dict)
    assert isinstance(result["nested"]["date"], str)
    assert isinstance(result["nested"]["time"], str)


def test_convert_avro_types_list():
    """Test _convert_avro_types recursively converts list."""
    test_uuid = uuid.uuid4()
    dt = datetime(2024, 1, 15, 10, 30, 45)
    data = [
        1,
        test_uuid,
        dt,
        {"nested": date(2024, 1, 15)}
    ]
    result = _convert_avro_types(data)
    assert isinstance(result, list)
    assert result[0] == 1
    assert isinstance(result[1], str)
    assert isinstance(result[2], str)
    assert isinstance(result[3], dict)
    assert isinstance(result[3]["nested"], str)


def test_convert_avro_types_nested_list():
    """Test _convert_avro_types handles nested lists."""
    test_uuid = uuid.uuid4()
    data = [[test_uuid, datetime(2024, 1, 15)]]
    result = _convert_avro_types(data)
    assert isinstance(result, list)
    assert isinstance(result[0], list)
    assert isinstance(result[0][0], str)
    assert isinstance(result[0][1], str)


def test_convert_avro_types_primitive():
    """Test _convert_avro_types leaves primitives unchanged."""
    assert _convert_avro_types(42) == 42
    assert _convert_avro_types("string") == "string"
    assert _convert_avro_types(3.14) == 3.14
    assert _convert_avro_types(True) is True
    assert _convert_avro_types(None) is None


def test_protobuf_value_to_python_string():
    """Test _protobuf_value_to_python handles string."""
    assert _protobuf_value_to_python("test") == "test"


def test_protobuf_value_to_python_int():
    """Test _protobuf_value_to_python handles int."""
    assert _protobuf_value_to_python(42) == 42


def test_protobuf_value_to_python_float():
    """Test _protobuf_value_to_python handles float."""
    assert _protobuf_value_to_python(3.14) == 3.14


def test_protobuf_value_to_python_bool():
    """Test _protobuf_value_to_python handles bool."""
    assert _protobuf_value_to_python(True) is True
    assert _protobuf_value_to_python(False) is False


def test_protobuf_value_to_python_bytes():
    """Test _protobuf_value_to_python converts bytes to base64."""
    data = b"binary data"
    result = _protobuf_value_to_python(data)
    assert isinstance(result, str)
    assert result == base64.b64encode(data).decode("utf-8")


def test_protobuf_value_to_python_list():
    """Test _protobuf_value_to_python recursively converts list."""
    data = [1, "test", b"binary"]
    result = _protobuf_value_to_python(data)
    assert isinstance(result, list)
    assert result[0] == 1
    assert result[1] == "test"
    assert isinstance(result[2], str)
    assert result[2] == base64.b64encode(b"binary").decode("utf-8")


def test_protobuf_value_to_python_tuple():
    """Test _protobuf_value_to_python converts tuple to list."""
    data = (1, "test")
    result = _protobuf_value_to_python(data)
    assert isinstance(result, list)
    assert result == [1, "test"]


def test_protobuf_value_to_python_nested_message():
    """Test _protobuf_value_to_python handles nested message-like object."""
    class MockMessage:
        def ListFields(self):
            return [
                (type('Field', (), {'name': 'id'})(), 42),
                (type('Field', (), {'name': 'name'})(), "test")
            ]
    
    msg = MockMessage()
    result = _protobuf_value_to_python(msg)
    assert isinstance(result, dict)
    assert result["id"] == 42
    assert result["name"] == "test"


def test_protobuf_value_to_python_nested_message_with_bytes():
    """Test _protobuf_value_to_python handles nested message with bytes."""
    class MockMessage:
        def ListFields(self):
            return [
                (type('Field', (), {'name': 'data'})(), b"binary")
            ]
    
    msg = MockMessage()
    result = _protobuf_value_to_python(msg)
    assert isinstance(result, dict)
    assert isinstance(result["data"], str)
    assert result["data"] == base64.b64encode(b"binary").decode("utf-8")


def test_protobuf_value_to_python_unknown_type():
    """Test _protobuf_value_to_python converts unknown types to string."""
    class UnknownType:
        def __str__(self):
            return "unknown"
    
    obj = UnknownType()
    result = _protobuf_value_to_python(obj)
    assert isinstance(result, str)
    assert result == "unknown"

