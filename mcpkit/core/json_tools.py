"""JSON manipulation and validation tools."""

import base64
import hashlib
import json
from typing import Any, Optional

import jmespath
import jsonschema

from .guards import GuardError


def base64_encode(data: bytes) -> str:
    """Encode bytes to base64 string."""
    return base64.b64encode(data).decode("utf-8")


def base64_decode(data: str) -> bytes:
    """Decode base64 string to bytes."""
    try:
        return base64.b64decode(data)
    except Exception as e:
        raise GuardError(f"Invalid base64: {e}")


def jq_transform(data: Any, expression: str) -> dict:
    """
    Transform data using JMESPath expression.
    
    If data is a JSON string, it will be parsed first.
    Returns dict with result.
    
    Args:
        data: Input data (dict, list, or JSON string)
        expression: JMESPath expression (e.g., "products[0].id", "items[*].name")
    
    Returns:
        dict with "result" key containing transformed data
    
    Examples:
        - Extract nested field: expression="price.value" on {"price": {"value": 100}}
        - Array projection: expression="products[*].id" on {"products": [{"id": 1}, {"id": 2}]}
        - Filter: expression="items[?price > `100`]" on {"items": [{"price": 50}, {"price": 150}]}
    """
    # If data is a string, try to parse as JSON
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            # If it's not JSON, use as-is (might be a plain string value)
            pass
    
    try:
        result = jmespath.search(expression, data)
        return {"result": result}
    except jmespath.exceptions.JMESPathError as e:
        raise GuardError(
            f"JMESPath expression error: {e}. "
            f"Expression: '{expression}'. "
            f"Check JMESPath syntax: https://jmespath.org/"
        )
    except Exception as e:
        raise GuardError(f"JMESPath error: {e}. Expression: '{expression}'")


def event_validate(record: dict, schema: dict) -> dict:
    """
    Validate record against JSONSchema.
    Returns dict with valid: bool and optional errors.
    """
    try:
        jsonschema.validate(instance=record, schema=schema)
        return {"valid": True, "errors": None}
    except jsonschema.ValidationError as e:
        return {
            "valid": False,
            "errors": [str(e)],
        }
    except jsonschema.SchemaError as e:
        raise GuardError(f"Invalid schema: {e}")


def event_fingerprint(record: Any, fields: Optional[list[str]] = None) -> dict:
    """
    Generate fingerprint for record.
    If fields specified, only fingerprint those fields.
    Returns dict with fingerprint (hex digest).
    """
    if fields:
        # Extract specified fields
        if isinstance(record, dict):
            subset = {k: record.get(k) for k in fields if k in record}
        else:
            raise GuardError("fields can only be used with dict records")
        data = json.dumps(subset, sort_keys=True).encode("utf-8")
    else:
        data = json.dumps(record, sort_keys=True).encode("utf-8")
    
    fingerprint = hashlib.sha256(data).hexdigest()
    return {"fingerprint": fingerprint}


def dedupe_by_id(records: list[dict], id_jmes: str) -> dict:
    """
    Deduplicate records by extracting ID using JMESPath.
    
    Keeps the first occurrence of each unique ID.
    Records where ID extraction fails are skipped.
    
    Args:
        records: List of record dictionaries
        id_jmes: JMESPath expression to extract ID (e.g., "id", "product.id", "metadata.userId")
    
    Returns:
        dict with unique_count, original_count, and records (deduplicated)
    """
    if not records:
        return {
            "unique_count": 0,
            "original_count": 0,
            "records": [],
        }
    
    seen = {}
    unique = []
    skipped = 0
    
    for record in records:
        try:
            record_id = jmespath.search(id_jmes, record)
            if record_id is None:
                skipped += 1
                continue
            # Convert to hashable type for dict key
            if isinstance(record_id, (dict, list)):
                record_id = json.dumps(record_id, sort_keys=True)
            if record_id not in seen:
                seen[record_id] = True
                unique.append(record)
        except Exception as e:
            # Skip records where JMESPath fails
            skipped += 1
            continue
    
    return {
        "unique_count": len(unique),
        "original_count": len(records),
        "records": unique,
        "skipped_count": skipped if skipped > 0 else None,
    }


def event_correlate(
    batches: list[list[dict]],
    join_key_jmes: str,
    timestamp_field: str = "timestamp"
) -> dict:
    """
    Correlate events across batches using join key and timestamp.
    Returns dict with correlated events.
    """
    # Build index: join_key -> list of (timestamp, batch_idx, record)
    index: dict[Any, list[tuple[Any, int, dict]]] = {}
    
    for batch_idx, batch in enumerate(batches):
        for record in batch:
            try:
                join_key = jmespath.search(join_key_jmes, record)
                timestamp = record.get(timestamp_field)
                if join_key is not None and timestamp is not None:
                    if join_key not in index:
                        index[join_key] = []
                    index[join_key].append((timestamp, batch_idx, record))
            except Exception:
                continue
    
    # Sort each key's events by timestamp
    for key in index:
        index[key].sort(key=lambda x: x[0])
    
    # Build correlated results
    correlated = []
    for join_key, events in index.items():
        if len(events) > 1:  # Only include keys with multiple events
            correlated.append({
                "join_key": join_key,
                "event_count": len(events),
                "events": [
                    {
                        "batch": batch_idx,
                        "timestamp": timestamp,
                        "record": record,
                    }
                    for timestamp, batch_idx, record in events
                ],
            })
    
    return {
        "correlation_count": len(correlated),
        "correlated": correlated,
    }

