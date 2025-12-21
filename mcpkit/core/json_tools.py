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
    Returns dict with result.
    """
    try:
        result = jmespath.search(expression, data)
        return {"result": result}
    except Exception as e:
        raise GuardError(f"JMESPath error: {e}")


def event_validate(record: dict, schema: dict) -> dict:
    """
    Validate record against JSONSchema.
    Returns dict with valid: bool and optional errors.
    """
    try:
        jsonschema.validate(instance=record, schema=schema)
        return {"valid": True}
    except jsonschema.ValidationError as e:
        return {
            "valid": False,
            "error": str(e),
            "path": list(e.path) if e.path else [],
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
    Returns dict with unique records and count.
    """
    seen = {}
    unique = []
    
    for record in records:
        try:
            record_id = jmespath.search(id_jmes, record)
            if record_id is None:
                continue
            if record_id not in seen:
                seen[record_id] = True
                unique.append(record)
        except Exception as e:
            # Skip records where JMESPath fails
            continue
    
    return {
        "unique_count": len(unique),
        "original_count": len(records),
        "records": unique,
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
        "correlated_count": len(correlated),
        "correlated": correlated,
    }

