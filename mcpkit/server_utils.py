"""Utility functions for MCP server tools."""

import json
import logging

from mcpkit.core.guards import GuardError

logger = logging.getLogger(__name__)


def _to_int(value, default=None):
    """Convert value to int if it's a string, otherwise return as-is or default."""
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    if isinstance(value, int):
        return value
    return default


def _parse_list_param(value, default=None):
    """Parse list parameter that might come as string (JSON array) or list."""
    if value is None:
        return default
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        if value.lower() == "null" or value == "":
            return default
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            else:
                raise GuardError(f"Expected JSON array (list), got {type(parsed).__name__}")
        except json.JSONDecodeError as e:
            raise GuardError(f"Invalid JSON string for list parameter: {e}. Expected JSON array, got: {value[:100]}")
        except GuardError:
            raise
        except Exception as e:
            raise GuardError(f"Error parsing list parameter: {e}")
    return default


def _parse_dict_param(value, default=None):
    """Parse dict parameter that might come as string (JSON object) or dict."""
    if value is None:
        return default
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        if value.lower() == "null" or value == "":
            return default
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
            else:
                raise GuardError(f"Expected JSON object (dict), got {type(parsed).__name__}")
        except json.JSONDecodeError as e:
            raise GuardError(f"Invalid JSON string for dict parameter: {e}. Expected JSON object, got: {value[:100]}")
        except GuardError:
            raise
        except Exception as e:
            raise GuardError(f"Error parsing dict parameter: {e}")
    return default


def _dict_validator(value):
    """Pydantic validator to convert string (JSON object) to dict.
    
    Handles FastMCP serialization issues where dicts might come as strings.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        if value.lower() == "null" or value == "":
            return None
        # Check for FastMCP serialization failure
        if "[object Object]" in value or value.strip() == "[object Object]":
            logger.warning(
                "=== SERIALIZATION ISSUE DETECTED ==="
                f"\n  Parameter: hints (or other dict parameter)"
                f"\n  Received: '{value}' (string literal)"
                f"\n  Issue: FastMCP serialized dict to '[object Object]' and lost data"
                f"\n  Cause: Complex types (dict) cannot be passed directly via MCP"
                f"\n  Workaround: Pass as JSON string: '{{\"key\": \"value\"}}'"
            )
            raise ValueError(
                "FastMCP cannot serialize dict parameters directly. WORKAROUND:\n"
                "Pass as JSON string: '{\"key\": \"value\"}'"
            )
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
    # Return as-is if we can't parse (let Pydantic handle validation)
    return value


def _list_validator(value):
    """Pydantic validator to convert string (JSON array) to list."""
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        if value.lower() == "null" or value == "":
            return None
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
    return value


def _sources_validator(value):
    """Pydantic validator to convert sources parameter to list of dicts.

    Handles:
    - None -> None
    - JSON string -> parsed list of dicts
    - dict/list (for direct Python calls) -> converted to list of dicts
    - "[object Object]" -> clear error with solution
    """
    logger.debug(
        f"_sources_validator called with: type={type(value).__name__}, "
        f"value={repr(value)[:200]}, "
        f"isinstance(value, str)={isinstance(value, str)}, "
        f"isinstance(value, dict)={isinstance(value, dict)}, "
        f"isinstance(value, list)={isinstance(value, list)}"
    )

    if value is None:
        logger.debug("_sources_validator: value is None, returning None")
        return None

    # Handle dict/list for direct Python calls (not via MCP)
    if isinstance(value, list):
        logger.debug(f"_sources_validator: value is list with {len(value)} items (direct Python call)")
        if value and not all(isinstance(item, dict) for item in value):
            logger.warning(f"_sources_validator: list contains non-dict items: {[type(item).__name__ for item in value]}")
            raise ValueError(f"All items in sources list must be dicts, got: {value}")
        logger.debug(f"_sources_validator: returning list as-is: {value}")
        return value

    if isinstance(value, dict):
        logger.debug(f"_sources_validator: value is dict (direct Python call), wrapping in list: {value}")
        return [value]

    # For MCP calls, we only accept strings (JSON)
    if isinstance(value, str):
        logger.debug(f"_sources_validator: value is string, length={len(value)}, content={repr(value)[:200]}")
        if value.lower() == "null" or value == "":
            logger.debug("_sources_validator: empty/null string, returning None")
            return None

        # Check for FastMCP serialization failure
        if "[object Object]" in value or value.strip() == "[object Object]":
            logger.warning(
                "=== SERIALIZATION ISSUE DETECTED ==="
                f"\n  Parameter: sources"
                f"\n  Received: '{value}' (string literal)"
                f"\n  Issue: FastMCP serialized dict/list to '[object Object]' and lost data"
                f"\n  Cause: Complex types (dict/list) cannot be passed directly via MCP"
                f"\n  Workaround: Use individual parameters (source_name, source_dataset_id) OR pass JSON string"
            )
            raise ValueError(
                "FastMCP cannot serialize dict/list parameters. WORKAROUNDS:\n"
                "1. Use individual parameters: source_name='view', source_dataset_id='id'\n"
                "2. Convert to JSON string: json.dumps([{'name': 'view', 'dataset_id': 'id'}])\n"
                "3. Pass as JSON string: '[{\"name\": \"view\", \"dataset_id\": \"id\"}]'"
            )

        try:
            logger.debug(f"_sources_validator: attempting to parse JSON string: {value[:100]}")
            parsed = json.loads(value)
            logger.debug(f"_sources_validator: JSON parsed successfully, type={type(parsed).__name__}, value={parsed}")
            if isinstance(parsed, dict):
                logger.debug(f"_sources_validator: parsed dict, wrapping in list")
                return [parsed]
            elif isinstance(parsed, list):
                logger.debug(f"_sources_validator: parsed list with {len(parsed)} items")
                if parsed and not all(isinstance(item, dict) for item in parsed):
                    logger.warning(f"_sources_validator: parsed list contains non-dict items")
                    raise ValueError(f"All items in sources array must be dicts, got: {parsed}")
                return parsed
            else:
                logger.warning(f"_sources_validator: parsed JSON is neither dict nor list: {type(parsed)}")
                raise ValueError(f"Expected JSON object or array, got {type(parsed).__name__}")
        except json.JSONDecodeError as e:
            logger.error(
                f"_sources_validator: JSON parsing failed. "
                f"Error: {e}, "
                f"Value: {repr(value)[:200]}, "
                f"Value type: {type(value)}, "
                f"Value length: {len(value) if isinstance(value, str) else 'N/A'}"
            )
            if value.startswith("{") or "name" in value.lower() or "dataset_id" in value.lower():
                raise ValueError(
                    f"Invalid JSON string (PARSING ERROR): {e}. "
                    f"Expected valid JSON like '[{{\"name\": \"view\", \"dataset_id\": \"id\"}}]'. "
                    f"Got: {value[:100]}"
                )
            raise ValueError(f"Invalid JSON string (PARSING ERROR): {e}")

    # If we get here, it's an unexpected type
    logger.error(
        f"_sources_validator: Unexpected type. "
        f"Type: {type(value).__name__}, "
        f"Value: {repr(value)[:200]}, "
        f"MRO: {type(value).__mro__ if hasattr(type(value), '__mro__') else 'N/A'}"
    )
    raise ValueError(
        f"Sources must be a JSON string (for MCP calls) or dict/list (for direct Python calls). "
        f"Got: {type(value).__name__} = {repr(value)[:200]}. "
        f"For MCP: pass as JSON string like '[{{\"name\": \"view\", \"dataset_id\": \"id\"}}]'"
    )


def _int_validator(value):
    """Pydantic validator to convert string to int for Optional[int] parameters."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return int(value)
        except (ValueError, TypeError):
            raise ValueError(f"Cannot convert {value} to int")
    if isinstance(value, int):
        return value
    raise ValueError(f"Expected int or str, got {type(value).__name__}")

