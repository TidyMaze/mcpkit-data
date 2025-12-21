# Parameter Validation Fixes

## Summary

Fixed parameter validation issues for `list[dict]` parameters that were failing when called via MCP. FastMCP doesn't handle complex nested types like `list[dict]` well, so we changed these parameters to accept JSON strings instead.

## Fixed Tools

### 1. `kafka_filter`
- **Parameter**: `records` (was `list[dict]`, now `str`)
- **Change**: Accepts JSON string of Kafka record dicts
- **Example**: `'[{"key": "value", "topic": "test"}]'`

### 2. `kafka_groupby_key`
- **Parameter**: `records` (was `list[dict]`, now `str`)
- **Change**: Accepts JSON string of Kafka record dicts
- **Example**: `'[{"user_id": "123", "event": "click"}]'`

### 3. `dedupe_by_id`
- **Parameter**: `records` (was `list[dict]`, now `str`)
- **Change**: Accepts JSON string of record dicts
- **Example**: `'[{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]'`

### 4. `event_correlate`
- **Parameter**: `batches` (was `list[list[dict]]`, now `str`)
- **Change**: Accepts JSON string of batches (each batch is a list of record dicts)
- **Example**: `'[[{"id": 1, "timestamp": "2024-01-01"}], [{"id": 2, "timestamp": "2024-01-02"}]]'`

### 5. `pandas_filter_query`
- **Parameter**: `filters` (was `list[dict]`, now `str`)
- **Change**: Accepts JSON string of filter dicts
- **Example**: `'[{"column": "age", "op": ">", "value": 18}, {"column": "name", "op": "==", "value": "John"}]'`

### 6. `duckdb_query_local`
- **Parameter**: `sources` (was `Union[list[dict], str]`, now `str`)
- **Change**: Accepts JSON string of source definitions
- **Example**: `'[{"name": "users", "dataset_id": "user_data"}]'`

## Implementation Details

All tools use the `_parse_list_param()` helper function to convert JSON strings to Python lists:

```python
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
            import json
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
    return default
```

## Testing

Created `tests/test_parameter_validation.py` with comprehensive edge case tests:
- List inputs (already lists)
- JSON string inputs
- None values
- Empty strings
- Invalid JSON
- Nested lists (for batches)
- Complex dicts (for filters)

## Usage Examples

### pandas_filter_query
```python
filters = '[{"column": "age", "op": ">", "value": 18}]'
result = pandas_filter_query(dataset_id="users", filters=filters)
```

### duckdb_query_local
```python
sources = '[{"name": "users", "dataset_id": "user_data"}]'
result = duckdb_query_local(sql="SELECT * FROM users", sources=sources)
```

### kafka_filter
```python
records = '[{"key": "user123", "value": {"action": "click"}}]'
result = kafka_filter(records=records, predicate_jmes="value.action == 'click'")
```

## Notes

- All tools validate that parsed result is a list before using it
- Error messages are clear when parsing fails
- Backward compatible: still accepts Python lists if passed directly (though MCP will pass strings)
- No breaking changes to core functions, only MCP tool wrappers changed

