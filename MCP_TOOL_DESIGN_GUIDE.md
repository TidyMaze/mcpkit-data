# MCP Tool Design Guide: Reusability, Clarity, and Documentation

## Core Principles

### 1. **Single Responsibility**
Each tool should do ONE thing well:
- ✅ `pandas_filter_query` - only filters datasets
- ✅ `pandas_join` - only joins datasets
- ❌ `pandas_filter_and_join` - does too much

### 2. **Composability**
Tools should work together seamlessly:
- Output of one tool should be input to another
- Use `dataset_id` as the standard interface
- Return structured responses that can be chained

### 3. **Clear Naming**
- Use verb_noun pattern: `pandas_filter_query`, `kafka_consume_batch`
- Be specific: `pandas_filter_time_range` not `pandas_filter`
- Group related tools with prefixes: `pandas_*`, `kafka_*`, `jdbc_*`

## Documentation Standards

### Function Docstring Pattern

```python
@mcp.tool()
def pandas_parse_json_column(
    dataset_id: Annotated[str, Field(description="Input dataset ID")],
    column: Annotated[str, Field(description="Column name containing JSON strings to parse")],
    expand_arrays: Annotated[bool, Field(description="If True, expand JSON arrays into multiple rows. If False, parse as single object per row. Default: False")] = False,
    target_columns: Annotated[Optional[list[str]], Field(description="Optional list of JSON keys to extract as columns. If None, extracts all top-level keys. Example: [\"id\", \"title\", \"price\"]")] = None,
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
) -> DatasetOperationResponse:
    """
    Parse JSON strings from a column into structured columns.
    
    This tool parses JSON strings stored as text in a column and extracts
    their fields as new columns. Useful for working with nested JSON data
    from Kafka events, API responses, or database JSONB columns.
    
    **Input Format:**
    - Column contains JSON strings: `'{"id": "123", "title": "Product"}'`
    - Or JSON arrays: `'[{"id": "1"}, {"id": "2"}]'`
    
    **Behavior:**
    - If `expand_arrays=True` and column contains JSON arrays, each array
      element becomes a separate row (cross join behavior)
    - If `expand_arrays=False`, arrays are parsed but kept as single value
    - Null/empty strings are skipped
    - Invalid JSON strings are skipped with warning
    
    **Examples:**
    
    1. Parse product records from Kafka events:
    ```python
    # Input: column "productRecords" = '["{\\"id\\":\\"123\\",\\"title\\":\\"Product\\"}"]'
    # With expand_arrays=True: creates one row per product
    pandas_parse_json_column(
        dataset_id="kafka_events",
        column="productRecords",
        expand_arrays=True
    )
    ```
    
    2. Extract specific fields:
    ```python
    pandas_parse_json_column(
        dataset_id="events",
        column="metadata",
        target_columns=["userId", "timestamp", "action"]
    )
    ```
    
    **Returns:**
    - New dataset with parsed JSON fields as columns
    - Original columns are preserved
    - New columns use dot notation for nested fields: `price.currency`, `price.value`
    
    **Error Handling:**
    - Raises `GuardError` if column doesn't exist
    - Skips invalid JSON with warning (doesn't fail entire operation)
    - Returns empty dataset if all JSON is invalid
    """
    # Implementation...
```

### Parameter Documentation Pattern

```python
# ✅ GOOD: Clear, specific, with examples
column: Annotated[str, Field(
    description="Column name containing JSON strings to parse"
)]

# ✅ GOOD: Includes constraints and defaults
max_rows: Annotated[int, Field(
    description="Maximum rows to return. Default: 200"
)] = 200

# ✅ GOOD: Explains format with example
filters: Annotated[str, Field(
    description='JSON string of filter dicts, each with: {"column": str, "op": str (==, !=, <, <=, >, >=, in, contains, startswith, endswith), "value": any}'
)]

# ❌ BAD: Vague, no examples
column: Annotated[str, Field(description="Column")]

# ❌ BAD: Missing constraints
max_rows: Annotated[int, Field(description="Max rows")]
```

### Response Model Documentation

```python
class DatasetOperationResponse(BaseModel):
    """Response for dataset operations (groupby, join, filter, etc.)."""
    dataset_id: str = Field(
        description="Dataset identifier (use this in subsequent operations)"
    )
    rows: int = Field(
        description="Number of rows in the resulting dataset"
    )
    columns: list[str] = Field(
        description="List of column names in the resulting dataset"
    )
```

## Reusability Patterns

### 1. **Standard Input/Output Interface**

```python
# ✅ Standard pattern: dataset_id in, dataset_id out
def pandas_filter_query(
    dataset_id: str,  # Input
    filters: list[dict],
    out_dataset_id: Optional[str] = None  # Output (optional)
) -> DatasetOperationResponse:
    # Returns dataset_id for chaining
```

### 2. **Optional Parameters with Sensible Defaults**

```python
# ✅ Good defaults that work for 80% of cases
def pandas_parse_json_column(
    dataset_id: str,
    column: str,
    expand_arrays: bool = False,  # Most common case: single object
    target_columns: Optional[list[str]] = None,  # Extract all by default
    out_dataset_id: Optional[str] = None,  # Auto-generate by default
):
```

### 3. **Flexible Input Types**

```python
# ✅ Accepts multiple formats, converts internally
def pandas_filter_time_range(
    dataset_id: str,
    timestamp_column: str,
    start_time: Optional[str] = None,  # ISO format or pandas-parsable
    end_time: Optional[str] = None,
):
    # Handles: "2024-01-01", "2024-01-01T00:00:00Z", "1 hour ago", etc.
```

### 4. **Composable Operations**

```python
# ✅ Each tool can be chained
dataset_1 = pandas_filter_query(dataset_id="raw", filters=[...])
dataset_2 = pandas_parse_json_column(dataset_id=dataset_1.dataset_id, column="payload")
dataset_3 = pandas_groupby(dataset_id=dataset_2.dataset_id, group_cols=["product_id"], aggs={...})
```

## Error Handling

### Clear Error Messages

```python
# ✅ GOOD: Specific, actionable error
if column not in df.columns:
    raise GuardError(
        f"Column '{column}' not found in dataset. "
        f"Available columns: {', '.join(df.columns)}"
    )

# ❌ BAD: Generic error
if column not in df.columns:
    raise GuardError("Column not found")
```

### Graceful Degradation

```python
# ✅ Skip invalid data, don't fail entire operation
valid_rows = []
for row in df.itertuples():
    try:
        parsed = json.loads(row.productRecords)
        valid_rows.append(parsed)
    except json.JSONDecodeError:
        # Log warning but continue
        logger.warning(f"Skipping invalid JSON in row {row.Index}")
        continue
```

## Testing Documentation

### Include Usage Examples in Docstrings

```python
"""
**Examples:**

1. Basic usage:
   ```python
   pandas_parse_json_column(
       dataset_id="events",
       column="metadata"
   )
   ```

2. Expand arrays:
   ```python
   pandas_parse_json_column(
       dataset_id="kafka_events",
       column="productRecords",
       expand_arrays=True
   )
   ```

3. Extract specific fields:
   ```python
   pandas_parse_json_column(
       dataset_id="events",
       column="payload",
       target_columns=["id", "title", "price.value"]
   )
   ```
"""
```

## Implementation Checklist

When creating a new MCP tool:

- [ ] **Single responsibility**: Does one thing well
- [ ] **Clear name**: Verb_noun pattern, specific and descriptive
- [ ] **Complete docstring**: Purpose, behavior, examples, error handling
- [ ] **Parameter docs**: Every parameter has clear description with examples
- [ ] **Response model**: Well-documented return type
- [ ] **Error handling**: Clear, actionable error messages
- [ ] **Composable**: Works with other tools (uses dataset_id)
- [ ] **Flexible**: Sensible defaults, optional parameters
- [ ] **Tested**: Unit tests with various inputs
- [ ] **Examples**: Usage examples in docstring

## Example: Complete Tool Implementation

```python
# mcpkit/core/pandas_ops.py
def pandas_parse_json_column(
    dataset_id: str,
    column: str,
    expand_arrays: bool = False,
    target_columns: Optional[list[str]] = None,
    out_dataset_id: Optional[str] = None
) -> dict:
    """
    Parse JSON strings from a column into structured columns.
    
    See server.py for full documentation.
    """
    df = load_dataset(dataset_id)
    
    if column not in df.columns:
        raise GuardError(
            f"Column '{column}' not found. Available: {', '.join(df.columns)}"
        )
    
    # Implementation...
    result = save_dataset(parsed_df, out_dataset_id)
    return result


# mcpkit/server.py
@mcp.tool()
def pandas_parse_json_column(
    dataset_id: Annotated[str, Field(description="Input dataset ID")],
    column: Annotated[str, Field(description="Column name containing JSON strings to parse")],
    expand_arrays: Annotated[bool, Field(description="If True, expand JSON arrays into multiple rows. Default: False")] = False,
    target_columns: Annotated[Optional[list[str]], Field(description="Optional list of JSON keys to extract. If None, extracts all. Example: [\"id\", \"title\"]")] = None,
    out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
) -> DatasetOperationResponse:
    """
    Parse JSON strings from a column into structured columns.
    
    [Full docstring with examples, behavior, error handling]
    """
    result = pandas_ops.pandas_parse_json_column(
        dataset_id, column, expand_arrays, target_columns, out_dataset_id
    )
    return DatasetOperationResponse(**result)
```

## Summary

**Reusability**: Single responsibility, composable, flexible parameters
**Clarity**: Clear naming, complete docstrings, examples, good error messages
**Documentation**: Every parameter documented, response models explained, usage examples included

