"""Pandas operations."""

from typing import Optional

import pandas as pd

from .guards import GuardError, cap_rows, get_max_rows
from .registry import load_dataset, save_dataset


def pandas_from_rows(columns: list[str], rows: list[list], dataset_id: Optional[str] = None) -> dict:
    """Create pandas DataFrame from rows and save to registry."""
    df = pd.DataFrame(rows, columns=columns)
    result = save_dataset(df, dataset_id)
    return result


def pandas_describe(dataset_id: str, include: str = "all") -> dict:
    """Describe dataset statistics."""
    df = load_dataset(dataset_id)
    desc = df.describe(include=include)
    return {
        "dataset_id": dataset_id,
        "description": desc.to_dict(),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
    }


def pandas_groupby(
    dataset_id: str,
    group_cols: list[str],
    aggs: dict[str, list[str]],
    out_dataset_id: Optional[str] = None
) -> dict:
    """
    Group by columns with aggregations.
    Allowed aggs: count, sum, min, max, mean, nunique
    """
    allowed_aggs = {"count", "sum", "min", "max", "mean", "nunique"}
    for col, agg_list in aggs.items():
        for agg in agg_list:
            if agg not in allowed_aggs:
                raise GuardError(f"Invalid aggregation: {agg}. Allowed: {allowed_aggs}")
    
    df = load_dataset(dataset_id)
    
    # Build aggregation dict for pandas
    agg_dict = {}
    for col, agg_list in aggs.items():
        for agg in agg_list:
            if agg == "count":
                agg_dict[col] = "size"
            elif agg == "nunique":
                agg_dict[col] = "nunique"
            else:
                agg_dict[col] = agg
    
    grouped = df.groupby(group_cols).agg(agg_dict).reset_index()
    
    if out_dataset_id:
        result = save_dataset(grouped, out_dataset_id)
    else:
        result = save_dataset(grouped, None)
    
    return result


def pandas_join(
    left_dataset_id: str,
    right_dataset_id: str,
    keys: list[str],
    how: str = "inner",
    out_dataset_id: Optional[str] = None
) -> dict:
    """Join two datasets."""
    if how not in ["inner", "left", "right", "outer"]:
        raise GuardError(f"Invalid join type: {how}")
    
    left_df = load_dataset(left_dataset_id)
    right_df = load_dataset(right_dataset_id)
    
    merged = pd.merge(left_df, right_df, on=keys, how=how)
    
    if out_dataset_id:
        result = save_dataset(merged, out_dataset_id)
    else:
        result = save_dataset(merged, None)
    
    return result


def pandas_filter_query(
    dataset_id: str,
    filters: list[dict],
    out_dataset_id: Optional[str] = None
) -> dict:
    """
    Filter dataset using query conditions.
    Filter format: {"column": str, "op": str, "value": Any}
    Allowed ops: ==, !=, <, <=, >, >=, in, contains, startswith, endswith
    """
    allowed_ops = {"==", "!=", "<", "<=", ">", ">=", "in", "contains", "startswith", "endswith"}
    
    df = load_dataset(dataset_id)
    
    for filter_dict in filters:
        col = filter_dict["column"]
        op = filter_dict["op"]
        value = filter_dict["value"]
        
        if op not in allowed_ops:
            raise GuardError(f"Invalid filter op: {op}. Allowed: {allowed_ops}")
        
        if col not in df.columns:
            raise GuardError(f"Column {col} not found")
        
        if op == "==":
            df = df[df[col] == value]
        elif op == "!=":
            df = df[df[col] != value]
        elif op == "<":
            df = df[df[col] < value]
        elif op == "<=":
            df = df[df[col] <= value]
        elif op == ">":
            df = df[df[col] > value]
        elif op == ">=":
            df = df[df[col] >= value]
        elif op == "in":
            if not isinstance(value, list):
                raise GuardError("'in' operator requires list value")
            df = df[df[col].isin(value)]
        elif op == "contains":
            df = df[df[col].astype(str).str.contains(str(value), na=False)]
        elif op == "startswith":
            df = df[df[col].astype(str).str.startswith(str(value), na=False)]
        elif op == "endswith":
            df = df[df[col].astype(str).str.endswith(str(value), na=False)]
    
    if out_dataset_id:
        result = save_dataset(df, out_dataset_id)
    else:
        result = save_dataset(df, None)
    
    return result


def pandas_diff_frames(
    dataset_a: str,
    dataset_b: str,
    key_cols: list[str],
    compare_cols: Optional[list[str]] = None,
    max_changed: int = 200
) -> dict:
    """Compare two datasets and find differences."""
    df_a = load_dataset(dataset_a)
    df_b = load_dataset(dataset_b)
    
    # Set index on key columns
    df_a_idx = df_a.set_index(key_cols)
    df_b_idx = df_b.set_index(key_cols)
    
    # Find common keys
    common_keys = df_a_idx.index.intersection(df_b_idx.index)
    only_a = df_a_idx.index.difference(df_b_idx.index)
    only_b = df_b_idx.index.difference(df_a_idx.index)
    
    changed = []
    if compare_cols:
        for key in common_keys:
            row_a = df_a_idx.loc[key]
            row_b = df_b_idx.loc[key]
            diff_cols = []
            for col in compare_cols:
                if col in row_a.index and col in row_b.index:
                    if row_a[col] != row_b[col]:
                        diff_cols.append({
                            "column": col,
                            "value_a": str(row_a[col]),
                            "value_b": str(row_b[col]),
                        })
            if diff_cols:
                changed.append({
                    "key": dict(zip(key_cols, key if isinstance(key, tuple) else [key])),
                    "differences": diff_cols,
                })
                if len(changed) >= max_changed:
                    break
    
    return {
        "only_in_a": len(only_a),
        "only_in_b": len(only_b),
        "common": len(common_keys),
        "changed": cap_rows(changed, max_changed),
        "changed_count": len(changed),
    }


def pandas_schema_check(
    dataset_id: str,
    required_columns: Optional[list[str]] = None,
    dtype_overrides: Optional[dict[str, str]] = None,
    non_null_columns: Optional[list[str]] = None
) -> dict:
    """Check dataset schema constraints."""
    df = load_dataset(dataset_id)
    
    issues = []
    
    # Check required columns
    if required_columns:
        missing = set(required_columns) - set(df.columns)
        if missing:
            issues.append({
                "type": "missing_columns",
                "columns": list(missing),
            })
    
    # Check dtypes
    if dtype_overrides:
        for col, expected_dtype in dtype_overrides.items():
            if col in df.columns:
                actual_dtype = str(df[col].dtype)
                if actual_dtype != expected_dtype:
                    issues.append({
                        "type": "dtype_mismatch",
                        "column": col,
                        "expected": expected_dtype,
                        "actual": actual_dtype,
                    })
    
    # Check non-null
    if non_null_columns:
        for col in non_null_columns:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    issues.append({
                        "type": "null_values",
                        "column": col,
                        "null_count": int(null_count),
                    })
    
    # Convert issues to error strings for response
    errors = []
    if issues:
        for issue in issues:
            if issue["type"] == "missing_columns":
                errors.append(f"Missing columns: {', '.join(issue['columns'])}")
            elif issue["type"] == "dtype_mismatch":
                errors.append(f"Column {issue['column']}: expected {issue['expected']}, got {issue['actual']}")
            elif issue["type"] == "null_values":
                errors.append(f"Column {issue['column']}: {issue['null_count']} null values")
    
    return {
        "dataset_id": dataset_id,
        "valid": len(issues) == 0,
        "errors": errors if errors else None,
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
    }


def pandas_sample_stratified(
    dataset_id: str,
    strata_cols: list[str],
    n_per_group: int = 5,
    out_dataset_id: Optional[str] = None
) -> dict:
    """Stratified sampling."""
    df = load_dataset(dataset_id)
    
    sampled = df.groupby(strata_cols, group_keys=False).apply(
        lambda x: x.sample(min(n_per_group, len(x))),
        include_groups=False
    ).reset_index(drop=True)
    
    if out_dataset_id:
        result = save_dataset(sampled, out_dataset_id)
    else:
        result = save_dataset(sampled, None)
    
    return result


def pandas_head_tail(dataset_id: str, head: Optional[int] = None, tail: Optional[int] = None) -> dict:
    """
    Get first N and/or last M rows from dataset.
    
    Args:
        dataset_id: Dataset ID
        head: Number of rows from start (default: None, returns all)
        tail: Number of rows from end (default: None, returns all)
    
    Returns:
        dict with head_rows, tail_rows, and metadata
    """
    df = load_dataset(dataset_id)
    
    head_rows = None
    tail_rows = None
    
    if head is not None and head > 0:
        head_rows = df.head(head).to_dict("records")
    
    if tail is not None and tail > 0:
        tail_rows = df.tail(tail).to_dict("records")
    
    return {
        "dataset_id": dataset_id,
        "total_rows": len(df),
        "head_rows": head_rows,
        "tail_rows": tail_rows,
        "head_count": len(head_rows) if head_rows else 0,
        "tail_count": len(tail_rows) if tail_rows else 0,
    }


def pandas_export(dataset_id: str, format: str, filename: str) -> dict:
    """Export dataset to file in artifact directory."""
    from .evidence import get_artifact_dir
    from .guards import check_filename_safe
    
    check_filename_safe(filename)
    df = load_dataset(dataset_id)
    artifact_dir = get_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    path = artifact_dir / filename
    
    if format.upper() == "CSV":
        # Use default CSV quoting (pandas automatically quotes fields with newlines/special chars)
        df.to_csv(path, index=False)
    elif format.upper() == "JSON":
        df.to_json(path, orient="records", indent=2)
    elif format.upper() == "PARQUET":
        df.to_parquet(path, index=False)
    else:
        raise GuardError(f"Unsupported format: {format}")
    
    return {
        "dataset_id": dataset_id,
        "format": format,
        "filename": filename,
        "path": str(path),
    }


def pandas_sample_random(
    dataset_id: str,
    n: int,
    out_dataset_id: Optional[str] = None,
    seed: Optional[int] = None
) -> dict:
    """
    Random sampling from dataset.
    
    Args:
        dataset_id: Input dataset ID
        n: Number of samples to take
        out_dataset_id: Optional output dataset ID
        seed: Optional random seed for reproducibility
    
    Returns:
        dict with dataset_id and row_count
    """
    df = load_dataset(dataset_id)
    
    if n < 0:
        raise GuardError("Sample size n must be >= 0")
    
    if n > len(df):
        n = len(df)
    
    sampled = df.sample(n=n, random_state=seed).reset_index(drop=True)
    
    if out_dataset_id:
        result = save_dataset(sampled, out_dataset_id)
    else:
        result = save_dataset(sampled, None)
    
    return result


def pandas_count_distinct(dataset_id: str, columns: Optional[list[str]] = None) -> dict:
    """
    Count distinct values per column.
    
    Args:
        dataset_id: Dataset ID
        columns: Optional list of columns to count. If None, counts all columns.
    
    Returns:
        dict with column names and distinct counts
    """
    df = load_dataset(dataset_id)
    
    if columns is None:
        columns = list(df.columns)
    
    distinct_counts = {}
    for col in columns:
        if col not in df.columns:
            raise GuardError(f"Column {col} not found")
        distinct_counts[col] = int(df[col].nunique())
    
    return {
        "dataset_id": dataset_id,
        "distinct_counts": distinct_counts,
    }


def pandas_filter_time_range(
    dataset_id: str,
    timestamp_column: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    out_dataset_id: Optional[str] = None
) -> dict:
    """
    Filter dataset by timestamp column.
    
    Args:
        dataset_id: Input dataset ID
        timestamp_column: Name of timestamp column
        start_time: Optional start time (ISO format string or pandas-parsable)
        end_time: Optional end time (ISO format string or pandas-parsable)
        out_dataset_id: Optional output dataset ID
    
    Returns:
        dict with dataset_id and row_count
    """
    df = load_dataset(dataset_id)
    
    if timestamp_column not in df.columns:
        raise GuardError(f"Timestamp column {timestamp_column} not found")
    
    # Convert timestamp column to datetime
    try:
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
    except Exception as e:
        raise GuardError(f"Failed to parse timestamp column: {e}")
    
    # Apply filters
    if start_time:
        try:
            start_dt = pd.to_datetime(start_time)
            df = df[df[timestamp_column] >= start_dt]
        except Exception as e:
            raise GuardError(f"Failed to parse start_time: {e}")
    
    if end_time:
        try:
            end_dt = pd.to_datetime(end_time)
            df = df[df[timestamp_column] <= end_dt]
        except Exception as e:
            raise GuardError(f"Failed to parse end_time: {e}")
    
    if out_dataset_id:
        result = save_dataset(df, out_dataset_id)
    else:
        result = save_dataset(df, None)
    
    return result

