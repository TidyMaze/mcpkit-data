"""Polars operations."""

from typing import Optional

import polars as pl

from .guards import GuardError
from .registry import get_dataset_path, load_index, save_index
from .registry import check_filename_safe, ensure_dataset_dir


def polars_from_rows(columns: list[str], rows: list[list], dataset_id: Optional[str] = None) -> dict:
    """Create polars DataFrame from rows and save to registry."""
    if rows:
        # Transpose rows to columns
        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    else:
        data = {col: [] for col in columns}
    df = pl.DataFrame(data)
    
    # Save as parquet (polars can write parquet)
    if dataset_id is None:
        from datetime import datetime
        dataset_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    else:
        check_filename_safe(dataset_id)
    
    ensure_dataset_dir()
    path = get_dataset_path(dataset_id)
    df.write_parquet(str(path))
    
    # Update index
    from .registry import load_index, save_index
    from datetime import datetime
    index = load_index()
    index[dataset_id] = {
        "created_at": datetime.now().isoformat(),
        "rows": len(df),
        "columns": list(df.columns),
        "path": str(path),
    }
    save_index(index)
    
    return {
        "dataset_id": dataset_id,
        "rows": len(df),
        "columns": list(df.columns),
        "path": str(path),
    }


def _load_polars_dataset(dataset_id: str) -> pl.DataFrame:
    """Load dataset as polars DataFrame."""
    check_filename_safe(dataset_id)
    path = get_dataset_path(dataset_id)
    if not path.exists():
        raise GuardError(f"Dataset {dataset_id} not found")
    return pl.read_parquet(str(path))


def _save_polars_dataset(df: pl.DataFrame, dataset_id: Optional[str] = None) -> dict:
    """Save polars DataFrame as dataset."""
    if dataset_id is None:
        from datetime import datetime
        dataset_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    else:
        check_filename_safe(dataset_id)
    
    ensure_dataset_dir()
    path = get_dataset_path(dataset_id)
    df.write_parquet(str(path))
    
    # Update index
    from .registry import load_index, save_index
    from datetime import datetime
    index = load_index()
    index[dataset_id] = {
        "created_at": datetime.now().isoformat(),
        "rows": len(df),
        "columns": list(df.columns),
        "path": str(path),
    }
    save_index(index)
    
    return {
        "dataset_id": dataset_id,
        "rows": len(df),
        "columns": list(df.columns),
        "path": str(path),
    }


def polars_groupby(
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
    
    df = _load_polars_dataset(dataset_id)
    
    # Build aggregation expressions
    agg_exprs = []
    for col, agg_list in aggs.items():
        for agg in agg_list:
            if agg == "count":
                agg_exprs.append(pl.col(col).count().alias(f"{col}_count"))
            elif agg == "sum":
                agg_exprs.append(pl.col(col).sum().alias(f"{col}_sum"))
            elif agg == "min":
                agg_exprs.append(pl.col(col).min().alias(f"{col}_min"))
            elif agg == "max":
                agg_exprs.append(pl.col(col).max().alias(f"{col}_max"))
            elif agg == "mean":
                agg_exprs.append(pl.col(col).mean().alias(f"{col}_mean"))
            elif agg == "nunique":
                agg_exprs.append(pl.col(col).n_unique().alias(f"{col}_nunique"))
    
    grouped = df.group_by(group_cols).agg(agg_exprs)
    
    return _save_polars_dataset(grouped, out_dataset_id)


def polars_export(dataset_id: str, format: str, filename: str) -> dict:
    """Export dataset to file in artifact directory."""
    from .evidence import get_artifact_dir
    
    check_filename_safe(filename)
    df = _load_polars_dataset(dataset_id)
    artifact_dir = get_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    path = artifact_dir / filename
    
    if format.upper() == "CSV":
        df.write_csv(str(path))
    elif format.upper() == "JSON":
        df.write_json(str(path))
    elif format.upper() == "PARQUET":
        df.write_parquet(str(path))
    else:
        raise GuardError(f"Unsupported format: {format}")
    
    return {
        "dataset_id": dataset_id,
        "format": format,
        "filename": filename,
        "path": str(path),
        "artifact_path": str(path),  # Alias for compatibility
    }

