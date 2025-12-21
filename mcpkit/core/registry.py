"""Dataset registry for storing and retrieving datasets."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from .guards import GuardError, check_filename_safe


def get_dataset_dir() -> Path:
    """Get dataset directory from env, default ~/.mcpkit_datasets."""
    dataset_dir = os.getenv("MCPKIT_DATASET_DIR")
    if dataset_dir:
        return Path(dataset_dir).expanduser()
    # Use Path.home() with caching to avoid potential blocking
    try:
        return Path.home() / ".mcpkit_datasets"
    except Exception:
        # Fallback to current directory if home() fails
        return Path.cwd() / ".mcpkit_datasets"


def get_index_path() -> Path:
    """Get path to index.json."""
    return get_dataset_dir() / "index.json"


def ensure_dataset_dir():
    """Ensure dataset directory exists."""
    get_dataset_dir().mkdir(parents=True, exist_ok=True)


def load_index() -> dict:
    """Load index.json, return dict mapping dataset_id to metadata."""
    index_path = get_index_path()
    if not index_path.exists():
        return {}
    try:
        with open(index_path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        # Return empty on error rather than raising
        return {}


def save_index(index: dict):
    """Save index.json."""
    ensure_dataset_dir()
    with open(get_index_path(), "w") as f:
        json.dump(index, f, indent=2)


def get_dataset_path(dataset_id: str) -> Path:
    """Get path to dataset parquet file."""
    check_filename_safe(dataset_id)  # Ensure safe filename
    return get_dataset_dir() / f"{dataset_id}.parquet"


def dataset_list() -> dict:
    """List all datasets in registry."""
    index = load_index()
    return {
        "datasets": [
            {
                "dataset_id": dataset_id,
                **meta
            }
            for dataset_id, meta in index.items()
        ]
    }


def dataset_info(dataset_id: str) -> dict:
    """Get info about a dataset."""
    check_filename_safe(dataset_id)
    index = load_index()
    if dataset_id not in index:
        raise GuardError(f"Dataset {dataset_id} not found")
    
    meta = index[dataset_id]
    path = get_dataset_path(dataset_id)
    
    result = {
        "dataset_id": dataset_id,
        **meta
    }
    
    # Add current file info if exists
    if path.exists():
        df = pd.read_parquet(path)
        result["current_rows"] = len(df)
        result["current_columns"] = list(df.columns)
    
    return result


def dataset_put_rows(columns: list[str], rows: list[list], dataset_id: Optional[str] = None) -> dict:
    """
    Store rows as parquet dataset.
    If dataset_id is None, generate one.
    Returns dict with dataset_id and metadata.
    """
    ensure_dataset_dir()
    
    if dataset_id is None:
        # Generate dataset_id from timestamp
        dataset_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    else:
        check_filename_safe(dataset_id)
    
    # Pre-process rows to ensure headers is always a string (before DataFrame creation)
    # This prevents pandas from inferring dict/struct type for Parquet
    if "headers" in columns:
        headers_idx = columns.index("headers")
        processed_rows = []
        for row in rows:
            new_row = list(row)
            headers_val = new_row[headers_idx]
            if isinstance(headers_val, dict):
                new_row[headers_idx] = json.dumps(headers_val)
            elif headers_val is None:
                new_row[headers_idx] = "{}"
            elif not isinstance(headers_val, str):
                new_row[headers_idx] = str(headers_val)
            processed_rows.append(new_row)
        rows = processed_rows
    
    # Create DataFrame
    df = pd.DataFrame(rows, columns=columns)
    
    # Double-check headers is string (safety measure)
    if "headers" in df.columns:
        df["headers"] = df["headers"].astype(str)
    
    # Save as parquet - use pandas to_parquet which handles string types correctly
    path = get_dataset_path(dataset_id)
    df.to_parquet(path, index=False)
    
    # Update index
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


def dataset_delete(dataset_id: str) -> dict:
    """Delete a dataset from registry."""
    check_filename_safe(dataset_id)
    index = load_index()
    if dataset_id not in index:
        raise GuardError(f"Dataset {dataset_id} not found")
    
    path = get_dataset_path(dataset_id)
    if path.exists():
        path.unlink()
    
    del index[dataset_id]
    save_index(index)
    
    return {"dataset_id": dataset_id, "deleted": True}


def load_dataset(dataset_id: str) -> pd.DataFrame:
    """Load dataset as pandas DataFrame."""
    check_filename_safe(dataset_id)
    path = get_dataset_path(dataset_id)
    if not path.exists():
        raise GuardError(f"Dataset {dataset_id} not found")
    return pd.read_parquet(path)


def save_dataset(df: pd.DataFrame, dataset_id: Optional[str] = None) -> dict:
    """Save DataFrame as dataset."""
    if dataset_id is None:
        dataset_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    check_filename_safe(dataset_id)
    ensure_dataset_dir()
    path = get_dataset_path(dataset_id)
    df.to_parquet(path, index=False)
    
    # Update index
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
    }

