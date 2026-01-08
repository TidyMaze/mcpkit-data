"""Dataset registry for storing and retrieving datasets."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

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


def load_index() -> dict[str, Any]:
    """Load index.json, return dict mapping dataset_id to metadata."""
    index_path = get_index_path()
    if not index_path.exists():
        return {}
    try:
        with open(index_path, "r") as f:
            return json.load(f)  # type: ignore[no-any-return]
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
    
    # Always ensure row_count is set from available sources
    # Priority: file exists > current_rows in meta > rows in meta
    if path.exists():
        df = pd.read_parquet(path)
        result["current_rows"] = len(df)
        result["row_count"] = len(df)  # Alias for compatibility
        result["current_columns"] = list(df.columns)
    else:
        # If file doesn't exist, use metadata
        if "current_rows" in result:
            result["row_count"] = result["current_rows"]
        elif "rows" in result:
            result["row_count"] = result["rows"]
            result["current_rows"] = result["rows"]
        else:
            # Fallback: set to 0 if no row info available
            result["row_count"] = 0
            result["current_rows"] = 0
    
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


def dataset_purge(
    older_than_days: Optional[int] = None,
    pattern: Optional[str] = None,
    max_delete: Optional[int] = None,
    purge_artifacts: Optional[bool] = None,
) -> dict:
    """
    Purge datasets matching criteria (age or pattern).
    
    Args:
        older_than_days: Delete datasets older than N days
        pattern: Delete datasets matching pattern (substring match in dataset_id)
        max_delete: Maximum number of datasets to delete (safety limit)
        purge_artifacts: If True, also purge artifacts. If None, auto-purge when no filters.
    
    Returns:
        dict with deleted_count, deleted_dataset_ids, and artifact_deleted_count
    """
    from .evidence import get_artifact_dir
    
    index = load_index()
    now = datetime.now()
    
    # Auto-purge artifacts if no filters (purging all)
    if purge_artifacts is None:
        purge_artifacts = (older_than_days is None and pattern is None)
    
    to_delete = []
    
    for dataset_id, meta in index.items():
        should_delete = False
        
        # Check age filter
        if older_than_days is not None:
            if "created_at" in meta:
                try:
                    created = datetime.fromisoformat(meta["created_at"].replace("Z", "+00:00"))
                    age_days = (now - created.replace(tzinfo=None)).days
                    if age_days >= older_than_days:
                        should_delete = True
                except (ValueError, TypeError):
                    pass
        
        # Check pattern filter
        if pattern and pattern in dataset_id:
            should_delete = True
        
        if should_delete:
            to_delete.append(dataset_id)
    
    # Apply max_delete limit
    if max_delete and len(to_delete) > max_delete:
        to_delete = to_delete[:max_delete]
    
    deleted_count = 0
    deleted_ids = []
    errors = []
    
    for dataset_id in to_delete:
        try:
            check_filename_safe(dataset_id)
            path = get_dataset_path(dataset_id)
            if path.exists():
                path.unlink()
            del index[dataset_id]
            deleted_count += 1
            deleted_ids.append(dataset_id)
        except Exception as e:
            errors.append(f"{dataset_id}: {str(e)}")
    
    if deleted_count > 0:
        save_index(index)
    
    # Purge artifacts
    artifact_deleted_count = 0
    if purge_artifacts:
        try:
            artifact_dir = get_artifact_dir()
            if artifact_dir.exists():
                # If no filters, delete all artifacts
                if older_than_days is None and pattern is None:
                    for artifact_file in artifact_dir.iterdir():
                        if artifact_file.is_file() and artifact_file.name != ".gitkeep":
                            try:
                                artifact_file.unlink()
                                artifact_deleted_count += 1
                            except Exception as e:
                                errors.append(f"artifact {artifact_file.name}: {str(e)}")
                else:
                    # With filters, delete artifacts matching pattern or older than threshold
                    for artifact_file in artifact_dir.iterdir():
                        if artifact_file.is_file() and artifact_file.name != ".gitkeep":
                            should_delete_artifact = False
                            
                            # Check pattern match
                            if pattern and pattern in artifact_file.name:
                                should_delete_artifact = True
                            
                            # Check age
                            if older_than_days is not None:
                                try:
                                    mtime = datetime.fromtimestamp(artifact_file.stat().st_mtime)
                                    age_days = (now - mtime).days
                                    if age_days >= older_than_days:
                                        should_delete_artifact = True
                                except Exception:
                                    pass
                            
                            if should_delete_artifact:
                                try:
                                    artifact_file.unlink()
                                    artifact_deleted_count += 1
                                except Exception as e:
                                    errors.append(f"artifact {artifact_file.name}: {str(e)}")
        except Exception as e:
            errors.append(f"artifact purge: {str(e)}")
    
    return {
        "deleted_count": deleted_count,
        "deleted_dataset_ids": deleted_ids,
        "artifact_deleted_count": artifact_deleted_count,
        "errors": errors if errors else None,
    }


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
        "path": str(path),
    }

