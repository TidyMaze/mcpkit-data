"""Evidence bundle generation."""

import json
import os
from pathlib import Path

import pandas as pd

from .guards import GuardError, check_filename_safe
from .registry import get_dataset_path, load_dataset


def get_artifact_dir() -> Path:
    """Get artifact directory from env, default ~/.mcpkit_artifacts."""
    artifact_dir = os.getenv("MCPKIT_ARTIFACT_DIR")
    if artifact_dir:
        return Path(artifact_dir).expanduser()
    return Path.home() / ".mcpkit_artifacts"


def reconcile_counts(
    left_dataset_id: str,
    right_dataset_id: str,
    key_cols: list[str],
    out_dataset_id: str | None = None,
    max_examples: int = 200
) -> dict:
    """Reconcile record counts between two datasets."""
    from .registry import load_dataset, save_dataset
    
    left_df = load_dataset(left_dataset_id)
    right_df = load_dataset(right_dataset_id)
    
    # Count by key
    left_counts = left_df.groupby(key_cols).size().reset_index(name="left_count")
    right_counts = right_df.groupby(key_cols).size().reset_index(name="right_count")
    
    # Merge
    merged = pd.merge(left_counts, right_counts, on=key_cols, how="outer")
    merged["left_count"] = merged["left_count"].fillna(0)
    merged["right_count"] = merged["right_count"].fillna(0)
    merged["diff"] = merged["left_count"] - merged["right_count"]
    
    # Find mismatches
    mismatches = merged[merged["diff"] != 0].head(max_examples)
    
    result = {
        "total_keys_left": len(left_counts),
        "total_keys_right": len(right_counts),
        "mismatch_count": len(merged[merged["diff"] != 0]),
        "mismatches": mismatches.to_dict("records")[:max_examples],
    }
    
    if out_dataset_id:
        save_dataset(merged, out_dataset_id)
        result["out_dataset_id"] = out_dataset_id
    
    return result


def evidence_bundle_plus(
    dataset_id: str,
    base_filename: str,
    include_describe: bool = True,
    include_sample_rows: int = 50
) -> dict:
    """
    Generate evidence bundle: dataset info, describe, sample rows, exported files.
    Returns dict with artifact paths.
    """
    check_filename_safe(base_filename)
    
    artifact_dir = get_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    
    df = load_dataset(dataset_id)
    
    artifacts = []
    
    # Export parquet
    parquet_path = artifact_dir / f"{base_filename}.parquet"
    df.to_parquet(parquet_path, index=False)
    artifacts.append(str(parquet_path))
    
    # Export CSV
    csv_path = artifact_dir / f"{base_filename}.csv"
    df.to_csv(csv_path, index=False)
    artifacts.append(str(csv_path))
    
    # Generate metadata JSON
    metadata = {
        "dataset_id": dataset_id,
        "rows": len(df),
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
    }
    
    if include_describe:
        metadata["describe"] = df.describe().to_dict()
    
    if include_sample_rows > 0:
        metadata["sample_rows"] = df.head(include_sample_rows).to_dict("records")
    
    json_path = artifact_dir / f"{base_filename}_metadata.json"
    with open(json_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)
    artifacts.append(str(json_path))
    
    return {
        "dataset_id": dataset_id,
        "base_filename": base_filename,
        "artifacts": artifacts,
        "artifact_count": len(artifacts),
    }

