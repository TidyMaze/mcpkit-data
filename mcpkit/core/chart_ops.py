"""Chart generation from datasets."""

import io
import os
from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from .evidence import get_artifact_dir
from .guards import (
    GuardError,
    cap_bytes,
    cap_rows,
    check_filename_safe,
    check_path_allowed,
    get_max_output_bytes,
    get_max_rows,
)
from .registry import load_dataset


def _load_dataset_from_path(path: str, input_format: str) -> pd.DataFrame:
    """Load dataset from file path."""
    resolved_path = check_path_allowed(path)
    
    if input_format == "auto":
        # Detect from extension
        ext = resolved_path.suffix.lower()
        if ext == ".csv":
            input_format = "csv"
        elif ext == ".parquet":
            input_format = "parquet"
        elif ext in [".json", ".jsonl"]:
            input_format = "jsonl"
        elif ext == ".avro":
            input_format = "avro"
        else:
            raise GuardError(f"Cannot auto-detect format for {ext}, specify input_format")
    
    input_format = input_format.lower()
    
    if input_format == "csv":
        df = pd.read_csv(resolved_path)
    elif input_format == "parquet":
        df = pd.read_parquet(resolved_path)
    elif input_format == "jsonl":
        df = pd.read_json(resolved_path, lines=True)
    elif input_format == "avro":
        # Read Avro container file
        try:
            import fastavro
        except ImportError:
            raise GuardError("fastavro not installed. Install with: pip install fastavro")
        records = []
        with open(resolved_path, "rb") as f:
            reader = fastavro.reader(f)
            for record in reader:
                records.append(record)
        df = pd.DataFrame(records)
    else:
        raise GuardError(f"Unsupported input_format: {input_format}")
    
    return df


def _detect_column_types(df: pd.DataFrame) -> dict:
    """Detect column types: datetime, numeric, categorical, id-like."""
    detected = {}
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        null_ratio = df[col].isna().sum() / len(df) if len(df) > 0 else 0
        cardinality = df[col].nunique()
        
        col_info = {
            "dtype": dtype,
            "null_ratio": float(null_ratio),
            "cardinality": int(cardinality),
            "type": None,
        }
        
        # Check if datetime
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            col_info["type"] = "datetime"
        elif pd.api.types.is_numeric_dtype(df[col]):
            col_info["type"] = "numeric"
        # Check if ID-like (high cardinality relative to dataset size, low null ratio, minimum rows)
        # Only classify as ID if dataset is large enough and cardinality is very high
        elif len(df) >= 10 and cardinality > len(df) * 0.95 and null_ratio < 0.1:
            col_info["type"] = "id"
        elif cardinality < 20 or dtype == "object":
            col_info["type"] = "categorical"
        else:
            col_info["type"] = "other"
        
        detected[col] = col_info
    
    return detected


def _auto_pick_chart(detected: dict, df: pd.DataFrame, goal: Optional[str] = None, hints: Optional[dict] = None) -> dict:
    """Auto-pick chart type and axes based on detected schema."""
    # Override with hints if provided
    if hints:
        chart_type = hints.get("chart_type")
        x = hints.get("x")
        y = hints.get("y")
        agg = hints.get("agg", "mean")
        group_by = hints.get("group_by")
        date_granularity = hints.get("date_granularity")
        top_k = hints.get("top_k")
        filters = hints.get("filters")
        
        return {
            "chart_type": chart_type or "bar",
            "x": x,
            "y": y,
            "agg": agg,
            "group_by": group_by,
            "date_granularity": date_granularity,
            "top_k": top_k,
            "filters": filters,
        }
    
    # Auto-detect (exclude ID-like columns)
    datetime_cols = [col for col, info in detected.items() if info["type"] == "datetime"]
    numeric_cols = [col for col, info in detected.items() if info["type"] == "numeric"]
    categorical_cols = [col for col, info in detected.items() if info["type"] == "categorical"]
    
    spec = {}
    
    # Rule 1: datetime + numeric => line (time series)
    if datetime_cols and numeric_cols:
        spec["chart_type"] = "line"
        spec["x"] = datetime_cols[0]
        spec["y"] = numeric_cols[0]
        # If many points, suggest aggregation/resampling
        if len(df) > 100:
            spec["date_granularity"] = "auto"  # Will resample if needed
    # Rule 2: categorical + numeric => bar
    elif categorical_cols and numeric_cols:
        spec["chart_type"] = "bar"
        spec["x"] = categorical_cols[0]
        # Prefer count-like columns (eventAction, count, etc.) over ID columns
        count_like_cols = [col for col in numeric_cols if any(keyword in col.lower() for keyword in ["count", "action", "total", "sum"])]
        if count_like_cols:
            spec["y"] = count_like_cols[0]
        else:
            spec["y"] = numeric_cols[0]
        spec["agg"] = "mean"
        # If multiple categorical columns and data looks pre-aggregated, use grouping
        if len(categorical_cols) >= 2:
            x_unique = df[categorical_cols[0]].nunique()
            y_unique = df[categorical_cols[1]].nunique()
            # If we have multiple categories per x value, it's likely grouped data
            # Check if any x value appears multiple times (indicating grouping)
            if df[categorical_cols[0]].value_counts().max() > 1:
                spec["group_by"] = categorical_cols[1]
        # If high cardinality, use top_k
        if detected[categorical_cols[0]]["cardinality"] > 20:
            spec["top_k"] = 20
    # Rule 3: >=2 numeric => scatter
    elif len(numeric_cols) >= 2:
        spec["chart_type"] = "scatter"
        spec["x"] = numeric_cols[0]
        spec["y"] = numeric_cols[1]
    # Rule 4: only numeric => histogram
    elif len(numeric_cols) == 1:
        spec["chart_type"] = "histogram"
        spec["x"] = numeric_cols[0]
    # Rule 5: else => bar of counts
    else:
        spec["chart_type"] = "bar"
        if categorical_cols:
            spec["x"] = categorical_cols[0]
        else:
            spec["x"] = df.columns[0]
        spec["y"] = None  # Count
    
    return spec


def _render_chart(df: pd.DataFrame, spec: dict, output_path: Path, output_format: str):
    """Render chart using matplotlib."""
    import matplotlib
    matplotlib.use("Agg")  # Non-interactive backend
    import matplotlib.pyplot as plt
    
    chart_type = spec["chart_type"]
    x_col = spec.get("x")
    y_col = spec.get("y")
    
    # Apply filters if specified
    if spec.get("filters"):
        for filter_dict in spec["filters"]:
            col = filter_dict["column"]
            op = filter_dict["op"]
            value = filter_dict["value"]
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
                df = df[df[col].isin(value)]
    
    # Prepare data based on chart type
    if chart_type == "line":
        if x_col and y_col:
            # Resample if many points and date_granularity specified
            if spec.get("date_granularity") and len(df) > 100:
                df[x_col] = pd.to_datetime(df[x_col])
                df = df.set_index(x_col)
                df = df.resample("D").mean() if len(df) > 365 else df.resample("H").mean()
                df = df.reset_index()
            plt.plot(df[x_col], df[y_col])
            plt.xlabel(x_col)
            plt.ylabel(y_col)
            plt.title(f"{y_col} over {x_col}")
    elif chart_type == "bar":
        if x_col:
            if y_col:
                # Check if data is already aggregated (each x_col value appears once)
                # If so, plot directly; otherwise aggregate
                if df[x_col].nunique() == len(df) and not spec.get("group_by"):
                    # Data is already aggregated - plot directly (no grouping)
                    plot_df = df.sort_values(by=y_col, ascending=False) if y_col in df.columns else df
                    # Apply top_k if specified
                    if spec.get("top_k"):
                        plot_df = plot_df.nlargest(spec["top_k"], y_col)
                    plt.bar(plot_df[x_col], plot_df[y_col])
                    plt.xlabel(x_col)
                    plt.ylabel(y_col)
                    plt.xticks(rotation=45, ha='right')
                elif spec.get("group_by") and spec["group_by"] in df.columns:
                    # Data has group_by column - create grouped bars
                    group_col = spec["group_by"]
                    pivot_df = df.pivot(index=x_col, columns=group_col, values=y_col)
                    pivot_df.plot(kind="bar", ax=plt.gca(), width=0.8)
                    plt.xlabel(x_col)
                    plt.ylabel(y_col)
                    plt.legend(title=group_col, bbox_to_anchor=(1.05, 1), loc='upper left')
                    plt.xticks(rotation=45, ha='right')
                else:
                    # Need to aggregate
                    agg = spec.get("agg", "mean")
                    if spec.get("group_by"):
                        grouped = df.groupby([spec["group_by"], x_col])[y_col].agg(agg).reset_index()
                        # Create grouped bar chart
                        group_col = spec["group_by"]
                        pivot_df = grouped.pivot(index=x_col, columns=group_col, values=y_col)
                        pivot_df.plot(kind="bar", ax=plt.gca(), width=0.8)
                        plt.xlabel(x_col)
                        plt.ylabel(f"{agg}({y_col})")
                        plt.legend(title=group_col, bbox_to_anchor=(1.05, 1), loc='upper left')
                        plt.xticks(rotation=45, ha='right')
                    else:
                        grouped = df.groupby(x_col)[y_col].agg(agg).reset_index()
                        
                        # Apply top_k if specified
                        if spec.get("top_k"):
                            grouped = grouped.nlargest(spec["top_k"], y_col)
                        
                        plt.bar(grouped[x_col], grouped[y_col])
                        plt.xlabel(x_col)
                        plt.ylabel(f"{agg}({y_col})")
                        plt.xticks(rotation=45, ha='right')
            else:
                # Count
                counts = df[x_col].value_counts()
                if spec.get("top_k"):
                    counts = counts.head(spec["top_k"])
                plt.bar(counts.index, counts.values)
                plt.xlabel(x_col)
                plt.ylabel("Count")
    elif chart_type == "scatter":
        if x_col and y_col:
            plt.scatter(df[x_col], df[y_col])
            plt.xlabel(x_col)
            plt.ylabel(y_col)
            plt.title(f"{y_col} vs {x_col}")
    elif chart_type == "histogram":
        if x_col:
            plt.hist(df[x_col].dropna(), bins=30)
            plt.xlabel(x_col)
            plt.ylabel("Frequency")
            plt.title(f"Distribution of {x_col}")
    
    plt.tight_layout()
    
    # Save
    if output_format == "png":
        plt.savefig(output_path, format="png", dpi=100)
    elif output_format == "svg":
        plt.savefig(output_path, format="svg")
    else:
        raise GuardError(f"Unsupported output_format: {output_format}")
    
    plt.close()


def dataset_to_chart(
    dataset_id: Optional[str] = None,
    path: Optional[str] = None,
    input_format: Literal["auto", "csv", "parquet", "avro", "jsonl"] = "auto",
    goal: Optional[str] = None,
    hints: Optional[dict] = None,
    max_rows: Optional[int] = None,
    output_filename: str = "chart.png",
    output_format: Literal["png", "svg"] = "png",
) -> dict:
    """
    Load dataset, auto-detect schema, pick chart type, render to artifact.
    
    Returns dict with artifact_path, chart_spec, detected, warnings, sample.
    """
    # Validate inputs
    if (dataset_id is None) == (path is None):
        raise GuardError("Exactly one of dataset_id or path must be provided")
    
    check_filename_safe(output_filename)
    
    # Cap max_rows
    if max_rows is None:
        max_rows = get_max_rows()
    else:
        max_rows = min(max_rows, get_max_rows())
    
    # Load dataset
    if dataset_id:
        df = load_dataset(dataset_id)
    else:
        df = _load_dataset_from_path(path, input_format)
    
    # Cap rows
    if len(df) > max_rows:
        df = df.head(max_rows)
    
    # Detect schema
    detected = _detect_column_types(df)
    
    # Auto-pick chart
    chart_spec = _auto_pick_chart(detected, df, goal, hints)
    
    # Render
    artifact_dir = get_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    output_path = artifact_dir / output_filename
    
    warnings = []
    try:
        _render_chart(df, chart_spec, output_path, output_format)
    except Exception as e:
        warnings.append(f"Chart rendering error: {e}")
        # Create empty file as fallback
        output_path.touch()
    
    # Check output size
    if output_path.exists():
        output_size = output_path.stat().st_size
        max_bytes = get_max_output_bytes()
        if output_size > max_bytes:
            warnings.append(f"Output file size {output_size} exceeds limit {max_bytes}")
    
    # Get sample (capped)
    sample = df.head(10).to_dict("records")
    
    return {
        "artifact_path": str(output_path),
        "chart_spec": chart_spec,
        "detected": detected,
        "warnings": warnings,
        "sample": sample,
    }

