"""DuckDB local SQL operations."""

from pathlib import Path
from typing import Optional

import duckdb

from .guards import GuardError, cap_rows, get_max_rows
from .registry import get_dataset_path, dataset_info


def duckdb_query_local(
    sql: str,
    sources: Optional[list[dict]] = None,
    max_rows: Optional[int] = None
) -> dict:
    """
    Execute SQL query on local sources using DuckDB.

    Args:
        sql: SQL query string (SELECT/WITH only, read-only)
        sources: Optional list of source dicts. Each dict has:
            - For registry datasets: {"name": str, "dataset_id": str}
            - For files: {"name": str, "path": str, "format": "parquet"|"csv"|"json"|"jsonl"}
        max_rows: Optional maximum rows to return

    Returns:
        dict with keys: columns (list[str]), rows (list[list]), row_count (int)
    """
    if max_rows is None:
        max_rows = get_max_rows()

    conn = duckdb.connect()

    try:
        # Register sources
        if sources:
            # Ensure sources is a list and each source is a dict
            if not isinstance(sources, list):
                raise GuardError(f"sources must be a list, got {type(sources)}: {sources}")
            for source in sources:
                if not isinstance(source, dict):
                    raise GuardError(f"Each source must be a dict, got {type(source)}: {source}")
                if "name" not in source:
                    raise GuardError("Source missing required field 'name'")
                name = source["name"]

                if "dataset_id" in source:
                    # Load from registry
                    dataset_id = source["dataset_id"]
                    # Get path from index (which has the actual stored path)
                    path = None
                    try:
                        info = dataset_info(dataset_id)
                        path = Path(info["path"])
                    except Exception:
                        # Fallback: try to find dataset in common locations
                        # First try constructed path
                        path = get_dataset_path(dataset_id)
                        if not path.exists():
                            # Try in current directory .datasets/
                            local_path = Path.cwd() / ".datasets" / f"{dataset_id}.parquet"
                            if local_path.exists():
                                path = local_path
                            else:
                                # Try relative to this file's directory
                                this_file = Path(__file__).parent.parent.parent / ".datasets" / f"{dataset_id}.parquet"
                                if this_file.exists():
                                    path = this_file

                    if not path or not path.exists():
                        raise GuardError(f"Dataset {dataset_id} not found. Tried: {path}")
                    # Read parquet in this connection - use absolute path
                    abs_path = str(path.resolve())
                    # Drop view if it exists to avoid conflicts
                    try:
                        conn.execute(f"DROP VIEW IF EXISTS {name}")
                    except Exception:
                        pass  # Ignore if view doesn't exist
                    # Create view
                    view_sql = f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{abs_path}')"
                    conn.execute(view_sql)
                    # Verify view was created immediately
                    views_check = conn.execute("SHOW TABLES").fetchall()
                    view_names = [v[0] if isinstance(v, tuple) else v for v in views_check]
                    if name not in view_names:
                        raise GuardError(f"View {name} creation failed. Available views: {view_names}, SQL: {view_sql}")
                elif "path" in source and "format" in source:
                    # Load from file
                    path = source["path"]
                    fmt = source["format"].lower()

                    # Use CREATE VIEW to avoid connection issues
                    if fmt == "parquet":
                        conn.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{str(path)}')")
                    elif fmt == "csv":
                        conn.execute(f"CREATE VIEW {name} AS SELECT * FROM read_csv('{str(path)}')")
                    elif fmt in ["json", "jsonl"]:
                        conn.execute(f"CREATE VIEW {name} AS SELECT * FROM read_json('{str(path)}')")
                    else:
                        raise GuardError(f"Unsupported format: {fmt}")
                else:
                    raise GuardError("Source must have either 'dataset_id' or both 'path' and 'format'")

            # Execute query and get columns
            # Note: We don't pre-validate table names here because DuckDB has built-in functions
            # like generate_series() that might appear in FROM clauses. Let DuckDB handle errors.

        cursor = conn.execute(sql)

        # Get columns from cursor description (available before fetchall)
        # DuckDB description is a list of tuples: (name, type, ...)
        columns = []
        try:
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
        except Exception:
            pass

        # If columns still empty, try using df() method as fallback
        if not columns:
            try:
                df = cursor.df()
                columns = list(df.columns)
                result = [list(row) for row in df.itertuples(index=False)]
            except Exception:
                # Last resort: fetchall and return empty columns
                result = cursor.fetchall()
        else:
            result = cursor.fetchall()

        # Cap rows
        rows = cap_rows(result, max_rows)
        rows_list = [list(row) for row in rows]

        return {
            "columns": columns,
            "rows": rows_list,
            "row_count": len(rows_list),
        }
    except GuardError:
        raise  # Re-raise GuardError as-is
    except Exception as e:
        # Provide more context in error message
        error_msg = str(e)
        if "does not exist" in error_msg.lower() or "not found" in error_msg.lower():
            # Try to list available views/tables for better error message
            try:
                views = conn.execute("SHOW TABLES").fetchall()
                view_names = [v[0] if isinstance(v, tuple) else v for v in views] if views else []
                if view_names:
                    raise GuardError(
                        f"DuckDB query failed: {error_msg}. "
                        f"Available views/tables: {', '.join(view_names)}"
                    )
            except Exception:
                pass  # If we can't get views, just use original error
        raise GuardError(f"DuckDB query failed: {error_msg}")
    finally:
        conn.close()

