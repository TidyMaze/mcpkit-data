"""DuckDB local SQL operations."""

from typing import Optional

import duckdb

from .guards import GuardError, cap_rows, get_max_rows
from .registry import get_dataset_path


def duckdb_query_local(
    sql: str,
    sources: Optional[list[dict]] = None,
    max_rows: Optional[int] = None
) -> dict:
    """
    Execute SQL query on local sources.
    sources: list of {name, dataset_id} OR {name, path, format}
    formats: parquet, csv, json, jsonl
    """
    if max_rows is None:
        max_rows = get_max_rows()
    
    conn = duckdb.connect()
    
    try:
        # Register sources
        if sources:
            # Ensure sources is a list and each source is a dict
            if not isinstance(sources, list):
                raise GuardError(f"sources must be a list, got {type(sources)}")
            for source in sources:
                if not isinstance(source, dict):
                    raise GuardError(f"Each source must be a dict, got {type(source)}: {source}")
                name = source["name"]
                
                if "dataset_id" in source:
                    # Load from registry
                    dataset_id = source["dataset_id"]
                    path = get_dataset_path(dataset_id)
                    if not path.exists():
                        raise GuardError(f"Dataset {dataset_id} not found")
                    # Read parquet in this connection
                    conn.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{str(path)}')")
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
                    raise GuardError("Source must have either 'dataset_id' or 'path' and 'format'")
        
        # Execute query and get columns
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
    except Exception as e:
        raise GuardError(f"DuckDB query failed: {e}")
    finally:
        conn.close()

