"""File format inspection and conversion."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .guards import GuardError, cap_rows, check_path_allowed


def parquet_inspect(path: str, sample_rows: int = 20) -> dict:
    """Inspect parquet file and return schema + sample rows."""
    resolved_path = check_path_allowed(path)
    
    try:
        parquet_file = pq.ParquetFile(str(resolved_path))
        schema = parquet_file.schema_arrow
        
        # Read sample rows
        df = pd.read_parquet(str(resolved_path))
        df = df.head(sample_rows)
        
        return {
            "path": str(resolved_path),
            "num_rows": parquet_file.metadata.num_rows,
            "num_row_groups": parquet_file.num_row_groups,
            "schema": {
                "fields": [
                    {
                        "name": field.name,
                        "type": str(field.type),
                        "nullable": field.nullable,
                    }
                    for field in schema
                ]
            },
            "sample_rows": cap_rows(df.to_dict("records"), sample_rows),
        }
    except Exception as e:
        raise GuardError(f"Parquet inspect failed: {e}")


def arrow_convert(
    input_path: str,
    input_format: str,
    output_format: str,
    output_filename: str
) -> dict:
    """Convert between arrow-supported formats."""
    from .evidence import get_artifact_dir
    from .guards import check_filename_safe
    
    check_path_allowed(input_path)
    check_filename_safe(output_filename)
    
    artifact_dir = get_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    output_path = artifact_dir / output_filename
    
    try:
        # Read input
        if input_format.upper() == "PARQUET":
            table = pa.parquet.read_table(input_path)
        elif input_format.upper() == "CSV":
            import pyarrow.csv as csv
            table = csv.read_csv(input_path)
        elif input_format.upper() == "JSON":
            import pyarrow.json as json
            table = json.read_json(input_path)
        else:
            raise GuardError(f"Unsupported input format: {input_format}")
        
        # Write output
        if output_format.upper() == "PARQUET":
            pa.parquet.write_table(table, str(output_path))
        elif output_format.upper() == "CSV":
            import pyarrow.csv as csv
            csv.write_csv(table, str(output_path))
        elif output_format.upper() == "JSON":
            import pyarrow.json as json
            json.write_json(table, str(output_path))
        else:
            raise GuardError(f"Unsupported output format: {output_format}")
        
        return {
            "input_path": input_path,
            "input_format": input_format,
            "output_path": str(output_path),
            "output_format": output_format,
            "rows": len(table),
        }
    except Exception as e:
        raise GuardError(f"Arrow convert failed: {e}")

