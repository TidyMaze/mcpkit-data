"""File format and evidence tools for MCP server."""

from typing import Annotated, Optional

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import evidence, formats
from mcpkit.server_models import (
    ArrowConvertResponse,
    EvidenceBundleResponse,
    ParquetField,
    ParquetInspectResponse,
    ParquetSchema,
    ReconcileResponse,
)
from mcpkit.server_utils import _to_int


def register_formats_tools(mcp: FastMCP):
    """Register file format and evidence tools with MCP server."""
    
    @mcp.tool()
    def parquet_inspect(
        path: Annotated[str, Field(description="Path to parquet file (must be within allowed roots)")],
        sample_rows: Annotated[int, Field(description="Number of sample rows to return. Default: 20")] = 20,
    ) -> ParquetInspectResponse:
        """Inspect parquet file and return schema + sample rows."""
        sample_rows = _to_int(sample_rows, 20)
        result = formats.parquet_inspect(path, sample_rows)
        schema = ParquetSchema(fields=[ParquetField(**f) for f in result["schema"]["fields"]])
        return ParquetInspectResponse(
            path=result["path"],
            num_rows=result["num_rows"],
            row_count=result.get("row_count", result["num_rows"]),  # Alias for compatibility
            num_row_groups=result["num_row_groups"],
            schema=schema,
            sample_rows=result["sample_rows"]
        )

    @mcp.tool()
    def arrow_convert(
        input_path: Annotated[str, Field(description="Input file path (must be within allowed roots)")],
        input_format: Annotated[str, Field(description="Input format: \"PARQUET\", \"CSV\", \"JSON\"")],
        output_format: Annotated[str, Field(description="Output format: \"PARQUET\", \"CSV\", \"JSON\"")],
        output_filename: Annotated[str, Field(description="Output filename (must be safe, no slashes)")],
    ) -> ArrowConvertResponse:
        """Convert between arrow-supported formats."""
        result = formats.arrow_convert(input_path, input_format, output_format, output_filename)
        return ArrowConvertResponse(**result)

    @mcp.tool()
    def reconcile_counts(
        left_dataset_id: Annotated[str, Field(description="First dataset ID")],
        right_dataset_id: Annotated[str, Field(description="Second dataset ID")],
        key_cols: Annotated[list[str], Field(description="List of column names to use as keys")],
        out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID for reconciliation results")] = None,
        max_examples: Annotated[int, Field(description="Maximum examples to return. Default: 200")] = 200,
    ) -> ReconcileResponse:
        """Reconcile record counts between two datasets."""
        max_examples = _to_int(max_examples, 200)
        result = evidence.reconcile_counts(left_dataset_id, right_dataset_id, key_cols, out_dataset_id, max_examples)
        return ReconcileResponse(**result)

    @mcp.tool()
    def evidence_bundle_plus(
        dataset_id: Annotated[str, Field(description="Dataset ID")],
        base_filename: Annotated[str, Field(description="Base filename for exports (must be safe, no slashes)")],
        include_describe: Annotated[bool, Field(description="Include statistical description (default: True)")] = True,
        include_sample_rows: Annotated[int, Field(description="Number of sample rows to include. Default: 50")] = 50,
    ) -> EvidenceBundleResponse:
        """Generate evidence bundle: dataset info, describe, sample rows, exported files."""
        include_sample_rows = _to_int(include_sample_rows, 50)
        result = evidence.evidence_bundle_plus(dataset_id, base_filename, include_describe, include_sample_rows)
        return EvidenceBundleResponse(**result)

