"""Great Expectations tools for MCP server."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.server_models import GEExpectationResult, GEValidationResponse


def register_ge_tools(mcp: FastMCP):
    """Register Great Expectations tools with MCP server."""
    
    @mcp.tool()
    def great_expectations_check(
        dataset_id: Annotated[str, Field(description="Dataset ID")],
        expectation_suite: Annotated[dict, Field(description="Great Expectations expectation suite dict")],
    ) -> GEValidationResponse:
        """Run Great Expectations validation suite on dataset."""
        try:
            import great_expectations as ge
        except ImportError:
            raise RuntimeError("Great Expectations not installed. Install with: pip install great-expectations")

        from mcpkit.core.registry import load_dataset

        df = load_dataset(dataset_id)
        context = ge.get_context()

        try:
            # Use Fluent API with pandas datasource - create fresh for each validation
            datasource_name = f"pandas_{dataset_id}"

            # Remove existing datasource if it exists
            try:
                existing_ds = context.data_sources.get(datasource_name)
                context.data_sources.delete(datasource_name)
            except (KeyError, ValueError, AttributeError):
                pass

            # Create new datasource and asset
            pandas_ds = context.data_sources.add_pandas(name=datasource_name)
            asset = pandas_ds.add_dataframe_asset(name="dataframe_asset")

            # Build batch request with dataframe
            batch_request = asset.build_batch_request(options={"dataframe": df})

            # Create expectation suite directly
            from great_expectations.core import ExpectationSuite
            suite_name = f"suite_{dataset_id}"
            suite = ExpectationSuite(name=suite_name)

            # Add expectations from suite dict
            from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
            expectations = expectation_suite.get("expectations", [])
            for exp in expectations:
                exp_config = ExpectationConfiguration(
                    type=exp["expectation_type"],
                    kwargs=exp.get("kwargs", {})
                )
                suite.add_expectation_configuration(exp_config)

            # Get validator and validate
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite=suite
            )

            validation_result = validator.validate()

            # Build result object - convert numpy types to native Python types
            def convert_to_native(obj):
                """Convert numpy types to native Python types for serialization."""
                import numpy as np
                if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
                    return int(obj)
                elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
                    return float(obj)
                elif isinstance(obj, np.bool_):
                    return bool(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, dict):
                    return {k: convert_to_native(v) for k, v in obj.items()}
                elif isinstance(obj, (list, tuple)):
                    return [convert_to_native(item) for item in obj]
                elif hasattr(obj, 'item'):  # numpy scalar
                    return obj.item()
                return obj

            results = []
            for r in validation_result.results:
                # Convert result dict
                result_dict = convert_to_native(dict(r.result)) if r.result else {}
                results.append(
                    GEExpectationResult(
                        expectation_type=str(r.expectation_config.type),
                        success=bool(r.success),
                        result=result_dict,
                    )
                )

            # Convert statistics
            stats = convert_to_native(dict(validation_result.statistics))

            return GEValidationResponse(
                dataset_id=dataset_id,
                success=bool(validation_result.success),
                statistics=stats,
                results=results
            )
        except Exception as e:
            raise RuntimeError(f"Great Expectations validation failed: {e}")

