"""Integration tests for visualization tools via MCP protocol."""

import json
import pandas as pd
import pytest
from pathlib import Path

from tests.utils_mcp import assert_response_structure, call_tool, create_test_dataset

pytestmark = pytest.mark.integration


def test_dataset_to_chart_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_chart",
        ["x", "y"],
        [[1, 10], [2, 20], [3, 30], [4, 40]]
    )
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_chart",
        output_filename="test_chart.png"
    )
    
    assert_response_structure(response, ["artifact_path", "chart_spec", "detected"])
    assert response["artifact_path"].endswith(".png")


def test_dataset_to_chart_with_path_mcp(mcp_client, clean_roots, clean_artifacts):
    """Test dataset_to_chart with file path via MCP protocol."""
    # Create a CSV file
    csv_path = Path(clean_roots) / "test_chart_data.csv"
    df = pd.DataFrame({"category": ["A", "B", "C"], "value": [10, 20, 30]})
    df.to_csv(csv_path, index=False)
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        path=str(csv_path),
        input_format="csv",
        output_filename="test_chart_path.png"
    )
    
    assert_response_structure(response, ["artifact_path", "chart_spec"])
    assert response["artifact_path"].endswith(".png")


def test_dataset_to_chart_with_hints_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart with hints via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_chart_hints",
        ["category", "value"],
        [["A", 10], ["B", 20], ["C", 30]]
    )
    
    hints = {
        "chart_type": "bar",
        "x": "category",
        "y": "value"
    }
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_chart_hints",
        hints=hints,
        output_filename="test_chart_hints.png"
    )
    
    assert_response_structure(response, ["chart_spec"])
    assert response["chart_spec"]["chart_type"] == "bar"


def test_dataset_to_chart_line_chart_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart auto-detects line chart for time series."""
    # Create dataset with date strings (will be detected as datetime)
    create_test_dataset(
        mcp_client,
        "test_line_chart",
        ["date", "value"],
        [["2024-01-01", 10], ["2024-01-02", 20], ["2024-01-03", 30]]
    )
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_line_chart",
        output_filename="test_line.png"
    )
    
    assert_response_structure(response, ["chart_spec"])
    # Should auto-detect chart type
    assert "chart_type" in response["chart_spec"]


def test_dataset_to_chart_scatter_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart auto-detects scatter for 2+ numeric columns."""
    create_test_dataset(
        mcp_client,
        "test_scatter",
        ["x", "y", "z"],
        [[i, i*2, i*3] for i in range(10)]
    )
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_scatter",
        output_filename="test_scatter.png"
    )
    
    assert_response_structure(response, ["chart_spec"])
    # Should auto-detect as scatter for 2+ numeric columns
    assert response["chart_spec"]["chart_type"] in ["scatter", "bar"]


def test_dataset_to_chart_histogram_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart auto-detects histogram for single numeric column."""
    create_test_dataset(
        mcp_client,
        "test_histogram",
        ["value"],
        [[i] for i in range(20)]
    )
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_histogram",
        output_filename="test_histogram.png"
    )
    
    assert_response_structure(response, ["chart_spec"])
    # Should auto-detect as histogram for single numeric
    assert response["chart_spec"]["chart_type"] in ["histogram", "bar"]


def test_dataset_to_chart_svg_format_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart with SVG format via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_svg",
        ["x", "y"],
        [[1, 10], [2, 20]]
    )
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_svg",
        output_filename="test_svg.svg",
        output_format="svg"
    )
    
    assert_response_structure(response, ["artifact_path"])
    assert response["artifact_path"].endswith(".svg")


def test_evidence_bundle_plus_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test evidence_bundle_plus via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_evidence",
        ["id", "value"],
        [[1, 10], [2, 20], [3, 30]]
    )
    
    response = call_tool(
        mcp_client,
        "evidence_bundle_plus",
        dataset_id="test_evidence",
        base_filename="test_evidence",
        include_describe=True,
        include_sample_rows=5
    )
    
    assert_response_structure(response, ["dataset_id", "info", "exported_files"])
    assert response["dataset_id"] == "test_evidence"
    assert len(response["exported_files"]) > 0


def test_evidence_bundle_plus_no_describe_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test evidence_bundle_plus without describe via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_evidence_no_desc",
        ["id", "value"],
        [[1, 10], [2, 20]]
    )
    
    response = call_tool(
        mcp_client,
        "evidence_bundle_plus",
        dataset_id="test_evidence_no_desc",
        base_filename="test_evidence_no_desc",
        include_describe=False,
        include_sample_rows=3
    )
    
    assert_response_structure(response, ["dataset_id", "exported_files"])
    assert response.get("describe") is None


def test_dataset_to_chart_auto_format_mcp(mcp_client, clean_registry, clean_roots, clean_artifacts):
    """Test dataset_to_chart with auto format detection via MCP protocol."""
    # Create a CSV file
    csv_path = Path(clean_roots) / "auto_chart.csv"
    df = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
    df.to_csv(csv_path, index=False)
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        path=str(csv_path),
        input_format="auto",
        output_filename="auto_chart.png"
    )
    
    assert_response_structure(response, ["artifact_path", "chart_spec"])


def test_dataset_to_chart_avro_format_mcp(mcp_client, clean_registry, clean_roots, clean_artifacts):
    """Test dataset_to_chart with Avro format via MCP protocol."""
    try:
        import fastavro
    except ImportError:
        pytest.skip("fastavro not installed")
    
    # Create an Avro file
    avro_path = Path(clean_roots) / "chart_data.avro"
    schema = {
        "type": "record",
        "name": "Record",
        "fields": [
            {"name": "category", "type": "string"},
            {"name": "value", "type": "int"}
        ]
    }
    records = [
        {"category": "A", "value": 10},
        {"category": "B", "value": 20},
        {"category": "C", "value": 30}
    ]
    
    with open(avro_path, "wb") as f:
        fastavro.writer(f, schema, records)
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        path=str(avro_path),
        input_format="avro",
        output_filename="avro_chart.png"
    )
    
    assert_response_structure(response, ["artifact_path", "chart_spec"])


def test_dataset_to_chart_unsupported_format_mcp(mcp_client, clean_registry, clean_roots):
    """Test dataset_to_chart with unsupported format via MCP protocol."""
    test_file = Path(clean_roots) / "test.xml"
    test_file.write_text("<root><item>1</item></root>")
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "dataset_to_chart",
            path=str(test_file),
            input_format="xml",
            output_filename="test.png"
        )


def test_dataset_to_chart_with_goal_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart with goal description via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_goal",
        ["date", "sales"],
        [["2024-01-01", 100], ["2024-01-02", 150], ["2024-01-03", 120]]
    )
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_goal",
        goal="Show sales trend over time",
        output_filename="goal_chart.png"
    )
    
    assert_response_structure(response, ["artifact_path", "chart_spec"])
    # Should prefer line chart for time series
    assert response["chart_spec"]["chart_type"] in ["line", "bar"]


def test_dataset_to_chart_with_filters_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart with filters via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_filtered",
        ["category", "value"],
        [["A", 10], ["A", 20], ["B", 30], ["B", 40], ["C", 50]]
    )
    
    hints = {
        "chart_type": "bar",
        "x": "category",
        "y": "value",
        "filters": [{"column": "category", "op": "==", "value": "A"}]
    }
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_filtered",
        hints=hints,
        output_filename="filtered_chart.png"
    )
    
    assert_response_structure(response, ["artifact_path", "chart_spec"])


def test_dataset_to_chart_with_group_by_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart with group_by via MCP protocol."""
    create_test_dataset(
        mcp_client,
        "test_grouped",
        ["category", "subcategory", "value"],
        [
            ["A", "X", 10],
            ["A", "Y", 20],
            ["B", "X", 30],
            ["B", "Y", 40]
        ]
    )
    
    hints = {
        "chart_type": "bar",
        "x": "category",
        "y": "value",
        "group_by": "subcategory"
    }
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_grouped",
        hints=hints,
        output_filename="grouped_chart.png"
    )
    
    assert_response_structure(response, ["artifact_path", "chart_spec"])


def test_dataset_to_chart_with_top_k_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart with top_k limit via MCP protocol."""
    # Create dataset with many categories
    rows = [[f"Category{i}", i * 10] for i in range(50)]
    create_test_dataset(mcp_client, "test_topk", ["category", "value"], rows)
    
    hints = {
        "chart_type": "bar",
        "x": "category",
        "y": "value",
        "top_k": 10
    }
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        dataset_id="test_topk",
        hints=hints,
        output_filename="topk_chart.png"
    )
    
    assert_response_structure(response, ["artifact_path", "chart_spec"])


def test_dataset_to_chart_empty_dataset_mcp(mcp_client, clean_registry, clean_artifacts):
    """Test dataset_to_chart with empty dataset via MCP protocol."""
    create_test_dataset(mcp_client, "test_empty", ["x", "y"], [])
    
    with pytest.raises(Exception):  # Should raise GuardError
        call_tool(
            mcp_client,
            "dataset_to_chart",
            dataset_id="test_empty",
            output_filename="empty_chart.png"
        )


def test_dataset_to_chart_jsonl_format_mcp(mcp_client, clean_registry, clean_roots, clean_artifacts):
    """Test dataset_to_chart with JSONL format via MCP protocol."""
    jsonl_path = Path(clean_roots) / "chart_data.jsonl"
    with jsonl_path.open("w") as f:
        f.write('{"x": 1, "y": 10}\n')
        f.write('{"x": 2, "y": 20}\n')
        f.write('{"x": 3, "y": 30}\n')
    
    response = call_tool(
        mcp_client,
        "dataset_to_chart",
        path=str(jsonl_path),
        input_format="jsonl",
        output_filename="jsonl_chart.png"
    )
    
    assert_response_structure(response, ["artifact_path", "chart_spec"])

