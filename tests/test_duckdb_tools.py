"""Tests for DuckDB tools."""

import json
import pytest

from mcpkit.core.duckdb_ops import duckdb_query_local
from mcpkit.core.guards import GuardError


def test_duckdb_query_local_simple():
    """Test simple DuckDB query."""
    result = duckdb_query_local("SELECT 1 as a, 2 as b")
    assert "columns" in result
    assert "rows" in result
    assert "row_count" in result
    assert result["columns"] == ["a", "b"]
    assert result["rows"][0] == [1, 2]
    assert result["row_count"] == 1


def test_duckdb_query_local_with_dataset(clean_registry):
    """Test DuckDB query with dataset source."""
    from mcpkit.core.registry import dataset_put_rows

    dataset_put_rows(columns=["a", "b"], rows=[[1, 2], [3, 4]], dataset_id="ds1")

    result = duckdb_query_local(
        "SELECT * FROM ds1",
        sources=[{"name": "ds1", "dataset_id": "ds1"}],
    )
    assert len(result["rows"]) == 2
    assert result["columns"] == ["a", "b"]
    assert result["row_count"] == 2


def test_duckdb_query_local_max_rows():
    """Test DuckDB query with max_rows limit."""
    # Create query that returns many rows
    result = duckdb_query_local(
        "SELECT * FROM generate_series(1, 1000) as x",
        max_rows=10,
    )
    assert len(result["rows"]) <= 10
    assert result["row_count"] <= 10


def test_duckdb_query_local_none_sources():
    """Test DuckDB query with None sources."""
    result = duckdb_query_local(
        "SELECT 1 as a, 'hello' as b",
        sources=None,
    )
    assert "columns" in result
    assert result["columns"] == ["a", "b"]
    assert result["rows"][0] == [1, "hello"]


def test_duckdb_query_local_empty_sources():
    """Test DuckDB query with empty sources list."""
    result = duckdb_query_local(
        "SELECT 2 as x, 3 as y",
        sources=[],
    )
    assert "columns" in result
    assert result["columns"] == ["x", "y"]
    assert result["rows"][0] == [2, 3]


def test_duckdb_query_local_with_file_source(clean_registry, tmp_path):
    """Test DuckDB query with CSV file source (simple)."""
    import pandas as pd

    # Create a CSV file
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    csv_file = tmp_path / "test.csv"
    df.to_csv(csv_file, index=False)

    result = duckdb_query_local(
        "SELECT * FROM test_csv",
        sources=[{"name": "test_csv", "path": str(csv_file), "format": "csv"}],
    )
    assert len(result["rows"]) == 3
    assert result["columns"] == ["id", "value"]


def test_duckdb_query_local_complex_query():
    """Test DuckDB query with complex SQL."""
    # Use VALUES to create a simple table for testing
    result = duckdb_query_local(
        """
        SELECT
            x,
            x * 2 as doubled,
            CASE WHEN x > 5 THEN 'high' ELSE 'low' END as category
        FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS t(x)
        WHERE MOD(x, 2) = 0
        ORDER BY x
        """,
        max_rows=5,
    )
    assert "columns" in result
    assert len(result["rows"]) == 5
    assert result["rows"][0][0] == 2  # First even number
    assert result["rows"][0][2] == "low"  # Category for 2
    assert result["rows"][-1][2] == "high"  # Category for 10


def test_duckdb_query_local_multiple_sources(clean_registry):
    """Test DuckDB query with multiple dataset sources."""
    from mcpkit.core.registry import dataset_put_rows

    dataset_put_rows(columns=["id", "name"], rows=[[1, "a"], [2, "b"]], dataset_id="ds1")
    dataset_put_rows(columns=["id", "value"], rows=[[1, 10], [2, 20]], dataset_id="ds2")

    result = duckdb_query_local(
        "SELECT a.id, a.name, b.value FROM ds1 a JOIN ds2 b ON a.id = b.id",
        sources=[
            {"name": "ds1", "dataset_id": "ds1"},
            {"name": "ds2", "dataset_id": "ds2"},
        ],
    )
    assert len(result["rows"]) == 2
    assert result["columns"] == ["id", "name", "value"]


def test_duckdb_query_local_aggregation(clean_registry):
    """Test DuckDB query with aggregation."""
    from mcpkit.core.registry import dataset_put_rows

    dataset_put_rows(
        columns=["category", "value"],
        rows=[["A", 10], ["A", 20], ["B", 30], ["B", 40]],
        dataset_id="agg_test",
    )

    result = duckdb_query_local(
        "SELECT category, SUM(value) as total FROM agg_test GROUP BY category",
        sources=[{"name": "agg_test", "dataset_id": "agg_test"}],
    )
    assert len(result["rows"]) == 2
    assert result["columns"] == ["category", "total"]


def test_duckdb_query_local_missing_table_error():
    """Test DuckDB query error when table doesn't exist."""
    with pytest.raises(GuardError, match="does not exist"):
        duckdb_query_local("SELECT * FROM nonexistent_table")


def test_duckdb_query_local_invalid_sources_type():
    """Test DuckDB query with invalid sources type."""
    with pytest.raises(GuardError, match="must be a list"):
        duckdb_query_local("SELECT 1", sources="not a list")


def test_duckdb_query_local_invalid_source_missing_name():
    """Test DuckDB query with source missing name field."""
    with pytest.raises(GuardError, match="missing required field 'name'"):
        duckdb_query_local("SELECT 1", sources=[{"dataset_id": "test"}])


def test_duckdb_query_local_invalid_source_missing_fields():
    """Test DuckDB query with source missing required fields."""
    with pytest.raises(GuardError, match="must have either 'dataset_id' or both 'path' and 'format'"):
        duckdb_query_local("SELECT 1", sources=[{"name": "test"}])


def test_duckdb_query_local_nonexistent_dataset(clean_registry):
    """Test DuckDB query with nonexistent dataset_id."""
    with pytest.raises(GuardError, match="not found"):
        duckdb_query_local(
            "SELECT * FROM test",
            sources=[{"name": "test", "dataset_id": "nonexistent"}],
        )


def test_duckdb_query_local_parquet_file(tmp_path):
    """Test DuckDB query with parquet file source."""
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file, index=False)

    result = duckdb_query_local(
        "SELECT * FROM test_parquet WHERE x > 1",
        sources=[{"name": "test_parquet", "path": str(parquet_file), "format": "parquet"}],
    )
    assert len(result["rows"]) == 2
    assert result["columns"] == ["x", "y"]


def test_duckdb_query_local_json_file(tmp_path):
    """Test DuckDB query with JSON file source (simple)."""
    import json

    data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    json_file = tmp_path / "test.json"
    with open(json_file, "w") as f:
        json.dump(data, f)

    result = duckdb_query_local(
        "SELECT * FROM test_json",
        sources=[{"name": "test_json", "path": str(json_file), "format": "json"}],
    )
    assert len(result["rows"]) == 2
    assert "id" in result["columns"]
    assert "name" in result["columns"]


def test_duckdb_query_local_json_file_complex(tmp_path):
    """Test DuckDB query with complex JSON file (multiple types, nested data)."""
    import json

    data = [
        {
            "id": 1,
            "name": "Product A",
            "price": 99.99,
            "in_stock": True,
            "tags": ["electronics", "gadget"],
            "metadata": {"warranty": "1 year", "rating": 4.5}
        },
        {
            "id": 2,
            "name": "Product B",
            "price": 149.99,
            "in_stock": False,
            "tags": ["electronics", "premium"],
            "metadata": {"warranty": "2 years", "rating": 4.8}
        },
        {
            "id": 3,
            "name": "Product C",
            "price": 29.99,
            "in_stock": True,
            "tags": ["clothing"],
            "metadata": {"size": "M", "color": "blue"}
        }
    ]
    json_file = tmp_path / "complex.json"
    with open(json_file, "w") as f:
        json.dump(data, f)

    result = duckdb_query_local(
        "SELECT id, name, price, in_stock FROM complex_json WHERE price > 50 ORDER BY price DESC",
        sources=[{"name": "complex_json", "path": str(json_file), "format": "json"}],
    )
    assert len(result["rows"]) == 2
    assert result["columns"] == ["id", "name", "price", "in_stock"]
    assert result["rows"][0][2] == 149.99  # Highest price first


def test_duckdb_query_local_jsonl_file(tmp_path):
    """Test DuckDB query with JSONL file source (simple)."""
    data_lines = [
        '{"id": 1, "name": "a", "value": 10}\n',
        '{"id": 2, "name": "b", "value": 20}\n',
        '{"id": 3, "name": "c", "value": 30}\n'
    ]
    jsonl_file = tmp_path / "test.jsonl"
    with open(jsonl_file, "w") as f:
        f.writelines(data_lines)

    result = duckdb_query_local(
        "SELECT * FROM test_jsonl WHERE value > 15 ORDER BY value",
        sources=[{"name": "test_jsonl", "path": str(jsonl_file), "format": "jsonl"}],
    )
    assert len(result["rows"]) == 2
    assert "id" in result["columns"]
    assert "name" in result["columns"]
    assert "value" in result["columns"]
    assert result["rows"][0][2] == 20  # First value > 15


def test_duckdb_query_local_jsonl_file_complex(tmp_path):
    """Test DuckDB query with complex JSONL file (realistic event data)."""
    import json

    events = [
        {
            "event_id": "evt_001",
            "timestamp": "2024-01-01T10:00:00Z",
            "event_type": "purchase",
            "user_id": "user_123",
            "amount": 99.99,
            "currency": "USD",
            "items": [{"product_id": "prod_1", "quantity": 2}],
            "metadata": {"source": "web", "campaign": "winter_sale"}
        },
        {
            "event_id": "evt_002",
            "timestamp": "2024-01-01T11:00:00Z",
            "event_type": "view",
            "user_id": "user_456",
            "amount": None,
            "currency": None,
            "items": [{"product_id": "prod_2", "quantity": 1}],
            "metadata": {"source": "mobile", "campaign": None}
        },
        {
            "event_id": "evt_003",
            "timestamp": "2024-01-01T12:00:00Z",
            "event_type": "purchase",
            "user_id": "user_789",
            "amount": 149.99,
            "currency": "EUR",
            "items": [{"product_id": "prod_3", "quantity": 1}],
            "metadata": {"source": "web", "campaign": "winter_sale"}
        }
    ]
    jsonl_file = tmp_path / "events.jsonl"
    with open(jsonl_file, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    result = duckdb_query_local(
        """
        SELECT 
            event_type,
            COUNT(*) as event_count,
            SUM(amount) as total_amount,
            COUNT(DISTINCT user_id) as unique_users
        FROM events_jsonl
        WHERE event_type = 'purchase'
        GROUP BY event_type
        """,
        sources=[{"name": "events_jsonl", "path": str(jsonl_file), "format": "jsonl"}],
    )
    assert len(result["rows"]) == 1
    assert result["columns"] == ["event_type", "event_count", "total_amount", "unique_users"]
    assert result["rows"][0][1] == 2  # 2 purchase events
    assert abs(result["rows"][0][2] - 249.98) < 0.01  # Total amount (floating point tolerance)


def test_duckdb_query_local_csv_file_complex(tmp_path):
    """Test DuckDB query with complex CSV file (multiple types, realistic data)."""
    import pandas as pd

    df = pd.DataFrame({
        "order_id": ["ORD001", "ORD002", "ORD003", "ORD004"],
        "customer_id": [1001, 1002, 1001, 1003],
        "product": ["Widget A", "Widget B", "Widget A", "Widget C"],
        "quantity": [2, 1, 3, 1],
        "unit_price": [29.99, 49.99, 29.99, 79.99],
        "order_date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        "status": ["shipped", "pending", "shipped", "cancelled"]
    })
    csv_file = tmp_path / "orders.csv"
    df.to_csv(csv_file, index=False)

    result = duckdb_query_local(
        """
        SELECT 
            customer_id,
            COUNT(*) as order_count,
            SUM(quantity * unit_price) as total_spent,
            COUNT(DISTINCT product) as unique_products
        FROM orders_csv
        WHERE status != 'cancelled'
        GROUP BY customer_id
        ORDER BY total_spent DESC
        """,
        sources=[{"name": "orders_csv", "path": str(csv_file), "format": "csv"}],
    )
    # Customer 1001: 2 orders (ORD001 shipped, ORD003 shipped) = 2*29.99 + 3*29.99 = 149.95
    # Customer 1002: 1 order (ORD002 pending) = 1*49.99 = 49.99
    # Customer 1003: 1 order (ORD004 cancelled) = filtered out
    assert len(result["rows"]) == 2  # Only 2 customers (1003 filtered out)
    assert result["columns"] == ["customer_id", "order_count", "total_spent", "unique_products"]
    assert result["rows"][0][0] == 1001  # Customer with highest spend


def test_duckdb_query_local_parquet_file_complex(tmp_path):
    """Test DuckDB query with complex Parquet file (multiple types, realistic schema)."""
    import pandas as pd

    df = pd.DataFrame({
        "transaction_id": [1, 2, 3, 4, 5],
        "account_id": [101, 102, 101, 103, 102],
        "transaction_type": ["deposit", "withdrawal", "deposit", "deposit", "withdrawal"],
        "amount": [1000.50, -250.75, 500.00, 750.25, -100.00],
        "balance_after": [1000.50, 749.25, 1249.25, 1999.50, 649.25],
        "timestamp": pd.to_datetime([
            "2024-01-01 10:00:00",
            "2024-01-01 11:00:00",
            "2024-01-01 12:00:00",
            "2024-01-01 13:00:00",
            "2024-01-01 14:00:00"
        ]),
        "is_fraud": [False, False, False, True, False]
    })
    parquet_file = tmp_path / "transactions.parquet"
    df.to_parquet(parquet_file, index=False)

    result = duckdb_query_local(
        """
        SELECT 
            account_id,
            COUNT(*) as transaction_count,
            SUM(CASE WHEN transaction_type = 'deposit' THEN amount ELSE 0 END) as total_deposits,
            SUM(CASE WHEN transaction_type = 'withdrawal' THEN amount ELSE 0 END) as total_withdrawals,
            SUM(amount) as net_amount,
            COUNT(CASE WHEN is_fraud THEN 1 END) as fraud_count
        FROM transactions_parquet
        GROUP BY account_id
        ORDER BY account_id
        """,
        sources=[{"name": "transactions_parquet", "path": str(parquet_file), "format": "parquet"}],
    )
    assert len(result["rows"]) == 3
    assert result["columns"] == [
        "account_id", "transaction_count", "total_deposits",
        "total_withdrawals", "net_amount", "fraud_count"
    ]
    # Account 101: 2 deposits, no withdrawals
    assert result["rows"][0][0] == 101
    assert result["rows"][0][2] == 1500.50  # Total deposits
    assert result["rows"][0][3] == 0.0  # No withdrawals


def test_duckdb_query_local_with_clause():
    """Test DuckDB query with WITH clause."""
    result = duckdb_query_local(
        """
        WITH numbers AS (
            SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)
        )
        SELECT x, x * 2 as doubled FROM numbers WHERE x > 2
        """
    )
    assert len(result["rows"]) == 3
    assert result["columns"] == ["x", "doubled"]


def test_duckdb_query_local_order_by():
    """Test DuckDB query with ORDER BY."""
    result = duckdb_query_local(
        """
        SELECT * FROM (VALUES (3), (1), (2)) AS t(x)
        ORDER BY x DESC
        """
    )
    assert result["rows"][0][0] == 3
    assert result["rows"][-1][0] == 1


def test_duckdb_query_local_response_structure():
    """Test that response has correct structure."""
    result = duckdb_query_local("SELECT 1 as a")

    assert isinstance(result, dict)
    assert "columns" in result
    assert "rows" in result
    assert "row_count" in result
    assert isinstance(result["columns"], list)
    assert isinstance(result["rows"], list)
    assert isinstance(result["row_count"], int)
    assert result["row_count"] == len(result["rows"])

