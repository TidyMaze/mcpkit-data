# MCP Protocol Integration Tests

This directory contains integration tests for all mcpkit-data tools that test via the MCP protocol (blackbox testing).

## Overview

All 67 tools are tested via MCP protocol to ensure:
- Tools are properly registered in FastMCP
- Parameter validation works correctly
- Response models serialize properly
- End-to-end functionality works as expected

## Test Structure

Tests are organized by tool category:

- `test_registry_mcp.py` - Dataset registry tools (4 tools)
- `test_kafka_mcp.py` - Kafka tools (8 tools)
- `test_schema_registry_mcp.py` - Schema Registry tools (4 tools)
- `test_db_mcp.py` - Database tools (2 tools)
- `test_aws_mcp.py` - AWS tools (9 tools)
- `test_pandas_mcp.py` - Pandas tools (12 tools)
- `test_polars_mcp.py` - Polars tools (3 tools)
- `test_duckdb_mcp.py` - DuckDB tools (1 tool)
- `test_json_mcp.py` - JSON/QA tools (5 tools)
- `test_formats_mcp.py` - File format tools (4 tools)
- `test_chart_mcp.py` - Visualization tools (2 tools)
- `test_infrastructure_mcp.py` - Infrastructure tools (8 tools)
- `test_http_mcp.py` - HTTP tools (1 tool)
- `test_dataset_ops_mcp.py` - Dataset operations (1 tool)
- `test_reconcile_mcp.py` - Reconciliation tools (1 tool)
- `test_ge_mcp.py` - Great Expectations (1 tool)

## Running Tests

### All Integration Tests

```bash
pytest tests/integration/ -v -m integration
```

### Specific Test File

```bash
pytest tests/integration/test_registry_mcp.py -v
```

### With Coverage

```bash
pytest tests/integration/ --cov=mcpkit --cov-report=html -v
```

## Test Infrastructure

- **`conftest_mcp.py`**: MCP client fixture and test setup
- **`utils_mcp.py`**: Helper functions for MCP protocol testing

## Test Client

The tests use a **true blackbox MCP test client** (`mcp_client` fixture) that:

### Full MCP Protocol Testing

- **Starts a real MCP server process** via stdio (subprocess)
- **Uses the official MCP Python SDK** (`mcp` package) to connect
- **Tests full protocol compliance**:
  - Protocol-level serialization/deserialization
  - MCP handshake and session initialization
  - Tool calls through the MCP protocol
  - Response parsing from MCP ContentBlock format
- **Validates end-to-end**:
  - Parameter validation through FastMCP
  - Response models serialize correctly
  - Tools are properly registered and callable
  - Protocol error handling works

### Implementation

The `mcp_client` fixture:
1. Creates `StdioServerParameters` with server command/args
2. Uses `stdio_client()` context manager to start server process
3. Creates `ClientSession` connected via stdio streams
4. Calls `session.initialize()` for MCP handshake
5. Calls `session.call_tool()` through full protocol
6. Parses response from MCP ContentBlock format

This ensures tests validate the **entire MCP protocol stack**, not just function calls.

## External Dependencies

### Docker Services (Auto-started)

The `docker_services` fixture automatically starts Docker containers:

- **Kafka** (`localhost:9092`) - Kafka broker
- **Schema Registry** (`localhost:8081`) - Schema Registry
- **PostgreSQL** (`localhost:5432`) - JDBC test database

Services are started before tests and kept running for faster test execution.

**Start manually:**
```bash
docker compose up -d
```

**Stop manually:**
```bash
docker compose down
```

### AWS Services (Mocked)

AWS services are mocked using **moto** - no real AWS credentials needed:

- **Athena** - Mocked via `moto.mock_athena`
- **S3** - Mocked via `moto.mock_s3`
- **Glue** - Mocked via `moto.mock_glue`

The `aws_mock` fixture automatically sets up mocks for each test.

### Database

PostgreSQL is started in Docker. Database tests use:
- **URL**: `postgresql://testuser:testpass@localhost:5432/testdb`
- **User**: `testuser`
- **Password**: `testpass`
- **Database**: `testdb`

**Note**: Database tests use native Python libraries (psycopg2) - no JVM required.

## Coverage

To measure coverage:

```bash
# Run tests with coverage
make test-coverage

# Generate HTML report
make coverage-html

# View report
open htmlcov/index.html
```

Target: **95% coverage** for `mcpkit/server.py` and `mcpkit/core/*.py`

## Prerequisites

### Required

- Docker and Docker Compose (for Kafka, Schema Registry, PostgreSQL)
- Python dependencies: `pip install -e ".[dev]"`

### Optional

- **Database**: PostgreSQL database for database tests
- **Nomad/Consul**: For infrastructure tests (tests skip if unavailable)

## Notes

- Tests are marked with `@pytest.mark.integration`
- Tests that require external services will skip if services are unavailable
- Each test validates response structure and basic functionality
- Tests use fixtures for clean test data setup/teardown
- Docker services are kept running between tests for performance
- AWS mocks are reset for each test function
