# Test Coverage

## Quick Start

```bash
# Run tests with coverage
make test-coverage

# Generate HTML report
make coverage-html
open htmlcov/index.html
```

## Commands

```bash
# Terminal report
pytest --cov=mcpkit --cov-report=term-missing -v

# HTML report
pytest --cov=mcpkit --cov-report=html -v
open htmlcov/index.html

# Integration tests only
pytest tests/integration/ -m integration --cov=mcpkit --cov-report=term-missing -v
```

## Targets

- **95%** coverage for `mcpkit/server.py` and `mcpkit/core/*.py`
- **90%** coverage for `mcpkit/server_tools/*.py`

## Current Status

- **Tests**: ~630 test functions
- **Code**: ~196 functions/classes
- **Ratio**: ~3.2 tests per function/class
