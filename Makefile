.PHONY: help test test-unit test-integration test-coverage coverage-html docker-up docker-down docker-logs clean

help:
	@echo "Available targets:"
	@echo "  test              - Run all unit tests"
	@echo "  test-unit         - Run unit tests only (no Docker required)"
	@echo "  test-integration  - Run integration tests (requires Docker)"
	@echo "  test-coverage     - Run tests with coverage report"
	@echo "  coverage-html     - Generate HTML coverage report"
	@echo "  docker-up         - Start Docker services (Kafka, Schema Registry)"
	@echo "  docker-down       - Stop Docker services"
	@echo "  docker-logs        - Show Docker service logs"
	@echo "  clean             - Clean up test artifacts"

test:
	pytest -v

test-unit:
	pytest -v -m "not integration"

test-integration:
	./run_integration_tests.sh

test-coverage:
	@echo "Running tests with subprocess coverage..."
	@rm -f .coverage
	@find . -name ".coverage.*" -delete 2>/dev/null || true
	COVERAGE_PROCESS_START=$$(pwd)/.coveragerc pytest --cov=mcpkit --cov-report=term-missing --cov-report=html -v
	@echo "Combining subprocess coverage files..."
	@python -m coverage combine
	@python -m coverage report

coverage-html:
	@rm -f .coverage
	@find . -name ".coverage.*" -delete 2>/dev/null || true
	COVERAGE_PROCESS_START=$$(pwd)/.coveragerc pytest --cov=mcpkit --cov-report=html -v
	@python -m coverage combine
	@python -m coverage html
	@echo "Coverage report generated in htmlcov/index.html"

docker-up:
	docker compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 10
	@docker compose ps

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

clean:
	rm -rf .datasets .artifacts .demo_datasets .demo_artifacts
	rm -rf __pycache__ .pytest_cache .coverage htmlcov
	rm -f .coverage.*
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
