"""Pytest configuration for integration tests."""

import subprocess
import time

import pytest


def pytest_configure(config):
    """Configure pytest for integration tests."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (deselect with '-m \"not integration\"')"
    )


def _get_docker_compose_cmd():
    """Get docker compose command (v2 or v1)."""
    import subprocess
    import shutil
    
    # Try docker compose (v2)
    if shutil.which("docker"):
        try:
            result = subprocess.run(
                ["docker", "compose", "version"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return ["docker", "compose"]
        except Exception:
            pass
    
    # Fallback to docker-compose (v1)
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    
    raise RuntimeError("Neither 'docker compose' nor 'docker-compose' found")


@pytest.fixture(scope="session")
def docker_compose_up():
    """Start docker-compose services."""
    import subprocess
    import os
    
    compose_cmd = _get_docker_compose_cmd()
    compose_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Start services
    subprocess.run(
        compose_cmd + ["up", "-d"],
        cwd=compose_dir,
        check=True,
    )
    
    # Wait for services to be ready
    print("Waiting for Kafka services to be ready...")
    max_wait = 60
    waited = 0
    while waited < max_wait:
        try:
            result = subprocess.run(
                compose_cmd + ["ps"],
                cwd=compose_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            if "kafka" in result.stdout and "schema-registry" in result.stdout:
                # Check if services are healthy
                health_check = subprocess.run(
                    compose_cmd + ["ps"],
                    cwd=compose_dir,
                    capture_output=True,
                    text=True,
                )
                if "healthy" in health_check.stdout.lower() or waited > 30:
                    print("Services are ready!")
                    break
        except Exception:
            pass
        time.sleep(2)
        waited += 2
    
    yield
    
    # Stop services (optional - comment out if you want to keep them running)
    # subprocess.run(
    #     compose_cmd + ["down"],
    #     cwd=compose_dir,
    #     check=False,
    # )

