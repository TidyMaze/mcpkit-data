"""Pytest configuration for MCP protocol integration tests.

This module provides fixtures for true blackbox integration testing using
the full MCP protocol: starting a real MCP server process and connecting
to it via the official MCP Python SDK client.

External dependencies are handled via:
- Docker services (Kafka, Schema Registry, PostgreSQL)
- AWS mocks (moto) for AWS services
"""

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict

import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


def _get_docker_compose_cmd():
    """Get docker compose command (v2 or v1)."""
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
def docker_services():
    """Start Docker services for integration tests.
    
    Starts: Kafka, Schema Registry, PostgreSQL
    """
    compose_cmd = _get_docker_compose_cmd()
    compose_dir = Path(__file__).parent.parent
    
    # Start services
    print("Starting Docker services...")
    subprocess.run(
        compose_cmd + ["up", "-d"],
        cwd=compose_dir,
        check=False,  # Don't fail if already running
    )
    
    # Wait for services to be ready
    print("Waiting for services to be ready...")
    max_wait = 120
    waited = 0
    
    # Check Kafka
    while waited < max_wait:
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"], consumer_timeout_ms=1000)
            consumer.close()
            print("Kafka is ready!")
            break
        except Exception:
            pass
        time.sleep(2)
        waited += 2
    
    # Check Schema Registry
    waited = 0
    while waited < max_wait:
        try:
            import requests
            response = requests.get("http://localhost:8081/subjects", timeout=2)
            if response.status_code in [200, 404]:  # 404 is OK, means registry is up
                print("Schema Registry is ready!")
                break
        except Exception:
            pass
        time.sleep(2)
        waited += 2
    
    # Check PostgreSQL
    waited = 0
    while waited < max_wait:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                user="testuser",
                password="testpass",
                database="testdb",
                connect_timeout=2
            )
            conn.close()
            print("PostgreSQL is ready!")
            break
        except Exception:
            pass
        time.sleep(2)
        waited += 2
    
    yield
    
    # Optionally stop services (commented out to keep them running for faster tests)
    # print("Stopping Docker services...")
    # subprocess.run(
    #     compose_cmd + ["down"],
    #     cwd=compose_dir,
    #     check=False,
    # )


@pytest.fixture(scope="function")
def aws_mock(monkeypatch):
    """Setup AWS mocks using moto.
    
    This fixture starts moto mocks for all AWS services (Athena, S3, Glue).
    Each test function gets fresh mocks.
    """
    try:
        from moto import mock_aws
        
        # Set dummy AWS credentials for moto
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        
        # Start AWS mocks (moto 5.x uses mock_aws for all services)
        aws_mock = mock_aws()
        aws_mock.start()
        
        yield aws_mock
        
        # Stop mocks
        aws_mock.stop()
    except ImportError:
        pytest.skip("moto not installed, skipping AWS mock tests")


@pytest.fixture
def kafka_setup(docker_services, monkeypatch):
    """Setup Kafka environment for tests."""
    monkeypatch.setenv("MCPKIT_KAFKA_BOOTSTRAP", "localhost:9092")
    monkeypatch.setenv("MCPKIT_SCHEMA_REGISTRY_URL", "http://localhost:8081")
    yield


@pytest.fixture
def jdbc_setup(docker_services, monkeypatch):
    """Setup PostgreSQL database environment for tests.
    
    Uses native psycopg2 (no JVM required).
    """
    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not available, skipping database tests")
    
    # Set database connection URL (native PostgreSQL format)
    monkeypatch.setenv("MCPKIT_DB_URL", "postgresql://testuser:testpass@localhost:5432/testdb")
    
    # Also set legacy JDBC-style env vars for backward compatibility
    monkeypatch.setenv("MCPKIT_JDBC_URL", "jdbc:postgresql://localhost:5432/testdb")
    monkeypatch.setenv("MCPKIT_JDBC_USER", "testuser")
    monkeypatch.setenv("MCPKIT_JDBC_PASSWORD", "testpass")
    
    yield


@pytest.fixture(scope="session")
def mcp_client():
    """Create MCP client connected to real MCP server via stdio.
    
    This fixture provides a true blackbox test client that:
    - Starts a real MCP server process via stdio (session-scoped, reused)
    - Connects using the official MCP Python SDK
    - Tests full protocol compliance (serialization, validation, etc.)
    """
    class MCPTestClient:
        """Test client for calling MCP tools through full protocol."""
        
        def __init__(self):
            self._loop = None
            self._read = None
            self._write = None
            self._session = None
            self._stdio_ctx = None
            self._session_ctx = None
            self._initialized = False
        
        def _get_loop(self):
            """Get or create event loop."""
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
            return self._loop
        
        async def _ensure_session(self):
            """Ensure MCP session is initialized (lazy initialization)."""
            if self._initialized and self._session:
                return self._session
            
            # Create server parameters for stdio transport
            # Enable coverage for subprocess (coverage.py subprocess support)
            env = os.environ.copy()
            
            # Set COVERAGE_PROCESS_START to enable subprocess coverage tracking
            # This tells coverage.py to start tracking in subprocesses
            if "COVERAGE_PROCESS_START" not in env:
                project_root = Path(__file__).parent.parent.parent
                coverage_rc = project_root / ".coveragerc"
                if coverage_rc.exists():
                    env["COVERAGE_PROCESS_START"] = str(coverage_rc)
                else:
                    # Fallback: use project root
                    env["COVERAGE_PROCESS_START"] = str(project_root)
            
            # Also ensure coverage is enabled in subprocess
            # Check if we're running under coverage
            if "COVERAGE_PROCESS_START" in os.environ or "COVERAGE_PROCESS_START" in env:
                # Prepend coverage run to command
                # This ensures the subprocess runs with coverage tracking
                pass  # Coverage will auto-start if COVERAGE_PROCESS_START is set
            
            server_params = StdioServerParameters(
                command=sys.executable,
                args=["-m", "mcpkit.server"],
                env=env,
            )
            
            # Connect via stdio using MCP SDK
            # Start the stdio client (this starts the server process)
            stdio_ctx = stdio_client(server_params)
            self._read, self._write = await stdio_ctx.__aenter__()
            
            # Store context manager for cleanup
            self._stdio_ctx = stdio_ctx
            
            # Create client session
            session_ctx = ClientSession(self._read, self._write)
            self._session = await session_ctx.__aenter__()
            
            # Store session context for cleanup
            self._session_ctx = session_ctx
            
            # Initialize session (handshake) with timeout
            await asyncio.wait_for(self._session.initialize(), timeout=10.0)
            self._initialized = True
            
            return self._session
        
        def call_tool_sync(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
            """Call an MCP tool via full protocol.
            
            Args:
                name: Tool name (e.g., 'dataset_list')
                arguments: Tool arguments as dict
                
            Returns:
                Tool response as dict
            """
            async def _call():
                try:
                    session = await self._ensure_session()
                    
                    # Call tool through full MCP protocol with timeout
                    result = await asyncio.wait_for(
                        session.call_tool(name, arguments),
                        timeout=30.0  # 30 second timeout per call
                    )
                    
                    # Extract response content
                    if result.content:
                        # Get first content block (usually JSON text)
                        first_content = result.content[0]
                        text_content = None
                        
                        if hasattr(first_content, 'text'):
                            # TextContentBlock
                            text_content = first_content.text
                        elif isinstance(first_content, dict):
                            text_content = first_content.get("text", "")
                        
                        if text_content:
                            # Check if it's an error message from FastMCP
                            if any(keyword in text_content for keyword in ["Error calling tool", "GuardError", "ValidationError", "validation error"]):
                                # Extract error message and raise as exception
                                raise RuntimeError(text_content)
                            
                            # Try to parse as JSON
                            try:
                                parsed = json.loads(text_content)
                                # Check if parsed JSON contains error info
                                if isinstance(parsed, dict):
                                    # Check for error fields
                                    if "error" in parsed or "Error" in str(parsed):
                                        raise RuntimeError(text_content)
                                    # Check if it's an error response structure (only text field = error)
                                    if "text" in parsed and len(parsed) == 1:
                                        text_val = parsed["text"]
                                        if any(keyword in str(text_val) for keyword in ["Error", "GuardError", "ValidationError", "validation error"]):
                                            raise RuntimeError(str(text_val))
                                    # Check if it's an error response structure
                                    if "text" in parsed and ("Error" in str(parsed["text"]) or "GuardError" in str(parsed["text"]) or "ValidationError" in str(parsed["text"])):
                                        raise RuntimeError(str(parsed["text"]))
                                return parsed
                            except json.JSONDecodeError:
                                # If it's not JSON but contains error keywords, raise
                                if "Error" in text_content or "GuardError" in text_content or "ValidationError" in text_content:
                                    raise RuntimeError(text_content)
                                return {"text": text_content}
                    
                    # Fallback: return empty dict
                    return {}
                except asyncio.TimeoutError:
                    raise RuntimeError(f"MCP tool '{name}' call timed out after 30s")
                except Exception as e:
                    # Check if this is an error response from the tool
                    error_msg = str(e)
                    if "Error calling tool" in error_msg or "GuardError" in error_msg:
                        # This is an error from the tool, raise it
                        raise RuntimeError(error_msg)
                    raise RuntimeError(f"MCP protocol call failed: {e}")
            
            # Run async call synchronously
            loop = self._get_loop()
            try:
                return loop.run_until_complete(_call())
            except Exception as e:
                raise RuntimeError(f"MCP protocol call failed: {e}")
        
        def cleanup(self):
            """Cleanup session and process."""
            if self._session:
                try:
                    loop = self._get_loop()
                    loop.run_until_complete(self._session.__aexit__(None, None, None))
                except Exception:
                    pass
                self._session = None
            
            if self._read and self._write:
                try:
                    loop = self._get_loop()
                    loop.run_until_complete(stdio_client.__aexit__(None, None, None))
                except Exception:
                    pass
                self._read = None
                self._write = None
            
            self._initialized = False
    
    client = MCPTestClient()
    yield client
    
    # Cleanup at end of session
    client.cleanup()
    if client._loop and not client._loop.is_closed():
        client._loop.close()


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def clean_registry(monkeypatch, temp_dir):
    """Clean dataset registry for testing."""
    dataset_dir = temp_dir / "datasets"
    dataset_dir.mkdir()
    monkeypatch.setenv("MCPKIT_DATASET_DIR", str(dataset_dir))
    yield dataset_dir


@pytest.fixture
def clean_artifacts(monkeypatch, temp_dir):
    """Clean artifact directory for testing."""
    artifact_dir = temp_dir / "artifacts"
    artifact_dir.mkdir()
    monkeypatch.setenv("MCPKIT_ARTIFACT_DIR", str(artifact_dir))
    yield artifact_dir


@pytest.fixture
def clean_roots(monkeypatch, temp_dir):
    """Set MCPKIT_ROOTS to temp directory."""
    monkeypatch.setenv("MCPKIT_ROOTS", str(temp_dir))
    yield temp_dir
