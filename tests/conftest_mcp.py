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
    
    Starts: Kafka, Schema Registry, PostgreSQL, LocalStack (AWS), Consul, Nomad
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
    
    # Wait for services to be ready (with shorter timeout and better error handling)
    print("Waiting for services to be ready...")
    max_wait = int(os.getenv("MCPKIT_TEST_SERVICE_WAIT", "30"))  # Default 30s, configurable
    
    def check_service(name, check_func, required=True):
        """Check if a service is ready."""
        waited = 0
        while waited < max_wait:
            try:
                if check_func():
                    print(f"{name} is ready!")
                    return True
            except Exception as e:
                if waited >= max_wait - 2:  # Last attempt
                    if required:
                        print(f"WARNING: {name} not ready after {max_wait}s: {e}")
                    else:
                        print(f"INFO: {name} not available (optional): {e}")
            time.sleep(1)
            waited += 1
        if required:
            print(f"ERROR: {name} failed to start after {max_wait}s")
        return False
    
    # Check Kafka (required)
    try:
        from kafka import KafkaConsumer
        check_service("Kafka", lambda: (
            KafkaConsumer(bootstrap_servers=["localhost:9092"], consumer_timeout_ms=500).close() or True
        ), required=True)
    except ImportError:
        print("WARNING: kafka-python not installed, skipping Kafka check")
    
    # Check Schema Registry (required)
    try:
        import requests
        check_service("Schema Registry", lambda: (
            requests.get("http://localhost:8081/subjects", timeout=1).status_code in [200, 404]
        ), required=True)
    except ImportError:
        print("WARNING: requests not installed, skipping Schema Registry check")
    
    # Check PostgreSQL (required)
    try:
        import psycopg2
        check_service("PostgreSQL", lambda: (
            psycopg2.connect(
                host="localhost",
                port=5433,
                user="testuser",
                password="testpass",
                database="testdb",
                connect_timeout=1
            ).close() or True
        ), required=True)
    except ImportError:
        print("WARNING: psycopg2 not installed, skipping PostgreSQL check")
    
    # Check LocalStack (optional - only needed for AWS tests)
    try:
        import requests
        check_service("LocalStack", lambda: (
            requests.get("http://localhost:4566/_localstack/health", timeout=1).status_code == 200
        ), required=False)
    except ImportError:
        pass
    
    # Check Consul (optional - only needed for infrastructure tests)
    try:
        import requests
        check_service("Consul", lambda: (
            requests.get("http://localhost:8500/v1/status/leader", timeout=1).status_code == 200
        ), required=False)
    except ImportError:
        pass
    
    # Check Nomad (optional - only needed for infrastructure tests)
    # Use shorter timeout since Nomad can be slow to start
    try:
        import requests
        # Quick check with very short timeout - don't block if slow
        try:
            response = requests.get("http://localhost:4646/v1/status/leader", timeout=0.5)
            if response.status_code == 200:
                print("Nomad is ready!")
        except Exception:
            print("INFO: Nomad not available (optional)")
    except ImportError:
        pass
    
    yield
    
    # Optionally stop services (commented out to keep them running for faster tests)
    # print("Stopping Docker services...")
    # subprocess.run(
    #     compose_cmd + ["down"],
    #     cwd=compose_dir,
    #     check=False,
    # )


@pytest.fixture
def aws_setup(docker_services, monkeypatch):
    """Setup AWS environment using LocalStack.
    
    Configures AWS endpoints to point to LocalStack for S3, Athena, Glue.
    """
    # Configure boto3 to use LocalStack
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    
    # LocalStack endpoint
    localstack_endpoint = "http://localhost:4566"
    monkeypatch.setenv("AWS_ENDPOINT_URL", localstack_endpoint)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", f"{localstack_endpoint}")
    monkeypatch.setenv("AWS_ENDPOINT_URL_ATHENA", f"{localstack_endpoint}")
    monkeypatch.setenv("AWS_ENDPOINT_URL_GLUE", f"{localstack_endpoint}")
    
    yield localstack_endpoint


@pytest.fixture
def kafka_setup(docker_services, monkeypatch):
    """Setup Kafka environment for tests."""
    monkeypatch.setenv("MCPKIT_KAFKA_BOOTSTRAP", "localhost:9092")
    monkeypatch.setenv("MCPKIT_SCHEMA_REGISTRY_URL", "http://localhost:8081")
    yield


@pytest.fixture
def db_setup(docker_services, monkeypatch):
    """Setup PostgreSQL database environment for tests.
    
    Uses native psycopg2 (no JVM required).
    """
    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not available, skipping database tests")
    
    # Try to connect to verify database is available (try both ports)
    conn = None
    db_url = None
    for port in [5433, 5432]:
        try:
            test_url = f"postgresql://testuser:testpass@localhost:{port}/testdb"
            conn = psycopg2.connect(test_url, connect_timeout=2)
            conn.close()
            db_url = test_url
            break
        except Exception:
            continue
    
    if db_url is None:
        pytest.skip("PostgreSQL not available with test credentials, skipping database tests")
    
    # Set database connection URL (native PostgreSQL format)
    monkeypatch.setenv("MCPKIT_DB_URL", db_url)
    
    # Also set legacy JDBC-style env vars for backward compatibility
    monkeypatch.setenv("MCPKIT_JDBC_URL", "jdbc:postgresql://localhost:5433/testdb")
    monkeypatch.setenv("MCPKIT_JDBC_USER", "testuser")
    monkeypatch.setenv("MCPKIT_JDBC_PASSWORD", "testpass")
    
    yield


@pytest.fixture
def nomad_setup(docker_services, monkeypatch):
    """Setup Nomad environment for tests."""
    nomad_addr = "http://localhost:4646"
    monkeypatch.setenv("MCPKIT_NOMAD_ADDRESS", nomad_addr)
    yield nomad_addr


@pytest.fixture
def consul_setup(docker_services, monkeypatch):
    """Setup Consul environment for tests."""
    consul_addr = "localhost:8500"
    monkeypatch.setenv("MCPKIT_CONSUL_ADDRESS", consul_addr)
    yield consul_addr


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
            # Check if environment variables changed (e.g., MCPKIT_ROOTS, MCPKIT_DB_URL, etc.)
            # If so, invalidate the session to pick up new env vars
            # Track key env vars that affect server behavior
            key_env_vars = {
                "MCPKIT_ROOTS": os.environ.get("MCPKIT_ROOTS"),
                "MCPKIT_DB_URL": os.environ.get("MCPKIT_DB_URL"),
                "MCPKIT_JDBC_URL": os.environ.get("MCPKIT_JDBC_URL"),
                "MCPKIT_KAFKA_BOOTSTRAP": os.environ.get("MCPKIT_KAFKA_BOOTSTRAP"),
                "MCPKIT_SCHEMA_REGISTRY_URL": os.environ.get("MCPKIT_SCHEMA_REGISTRY_URL"),
            }
            
            if self._initialized and self._session:
                # If env vars changed, we need to recreate the session
                if hasattr(self, "_last_env_vars") and self._last_env_vars != key_env_vars:
                    # Environment changed, close old session
                    await self._close_session()
                else:
                    return self._session
            self._last_env_vars = key_env_vars
            
            # Create server parameters for stdio transport
            # Enable coverage for subprocess (coverage.py subprocess support)
            env = os.environ.copy()
            
            # Set COVERAGE_PROCESS_START to enable subprocess coverage tracking
            # This tells coverage.py to start tracking in subprocesses
            if "COVERAGE_PROCESS_START" not in env:
                project_root = Path(__file__).parent.parent.parent
                coverage_rc = project_root / ".coveragerc"
                if coverage_rc.exists():
                    # Use absolute path, properly quoted if needed
                    env["COVERAGE_PROCESS_START"] = str(coverage_rc.absolute())
                else:
                    # Fallback: use project root
                    env["COVERAGE_PROCESS_START"] = str(project_root.absolute())
            
            # Also ensure coverage is enabled in subprocess
            # Check if we're running under coverage
            if "COVERAGE_PROCESS_START" in os.environ or "COVERAGE_PROCESS_START" in env:
                # Prepend coverage run to command
                # This ensures the subprocess runs with coverage tracking
                pass  # Coverage will auto-start if COVERAGE_PROCESS_START is set
            
            # Pass through all MCPKIT_* environment variables to server subprocess
            # This allows tests to configure the server via fixtures (db_setup, kafka_setup, etc.)
            for key, value in os.environ.items():
                if key.startswith("MCPKIT_"):
                    env[key] = value
            
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
            await asyncio.wait_for(self._session.initialize(), timeout=15.0)  # Increased from 5.0 to handle slow initialization under load
            self._initialized = True
            
            return self._session
        
        async def _close_session(self):
            """Close current session and cleanup resources."""
            if self._session_ctx:
                try:
                    await self._session_ctx.__aexit__(None, None, None)
                except Exception:
                    pass
                self._session_ctx = None
            if self._stdio_ctx:
                try:
                    await self._stdio_ctx.__aexit__(None, None, None)
                except Exception:
                    pass
                self._stdio_ctx = None
            self._session = None
            self._read = None
            self._write = None
            self._initialized = False
        
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
                        timeout=30.0  # 30 second timeout per call (increased to handle slow session initialization under load)
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
