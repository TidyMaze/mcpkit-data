#!/usr/bin/env python3
"""Check mcpkit-data configuration and service availability."""

import os
import sys
from typing import Dict, List, Tuple

def check_env_var(name: str, required: bool = False) -> Tuple[bool, str]:
    """Check if environment variable is set."""
    value = os.getenv(name)
    if value:
        return True, f"✓ {name}={value[:50]}"
    elif required:
        return False, f"✗ {name} (REQUIRED - not set)"
    else:
        return False, f"○ {name} (optional - not set)"

def test_kafka(bootstrap: str = None) -> Tuple[bool, str]:
    """Test Kafka connectivity."""
    try:
        from kafka import KafkaConsumer
        bootstrap = bootstrap or os.getenv("MCPKIT_KAFKA_BOOTSTRAP", "localhost:9092")
        consumer = KafkaConsumer(bootstrap_servers=bootstrap, consumer_timeout_ms=2000)
        topics = consumer.list_topics(timeout_ms=2000)
        consumer.close()
        return True, f"✓ Kafka: {bootstrap} (connected, {len(topics)} topics)"
    except Exception as e:
        return False, f"✗ Kafka: {bootstrap or 'not configured'} ({str(e)[:60]})"

def test_schema_registry(url: str = None) -> Tuple[bool, str]:
    """Test Schema Registry connectivity."""
    try:
        import requests
        url = url or os.getenv("MCPKIT_SCHEMA_REGISTRY_URL", "http://localhost:8081")
        auth_str = os.getenv("MCPKIT_SCHEMA_REGISTRY_BASIC_AUTH")
        headers = {}
        if auth_str:
            from base64 import b64encode
            auth_bytes = auth_str.encode('ascii')
            b64_bytes = b64encode(auth_bytes)
            headers['Authorization'] = f'Basic {b64_bytes.decode("ascii")}'
        
        resp = requests.get(f"{url}/subjects", headers=headers, timeout=2)
        resp.raise_for_status()
        subjects = resp.json()
        return True, f"✓ Schema Registry: {url} (connected, {len(subjects)} subjects)"
    except Exception as e:
        return False, f"✗ Schema Registry: {url or 'not configured'} ({str(e)[:60]})"

def test_db() -> Tuple[bool, str]:
    """Test database configuration."""
    # Try native URL first
    db_url = os.getenv("MCPKIT_DB_URL")
    if db_url:
        try:
            if db_url.startswith("postgresql://") or db_url.startswith("postgres://"):
                import psycopg2
                conn = psycopg2.connect(db_url)
                conn.close()
                return True, f"✓ Database: PostgreSQL @ {db_url.split('@')[-1] if '@' in db_url else db_url}"
            elif db_url.startswith("mysql://") or db_url.startswith("mysql+pymysql://"):
                import pymysql
                # Parse and test connection
                return True, f"✓ Database: MySQL @ {db_url.split('@')[-1] if '@' in db_url else db_url}"
        except ImportError:
            return False, "✗ Database: Required library not installed (psycopg2-binary or pymysql)"
        except Exception as e:
            return False, f"✗ Database: Connection failed ({str(e)[:60]})"
    
    # Fallback to JDBC-style env vars (for backward compatibility)
    jdbc_url = os.getenv("MCPKIT_JDBC_URL")
    if jdbc_url:
        if jdbc_url.startswith("jdbc:postgresql://"):
            try:
                import psycopg2
                pg_url = jdbc_url.replace("jdbc:postgresql://", "postgresql://")
                user = os.getenv("MCPKIT_JDBC_USER") or os.getenv("MCPKIT_DB_USER")
                password = os.getenv("MCPKIT_JDBC_PASSWORD") or os.getenv("MCPKIT_DB_PASSWORD")
                if user and password:
                    pg_url = pg_url.replace("postgresql://", f"postgresql://{user}:{password}@")
                conn = psycopg2.connect(pg_url)
                conn.close()
                return True, f"✓ Database: PostgreSQL (via JDBC URL) @ {jdbc_url.split('@')[-1] if '@' in jdbc_url else jdbc_url}"
            except Exception as e:
                return False, f"✗ Database: Connection failed ({str(e)[:60]})"
    
    return False, "○ Database: Not configured (set MCPKIT_DB_URL or MCPKIT_JDBC_URL)"

def test_aws() -> Tuple[bool, str]:
    """Test AWS configuration."""
    try:
        import boto3
        # Try to get credentials
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials:
            region = session.region_name or os.getenv("AWS_REGION", "not set")
            return True, f"✓ AWS: Credentials found (region: {region})"
        else:
            return False, "✗ AWS: No credentials found (check ~/.aws/credentials or env vars)"
    except Exception as e:
        return False, f"✗ AWS: {str(e)[:60]}"

def test_docker_compose() -> Tuple[bool, str]:
    """Check if docker-compose services are available."""
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "compose", "ps"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=os.path.dirname(__file__)
        )
        if result.returncode == 0 and "kafka" in result.stdout.lower():
            return True, "✓ Docker Compose: Services available (check 'docker compose ps')"
        else:
            return False, "○ Docker Compose: Services not running (run 'docker compose up -d')"
    except Exception as e:
        return False, f"○ Docker Compose: {str(e)[:60]}"

def main():
    """Run all configuration checks."""
    print("🔍 mcpkit-data Configuration Check\n")
    print("=" * 60)
    
    # Core configuration
    print("\n📋 Core Configuration:")
    print("-" * 60)
    core_vars = [
        ("MCPKIT_ROOTS", False),
        ("MCPKIT_DATASET_DIR", False),
        ("MCPKIT_ARTIFACT_DIR", False),
        ("MCPKIT_TIMEOUT_SECS", False),
        ("MCPKIT_MAX_ROWS", False),
    ]
    for name, required in core_vars:
        status, msg = check_env_var(name, required)
        print(f"  {msg}")
    
    # Kafka configuration
    print("\n🎧 Kafka Configuration:")
    print("-" * 60)
    kafka_vars = [
        ("MCPKIT_KAFKA_BOOTSTRAP", False),
        ("MCPKIT_KAFKA_SECURITY_PROTOCOL", False),
        ("MCPKIT_KAFKA_SASL_MECHANISM", False),
        ("MCPKIT_KAFKA_SASL_USERNAME", False),
        ("MCPKIT_KAFKA_SASL_PASSWORD", False),
    ]
    for name, required in kafka_vars:
        status, msg = check_env_var(name, required)
        print(f"  {msg}")
    
    kafka_status, kafka_msg = test_kafka()
    print(f"  {kafka_msg}")
    
    # Schema Registry
    print("\n📋 Schema Registry Configuration:")
    print("-" * 60)
    schema_vars = [
        ("MCPKIT_SCHEMA_REGISTRY_URL", False),
        ("MCPKIT_SCHEMA_REGISTRY_BASIC_AUTH", False),
    ]
    for name, required in schema_vars:
        status, msg = check_env_var(name, required)
        print(f"  {msg}")
    
    schema_status, schema_msg = test_schema_registry()
    print(f"  {schema_msg}")
    
    # Database
    print("\n🗄️ Database Configuration:")
    print("-" * 60)
    db_vars = [
        ("MCPKIT_DB_URL", False),
        ("MCPKIT_DB_HOST", False),
        ("MCPKIT_DB_PORT", False),
        ("MCPKIT_DB_NAME", False),
        ("MCPKIT_DB_USER", False),
        ("MCPKIT_DB_PASSWORD", False),
        # Legacy JDBC vars for backward compatibility
        ("MCPKIT_JDBC_URL", False),
        ("MCPKIT_JDBC_USER", False),
        ("MCPKIT_JDBC_PASSWORD", False),
    ]
    for name, required in db_vars:
        status, msg = check_env_var(name, required)
        print(f"  {msg}")
    
    db_status, db_msg = test_db()
    print(f"  {db_msg}")
    
    # AWS
    print("\n☁️ AWS Configuration:")
    print("-" * 60)
    aws_vars = [
        ("AWS_ACCESS_KEY_ID", False),
        ("AWS_SECRET_ACCESS_KEY", False),
        ("AWS_REGION", False),
    ]
    for name, required in aws_vars:
        status, msg = check_env_var(name, required)
        print(f"  {msg}")
    
    aws_status, aws_msg = test_aws()
    print(f"  {aws_msg}")
    
    # Docker Compose
    print("\n🐳 Docker Compose:")
    print("-" * 60)
    docker_status, docker_msg = test_docker_compose()
    print(f"  {docker_msg}")
    
    # Summary
    print("\n" + "=" * 60)
    print("\n📊 Summary:")
    print("-" * 60)
    
    services = [
        ("Kafka", kafka_status),
        ("Schema Registry", schema_status),
        ("Database", db_status),
        ("AWS", aws_status),
        ("Docker Compose", docker_status),
    ]
    
    available = [name for name, status in services if status]
    missing = [name for name, status in services if not status]
    
    if available:
        print(f"  ✓ Available: {', '.join(available)}")
    if missing:
        print(f"  ✗ Missing: {', '.join(missing)}")
    
    print("\n💡 Quick Start:")
    print("-" * 60)
    if not kafka_status or not schema_status:
        print("  To start Kafka & Schema Registry:")
        print(f"    cd {os.path.dirname(os.path.abspath(__file__))}")
        print("    docker compose up -d")
        print("    export MCPKIT_KAFKA_BOOTSTRAP=localhost:9092")
        print("    export MCPKIT_SCHEMA_REGISTRY_URL=http://localhost:8081")
    
    if not db_status:
        print("  To configure Database:")
        print("    export MCPKIT_DB_URL=postgresql://user:pass@localhost:5432/dbname")
        print("    # Or for MySQL:")
        print("    export MCPKIT_DB_URL=mysql://user:pass@localhost:3306/dbname")
    
    if not aws_status:
        print("  To configure AWS:")
        print("    export AWS_ACCESS_KEY_ID=your-key")
        print("    export AWS_SECRET_ACCESS_KEY=your-secret")
        print("    export AWS_REGION=eu-central-1")
        print("  Or use: aws configure")
    
    print()

if __name__ == "__main__":
    main()

