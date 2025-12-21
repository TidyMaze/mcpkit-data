# Cursor MCP Configuration Guide

## Step 1: Locate Cursor MCP Settings

The Cursor MCP configuration file is typically located at:

**macOS:**
```
~/Library/Application Support/Cursor/User/globalStorage/rooveterinaryinc.roo-cline/settings/cline_mcp_settings.json
```

**Linux:**
```
~/.config/Cursor/User/globalStorage/rooveterinaryinc.roo-cline/settings/cline_mcp_settings.json
```

**Windows:**
```
%APPDATA%\Cursor\User\globalStorage\rooveterinaryinc.roo-cline\settings\cline_mcp_settings.json
```

## Step 2: Add the Configuration

Open the `cline_mcp_settings.json` file and add the `mcpkit-data` server configuration.

### Option A: Using Virtual Environment (Recommended)

If you installed the package in a virtual environment (`.venv`):

```json
{
  "mcpServers": {
    "mcpkit-data": {
      "command": "/path/to/mcpkit-data/.venv/bin/python",
      "args": ["-m", "mcpkit.server"],
      "env": {
        "MCPKIT_ROOTS": "/path/to/mcpkit-data:/path/to/allowed",
        "MCPKIT_DATASET_DIR": "/path/to/mcpkit-data/.datasets",
        "MCPKIT_ARTIFACT_DIR": "/path/to/mcpkit-data/.artifacts"
      }
    }
  }
}
```

### Option B: Using System Python

If you installed globally or want to use system Python:

```json
{
  "mcpServers": {
    "mcpkit-data": {
      "command": "python3",
      "args": ["-m", "mcpkit.server"],
      "env": {
        "MCPKIT_ROOTS": "/path/to/mcpkit-data:/path/to/allowed",
        "MCPKIT_DATASET_DIR": "/path/to/mcpkit-data/.datasets",
        "MCPKIT_ARTIFACT_DIR": "/path/to/mcpkit-data/.artifacts"
      }
    }
  }
}
```

## Step 3: Add Optional Service Configurations

### With Kafka and Schema Registry

```json
{
  "mcpServers": {
    "mcpkit-data": {
      "command": "/path/to/mcpkit-data/.venv/bin/python",
      "args": ["-m", "mcpkit.server"],
      "env": {
        "MCPKIT_ROOTS": "/path/to/mcpkit-data:/path/to/allowed",
        "MCPKIT_DATASET_DIR": "/path/to/mcpkit-data/.datasets",
        "MCPKIT_ARTIFACT_DIR": "/path/to/mcpkit-data/.artifacts",
        "MCPKIT_KAFKA_BOOTSTRAP": "localhost:9092",
        "MCPKIT_KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "MCPKIT_KAFKA_SASL_MECHANISM": "PLAIN",
        "MCPKIT_KAFKA_SASL_USERNAME": "your-username",
        "MCPKIT_KAFKA_SASL_PASSWORD": "your-password",
        "MCPKIT_SCHEMA_REGISTRY_URL": "http://localhost:8081",
        "MCPKIT_SCHEMA_REGISTRY_BASIC_AUTH": "user:pass"
      }
    }
  }
}
```

### With JDBC Database

```json
{
  "mcpServers": {
    "mcpkit-data": {
      "command": "/path/to/mcpkit-data/.venv/bin/python",
      "args": ["-m", "mcpkit.server"],
      "env": {
        "MCPKIT_ROOTS": "/path/to/mcpkit-data:/path/to/allowed",
        "MCPKIT_JDBC_DRIVER_CLASS": "org.postgresql.Driver",
        "MCPKIT_JDBC_URL": "jdbc:postgresql://localhost:5432/mydb",
        "MCPKIT_JDBC_JARS": "/path/to/postgresql.jar",
        "MCPKIT_JDBC_USER": "myuser",
        "MCPKIT_JDBC_PASSWORD": "mypassword"
      }
    }
  }
}
```

### With AWS Athena

```json
{
  "mcpServers": {
    "mcpkit-data": {
      "command": "/path/to/mcpkit-data/.venv/bin/python",
      "args": ["-m", "mcpkit.server"],
      "env": {
        "MCPKIT_ROOTS": "/path/to/mcpkit-data:/path/to/allowed",
        "AWS_ACCESS_KEY_ID": "your-access-key",
        "AWS_SECRET_ACCESS_KEY": "your-secret-key",
        "AWS_REGION": "eu-central-1"
      }
    }
  }
}
```

**Note**: Alternatively, use AWS credentials file (`~/.aws/credentials`) or IAM roles instead of environment variables.

### Complete Example (All Services)

```json
{
  "mcpServers": {
    "mcpkit-data": {
      "command": "/path/to/mcpkit-data/.venv/bin/python",
      "args": ["-m", "mcpkit.server"],
      "env": {
        "MCPKIT_ROOTS": "/path/to/mcpkit-data:/path/to/allowed:/data",
        "MCPKIT_DATASET_DIR": "/path/to/mcpkit-data/.datasets",
        "MCPKIT_ARTIFACT_DIR": "/path/to/mcpkit-data/.artifacts",
        "MCPKIT_TIMEOUT_SECS": "30",
        "MCPKIT_MAX_ROWS": "1000",
        "MCPKIT_MAX_RECORDS": "1000",
        "MCPKIT_KAFKA_BOOTSTRAP": "kafka1:9092,kafka2:9092",
        "MCPKIT_SCHEMA_REGISTRY_URL": "https://schema-registry.example.com",
        "MCPKIT_SCHEMA_REGISTRY_BASIC_AUTH": "user:pass",
        "MCPKIT_JDBC_DRIVER_CLASS": "org.postgresql.Driver",
        "MCPKIT_JDBC_URL": "jdbc:postgresql://localhost:5432/mydb",
        "MCPKIT_JDBC_JARS": "/opt/postgresql/postgresql.jar",
        "MCPKIT_JDBC_USER": "dbuser",
        "MCPKIT_JDBC_PASSWORD": "dbpass",
        "AWS_ACCESS_KEY_ID": "your-aws-access-key",
        "AWS_SECRET_ACCESS_KEY": "your-aws-secret-key",
        "AWS_REGION": "eu-central-1"
      }
    }
  }
}
```

## Step 4: Restart Cursor

After saving the configuration file, restart Cursor to load the new MCP server.

## Step 5: Verify Installation

1. Open Cursor
2. Check the MCP servers status (usually in settings or status bar)
3. Try using a tool like `dataset_list()` or `fs_read()` to verify it's working

## Troubleshooting

### Server Not Starting

1. **Check Python path**: Make sure the `command` path is correct
   ```bash
   which python3  # or check your venv path
   ```

2. **Verify package is installed**:
   ```bash
   cd /path/to/mcpkit-data
   source .venv/bin/activate
   python -m mcpkit.server --help
   ```

3. **Check Cursor logs**: Look for MCP server errors in Cursor's developer console

### Permission Errors

If you get filesystem permission errors:
- Ensure `MCPKIT_ROOTS` includes the directories you want to access
- Check that the paths in `MCPKIT_ROOTS` exist and are readable

### Import Errors

If you see import errors:
- Make sure all dependencies are installed: `pip install -e ".[dev]"`
- Verify the virtual environment is activated when Cursor runs the server

## Quick Test Configuration (Minimal)

For testing without external services:

```json
{
  "mcpServers": {
    "mcpkit-data": {
      "command": "/path/to/mcpkit-data/.venv/bin/python",
      "args": ["-m", "mcpkit.server"],
      "env": {
        "MCPKIT_ROOTS": "/path/to/mcpkit-data"
      }
    }
  }
}
```

This minimal config will work for:
- Dataset registry operations
- File reading (within allowed roots)
- Pandas/polars/duckdb operations
- JSON tools
- Evidence generation

Services that require additional config (and will error if used without it):
- Kafka tools (needs `MCPKIT_KAFKA_BOOTSTRAP`)
- Schema Registry (needs `MCPKIT_SCHEMA_REGISTRY_URL`)
- JDBC (needs `MCPKIT_JDBC_*` vars)
- AWS (uses default boto3 credentials)

