"""Database query helpers using native Python libraries.

Supports PostgreSQL and MySQL (no JVM required).
"""

import os
from typing import Any, Optional

from .guards import GuardError, cap_rows, get_max_rows, validate_db_query


def get_db_config() -> dict:
    """Get database configuration from environment.

    Supports:
    - PostgreSQL via postgresql:// URL or JDBC-style env vars
    - MySQL via mysql:// URL or JDBC-style env vars

    Returns dict with 'url', 'type' (postgresql/mysql), and connection kwargs.
    """
    # Try native URL first (postgresql:// or mysql://)
    db_url = os.getenv("MCPKIT_DB_URL")
    if db_url:
        if db_url.startswith("postgresql://") or db_url.startswith("postgres://"):
            return {"url": db_url, "type": "postgresql"}
        elif db_url.startswith("mysql://") or db_url.startswith("mysql+pymysql://"):
            return {"url": db_url, "type": "mysql"}
        else:
            raise GuardError(f"Unsupported database URL scheme: {db_url.split('://')[0]}")

    # Fallback to JDBC-style env vars for backward compatibility
    jdbc_url = os.getenv("MCPKIT_JDBC_URL", "")
    user = os.getenv("MCPKIT_JDBC_USER") or os.getenv("MCPKIT_DB_USER")
    password = os.getenv("MCPKIT_JDBC_PASSWORD") or os.getenv("MCPKIT_DB_PASSWORD")

    if jdbc_url.startswith("jdbc:postgresql://"):
        # Convert JDBC PostgreSQL URL to native URL
        pg_url = jdbc_url.replace("jdbc:postgresql://", "postgresql://")
        if user and password and "@" not in pg_url:
            pg_url = pg_url.replace("postgresql://", f"postgresql://{user}:{password}@")
        return {"url": pg_url, "type": "postgresql"}

    elif jdbc_url.startswith("jdbc:mysql://"):
        # Convert JDBC MySQL URL to native URL
        mysql_url = jdbc_url.replace("jdbc:mysql://", "mysql://")
        if user and password and "@" not in mysql_url:
            mysql_url = mysql_url.replace("mysql://", f"mysql://{user}:{password}@")
        return {"url": mysql_url, "type": "mysql"}

    # Try direct env vars (defaults to PostgreSQL)
    host = os.getenv("MCPKIT_DB_HOST") or os.getenv("MCPKIT_JDBC_HOST", "localhost")
    port = os.getenv("MCPKIT_DB_PORT") or os.getenv("MCPKIT_JDBC_PORT", "5432")
    database = os.getenv("MCPKIT_DB_NAME") or os.getenv("MCPKIT_JDBC_DATABASE", "postgres")
    user = user or os.getenv("MCPKIT_DB_USER", "postgres")
    password = password or os.getenv("MCPKIT_DB_PASSWORD", "")

    # Detect database type from port or explicit env var
    db_type = os.getenv("MCPKIT_DB_TYPE", "").lower()
    if not db_type:
        # Guess from port: MySQL default is 3306, PostgreSQL is 5432
        db_type = "mysql" if port == "3306" else "postgresql"

    if db_type == "mysql":
        mysql_url = f"mysql://{user}:{password}@{host}:{port}/{database}"
        return {"url": mysql_url, "type": "mysql"}
    else:
        pg_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        return {"url": pg_url, "type": "postgresql"}


def db_query_ro(query: str, params: Optional[list[Any]] = None) -> dict:
    """
    Execute read-only database query using native Python libraries.

    Supports PostgreSQL (psycopg2) and MySQL (PyMySQL) - no JVM required.
    Returns dict with columns and rows.
    """
    validate_db_query(query)
    config = get_db_config()
    db_type = config["type"]

    # Convert JDBC-style ? placeholders to database-specific format
    # PostgreSQL uses %s, MySQL uses %s (both use %s!)
    db_query = query
    if params and "?" in query:
        placeholder_count = query.count("?")
        if placeholder_count != len(params):
            raise GuardError(f"Parameter count mismatch: {len(params)} params but {placeholder_count} ? placeholders")
        # Both PostgreSQL and MySQL use %s
        db_query = query.replace("?", "%s")

    conn = None
    try:
        if db_type == "postgresql":
            try:
                import psycopg2
            except ImportError:
                raise GuardError("psycopg2 not installed. Install with: pip install psycopg2-binary")

            conn = psycopg2.connect(config["url"])
            cursor = conn.cursor()

        elif db_type == "mysql":
            try:
                import pymysql
            except ImportError:
                raise GuardError("PyMySQL not installed. Install with: pip install pymysql")

            # Parse MySQL URL and connect
            url = config["url"]
            if url.startswith("mysql://"):
                url = url.replace("mysql://", "")
            elif url.startswith("mysql+pymysql://"):
                url = url.replace("mysql+pymysql://", "")

            # Parse URL: user:password@host:port/database
            if "@" in url:
                auth, rest = url.split("@", 1)
                if ":" in auth:
                    user, password = auth.split(":", 1)
                else:
                    user, password = auth, ""
            else:
                user, password = "", ""

            if "/" in rest:
                host_port, database = rest.split("/", 1)
            else:
                host_port, database = rest, ""

            if ":" in host_port:
                host, port = host_port.split(":", 1)
                port = int(port)
            else:
                host, port = host_port, 3306

            conn = pymysql.connect(
                host=host,
                port=port,
                user=user or None,
                password=password or None,
                database=database,
                cursorclass=pymysql.cursors.DictCursor
            )
            cursor = conn.cursor()
        else:
            raise GuardError(f"Unsupported database type: {db_type}")

        # Execute query
        if params:
            cursor.execute(db_query, params)
        else:
            cursor.execute(db_query)

        # Get columns
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        # Fetch rows (capped)
        rows = cursor.fetchall()
        rows = cap_rows(rows, get_max_rows())

        # Convert rows to lists (tuples to lists)
        # MySQL DictCursor returns dicts, PostgreSQL returns tuples
        if db_type == "mysql" and rows and isinstance(rows[0], dict):
            rows_list = [[row[col] for col in columns] for row in rows]
        else:
            rows_list = [list(row) for row in rows]

        return {
            "columns": columns,
            "rows": rows_list,
            "row_count": len(rows_list),
        }
    except Exception as e:
        raise GuardError(f"Database query failed: {e}")
    finally:
        if conn:
            conn.close()


def db_introspect(
    schema_like: Optional[str] = None,
    table_like: Optional[str] = None,
    max_tables: int = 200
) -> dict:
    """
    Introspect database schema using native Python libraries.

    Supports PostgreSQL (psycopg2) and MySQL (PyMySQL).
    Returns dict with tables and columns.
    """
    config = get_db_config()
    db_type = config["type"]

    # Build query (both PostgreSQL and MySQL use information_schema)
    query = """
    SELECT
        table_schema,
        table_name,
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns
    WHERE 1=1
    """
    params = []

    if schema_like:
        query += " AND table_schema LIKE %s"
        params.append(schema_like)

    if table_like:
        query += " AND table_name LIKE %s"
        params.append(table_like)

    query += " ORDER BY table_schema, table_name, ordinal_position"

    conn = None
    try:
        if db_type == "postgresql":
            try:
                import psycopg2
            except ImportError:
                raise GuardError("psycopg2 not installed. Install with: pip install psycopg2-binary")

            conn = psycopg2.connect(config["url"])
            cursor = conn.cursor()

        elif db_type == "mysql":
            try:
                import pymysql
            except ImportError:
                raise GuardError("PyMySQL not installed. Install with: pip install pymysql")

            # Parse MySQL URL (same logic as db_query_ro)
            url = config["url"]
            if url.startswith("mysql://"):
                url = url.replace("mysql://", "")
            elif url.startswith("mysql+pymysql://"):
                url = url.replace("mysql+pymysql://", "")

            if "@" in url:
                auth, rest = url.split("@", 1)
                if ":" in auth:
                    user, password = auth.split(":", 1)
                else:
                    user, password = auth, ""
            else:
                user, password = "", ""

            if "/" in rest:
                host_port, database = rest.split("/", 1)
            else:
                host_port, database = rest, ""

            if ":" in host_port:
                host, port = host_port.split(":", 1)
                port = int(port)
            else:
                host, port = host_port, 3306

            conn = pymysql.connect(
                host=host,
                port=port,
                user=user or None,
                password=password or None,
                database=database,
                cursorclass=pymysql.cursors.DictCursor
            )
            cursor = conn.cursor()
        else:
            raise GuardError(f"Unsupported database type: {db_type}")

        cursor.execute(query, params)

        tables = {}
        for row in cursor.fetchall():
            # Handle both tuple (PostgreSQL) and dict (MySQL) results
            if isinstance(row, dict):
                schema = row["table_schema"]
                table = row["table_name"]
                column = row["column_name"]
                dtype = row["data_type"]
                nullable = row["is_nullable"]
            else:
                schema, table, column, dtype, nullable = row

            key = f"{schema}.{table}"
            if key not in tables:
                tables[key] = {
                    "schema": schema,
                    "table": table,
                    "columns": [],
                }
            tables[key]["columns"].append({
                "name": column,
                "type": dtype,
                "nullable": nullable == "YES",
            })

        # Cap tables
        table_list = list(tables.values())[:max_tables]

        return {
            "tables": table_list,
            "table_count": len(table_list),
        }
    except Exception as e:
        raise GuardError(f"Database introspect failed: {e}")
    finally:
        if conn:
            conn.close()
