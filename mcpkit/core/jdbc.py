"""JDBC read-only query helpers."""

import os
from typing import Any, Optional

import jaydebeapi

from .guards import GuardError, cap_rows, get_max_rows, validate_jdbc_query


def get_jdbc_config() -> dict:
    """Get JDBC configuration from environment."""
    driver = os.getenv("MCPKIT_JDBC_DRIVER_CLASS")
    url = os.getenv("MCPKIT_JDBC_URL")
    jars = os.getenv("MCPKIT_JDBC_JARS")
    
    if not driver or not url or not jars:
        raise GuardError("MCPKIT_JDBC_DRIVER_CLASS, MCPKIT_JDBC_URL, and MCPKIT_JDBC_JARS must be set")
    
    config = {
        "driver": driver,
        "url": url,
        "jars": jars.split(os.pathsep),
    }
    
    user = os.getenv("MCPKIT_JDBC_USER")
    password = os.getenv("MCPKIT_JDBC_PASSWORD")
    
    if user:
        config["user"] = user
    if password:
        config["password"] = password
    
    return config


def jdbc_query_ro(query: str, params: Optional[list[Any]] = None) -> dict:
    """
    Execute read-only JDBC query.
    Returns dict with columns and rows.
    """
    validate_jdbc_query(query)
    config = get_jdbc_config()
    
    conn = None
    try:
        conn = jaydebeapi.connect(
            config["driver"],
            config["url"],
            config.get("user"),
            config.get("password"),
            config["jars"],
        )
        
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Get columns
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Fetch rows (capped)
        rows = cursor.fetchall()
        rows = cap_rows(rows, get_max_rows())
        
        # Convert rows to lists (tuples to lists)
        rows_list = [list(row) for row in rows]
        
        return {
            "columns": columns,
            "rows": rows_list,
            "row_count": len(rows_list),
        }
    except Exception as e:
        raise GuardError(f"JDBC query failed: {e}")
    finally:
        if conn:
            conn.close()


def jdbc_introspect(
    schema_like: Optional[str] = None,
    table_like: Optional[str] = None,
    max_tables: int = 200
) -> dict:
    """
    Introspect JDBC database schema.
    Returns dict with tables and columns.
    """
    config = get_jdbc_config()
    
    # Build query
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
        query += " AND table_schema LIKE ?"
        params.append(schema_like)
    
    if table_like:
        query += " AND table_name LIKE ?"
        params.append(table_like)
    
    query += " ORDER BY table_schema, table_name, ordinal_position"
    
    conn = None
    try:
        conn = jaydebeapi.connect(
            config["driver"],
            config["url"],
            config.get("user"),
            config.get("password"),
            config["jars"],
        )
        
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        tables = {}
        for row in cursor.fetchall():
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
        raise GuardError(f"JDBC introspect failed: {e}")
    finally:
        if conn:
            conn.close()

