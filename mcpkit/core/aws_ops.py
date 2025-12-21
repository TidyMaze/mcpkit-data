"""AWS operations (Athena, S3, Glue)."""

import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from .guards import GuardError, cap_rows, get_max_rows, validate_sql_readonly


def get_athena_client(region: Optional[str] = None):
    """Get Athena boto3 client."""
    if region:
        return boto3.client("athena", region_name=region)
    # Try to get region from env
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if region:
        return boto3.client("athena", region_name=region)
    return boto3.client("athena")


def get_s3_client():
    """Get S3 boto3 client."""
    return boto3.client("s3")


def get_glue_client():
    """Get Glue boto3 client."""
    return boto3.client("glue")


def athena_start_query(
    sql: str,
    database: str,
    workgroup: Optional[str] = None,
    output_s3: Optional[str] = None,
    validate_readonly: bool = True,
    region: Optional[str] = None
) -> dict:
    """
    Start Athena query execution.
    
    Args:
        sql: SQL query string
        database: Athena database name
        workgroup: Optional Athena workgroup
        output_s3: Optional S3 path for results
        validate_readonly: If True (default), validates query is read-only (SELECT/WITH only)
        region: Optional AWS region (default: from AWS_REGION env var or AWS config)
    
    Returns:
        dict with query_execution_id and status
    """
    # Validate query is read-only by default
    if validate_readonly:
        sql = validate_sql_readonly(sql)
    
    client = get_athena_client(region)
    
    config = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": database},
    }
    
    if workgroup:
        config["WorkGroup"] = workgroup
    
    if output_s3:
        config["ResultConfiguration"] = {"OutputLocation": output_s3}
    
    try:
        response = client.start_query_execution(**config)
        return {
            "query_execution_id": response["QueryExecutionId"],
            "status": "RUNNING",
        }
    except ClientError as e:
        raise GuardError(f"Athena query start failed: {e}")


def athena_poll_query(query_execution_id: str, region: Optional[str] = None) -> dict:
    """Poll Athena query execution status."""
    client = get_athena_client(region)
    
    try:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        execution = response["QueryExecution"]
        status = execution["Status"]["State"]
        
        result = {
            "query_execution_id": query_execution_id,
            "status": status,
        }
        
        if status == "SUCCEEDED":
            result["data_scanned_bytes"] = execution.get("Statistics", {}).get("DataScannedInBytes", 0)
            result["execution_time_ms"] = execution.get("Statistics", {}).get("TotalExecutionTimeInMillis", 0)
            result["error"] = None
        elif status == "FAILED":
            result["error"] = execution["Status"].get("StateChangeReason", "Unknown error")
            result["data_scanned_bytes"] = 0
            result["execution_time_ms"] = 0
        else:
            # QUEUED, RUNNING, CANCELLED
            result["data_scanned_bytes"] = 0
            result["execution_time_ms"] = 0
            result["error"] = None
        
        return result
    except ClientError as e:
        raise GuardError(f"Athena poll failed: {e}")


def athena_get_results(query_execution_id: str, max_rows: Optional[int] = None, region: Optional[str] = None) -> dict:
    """Get Athena query results."""
    client = get_athena_client(region)
    
    if max_rows is None:
        max_rows = get_max_rows()
    
    try:
        # Get first page
        response = client.get_query_results(
            QueryExecutionId=query_execution_id,
            MaxResults=min(max_rows, 1000),  # Athena max is 1000 per page
        )
        
        result_set = response["ResultSet"]
        columns = [col["Name"] for col in result_set["ResultSetMetadata"]["ColumnInfo"]]
        
        rows = []
        for row in result_set["Rows"][1:]:  # Skip header row
            if len(rows) >= max_rows:
                break
            values = [cell.get("VarCharValue", "") for cell in row["Data"]]
            rows.append(values)
        
        # Paginate if needed
        next_token = response.get("NextToken")
        while next_token and len(rows) < max_rows:
            response = client.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=next_token,
                MaxResults=min(max_rows - len(rows), 1000),
            )
            result_set = response["ResultSet"]
            for row in result_set["Rows"]:
                if len(rows) >= max_rows:
                    break
                values = [cell.get("VarCharValue", "") for cell in row["Data"]]
                rows.append(values)
            next_token = response.get("NextToken")
        
        return {
            "columns": columns,
            "rows": cap_rows(rows, max_rows),
            "row_count": len(rows),
        }
    except ClientError as e:
        raise GuardError(f"Athena get results failed: {e}")


def athena_explain(
    sql: str,
    database: str,
    workgroup: Optional[str] = None,
    output_s3: Optional[str] = None
) -> dict:
    """Explain Athena query plan."""
    explain_sql = f"EXPLAIN {sql}"
    return athena_start_query(explain_sql, database, workgroup, output_s3)


def athena_partitions_list(database: str, table: str, max_partitions: int = 200) -> dict:
    """List Athena table partitions."""
    client = get_glue_client()
    
    try:
        response = client.get_partitions(
            DatabaseName=database,
            TableName=table,
            MaxResults=min(max_partitions, 1000),
        )
        
        partitions = []
        for partition in response.get("Partitions", [])[:max_partitions]:
            partitions.append({
                "values": partition.get("Values", []),
                "location": partition.get("StorageDescriptor", {}).get("Location", ""),
                "parameters": partition.get("Parameters", {}),
            })
        
        return {
            "database": database,
            "table": table,
            "partitions": partitions,
            "partition_count": len(partitions),
        }
    except ClientError as e:
        raise GuardError(f"Athena partitions list failed: {e}")


def athena_repair_table(
    database: str,
    table: str,
    workgroup: Optional[str] = None,
    output_s3: Optional[str] = None
) -> dict:
    """Repair Athena table (MSCK REPAIR TABLE)."""
    sql = f"MSCK REPAIR TABLE {database}.{table}"
    # MSCK REPAIR is a metadata operation, skip read-only validation
    return athena_start_query(sql, database, workgroup, output_s3, validate_readonly=False)


def athena_ctas_export(
    database: str,
    dest_table: str,
    select_sql: str,
    output_s3: str,
    format: str = "PARQUET",
    workgroup: Optional[str] = None
) -> dict:
    """Create table as select (CTAS) for export."""
    # Validate the SELECT part is read-only, but allow CREATE TABLE for export
    # Note: This is a metadata operation (creates table in S3), not a data mutation
    validate_sql_readonly(select_sql)  # Validate SELECT part
    sql = f"CREATE TABLE {database}.{dest_table} WITH (format='{format}', external_location='{output_s3}') AS {select_sql}"
    # Skip full validation since we already validated the SELECT part
    return athena_start_query(sql, database, workgroup, output_s3, validate_readonly=False)


def s3_list_prefix(bucket: str, prefix: str, max_keys: int = 200) -> dict:
    """List S3 objects with prefix."""
    client = get_s3_client()
    
    try:
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=min(max_keys, 1000),
        )
        
        objects = []
        for obj in response.get("Contents", [])[:max_keys]:
            last_modified = obj["LastModified"]
            if hasattr(last_modified, "isoformat"):
                last_modified_str = last_modified.isoformat()
            else:
                last_modified_str = str(last_modified)
            objects.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": last_modified_str,
                "etag": obj["ETag"],
            })
        
        return {
            "bucket": bucket,
            "prefix": prefix,
            "objects": objects,
            "object_count": len(objects),
        }
    except ClientError as e:
        raise GuardError(f"S3 list failed: {e}")

