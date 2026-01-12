"""Kafka tools for MCP server."""

from typing import Annotated, Any, Optional, Union

from fastmcp import FastMCP
from pydantic import Field

from mcpkit.core import json_tools, kafka_client
from mcpkit.core.guards import GuardError
from mcpkit.server_models import (
    KafkaConsumeResponse,
    KafkaDescribeTopicResponse,
    KafkaFilterResponse,
    KafkaGroupResponse,
    KafkaOffsetsResponse,
    KafkaPartitionInfo,
    PartitionInfo,
    TopicListResponse,
)
from mcpkit.server_utils import _parse_list_param, _to_int


def register_kafka_tools(mcp: FastMCP):
    """Register Kafka tools with MCP server."""
    
    @mcp.tool()
    def kafka_list_topics(
        bootstrap_servers: Annotated[
            Optional[str],
            Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
        ] = None,
    ) -> TopicListResponse:
        """List all Kafka topics."""
        result = kafka_client.kafka_list_topics(bootstrap_servers)
        return TopicListResponse(**result)

    @mcp.tool()
    def kafka_offsets(
        topic: Annotated[str, Field(description="Topic name")],
        group_id: Annotated[Optional[str], Field(description="Optional consumer group ID (if provided, returns committed offsets)")] = None,
        bootstrap_servers: Annotated[
            Optional[str],
            Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
        ] = None,
    ) -> KafkaOffsetsResponse:
        """Get Kafka topic offsets."""
        result = kafka_client.kafka_offsets(topic, group_id, bootstrap_servers)
        partitions = {int(k): PartitionInfo(**v) for k, v in result["partitions"].items()}
        return KafkaOffsetsResponse(topic=result["topic"], partitions=partitions)

    @mcp.tool()
    def kafka_consume_batch(
        topic: Annotated[str, Field(description="Topic name")],
        partition: Annotated[Optional[Union[int, str]], Field(description="Optional partition number. If None, consumes from all partitions")] = None,
        from_offset: Annotated[Optional[Union[int, str]], Field(description="Optional starting offset. If None, starts from beginning")] = None,
        max_records: Annotated[Optional[Union[int, str]], Field(description="Optional max records to consume. Default: MCPKIT_MAX_RECORDS")] = None,
        timeout_secs: Annotated[Optional[Union[int, str]], Field(description="Optional timeout in seconds. Default: MCPKIT_TIMEOUT_SECS")] = None,
        bootstrap_servers: Annotated[
            Optional[str],
            Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
        ] = None,
        schema_registry_url: Annotated[
            Optional[str],
            Field(description="Optional Schema Registry URL for Avro decoding. If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var")
        ] = None,
    ) -> KafkaConsumeResponse:
        """Consume batch of records from Kafka topic and store as dataset."""
        # Convert string inputs to int (MCP client may pass strings)
        partition = _to_int(partition, None)
        from_offset = _to_int(from_offset, None)
        max_records = _to_int(max_records, None)
        timeout_secs = _to_int(timeout_secs, None)
        result = kafka_client.kafka_consume_batch(topic, partition, from_offset, max_records, timeout_secs, bootstrap_servers, schema_registry_url)  # type: ignore[arg-type]
        return KafkaConsumeResponse(**result)

    @mcp.tool()
    def kafka_filter(
        records: Annotated[str, Field(description="JSON string of Kafka record dicts (from kafka_consume_batch)")],
        predicate_jmes: Annotated[str, Field(description="JMESPath expression (evaluates to truthy to include record)")],
    ) -> KafkaFilterResponse:
        """Filter Kafka records using JMESPath predicate."""
        records_list = _parse_list_param(records, [])
        if not isinstance(records_list, list):
            raise GuardError(f"records must be a list, got {type(records_list)}")
        filtered = []
        for record in records_list:
            try:
                result = json_tools.jq_transform(record, predicate_jmes)
                if result.get("result"):
                    filtered.append(record)
            except Exception:
                continue
        return KafkaFilterResponse(records=filtered, count=len(filtered))

    @mcp.tool()
    def kafka_flatten(
        dataset_id: Annotated[str, Field(description="Input dataset ID containing Kafka records")],
        out_dataset_id: Annotated[Optional[str], Field(description="Optional output dataset ID (auto-generated if None)")] = None,
    ) -> dict:
        """
        Flatten Kafka records from a dataset generically.

        Parses JSON values and flattens nested structures into columns.
        Works with any Kafka event structure. Input dataset should contain Kafka records
        with decoded key, value, and headers.

        Returns:
            dict with dataset_id, columns, rows, and record_count.
        """
        result = kafka_client.kafka_flatten_dataset(dataset_id, out_dataset_id)
        return result

    @mcp.tool()
    def kafka_groupby_key(
        records: Annotated[str, Field(description="JSON string of Kafka record dicts")],
        key_jmes: Annotated[str, Field(description="JMESPath expression to extract grouping key")],
        max_groups: Annotated[int, Field(description="Maximum number of groups to return. Default: 200")] = 200,
    ) -> KafkaGroupResponse:
        """Group Kafka records by key extracted via JMESPath."""
        records_list = _parse_list_param(records, [])
        if not isinstance(records_list, list):
            raise GuardError(f"records must be a list, got {type(records_list)}")
        groups: dict[str, list[Any]] = {}
        for record in records_list:
            try:
                result = json_tools.jq_transform(record, key_jmes)
                key = result.get("result")
                if key is not None:
                    key_str = str(key)
                    if key_str not in groups:
                        groups[key_str] = []
                    groups[key_str].append(record)
            except Exception:
                continue
        # Cap groups
        group_list = dict(list(groups.items())[:max_groups])
        return KafkaGroupResponse(groups=group_list, group_count=len(group_list))

    @mcp.tool()
    def kafka_consume_tail(
        topic: Annotated[str, Field(description="Topic name")],
        n_messages: Annotated[Union[int, str], Field(description="Number of messages to consume from tail (default: 10)")] = 10,
        partition: Annotated[Optional[Union[int, str]], Field(description="Optional partition number. If None, consumes from all partitions")] = None,
        bootstrap_servers: Annotated[
            Optional[str],
            Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
        ] = None,
        schema_registry_url: Annotated[
            Optional[str],
            Field(description="Optional Schema Registry URL for Avro decoding. If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var")
        ] = None,
    ) -> KafkaConsumeResponse:
        """Consume last N messages from a Kafka topic (for debugging)."""
        n_messages = _to_int(n_messages, 10)  # type: ignore[arg-type]
        partition = _to_int(partition, None)  # type: ignore[arg-type]
        result = kafka_client.kafka_consume_tail(topic, n_messages, partition, bootstrap_servers, schema_registry_url)  # type: ignore[arg-type]
        return KafkaConsumeResponse(**result)

    @mcp.tool()
    def kafka_describe_topic(
        topic: Annotated[str, Field(description="Topic name")],
        bootstrap_servers: Annotated[
            Optional[str],
            Field(description="Optional Kafka bootstrap servers (comma-separated). If None, uses MCPKIT_KAFKA_BOOTSTRAP env var")
        ] = None,
    ) -> KafkaDescribeTopicResponse:
        """Get topic configuration, partitions, and replication information."""
        result = kafka_client.kafka_describe_topic(topic, bootstrap_servers)
        partitions = [KafkaPartitionInfo(**p) for p in result["partitions"]]
        return KafkaDescribeTopicResponse(
            topic=result["topic"],
            partitions=partitions,
            partition_count=result["partition_count"],
            replication_factor=result["replication_factor"],
            config=result["config"],
        )

