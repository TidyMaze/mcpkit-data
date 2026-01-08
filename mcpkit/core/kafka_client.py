"""Kafka client helpers."""

import base64
import json
import os
from typing import Any, Optional

from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition
from kafka.errors import KafkaError

# Import compression-related errors for better error handling
try:
    from kafka.errors import UnsupportedCodecError, UnsupportedCompressionTypeError
except ImportError:
    # Fallback for older kafka-python versions
    UnsupportedCodecError = None
    UnsupportedCompressionTypeError = None

from .guards import GuardError, cap_records, get_max_records, get_timeout_secs
from .decode import avro_decode


def _decode_kafka_value(value_bytes: bytes, schema_registry_url: Optional[str] = None) -> Optional[str]:
    """
    Decode Kafka message value, automatically detecting and decoding Avro if present.
    
    This function is called during Kafka consumption to decode messages immediately.
    Avro messages are decoded using the Schema Registry before storing in the dataset.
    
    Args:
        value_bytes: Raw bytes from Kafka message
        schema_registry_url: Optional Schema Registry URL for Avro decoding.
            If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var.
        
    Returns:
        JSON string of decoded value (or UTF-8 string if not Avro), or None if empty.
        Avro messages are decoded to JSON strings during consumption.
        
    Raises:
        GuardError: If Avro is detected but Schema Registry URL is not provided.
    """
    if not value_bytes:
        return None
    
    # Ensure we have bytes, not a string
    if isinstance(value_bytes, str):
        # If it's already a string (shouldn't happen, but handle it)
        # Try to detect if it's binary data that was incorrectly decoded
        if value_bytes.startswith('\x00\x00\x00\x00') or '\x00' in value_bytes[:10]:
            # This was binary data incorrectly decoded - we can't recover it
            # Return as-is (will be base64 encoded later)
            return value_bytes
        return value_bytes
    
    # Check for Confluent Avro format: magic byte 0 + schema_id (4 bytes) + payload
    if len(value_bytes) >= 5 and value_bytes[0] == 0:
        # Use provided URL or fall back to environment variable
        registry_url = schema_registry_url or os.getenv("MCPKIT_SCHEMA_REGISTRY_URL")
        if registry_url:
            # Base64 encode for avro_decode
            value_base64 = base64.b64encode(value_bytes).decode("utf-8")
            decoded_result = avro_decode(value_base64, schema_registry_url=registry_url)
            decoded_data = decoded_result.get("decoded", {})
            # Convert to JSON string with datetime handling
            from datetime import datetime, date, time
            class DateTimeEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, (datetime, date, time)):
                        return obj.isoformat()
                    return super().default(obj)
            return json.dumps(decoded_data, cls=DateTimeEncoder)
        else:
            # Avro detected but no Schema Registry URL provided - cannot decode
            raise GuardError("Avro message detected but MCPKIT_SCHEMA_REGISTRY_URL not set. Provide schema_registry_url parameter or set environment variable.")
    
    # Not Avro format - try UTF-8 decode
    try:
        decoded = value_bytes.decode("utf-8", errors="strict")
        # Check if it looks like valid UTF-8 text (not binary)
        # If it has many control characters, it's probably binary
        control_chars = sum(1 for c in decoded if ord(c) < 32 and c not in '\t\n\r')
        if control_chars > len(decoded) * 0.1:  # More than 10% control chars
            # Probably binary data, base64 encode it
            return base64.b64encode(value_bytes).decode("utf-8")
        return decoded
    except (AttributeError, UnicodeDecodeError):
        # UTF-8 decode failed - base64 encode binary data
        return base64.b64encode(value_bytes).decode("utf-8")


def get_kafka_config(bootstrap_servers: Optional[str] = None) -> dict:
    """
    Get Kafka configuration from parameter or environment.
    
    Args:
        bootstrap_servers: Optional bootstrap servers (comma-separated).
            If None, uses MCPKIT_KAFKA_BOOTSTRAP env var.
    """
    bootstrap = bootstrap_servers or os.getenv("MCPKIT_KAFKA_BOOTSTRAP")
    if not bootstrap:
        raise GuardError("Kafka bootstrap servers not provided. Set bootstrap_servers parameter or MCPKIT_KAFKA_BOOTSTRAP env var")
    
    config = {
        "bootstrap_servers": bootstrap.split(","),
        "consumer_timeout_ms": get_timeout_secs() * 1000,
    }
    
    # SASL config if provided
    security_protocol = os.getenv("MCPKIT_KAFKA_SECURITY_PROTOCOL")
    if security_protocol:
        config["security_protocol"] = security_protocol
        config["sasl_mechanism"] = os.getenv("MCPKIT_KAFKA_SASL_MECHANISM", "PLAIN")
        config["sasl_plain_username"] = os.getenv("MCPKIT_KAFKA_SASL_USERNAME", "")
        config["sasl_plain_password"] = os.getenv("MCPKIT_KAFKA_SASL_PASSWORD", "")
    
    return config


def kafka_offsets(topic: str, group_id: Optional[str] = None, bootstrap_servers: Optional[str] = None) -> dict:
    """
    Get Kafka topic offsets.
    If group_id provided, get committed offsets for consumer group.
    Otherwise, get high water marks.
    """
    config = get_kafka_config(bootstrap_servers)
    # Reduce timeout for faster failure
    config["consumer_timeout_ms"] = 5000
    
    consumer = None
    
    try:
        # For kafka_offsets: disable auto-commit, use group_id only if explicitly provided
        # When group_id is None, no consumer group is created
        config["enable_auto_commit"] = False
        consumer = KafkaConsumer(**config, group_id=group_id)
        
        # Get partitions by assigning a dummy partition first to trigger metadata fetch
        # Then we can query for all partitions
        # Assign partition 0 to trigger metadata fetch (assuming at least one partition exists)
        dummy_tp = TopicPartition(topic, 0)
        consumer.assign([dummy_tp])
        # Poll to get metadata
        consumer.poll(timeout_ms=5000)
        
        # Now we can get all partitions
        partitions = consumer.partitions_for_topic(topic)
        
        if not partitions:
            return {"topic": topic, "partitions": {}}
        
        offsets = {}
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        
        # Reassign with all partitions
        consumer.assign(topic_partitions)
        
        # Get offsets using seek_to_end/beginning + position
        for tp in topic_partitions:
            try:
                consumer.seek_to_end(tp)
                high = consumer.position(tp)
                
                consumer.seek_to_beginning(tp)
                low = consumer.position(tp)
                
                committed = None
                if group_id:
                    try:
                        committed = consumer.committed(tp)
                    except Exception:
                        pass
                
                offsets[tp.partition] = {
                    "high_water_mark": high,
                    "low_water_mark": low,
                    "committed": committed,
                }
            except Exception as e:
                offsets[tp.partition] = {
                    "high_water_mark": 0,
                    "low_water_mark": 0,
                    "committed": None,
                }
        
        return {"topic": topic, "partitions": offsets}
    except KafkaError as e:
        # Check for compression codec errors and provide helpful message
        error_str = str(e).lower()
        if (UnsupportedCodecError and isinstance(e, UnsupportedCodecError)) or \
           (UnsupportedCompressionTypeError and isinstance(e, UnsupportedCompressionTypeError)) or \
           "snappy" in error_str and ("not found" in error_str or "libraries" in error_str):
            raise GuardError(
                f"Kafka compression codec error: {e}\n"
                f"To fix this, install python-snappy: pip install python-snappy\n"
                f"Or install with optional dependency: pip install mcpkit-data[kafka-snappy]"
            )
        raise GuardError(f"Kafka error: {e}")
    finally:
        if consumer:
            consumer.close()


def kafka_consume_batch(
    topic: str,
    partition: Optional[int] = None,
    from_offset: Optional[int] = None,
    max_records: Optional[int] = None,
    timeout_secs: Optional[int] = None,
    bootstrap_servers: Optional[str] = None,
    schema_registry_url: Optional[str] = None,
) -> dict:
    """
    Consume batch of records from Kafka topic and store as dataset.
    
    Avro messages are automatically decoded during consumption using the Schema Registry.
    The decoded JSON is stored directly in the dataset - no base64 encoding.
    
    Args:
        topic: Topic name
        partition: Optional partition number. If None, consumes from all partitions
        from_offset: Optional starting offset. If None, starts from beginning
        max_records: Optional max records to consume. Default: MCPKIT_MAX_RECORDS
        timeout_secs: Optional timeout in seconds. Default: MCPKIT_TIMEOUT_SECS
        bootstrap_servers: Optional Kafka bootstrap servers (comma-separated).
            If None, uses MCPKIT_KAFKA_BOOTSTRAP env var
        schema_registry_url: Optional Schema Registry URL for Avro decoding.
            If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var.
            Required if topic contains Avro messages.
    
    Returns:
        dict with dataset_id, record_count, and columns
    """
    """
    Consume batch of records from Kafka topic.
    Returns records with decoded key, value, and headers as UTF-8 strings.
    """
    config = get_kafka_config(bootstrap_servers)
    if timeout_secs:
        config["consumer_timeout_ms"] = timeout_secs * 1000
    
    # Set auto_offset_reset to 'earliest' to read from beginning if no offset specified
    if "auto_offset_reset" not in config:
        config["auto_offset_reset"] = "earliest"
    
    # Disable auto-commit and use no group_id to avoid creating consumer groups
    # This prevents offset commits and lag visibility in AKHQ
    config["enable_auto_commit"] = False
    config["group_id"] = None
    
    consumer = None
    try:
        if partition is not None:
            # Assign specific partition
            tp = TopicPartition(topic, partition)
            consumer = KafkaConsumer(**config)
            consumer.assign([tp])
            
            if from_offset is not None:
                consumer.seek(tp, from_offset)
            else:
                # Seek to beginning if no offset specified
                consumer.seek_to_beginning(tp)
        else:
            # Subscribe to topic (with group_id=None and enable_auto_commit=False to avoid commits)
            consumer = KafkaConsumer(topic, **config)
            
            # Wait for partition assignment with timeout
            partitions = None
            import time
            assignment_start = time.time()
            assignment_timeout_ms = 5000  # 5 seconds max for assignment
            for _ in range(20):  # Wait up to 5 seconds (20 * 250ms)
                partitions = consumer.assignment()
                if partitions:
                    break
                elapsed = (time.time() - assignment_start) * 1000
                if elapsed >= assignment_timeout_ms:
                    break
                consumer.poll(timeout_ms=250)
            
            # Seek to beginning of all assigned partitions
            if partitions:
                consumer.seek_to_beginning(*partitions)
        
        records: list[dict[str, Any]] = []
        limit = max_records if max_records else get_max_records()
        
        # Use poll with timeout instead of iterator to avoid hanging
        import time
        start_time = time.time()
        # Always use a timeout - default if not provided
        timeout_ms = (timeout_secs * 1000) if timeout_secs else (get_timeout_secs() * 1000)
        poll_timeout_ms = min(1000, timeout_ms)  # Poll in 1s intervals
        
        # Track consecutive empty polls to detect end of topic
        consecutive_empty_polls = 0
        max_empty_polls = 3  # Stop after 3 consecutive empty polls (3 seconds of no messages)
        
        while len(records) < limit:
            elapsed_ms = (time.time() - start_time) * 1000
            # Always check timeout, not just when timeout_secs is provided
            if elapsed_ms >= timeout_ms:
                break
            
            # Poll for messages with short timeout
            remaining_timeout = max(100, timeout_ms - elapsed_ms)
            poll_timeout = int(min(poll_timeout_ms, remaining_timeout))
            message_batch = consumer.poll(timeout_ms=poll_timeout, max_records=limit - len(records))
            
            if not message_batch:
                # No messages in this poll
                consecutive_empty_polls += 1
                elapsed_ms = (time.time() - start_time) * 1000
                
                # Stop if timeout reached
                if elapsed_ms >= timeout_ms:
                    break
                
                # Stop after consecutive empty polls (indicates no more messages available)
                # This prevents waiting the full timeout when at end of topic
                if consecutive_empty_polls >= max_empty_polls:
                    # We've polled multiple times with no messages - stop now
                    break
                
                continue
            
            # Reset empty poll counter when we get messages
            consecutive_empty_polls = 0
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    if len(records) >= limit:
                        break
                    
                    # Decode key as UTF-8 string
                    key_str = None
                    if message.key:
                        try:
                            key_str = message.key.decode("utf-8", errors="replace")
                        except (AttributeError, UnicodeDecodeError):
                            key_str = None
                    
                    # Decode value (automatically detects and decodes Avro)
                    value_str = None
                    if message.value:
                        value_str = _decode_kafka_value(message.value, schema_registry_url=schema_registry_url)
                    
                    # Decode headers
                    headers = {}
                    if message.headers:
                        for k, v in message.headers:
                            if v:
                                try:
                                    headers[k] = v.decode("utf-8", errors="replace")
                                except (AttributeError, UnicodeDecodeError):
                                    headers[k] = None
                            else:
                                headers[k] = None
                    
                    record = {
                        "partition": message.partition,
                        "offset": message.offset,
                        "timestamp": message.timestamp,
                        "key": key_str,
                        "value": value_str,
                        "headers": headers,
                    }
                    records.append(record)
        
        records = cap_records(records, max_records)
        
        # Store as dataset
        from .registry import dataset_put_rows
        from datetime import datetime
        
        if records:
            # Convert records to rows
            columns = ["partition", "offset", "timestamp", "key", "value", "headers"]
            rows = []
            for record in records:
                # Convert headers dict to JSON string for Parquet compatibility
                # Empty dict causes Parquet struct type error, so always use JSON string
                headers = record.get("headers", {})
                if isinstance(headers, dict):
                    headers_str = json.dumps(headers)
                elif headers is None:
                    headers_str = "{}"
                else:
                    headers_str = str(headers)
                
                row = [
                    record.get("partition"),
                    record.get("offset"),
                    record.get("timestamp"),
                    record.get("key"),
                    record.get("value"),
                    headers_str,
                ]
                rows.append(row)
            
            # Generate dataset_id
            dataset_id = f"kafka_{topic}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            
            # Store dataset
            dataset_result = dataset_put_rows(columns, rows, dataset_id)
            
            return {
                "dataset_id": dataset_result["dataset_id"],
                "record_count": len(records),
                "columns": columns,
            }
        else:
            # Empty result - still create dataset
            dataset_id = f"kafka_{topic}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            columns = ["partition", "offset", "timestamp", "key", "value", "headers"]
            dataset_result = dataset_put_rows(columns, [], dataset_id)
            
            return {
                "dataset_id": dataset_result["dataset_id"],
                "record_count": 0,
                "columns": columns,
            }
    except KafkaError as e:
        # Check for compression codec errors and provide helpful message
        error_str = str(e).lower()
        if (UnsupportedCodecError and isinstance(e, UnsupportedCodecError)) or \
           (UnsupportedCompressionTypeError and isinstance(e, UnsupportedCompressionTypeError)) or \
           "snappy" in error_str and ("not found" in error_str or "libraries" in error_str):
            raise GuardError(
                f"Kafka compression codec error: {e}\n"
                f"To fix this, install python-snappy: pip install python-snappy\n"
                f"Or install with optional dependency: pip install mcpkit-data[kafka-snappy]"
            )
        raise GuardError(f"Kafka error: {e}")
    finally:
        if consumer:
            consumer.close()


def kafka_consume_tail(
    topic: str,
    n_messages: int = 10,
    partition: Optional[int] = None,
    bootstrap_servers: Optional[str] = None,
    schema_registry_url: Optional[str] = None,
) -> dict:
    """
    Consume last N messages from a Kafka topic (for debugging).
    
    Avro messages are automatically decoded during consumption using the Schema Registry.
    The decoded JSON is stored directly in the dataset - no base64 encoding.
    
    Args:
        topic: Topic name
        n_messages: Number of messages to consume from tail (default: 10)
        partition: Optional partition number. If None, consumes from all partitions
        bootstrap_servers: Optional Kafka bootstrap servers (comma-separated).
            If None, uses MCPKIT_KAFKA_BOOTSTRAP env var
        schema_registry_url: Optional Schema Registry URL for Avro decoding.
            If None, uses MCPKIT_SCHEMA_REGISTRY_URL env var.
            Required if topic contains Avro messages.
    
    Returns:
        dict with dataset_id and record_count
    """
    config = get_kafka_config(bootstrap_servers)
    config["enable_auto_commit"] = False
    config["group_id"] = None  # No consumer group for tailing
    
    consumer = None
    try:
        consumer = KafkaConsumer(**config)
        
        # Get partitions
        if partition is not None:
            partitions = [TopicPartition(topic, partition)]
        else:
            # Get all partitions
            dummy_tp = TopicPartition(topic, 0)
            consumer.assign([dummy_tp])
            consumer.poll(timeout_ms=5000)
            all_partitions = consumer.partitions_for_topic(topic)
            if not all_partitions:
                raise GuardError(f"Topic {topic} has no partitions")
            partitions = [TopicPartition(topic, p) for p in all_partitions]
        
        consumer.assign(partitions)
        
        # Seek to end for each partition
        for tp in partitions:
            consumer.seek_to_end(tp)
        
        # Calculate how many messages to read per partition
        messages_per_partition = max(1, n_messages // len(partitions))
        
        records = []
        for tp in partitions:
            # Get current position (end offset)
            end_offset = consumer.position(tp)
            
            # Calculate start offset
            start_offset = max(0, end_offset - messages_per_partition)
            
            # Seek to start offset
            consumer.seek(tp, start_offset)
            
            # Poll messages
            poll_timeout = 5000
            message_batch = consumer.poll(timeout_ms=poll_timeout, max_records=messages_per_partition)
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    # Decode key as UTF-8 string
                    key_str = None
                    if message.key:
                        try:
                            key_str = message.key.decode("utf-8", errors="replace")
                        except (AttributeError, UnicodeDecodeError):
                            key_str = None
                    
                    # Decode value (automatically detects and decodes Avro)
                    value_str = None
                    if message.value:
                        value_str = _decode_kafka_value(message.value, schema_registry_url=schema_registry_url)
                    
                    # Decode headers
                    headers = {}
                    if message.headers:
                        for k, v in message.headers:
                            if v:
                                try:
                                    headers[k] = v.decode("utf-8", errors="replace")
                                except (AttributeError, UnicodeDecodeError):
                                    headers[k] = None
                            else:
                                headers[k] = None
                    
                    record = {
                        "partition": message.partition,
                        "offset": message.offset,
                        "timestamp": message.timestamp,
                        "key": key_str,
                        "value": value_str,
                        "headers": headers,
                    }
                    records.append(record)
        
        # Sort by offset descending (most recent first) and limit
        records.sort(key=lambda x: (x["partition"], -x["offset"]))
        records = records[:n_messages]
        records.reverse()  # Reverse to show oldest first
        
        records = cap_records(records, n_messages)
        
        # Store as dataset
        from .registry import dataset_put_rows
        from datetime import datetime
        
        if records:
            columns = ["partition", "offset", "timestamp", "key", "value", "headers"]
            rows = []
            for record in records:
                headers = record.get("headers", {})
                if isinstance(headers, dict):
                    headers_str = json.dumps(headers)
                elif headers is None:
                    headers_str = "{}"
                else:
                    headers_str = str(headers)
                
                row = [
                    record.get("partition"),
                    record.get("offset"),
                    record.get("timestamp"),
                    record.get("key"),
                    record.get("value"),
                    headers_str,
                ]
                rows.append(row)
            
            dataset_id = f"kafka_tail_{topic}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            dataset_result = dataset_put_rows(columns, rows, dataset_id)
            
            return {
                "dataset_id": dataset_result["dataset_id"],
                "record_count": len(records),
                "columns": columns,
            }
        else:
            dataset_id = f"kafka_tail_{topic}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            columns = ["partition", "offset", "timestamp", "key", "value", "headers"]
            dataset_result = dataset_put_rows(columns, [], dataset_id)
            
            return {
                "dataset_id": dataset_result["dataset_id"],
                "record_count": 0,
                "columns": columns,
            }
    except KafkaError as e:
        # Check for compression codec errors and provide helpful message
        error_str = str(e).lower()
        if (UnsupportedCodecError and isinstance(e, UnsupportedCodecError)) or \
           (UnsupportedCompressionTypeError and isinstance(e, UnsupportedCompressionTypeError)) or \
           "snappy" in error_str and ("not found" in error_str or "libraries" in error_str):
            raise GuardError(
                f"Kafka compression codec error: {e}\n"
                f"To fix this, install python-snappy: pip install python-snappy\n"
                f"Or install with optional dependency: pip install mcpkit-data[kafka-snappy]"
            )
        raise GuardError(f"Kafka error: {e}")
    finally:
        if consumer:
            consumer.close()


def kafka_list_topics(bootstrap_servers: Optional[str] = None) -> dict:
    """
    List all Kafka topics.
    
    Args:
        bootstrap_servers: Optional Kafka bootstrap servers (comma-separated).
            If None, uses MCPKIT_KAFKA_BOOTSTRAP env var
    
    Returns:
        dict: {
            "topics": list[str],
            "topic_count": int
        }
    """
    config = get_kafka_config(bootstrap_servers)
    config["consumer_timeout_ms"] = 10000  # 10 second timeout
    config["enable_auto_commit"] = False
    config["group_id"] = None  # No consumer group
    
    consumer = None
    try:
        # Use KafkaConsumer to get cluster metadata
        consumer = KafkaConsumer(**config)
        
        # Subscribe to all topics using pattern to trigger metadata fetch
        # This forces the consumer to fetch metadata for all topics
        consumer.subscribe(pattern=r'.*')
        
        # Poll to trigger metadata fetch and connection
        consumer.poll(timeout_ms=5000)
        
        # Wait a bit for metadata to be fetched
        import time
        time.sleep(1)
        
        # Access cluster metadata from consumer's internal client
        # After polling, the consumer has cluster metadata
        cluster = consumer._client.cluster
        topics = list(cluster.topics())
        
        return {
            "topics": sorted(topics),
            "topic_count": len(topics)
        }
    except KafkaError as e:
        # Check for compression codec errors and provide helpful message
        error_str = str(e).lower()
        if (UnsupportedCodecError and isinstance(e, UnsupportedCodecError)) or \
           (UnsupportedCompressionTypeError and isinstance(e, UnsupportedCompressionTypeError)) or \
           "snappy" in error_str and ("not found" in error_str or "libraries" in error_str):
            raise GuardError(
                f"Kafka compression codec error: {e}\n"
                f"To fix this, install python-snappy: pip install python-snappy\n"
                f"Or install with optional dependency: pip install mcpkit-data[kafka-snappy]"
            )
        raise GuardError(f"Kafka error listing topics: {e}")
    except Exception as e:
        raise GuardError(f"Error listing topics: {e}")
    finally:
        if consumer:
            consumer.close()


def kafka_describe_topic(topic: str, bootstrap_servers: Optional[str] = None) -> dict:
    """
    Get topic configuration, partitions, and replication information.
    
    Args:
        topic: Topic name
        bootstrap_servers: Optional Kafka bootstrap servers (comma-separated).
            If None, uses MCPKIT_KAFKA_BOOTSTRAP env var
    
    Returns:
        dict with topic metadata including partitions, replication, and config
    """
    config = get_kafka_config(bootstrap_servers)
    
    consumer = None
    admin_client = None
    
    try:
        # Use consumer to get partition metadata
        # For existing topics, we'll subscribe and check partitions
        # For nonexistent topics, we check first to avoid auto-creation (if AdminClient available)
        consumer_config = config.copy()
        consumer_config["consumer_timeout_ms"] = 10000
        consumer_config["enable_auto_commit"] = False
        consumer_config["group_id"] = None
        
        consumer = KafkaConsumer(**consumer_config)
        
        # Quick check: try to get metadata without subscribing first
        consumer.poll(timeout_ms=500)
        cluster = consumer._client.cluster
        existing_topics = cluster.topics()
        
        # If topic not in metadata, try AdminClient to confirm it doesn't exist (avoid auto-creation)
        # Use short timeout to avoid blocking
        if topic not in existing_topics:
            try:
                from kafka import KafkaAdminClient
                bootstrap = bootstrap_servers or os.getenv("MCPKIT_KAFKA_BOOTSTRAP")
                if bootstrap:
                    admin_config = {"bootstrap_servers": bootstrap.split(",") if isinstance(bootstrap, str) else bootstrap}
                    admin_client_check = KafkaAdminClient(**admin_config)
                    try:
                        from kafka.admin import ConfigResource, ConfigResourceType
                        config_resource = ConfigResource(ConfigResourceType.TOPIC, topic)
                        # Short timeout - if it fails quickly, topic doesn't exist
                        configs = admin_client_check.describe_configs([config_resource], request_timeout_ms=1000)
                        if config_resource not in configs:
                            # Topic doesn't exist
                            consumer.close()
                            admin_client_check.close()
                            raise GuardError(f"Topic {topic} not found")
                    except Exception as e:
                        error_str = str(e).lower()
                        # Only raise if it's clearly a "not found" error
                        if any(phrase in error_str for phrase in ["not found", "does not exist", "unknown topic", "invalid topic", "topic or partition"]):
                            consumer.close()
                            try:
                                admin_client_check.close()
                            except Exception:
                                pass
                            raise GuardError(f"Topic {topic} not found")
                        # Other errors (timeout, network) - proceed to subscribe
                    finally:
                        try:
                            admin_client_check.close()
                        except Exception:
                            pass
            except (ImportError, Exception):
                # AdminClient not available - will proceed to subscribe (may auto-create)
                pass
        
        # Subscribe to the topic to get partition metadata
        consumer.subscribe([topic])
        consumer.poll(timeout_ms=5000)
        import time
        time.sleep(1)
        
        # Get cluster metadata after subscription
        cluster = consumer._client.cluster
        
        # Get partitions for the topic
        partitions = cluster.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            raise GuardError(f"Topic {topic} not found")
        
        # Get basic partition info - just partition numbers
        # Detailed leader/replicas/isr info requires AdminClient describe_topics
        # which may not be available or may require additional permissions
        partition_metadata = []
        for partition_id in sorted(partitions):
            partition_metadata.append({
                "partition": partition_id,
                "leader": None,  # Requires AdminClient describe_topics
                "replicas": [],  # Requires AdminClient describe_topics
                "isr": [],  # Requires AdminClient describe_topics
            })
        
        # Get topic config via admin client
        topic_config = {}
        try:
            from kafka import KafkaAdminClient
            admin_config = get_kafka_config(bootstrap_servers)
            admin_client = KafkaAdminClient(**admin_config)
            
            from kafka.admin import ConfigResource, ConfigResourceType
            config_resource = ConfigResource(ConfigResourceType.TOPIC, topic)
            configs = admin_client.describe_configs([config_resource])
            
            if config_resource in configs:
                config_result = configs[config_resource]
                topic_config = {
                    str(k): str(v) for k, v in config_result.items()
                }
        except Exception:
            # If admin client fails, continue without config
            pass
        
        return {
            "topic": topic,
            "partitions": partition_metadata,
            "partition_count": len(partition_metadata),
            "replication_factor": len(partition_metadata[0]["replicas"]) if partition_metadata and partition_metadata[0]["replicas"] else 0,
            "config": topic_config,
        }
    except KafkaError as e:
        # Check for compression codec errors and provide helpful message
        error_str = str(e).lower()
        if (UnsupportedCodecError and isinstance(e, UnsupportedCodecError)) or \
           (UnsupportedCompressionTypeError and isinstance(e, UnsupportedCompressionTypeError)) or \
           "snappy" in error_str and ("not found" in error_str or "libraries" in error_str):
            raise GuardError(
                f"Kafka compression codec error: {e}\n"
                f"To fix this, install python-snappy: pip install python-snappy\n"
                f"Or install with optional dependency: pip install mcpkit-data[kafka-snappy]"
            )
        raise GuardError(f"Kafka error describing topic: {e}")
    except Exception as e:
        raise GuardError(f"Error describing topic: {e}")
    finally:
        if consumer:
            consumer.close()
        if admin_client:
            try:
                admin_client.close()
            except Exception:
                pass


def _flatten_dict(d: dict, parent_key: str = "", sep: str = "_") -> dict:
    """
    Recursively flatten a nested dictionary.
    
    Args:
        d: Dictionary to flatten
        parent_key: Prefix for keys (used in recursion)
        sep: Separator for nested keys (default: "_")
    
    Returns:
        Flattened dictionary
    """
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # For lists, convert to JSON string or handle each item
            # Simple approach: convert to JSON string
            items.append((new_key, json.dumps(v) if v else None))
        else:
            items.append((new_key, v))
    return dict(items)


def kafka_flatten_records(records: list[dict]) -> dict:
    """
    Flatten Kafka records generically.
    
    Args:
        records: List of Kafka records with decoded key, value, and headers
    
    Returns:
        dict with:
            - columns: list of column names
            - rows: list of flattened rows
            - record_count: number of records processed
    """
    if not records:
        return {
            "columns": [],
            "rows": [],
            "record_count": 0,
        }
    
    # Process all records to collect all possible columns
    flattened_records: list[dict[str, Any]] = []
    all_columns: set[str] = set()
    
    for record in records:
        flat = {
            "partition": record.get("partition"),
            "offset": record.get("offset"),
            "timestamp": record.get("timestamp"),
        }
        
        # Key is already decoded
        flat["key"] = record.get("key")
        
        # Parse value (should be JSON string if Avro was decoded, or plain string, or base64-encoded Avro)
        value_str = record.get("value")
        if value_str:
            try:
                value_json = json.loads(value_str)
                if isinstance(value_json, dict):
                    # Flatten the JSON structure
                    flat_value = _flatten_dict(value_json)
                    flat.update(flat_value)
                elif isinstance(value_json, list):
                    # For arrays, convert to JSON string
                    flat["value"] = json.dumps(value_json)
                else:
                    flat["value"] = value_str
            except (json.JSONDecodeError, TypeError):
                # Not JSON - should have been decoded during consumption
                # If we see base64 here, it means decoding failed during consumption
                # Store as-is (shouldn't happen if decoding worked properly)
                flat["value"] = value_str
        else:
            flat["value"] = None
        
        # Headers are already decoded - base64 encode binary/non-printable headers for CSV compatibility
        headers = record.get("headers", {})
        if headers:
            import base64
            for k, v in headers.items():
                if v and isinstance(v, str):
                    # Check if string is safe for CSV (printable, no newlines except in quoted strings)
                    # For simplicity, base64-encode if contains control chars or newlines
                    has_control_chars = any(ord(c) < 32 and c not in '\t\n\r' for c in v)
                    has_newlines = '\n' in v or '\r' in v
                    
                    if has_control_chars or has_newlines or not v.isprintable():
                        # Binary or problematic data - base64 encode
                        try:
                            flat[f"header_{k}_base64"] = base64.b64encode(v.encode('utf-8', errors='replace')).decode('ascii')
                        except Exception:
                            # Skip if can't encode
                            pass
                    else:
                        # Safe printable string
                        flat[f"header_{k}"] = v
        
        flattened_records.append(flat)
        all_columns.update(flat.keys())
    
    # Sort columns: metadata first, then alphabetical
    metadata_cols = ["partition", "offset", "timestamp", "key"]
    other_cols = sorted([c for c in all_columns if c not in metadata_cols])
    columns = [c for c in metadata_cols if c in all_columns] + other_cols
    
    # Build rows with consistent column order
    rows = []
    for flat in flattened_records:
        row = [flat.get(col) for col in columns]
        rows.append(row)
    
    return {
        "columns": columns,
        "rows": rows,
        "record_count": len(rows),
    }


def kafka_flatten_dataset(dataset_id: str, out_dataset_id: Optional[str] = None) -> dict:
    """
    Flatten Kafka records from a dataset generically.
    
    Args:
        dataset_id: Input dataset ID containing Kafka records
        out_dataset_id: Optional output dataset ID (auto-generated if None)
    
    Returns:
        dict with:
            - dataset_id: output dataset ID
            - columns: list of column names
            - rows: number of rows
    """
    from .registry import load_dataset, save_dataset
    import json
    import pandas as pd
    
    # Load dataset
    df = load_dataset(dataset_id)
    
    if len(df) == 0:
        # Empty dataset - create empty output
        result = {
            "columns": [],
            "rows": [],
            "record_count": 0,
        }
    else:
        # Convert DataFrame rows to records
        records = []
        for _, row in df.iterrows():
            # Handle headers - could be dict, string, or None
            headers = row.get("headers")
            if headers is None:
                headers = {}
            elif isinstance(headers, str):
                try:
                    headers = json.loads(headers)
                except (json.JSONDecodeError, TypeError):
                    headers = {}
            elif not isinstance(headers, dict):
                headers = {}
            
            record = {
                "partition": int(row.get("partition", 0)) if pd.notna(row.get("partition")) else 0,
                "offset": int(row.get("offset", 0)) if pd.notna(row.get("offset")) else 0,
                "timestamp": int(row.get("timestamp", 0)) if pd.notna(row.get("timestamp")) else 0,
                "key": row.get("key") if pd.notna(row.get("key")) else None,
                "value": row.get("value") if pd.notna(row.get("value")) else None,
                "headers": headers,
            }
            records.append(record)
        
        # Flatten records
        result = kafka_flatten_records(records)
    
    # Save as new dataset
    rows: list[list[Any]] = result["rows"]  # type: ignore[assignment]
    columns: list[str] = result["columns"]  # type: ignore[assignment]
    if rows:
        df_out = pd.DataFrame(rows, columns=columns)
    else:
        df_out = pd.DataFrame(columns=columns)
    
    save_result = save_dataset(df_out, out_dataset_id)
    
    return {
        "dataset_id": save_result["dataset_id"],
        "columns": result["columns"],
        "rows": len(result["rows"]),  # type: ignore[arg-type]
        "record_count": result["record_count"],
    }
