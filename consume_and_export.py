#!/usr/bin/env python3
import os
import json
import sys

from mcpkit.core.kafka_client import kafka_consume_tail, kafka_flatten_dataset
from mcpkit.core.registry import load_dataset
from mcpkit.core.pandas_ops import pandas_export

try:
    topic = os.getenv("MCPKIT_KAFKA_TOPIC")
    if not topic:
        print("ERROR: MCPKIT_KAFKA_TOPIC environment variable not set", file=sys.stderr)
        sys.exit(1)
    
    print(f"Consuming 10 events from topic: {topic}...")
    schema_registry_url = os.getenv("MCPKIT_SCHEMA_REGISTRY_URL")
    result = kafka_consume_tail(
        topic,
        n_messages=10,
        schema_registry_url=schema_registry_url
    )
    dataset_id = result["dataset_id"]
    print(f"Dataset: {dataset_id}")
    
    # Verify decoded
    df = load_dataset(dataset_id)
    if len(df) > 0:
        first_value = df.iloc[0]["value"]
        if first_value and isinstance(first_value, str) and first_value.startswith("AAAA"):
            print("ERROR: Values are still base64!", file=sys.stderr)
            sys.exit(1)
        print("✓ Values are decoded JSON")
    
    print("Flattening...")
    flatten_result = kafka_flatten_dataset(dataset_id)
    print(f"Flattened dataset: {flatten_result['dataset_id']}")
    
    print("Exporting to JSON...")
    export_result = pandas_export(flatten_result["dataset_id"], "json", "kafka_events_flattened.json")
    print(f"✓ Exported to: {export_result['path']}")
    
except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc()
    sys.exit(1)

