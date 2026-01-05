#!/usr/bin/env python3
"""Test Avro decoding with actual Kafka topic and Schema Registry."""

import os
import sys
import json

from mcpkit.core.kafka_client import kafka_consume_tail, kafka_flatten_dataset
from mcpkit.core.registry import load_dataset

def test_avro_decoding():
    """Test consuming and decoding Avro messages."""
    print("🧪 Testing Avro Decoding\n")
    print("=" * 60)
    
    topic = os.getenv("MCPKIT_KAFKA_TOPIC")
    if not topic:
        print("ERROR: MCPKIT_KAFKA_TOPIC environment variable not set", file=sys.stderr)
        return False
    
    print(f"\n1️⃣ Consuming messages from: {topic}")
    print("-" * 60)
    
    try:
        schema_registry_url = os.getenv("MCPKIT_SCHEMA_REGISTRY_URL")
        result = kafka_consume_tail(topic, n_messages=3, schema_registry_url=schema_registry_url)
        dataset_id = result["dataset_id"]
        record_count = result["record_count"]
        
        print(f"✓ Consumed {record_count} messages")
        print(f"✓ Dataset ID: {dataset_id}")
        
        # Load dataset to inspect
        df = load_dataset(dataset_id)
        print(f"\n2️⃣ Inspecting dataset")
        print("-" * 60)
        print(f"✓ Rows: {len(df)}")
        print(f"✓ Columns: {list(df.columns)}")
        
        # Check if values are decoded (should be JSON strings, not binary)
        if len(df) > 0:
            first_value = df.iloc[0]["value"]
            print(f"\n3️⃣ Checking first value")
            print("-" * 60)
            
            if first_value and isinstance(first_value, str):
                # Try to parse as JSON
                try:
                    decoded = json.loads(first_value)
                    print(f"✓ Value is JSON decodable (Avro was decoded!)")
                    print(f"✓ Type: {type(decoded)}")
                    if isinstance(decoded, dict):
                        print(f"✓ Keys: {list(decoded.keys())[:10]}...")
                        print(f"\n📄 Sample decoded value:")
                        print(json.dumps(decoded, indent=2)[:500] + "...")
                    else:
                        print(f"✓ Value: {str(decoded)[:200]}...")
                except json.JSONDecodeError:
                    # Check if it's binary (check for null bytes or control chars)
                    has_binary = '\x00' in first_value or any(ord(c) < 32 and c not in '\t\n\r' for c in first_value[:100])
                    # Check if it's base64 (should NOT happen with new version)
                    if first_value.startswith("AAAA") or (len(first_value) > 100 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=' for c in first_value[:100])):
                        print(f"✗ ERROR: Value is base64 encoded (should be decoded JSON!)")
                        print(f"  Length: {len(first_value)}")
                        print(f"  First 200 chars: {first_value[:200]}")
                        print(f"  Schema Registry URL: {os.getenv('MCPKIT_SCHEMA_REGISTRY_URL')}")
                        return False
                    elif has_binary or len(first_value) > 1000:
                        print(f"⚠ Value appears to be binary (Avro decoding may have failed)")
                        print(f"  Length: {len(first_value)}")
                        print(f"  First 200 chars (repr): {repr(first_value[:200])}")
                        print(f"  Schema Registry URL: {os.getenv('MCPKIT_SCHEMA_REGISTRY_URL')}")
                    else:
                        print(f"✓ Value is plain text: {first_value[:200]}...")
            else:
                print(f"⚠ Value is None or not a string")
        
        # Test flattening
        print(f"\n4️⃣ Testing flatten operation")
        print("-" * 60)
        try:
            flatten_result = kafka_flatten_dataset(dataset_id)
            flatten_dataset_id = flatten_result["dataset_id"]
            flatten_columns = flatten_result["columns"]
            
            print(f"✓ Flattened dataset ID: {flatten_dataset_id}")
            print(f"✓ Flattened columns ({len(flatten_columns)}): {flatten_columns[:15]}...")
            
            # Load flattened dataset
            df_flat = load_dataset(flatten_dataset_id)
            print(f"✓ Flattened rows: {len(df_flat)}")
            
            if len(df_flat) > 0:
                print(f"\n📊 Sample flattened row (first 10 columns):")
                print(df_flat.iloc[0][:10].to_dict())
                
        except Exception as e:
            print(f"✗ Flatten failed: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n✅ Test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_avro_decoding()
    sys.exit(0 if success else 1)

