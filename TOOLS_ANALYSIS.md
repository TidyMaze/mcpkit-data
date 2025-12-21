# MCP Tools Analysis & Gap Identification

## Current Tools (53 total)

### Dataset Registry (4)
- `dataset_list` - List all datasets
- `dataset_info` - Get dataset metadata
- `dataset_put_rows` - Store rows as parquet
- `dataset_delete` - Delete dataset

### Kafka (6)
- `kafka_list_topics` - List topics
- `kafka_offsets` - Get topic/partition offsets
- `kafka_consume_batch` - Consume messages → dataset
- `kafka_filter` - Filter records by JMESPath
- `kafka_flatten` - Flatten nested Kafka records
- `kafka_groupby_key` - Group records by extracted key

### Schema Registry / Decoding (3)
- `schema_registry_get` - Get schema by ID/subject
- `avro_decode` - Decode Avro messages
- `protobuf_decode` - Decode Protobuf messages

### JDBC (2)
- `jdbc_query_ro` - Read-only SQL queries
- `jdbc_introspect` - Schema introspection

### AWS (8)
- `athena_start_query` - Start query
- `athena_poll_query` - Poll status
- `athena_get_results` - Get results
- `athena_explain` - Explain query plan
- `athena_partitions_list` - List partitions
- `athena_repair_table` - MSCK REPAIR TABLE
- `athena_ctas_export` - CTAS export
- `s3_list_prefix` - List S3 objects

### JSON / QA (5)
- `jq_transform` - JMESPath transform
- `event_validate` - JSONSchema validation
- `event_fingerprint` - Generate fingerprint
- `dedupe_by_id` - Deduplicate by ID
- `event_correlate` - Correlate events across batches

### Pandas (9)
- `pandas_from_rows` - Create DataFrame
- `pandas_describe` - Statistics
- `pandas_groupby` - Group by + aggregations
- `pandas_join` - Join datasets
- `pandas_filter_query` - Filter with conditions
- `pandas_diff_frames` - Compare datasets
- `pandas_schema_check` - Schema validation
- `pandas_sample_stratified` - Stratified sampling
- `pandas_export` - Export to file

### Polars (3)
- `polars_from_rows` - Create DataFrame
- `polars_groupby` - Group by + aggregations
- `polars_export` - Export to file

### DuckDB (1)
- `duckdb_query_local` - SQL on local sources

### File Formats (2)
- `parquet_inspect` - Inspect parquet schema/rows
- `arrow_convert` - Convert between formats

### Evidence (2)
- `reconcile_counts` - Reconcile dataset counts
- `evidence_bundle_plus` - Generate evidence bundle

### Repo (2)
- `fs_read` - Read file with offset
- `rg_search` - Search with ripgrep

### Charts (1)
- `dataset_to_chart` - Auto-generate charts

### Nomad / Consul (4)
- `nomad_list_jobs` - List jobs (fuzzy search)
- `consul_get_service_ips` - Get service IPs (fuzzy search)
- `nomad_get_job_status` - Get full job status
- `nomad_get_allocation_logs` - Read allocation logs

### Great Expectations (1)
- `great_expectations_check` - Run validation suite

---

## Missing Tools (Gap Analysis)

### 1. Infrastructure / Operations (High Priority)

#### Nomad / Consul Gaps
- ❌ `nomad_get_node_status` - Get node health/resources
- ❌ `consul_list_services` - List all services (not just IPs)
- ❌ `consul_get_service_health` - Get detailed health for service
- ❌ `nomad_get_allocation_events` - Get allocation lifecycle events

**Rationale**: Devs need to understand infrastructure state beyond just job status. Node health, service catalog, and event history are critical for debugging.

#### HTTP / API Testing
- ❌ `http_request` - Simple HTTP GET/POST (read-only, no auth mutations)
- ❌ `http_health_check` - Check endpoint health/status

**Rationale**: Devs often need to test APIs, check health endpoints, or fetch data from HTTP services. Keep it simple: GET/POST only, no auth mutations.

### 2. Data Pipeline Debugging (Medium Priority)

#### Kafka Gaps
- ❌ `kafka_describe_topic` - Get topic config, partitions, replication
- ❌ `kafka_consume_tail` - Consume last N messages (for debugging)
- ❌ `kafka_produce_test` - Produce test message (read-only, no prod topics)

**Rationale**: Debugging requires topic inspection and tailing recent messages. Test produce helps validate pipelines.

#### Schema Registry Gaps
- ❌ `schema_registry_list_subjects` - List all subjects
- ❌ `schema_registry_get_versions` - Get all versions of subject

**Rationale**: Need to discover available schemas and version history.

### 3. Data Quality / Validation (Medium Priority)

#### Missing Validation Tools
- ❌ `dataset_sample_random` - Random sample (complement to stratified)
- ❌ `dataset_head_tail` - Get first/last N rows
- ❌ `dataset_count_distinct` - Count distinct values per column

**Rationale**: Simple, composable operations for quick data inspection. Head/tail is essential for debugging.

### 4. File Operations (Low Priority)

#### File System Gaps
- ❌ `fs_list_dir` - List directory contents
- ❌ `fs_stat` - Get file metadata (size, mtime, etc.)

**Rationale**: Basic file operations for exploring codebase/data directories.

### 5. Time Series / Temporal (Low Priority)

#### Time-based Operations
- ❌ `dataset_filter_time_range` - Filter by timestamp column
- ❌ `dataset_resample_time` - Resample time series data

**Rationale**: Many data pipelines work with time-series. Simple time filtering is common.

---

## Recommended Implementation Order

### Phase 1: Critical Dev Tools (KISS, High Reuse)
1. `http_request` - Universal HTTP client (GET/POST only)
2. `dataset_head_tail` - Essential for debugging
3. `kafka_consume_tail` - Debug recent messages
4. `consul_list_services` - Service discovery

### Phase 2: Infrastructure Visibility
5. `nomad_get_node_status` - Node health
6. `consul_get_service_health` - Service health details
7. `kafka_describe_topic` - Topic inspection

### Phase 3: Data Quality Helpers
8. `dataset_sample_random` - Random sampling
9. `dataset_count_distinct` - Distinct counts
10. `schema_registry_list_subjects` - Schema discovery

### Phase 4: Nice-to-Have
11. `fs_list_dir` - Directory listing
12. `dataset_filter_time_range` - Time filtering
13. `nomad_get_allocation_events` - Event history

---

## Design Principles Applied

### ✅ KISS (Keep It Simple, Stupid)
- Each tool does ONE thing well
- No complex orchestration in tools
- Composability > monolithic features

### ✅ Reusability
- Tools work with dataset registry (composable)
- HTTP tool can be used for any API
- Head/tail works with any dataset

### ✅ Composability
- Tools chain together: `kafka_consume_tail` → `dataset_head_tail` → `jq_transform`
- Registry as common interface
- No tight coupling between tools

### ✅ Read-Only Focus
- No mutations (except dataset registry which is local)
- HTTP: GET/POST only, no auth mutations
- Kafka produce: test topics only

---

## Notes

- **Avoid**: Complex orchestration tools, workflow engines, stateful operations
- **Prefer**: Simple, stateless, composable primitives
- **Pattern**: Input → Process → Output (dataset or artifact)
- **Registry**: Use as common storage layer for composability

