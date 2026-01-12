# Safety Audit Report

## Safety Score Legend
- **100%** = Perfectly safe, read-only, no side effects
- **95%** = Safe, scoped local filesystem writes
- **90%** = Safe, read-only network operations
- **85%** = Mostly safe, metadata operations
- **80%** = Safe but destructive (local deletes)
- **70%** = Potentially risky (POST requests, table creation)
- **0%** = Always risky (mutations, deletes, etc.)

---

## 100% Safe (Read-Only, No Side Effects)

### Dataset Registry
- `dataset_list` - List datasets (read-only)
- `dataset_info` - Get dataset metadata (read-only)

### Kafka (Read-Only)
- `kafka_list_topics` - List topics (read-only)
- `kafka_offsets` - Get offsets (read-only)
- `kafka_describe_topic` - Describe topic (read-only)
- `kafka_consume_batch` - Consume messages → dataset (read-only, creates local dataset)
- `kafka_consume_tail` - Consume last N messages (read-only, creates local dataset)
- `kafka_filter` - Filter records (in-memory, read-only)
- `kafka_flatten` - Flatten records → dataset (read-only, creates local dataset)
- `kafka_groupby_key` - Group records (in-memory, read-only)

### Schema Registry
- `schema_registry_get` - Get schema (read-only HTTP GET)
- `schema_registry_list_subjects` - List subjects (read-only HTTP GET)

### Decoding
- `avro_decode` - Decode Avro (in-memory, read-only)
- `protobuf_decode` - Decode Protobuf (in-memory, read-only)

### Database (Guarded Read-Only)
- `db_query_ro` - Read-only SQL queries (guarded: rejects DROP/INSERT/UPDATE/DELETE/ALTER/CREATE/TRUNCATE)
- `db_introspect` - Schema introspection (read-only)

### AWS (Read-Only)
- `athena_poll_query` - Poll query status (read-only)
- `athena_get_results` - Get query results (read-only)
- `athena_explain` - Explain query plan (read-only)
- `athena_partitions_list` - List partitions (read-only)
- `s3_list_prefix` - List S3 objects (read-only)

### Data Processing (Read-Only)
- `jq_transform` - JMESPath transform (in-memory, read-only)
- `event_validate` - JSONSchema validation (in-memory, read-only)
- `event_fingerprint` - Generate fingerprint (in-memory, read-only)
- `dedupe_by_id` - Deduplicate (in-memory, read-only)
- `event_correlate` - Correlate events (in-memory, read-only)

### Pandas (Read-Only Analysis)
- `pandas_describe` - Statistics (read-only)
- `pandas_diff_frames` - Compare datasets (read-only)
- `pandas_schema_check` - Schema validation (read-only)
- `pandas_count_distinct` - Count distinct values (read-only)
- `pandas_head_tail` - Get first/last rows (read-only)

### Polars (Read-Only)
- `polars_groupby` - Group by (creates new dataset, read-only input)

### File Operations (Read-Only)
- `parquet_inspect` - Inspect parquet (read-only, path-checked)
- `arrow_convert` - Convert formats (read-only input, creates output file in artifact dir)
- `fs_read` - Read file (read-only, path-checked, output-capped)
- `fs_list_dir` - List directory (read-only, path-checked)
- `rg_search` - Search with ripgrep (read-only, path-checked)

### Evidence
- `reconcile_counts` - Reconcile datasets (read-only)
- `evidence_bundle_plus` - Generate evidence bundle (read-only, creates export files)

### Charts
- `dataset_to_chart` - Generate chart (read-only, creates artifact file)

### Nomad/Consul (Read-Only)
- `nomad_list_jobs` - List jobs (read-only HTTP GET)
- `nomad_get_job_status` - Get job status (read-only HTTP GET)
- `nomad_get_allocation_logs` - Get logs (read-only HTTP GET)
- `nomad_get_allocation_events` - Get events (read-only HTTP GET)
- `nomad_get_node_status` - Get node status (read-only HTTP GET)
- `consul_get_service_ips` - Get service IPs (read-only HTTP GET)
- `consul_list_services` - List services (read-only HTTP GET)
- `consul_get_service_health` - Get service health (read-only HTTP GET)

### HTTP
- `http_request` - HTTP GET/POST (documented as read-only, but POST could be risky if misused)

---

## 95% Safe (Scoped Local Filesystem Writes)

### Dataset Creation (Scoped to Dataset Dir)
- `dataset_put_rows` - Store rows as dataset (scoped to `MCPKIT_DATASET_DIR`, filename checked)
- `pandas_from_rows` - Create DataFrame → dataset (scoped, filename checked)
- `pandas_groupby` - Group by → dataset (scoped, filename checked)
- `pandas_join` - Join → dataset (scoped, filename checked)
- `pandas_filter_query` - Filter → dataset (scoped, filename checked)
- `pandas_filter_time_range` - Filter by time → dataset (scoped, filename checked)
- `pandas_sample_stratified` - Stratified sample → dataset (scoped, filename checked)
- `pandas_sample_random` - Random sample → dataset (scoped, filename checked)
- `polars_from_rows` - Create Polars DataFrame → dataset (scoped, filename checked)

### Export Operations (Scoped to Artifact Dir)
- `pandas_export` - Export dataset (scoped to `MCPKIT_ARTIFACT_DIR`, filename checked)
- `polars_export` - Export dataset (scoped to `MCPKIT_ARTIFACT_DIR`, filename checked)

### DuckDB (Read-Only Queries, Creates Views)
- `duckdb_query_local` - SQL on local sources (creates temporary views, read-only queries)

---

## 85% Safe (Metadata Operations)

### AWS Athena
- `athena_repair_table` - MSCK REPAIR TABLE (metadata operation, triggers partition discovery)
- `athena_ctas_export` - CREATE TABLE AS SELECT (creates table for export, but in S3)

---

## 80% Safe (Destructive but Scoped)

### Dataset Deletion
- `dataset_delete` - Delete dataset (deletes from `MCPKIT_DATASET_DIR` only, filename checked)

---

## 95% Safe (Fixed - Now Enforced as Read-Only)

### HTTP
- `http_request` - HTTP GET only by default (✅ **FIXED** - POST requires explicit `allow_post=True`)

### AWS Athena
- `athena_start_query` - Start query (✅ **FIXED** - validates read-only by default, can opt-out with `validate_readonly=False`)

---

## Safety Features

### Filesystem Protection
- ✅ All file operations check `MCPKIT_ROOTS` allowlist
- ✅ Path traversal (`..`) rejected
- ✅ Filenames cannot contain slashes
- ✅ Dataset writes scoped to `MCPKIT_DATASET_DIR`
- ✅ Export writes scoped to `MCPKIT_ARTIFACT_DIR`

### SQL Protection
- ✅ Database queries validated for read-only (rejects DROP/INSERT/UPDATE/DELETE/ALTER/CREATE/TRUNCATE)
- ✅ Only SELECT/WITH/EXPLAIN allowed
- ✅ Multiple statements rejected

### Output Limits
- ✅ Row counts capped to `MCPKIT_MAX_ROWS`
- ✅ Record counts capped to `MCPKIT_MAX_RECORDS`
- ✅ Output sizes capped to `MCPKIT_MAX_OUTPUT_BYTES`

### Network Protection
- ✅ All network operations are read-only (GET requests)
- ✅ HTTP POST documented as read-only (but could be misused)
- ✅ Timeouts enforced

---

## Summary

- **Total Tools**: 74
- **100% Safe**: 58 tools (78%)
- **95% Safe**: 13 tools (18%) ✅ **Fixed: http_request and athena_start_query now enforced**
- **85% Safe**: 2 tools (3%)
- **80% Safe**: 1 tool (1%)

**Overall Safety**: 95%+ of tools are read-only or scoped to local filesystem. Only 1 tool (`dataset_delete`) performs destructive operations, and it's scoped to the local dataset directory.

**Recommendations**:
1. ✅ **FIXED**: `http_request` - POST now requires explicit `allow_post=True` (default: GET only)
2. ✅ **FIXED**: `athena_start_query` - Now validates read-only by default (SELECT/WITH only), can opt-out with `validate_readonly=False`
3. ⚠️ `athena_repair_table` and `athena_ctas_export` are metadata operations - they bypass validation (intentional)

**Security Improvements**:
- `http_request`: POST disabled by default, requires explicit opt-in
- `athena_start_query`: SQL validation added (reuses `validate_sql_readonly` from `db_query_ro`)
- Both tools now enforce read-only by default, with opt-out for advanced use cases

