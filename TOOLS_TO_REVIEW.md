# Tools to Review for Potential Removal

> **Status:** All tools kept for now. This document is for future reference.

## Analysis Summary

**Total tools:** 66  
**Tested tools:** 41 (62%)  
**Untested tools:** 25 (38%)

## Categories of Concern

### 🔴 High Priority - Consider Removing

#### 1. **`kafka_flatten`** - Potentially Redundant/Weird
- **Status:** Untested
- **Issue:** Generic flattening of Kafka records - unclear when this is better than using pandas operations
- **Usage:** Requires dataset with Kafka records, then flattens nested JSON
- **Alternative:** Could use `pandas` operations or `jq_transform` for specific fields
- **Recommendation:** Remove if rarely used or replace with more specific tool

#### 2. **`event_fingerprint`** - Unclear Purpose
- **Status:** Tested but might be rarely used
- **Issue:** Generates fingerprint/hash for records - unclear use case
- **Usage:** Creates unique identifier from record fields
- **Alternative:** Could be done with pandas operations
- **Recommendation:** Remove if not commonly used

#### 3. **`arrow_convert`** - Format Conversion (Potentially Redundant)
- **Status:** Tested but might be redundant
- **Issue:** Converts between Parquet/CSV/JSON - but pandas/polars already do this
- **Usage:** Format conversion using Arrow
- **Alternative:** `pandas_export` / `polars_export` can handle format conversion
- **Recommendation:** Consider removing if redundant with pandas/polars export

#### 4. **`reconcile_counts`** - Very Specific Use Case
- **Status:** Tested but very niche
- **Issue:** Reconciles record counts between two datasets - very specific debugging tool
- **Usage:** Compare dataset sizes and find missing/extra records
- **Alternative:** Could use `pandas_diff_frames` for more general comparison
- **Recommendation:** Remove if too specific, or keep if it's actually used

#### 5. **`protobuf_decode`** - Very Specific, Rarely Used
- **Status:** Untested
- **Issue:** Decodes Protobuf messages - very specific use case
- **Usage:** Requires file descriptor set and message type name
- **Alternative:** Most users probably use Avro or JSON
- **Recommendation:** Remove if not commonly used (keep Avro, remove Protobuf)

#### 6. **`rg_search`** - Weird/Unusual Tool
- **Status:** Tested but unusual
- **Issue:** Uses ripgrep to search files - seems out of scope for data engineering
- **Usage:** Search for regex patterns in files
- **Alternative:** Standard grep or file reading tools
- **Recommendation:** Remove - seems like it belongs in a different toolset

### 🟡 Medium Priority - Review Usage

#### 7. **`athena_repair_table`** - Very Specific AWS Operation
- **Status:** Untested
- **Issue:** Runs MSCK REPAIR TABLE - very specific AWS Glue operation
- **Usage:** Fixes partition metadata in Athena
- **Recommendation:** Keep if actually used, but very niche

#### 8. **`athena_ctas_export`** - Specific AWS Export
- **Status:** Untested
- **Issue:** CTAS (Create Table As Select) export to S3 - specific use case
- **Usage:** Export query results to S3
- **Recommendation:** Keep if used, but could be replaced with `athena_get_results` + `pandas_export`

#### 9. **`athena_explain`** - Query Explanation
- **Status:** Untested
- **Issue:** Explains query plan - useful but might be rarely used
- **Usage:** Get query execution plan
- **Recommendation:** Keep - useful for debugging, but low priority

#### 10. **`event_correlate`** - Complex, Rarely Used
- **Status:** Tested but complex
- **Issue:** Correlates events across batches using join key and timestamp - complex operation
- **Usage:** Join events from different batches
- **Recommendation:** Keep if used, but consider simplifying or documenting better

#### 11. **`fs_list_dir`** - Basic File Operation
- **Status:** Untested
- **Issue:** Lists directory contents - basic but might be redundant
- **Usage:** Browse filesystem
- **Recommendation:** Keep - simple and useful, just needs tests

### 🟢 Low Priority - Just Need Tests

These tools are fine but just need test coverage:

- `dataset_head_tail` - Essential tool, just needs tests
- `http_request` - Essential tool, just needs tests
- `kafka_consume_tail` - Essential tool, just needs tests
- `kafka_describe_topic` - Essential tool, just needs tests
- `kafka_list_topics` - Essential tool, just needs tests
- `consul_list_services` - Essential tool, just needs tests
- `consul_get_service_health` - Essential tool, just needs tests
- `consul_get_service_ips` - Essential tool, just needs tests
- `nomad_list_jobs` - Essential tool, just needs tests
- `nomad_get_job_status` - Essential tool, just needs tests
- `nomad_get_allocation_logs` - Essential tool, just needs tests
- `nomad_get_allocation_events` - Essential tool, just needs tests
- `nomad_get_node_status` - Essential tool, just needs tests
- `pandas_count_distinct` - Essential tool, just needs tests
- `pandas_export` - Essential tool, just needs tests
- `pandas_filter_time_range` - Essential tool, just needs tests
- `pandas_sample_random` - Essential tool, just needs tests
- `polars_export` - Essential tool, just needs tests
- `schema_registry_list_subjects` - Essential tool, just needs tests

## Recommended Actions

### Remove (6 tools):
1. `kafka_flatten` - Redundant with pandas operations
2. `event_fingerprint` - Unclear purpose, rarely used
3. `arrow_convert` - Redundant with pandas/polars export
4. `reconcile_counts` - Too specific, use `pandas_diff_frames` instead
5. `protobuf_decode` - Very specific, rarely used (keep Avro)
6. `rg_search` - Out of scope for data engineering tools

### Review Usage Before Removing (5 tools):
1. `athena_repair_table` - Very specific, check if used
2. `athena_ctas_export` - Specific, check if used
3. `athena_explain` - Useful but low priority
4. `event_correlate` - Complex, check if used
5. `fs_list_dir` - Basic, probably keep

### Add Tests (19 tools):
All the "Low Priority" tools above - they're fine, just need test coverage.

## Impact Analysis

**If we remove the 6 recommended tools:**
- **Tools remaining:** 60 (from 66)
- **Reduction:** 9%
- **Complexity reduction:** Removes niche/specialized tools
- **Maintenance:** Easier to maintain with fewer edge cases

**Benefits:**
- Cleaner API surface
- Less confusion about which tool to use
- Easier maintenance
- Better focus on core data engineering workflows

**Risks:**
- Might remove something someone actually uses
- Need to check usage before removing

## Next Steps

1. **Check usage:** Search codebase/docs for references to these tools
2. **Get feedback:** Ask users if they use these tools
3. **Remove gradually:** Remove one at a time, monitor for issues
4. **Add tests:** For tools we keep, add comprehensive tests

