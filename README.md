# 🚀 mcpkit-data

> **The ultimate MCP server for data engineers who want to stop context-switching and start shipping** ✨

A comprehensive [Model Context Protocol](https://modelcontextprotocol.io) server that gives your AI assistant superpowers to work with Kafka, databases, AWS, data pipelines, and more. Stop jumping between terminals, dashboards, and docs. Just ask your AI to do it.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## ⚠️ Early Days Disclaimer

**Hey there! 👋** Before you dive in, here's the deal:

This is a **brand new project** fresh out of the oven 🍞. We're talking:
- 🆕 **First release** - Like, literally just born
- 🧪 **Not production-ready** - More like "works on my machine" ready
- 👥 **Limited testing** - It's mostly been tested by... well, me
- 🛠️ **Actively evolving** - Things might break, change, or surprise you

**What this means:**
- ✅ Great for experimentation and learning
- ✅ Perfect for side projects and personal use
- ✅ Awesome if you want to contribute and shape the future
- ❌ Probably not ideal for critical production systems (yet!)
- ❌ Might have bugs, quirks, or "interesting" behaviors
- ❌ Documentation might be incomplete or confusing

**But here's the cool part:** You can help make it better! 🚀 Found a bug? Report it! Want a feature? Ask for it! Want to contribute? We'd love that!

*TL;DR: This is the "early access" version. Use at your own risk, have fun, and help us make it awesome!* 🎉

---

## 🎯 What is this?

**mcpkit-data** is your AI assistant's Swiss Army knife for data engineering. It exposes **66+ tools** through the MCP protocol, letting your AI:

- 🔍 **Debug Kafka pipelines** without leaving your editor
- 📊 **Query databases** (JDBC, Athena) and analyze results
- 🐼 **Process data** with pandas, polars, and DuckDB
- ☁️ **Work with AWS** (Athena, S3, Glue) seamlessly
- 🏗️ **Inspect infrastructure** (Nomad, Consul) in real-time
- 📈 **Generate charts** and evidence bundles automatically
- ✅ **Validate data quality** with Great Expectations

**All without you writing a single line of code.** Just chat with your AI. 🎉

---

## ⚡ Quick Start

### Installation

```bash
pip install -e ".[dev]"
```

### Run the Server

```bash
python -m mcpkit.server
```

### Configure Cursor (or your MCP client)

Add to your Cursor MCP settings:

```json
{
  "mcpServers": {
    "mcpkit-data": {
      "command": "python",
      "args": ["-m", "mcpkit.server"],
      "env": {
        "MCPKIT_KAFKA_BOOTSTRAP": "localhost:9092",
        "MCPKIT_SCHEMA_REGISTRY_URL": "http://localhost:8081",
        "MCPKIT_JDBC_DRIVER_CLASS": "org.postgresql.Driver",
        "MCPKIT_JDBC_URL": "jdbc:postgresql://localhost/test",
        "MCPKIT_JDBC_JARS": "/path/to/postgresql.jar",
        "MCPKIT_ROOTS": "/allowed/path1:/allowed/path2"
      }
    }
  }
}
```

**That's it!** Your AI can now use all 66+ tools. 🎊

---

## 🛠️ Tools Overview

### 📦 Dataset Registry (4 tools)

Your data's home base. Store, query, and manage datasets as Parquet files.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `dataset_list` | 📋 List all datasets | See what data you have |
| `dataset_info` | ℹ️ Get dataset metadata | Check schema, row count |
| `dataset_put_rows` | 💾 Store rows as Parquet | Save query results |
| `dataset_delete` | 🗑️ Delete a dataset | Clean up old data |

---

### 🎧 Kafka Tools (8 tools)

Debug, consume, and analyze Kafka topics like a pro.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `kafka_list_topics` | 📝 List all topics | Discover what's available |
| `kafka_offsets` | 📊 Get partition offsets | Check lag, find positions |
| `kafka_consume_batch` | 📥 Consume messages → dataset | Extract data for analysis |
| `kafka_consume_tail` | 🔚 Get last N messages | Debug recent events |
| `kafka_filter` | 🔍 Filter with JMESPath | Find specific records |
| `kafka_flatten` | 📐 Flatten nested records | Normalize JSON structures |
| `kafka_groupby_key` | 🔑 Group by extracted key | Aggregate by message key |
| `kafka_describe_topic` | 📋 Get topic config/partitions | Inspect topic metadata |

---

### 📋 Schema Registry & Decoding (4 tools)

Work with Avro, Protobuf, and schema discovery.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `schema_registry_get` | 📄 Get schema by ID/subject | Fetch schema definitions |
| `schema_registry_list_subjects` | 📚 List all subjects | Discover available schemas |
| `avro_decode` | 🔓 Decode Avro messages | Parse binary Avro data |
| `protobuf_decode` | 🔓 Decode Protobuf messages | Parse binary Protobuf data |

---

### 🗄️ Database Tools (2 tools)

Query databases safely with read-only enforcement.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `jdbc_query_ro` | 🔍 Execute read-only SQL | Query any JDBC database |
| `jdbc_introspect` | 🔎 Introspect schema | Discover tables/columns |

**Safety**: Automatically blocks `DROP`, `INSERT`, `UPDATE`, `DELETE`, etc. ✅

---

### ☁️ AWS Tools (9 tools)

Query Athena, list S3, manage Glue partitions.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `athena_start_query` | 🚀 Start SQL query | Run analytics queries |
| `athena_poll_query` | ⏳ Check query status | Monitor execution |
| `athena_get_results` | 📊 Get query results | Fetch data |
| `athena_explain` | 📖 Explain query plan | Optimize queries |
| `athena_partitions_list` | 📂 List partitions | Check table structure |
| `athena_repair_table` | 🔧 Repair table (MSCK) | Fix partition metadata |
| `athena_ctas_export` | 📤 CTAS export | Export to S3 |
| `s3_list_prefix` | 📁 List S3 objects | Browse buckets |

**Safety**: SQL validation blocks dangerous operations by default. ✅

---

### 🐼 Pandas Tools (12 tools)

The data scientist's best friend, now in your AI assistant.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `pandas_from_rows` | 📊 Create DataFrame | Build datasets |
| `pandas_describe` | 📈 Get statistics | Understand your data |
| `pandas_groupby` | 🔢 Group + aggregate | Summarize by categories |
| `pandas_join` | 🔗 Join datasets | Combine data sources |
| `pandas_filter_query` | 🔍 Filter rows | Subset your data |
| `pandas_filter_time_range` | ⏰ Filter by time | Time-series analysis |
| `pandas_diff_frames` | 🔄 Compare datasets | Find differences |
| `pandas_schema_check` | ✅ Validate schema | Ensure data quality |
| `pandas_sample_stratified` | 🎲 Stratified sample | Balanced sampling |
| `pandas_sample_random` | 🎲 Random sample | Quick data preview |
| `pandas_count_distinct` | 🔢 Count unique values | Cardinality analysis |
| `pandas_export` | 💾 Export to CSV/Parquet/Excel | Save results |
| `dataset_head_tail` | 📄 Get first/last N rows | Quick data preview |

---

### ⚡ Polars Tools (3 tools)

Lightning-fast data processing with Polars.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `polars_from_rows` | 📊 Create DataFrame | Build datasets |
| `polars_groupby` | 🔢 Group + aggregate | Fast aggregations |
| `polars_export` | 💾 Export to CSV/Parquet/JSON | Save results |

---

### 🦆 DuckDB Tools (1 tool)

SQL on local data sources. No server needed.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `duckdb_query_local` | 🔍 SQL on local data | Query Parquet/CSV files |

---

### 🎨 JSON & Data Quality (5 tools)

Transform, validate, and correlate events.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `jq_transform` | 🔄 JMESPath transform | Extract/transform JSON |
| `event_validate` | ✅ JSONSchema validation | Validate data structure |
| `event_fingerprint` | 🔑 Generate fingerprint | Create unique IDs |
| `dedupe_by_id` | 🧹 Deduplicate records | Remove duplicates |
| `event_correlate` | 🔗 Correlate events | Join across batches |

---

### 📁 File & Format Tools (4 tools)

Work with files and convert formats.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `parquet_inspect` | 🔍 Inspect Parquet schema | Understand file structure |
| `arrow_convert` | 🔄 Convert formats | Parquet ↔ CSV ↔ JSON |
| `fs_read` | 📖 Read file (with offset) | Read large files efficiently |
| `fs_list_dir` | 📂 List directory | Browse filesystem |
| `rg_search` | 🔎 Search with ripgrep | Find patterns in code/data |

---

### 📊 Visualization & Evidence (2 tools)

Generate charts and evidence bundles automatically.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `dataset_to_chart` | 📈 Auto-generate charts | Visualize data instantly |
| `evidence_bundle_plus` | 📦 Generate evidence bundle | Export stats + samples |

---

### 🏗️ Infrastructure Tools (8 tools)

Monitor Nomad jobs and Consul services.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `nomad_list_jobs` | 📋 List Nomad jobs (fuzzy search) | Find running services |
| `nomad_get_job_status` | 📊 Get full job status | Inspect job details |
| `nomad_get_allocation_logs` | 📜 Read allocation logs | Debug failing jobs |
| `nomad_get_allocation_events` | 📅 Get lifecycle events | Track job history |
| `nomad_get_node_status` | 🖥️ Get node status | Check node health |
| `consul_get_service_ips` | 🌐 Get service IPs (fuzzy) | Find service endpoints |
| `consul_list_services` | 📚 List all services | Discover service catalog |
| `consul_get_service_health` | ❤️ Get service health | Check service status |

**Fuzzy search**: Find services even with partial names! 🎯

---

### 🌐 HTTP Tools (1 tool)

Make HTTP requests safely.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `http_request` | 🌐 HTTP GET/POST | Call APIs, test endpoints |

**Safety**: POST disabled by default. Opt-in required. ✅

---

### ✅ Great Expectations (1 tool)

Data quality validation with GE.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `great_expectations_check` | ✅ Run GE validation | Validate data quality |

---

### 🔄 Reconciliation (1 tool)

Compare datasets and find differences.

| Tool | What it does | When to use |
|------|-------------|-------------|
| `reconcile_counts` | 🔍 Reconcile record counts | Compare datasets |

---

## 🔒 Security & Guardrails

### 🛡️ Built-in Safety

- **Filesystem Allowlist**: All file operations restricted to `MCPKIT_ROOTS`
- **SQL Validation**: JDBC and Athena queries validated (read-only by default)
- **HTTP Safety**: POST requests disabled by default
- **Output Limits**: Automatic caps on rows, records, and bytes
- **Path Traversal Protection**: Blocks `..` and unsafe paths

### 📏 Configurable Limits

| Variable | Default | What it does |
|----------|---------|--------------|
| `MCPKIT_TIMEOUT_SECS` | `15` | Operation timeout |
| `MCPKIT_MAX_OUTPUT_BYTES` | `1000000` | Max output size |
| `MCPKIT_MAX_ROWS` | `500` | Max rows returned |
| `MCPKIT_MAX_RECORDS` | `500` | Max records returned |

---

## ⚙️ Configuration

### 🔐 AWS Credentials

AWS uses the standard boto3 credential chain:

1. **Environment Variables** (recommended):
   ```bash
   export AWS_ACCESS_KEY_ID=your-key
   export AWS_SECRET_ACCESS_KEY=your-secret
   export AWS_REGION=eu-central-1
   ```

2. **AWS Credentials File** (`~/.aws/credentials`):
   ```ini
   [default]
   aws_access_key_id = your-key
   aws_secret_access_key = your-secret
   ```

3. **IAM Roles** (auto-detected on EC2/ECS/Lambda)

4. **AWS SSO** (use `aws sso login` first)

### 📁 Filesystem Security

```bash
export MCPKIT_ROOTS="/allowed/path1:/allowed/path2"
```

### 🎧 Kafka Configuration

```bash
export MCPKIT_KAFKA_BOOTSTRAP="localhost:9092"
export MCPKIT_KAFKA_SECURITY_PROTOCOL="SASL_SSL"  # optional
export MCPKIT_KAFKA_SASL_MECHANISM="PLAIN"  # optional
export MCPKIT_KAFKA_SASL_USERNAME="user"  # optional
export MCPKIT_KAFKA_SASL_PASSWORD="pass"  # optional
```

### 📋 Schema Registry

```bash
export MCPKIT_SCHEMA_REGISTRY_URL="http://localhost:8081"
export MCPKIT_SCHEMA_REGISTRY_BASIC_AUTH="user:pass"  # optional
```

### 🗄️ JDBC

```bash
export MCPKIT_JDBC_DRIVER_CLASS="org.postgresql.Driver"
export MCPKIT_JDBC_URL="jdbc:postgresql://localhost/test"
export MCPKIT_JDBC_JARS="/path/to/postgresql.jar"
export MCPKIT_JDBC_USER="user"  # optional
export MCPKIT_JDBC_PASSWORD="pass"  # optional
```

---

## 🎯 Example Workflows

### 🔍 End-to-End Kafka Pipeline Debugging

**You:** "Debug the 'order-events' topic: consume recent messages, decode Avro, filter errors, analyze patterns, and create a chart"

**AI Response:**
```
I'll debug the order-events pipeline end-to-end.

[Step 1: kafka_consume_tail] Consuming last 50 messages...
✓ Consumed 50 messages from 'order-events'

[Step 2: schema_registry_get] Fetching Avro schema...
✓ Schema retrieved: order-event v2.3 (ID: 145)

[Step 3: avro_decode] Decoding Avro messages...
✓ Decoded 50 messages successfully

[Step 4: kafka_filter] Filtering error events...
✓ Found 8 error events matching criteria

[Step 5: pandas_groupby] Analyzing error patterns...
Error breakdown:
- Payment failures: 4 (50%)
- Validation errors: 2 (25%)
- Timeout errors: 2 (25%)

[Step 6: dataset_to_chart] Creating visualization...
✓ Chart saved: .artifacts/order_errors_analysis.png

[Step 7: pandas_export] Exporting error details...
✓ CSV exported: .artifacts/order_errors_20251221.csv

Summary: 8 errors out of 50 messages (16% error rate). Payment failures are the primary issue.
```

**Tools used:** `kafka_consume_tail` → `schema_registry_get` → `avro_decode` → `kafka_filter` → `pandas_groupby` → `dataset_to_chart` → `pandas_export` (7 tools)

---

### 📊 Multi-Source Data Reconciliation

**You:** "Compare sales data between Athena and Postgres: query both, join on order_id, find discrepancies, and export differences"

**AI Response:**
```
I'll reconcile sales data between Athena and Postgres.

[Step 1: athena_start_query] Querying Athena sales table...
✓ Query started: execution_id abc123

[Step 2: athena_poll_query] Waiting for completion...
✓ Query completed in 3.2s (scanned 1.2 GB)

[Step 3: athena_get_results] Fetching results...
✓ Retrieved 5,234 rows from Athena

[Step 4: pandas_from_rows] Storing Athena data...
✓ Dataset created: athena_sales_20251221

[Step 5: jdbc_query_ro] Querying Postgres sales table...
✓ Retrieved 5,189 rows from Postgres

[Step 6: pandas_from_rows] Storing Postgres data...
✓ Dataset created: postgres_sales_20251221

[Step 7: pandas_join] Joining datasets on order_id...
✓ Joined datasets: 5,180 matching orders

[Step 8: pandas_diff_frames] Finding discrepancies...
Discrepancies found:
- Missing in Postgres: 54 orders
- Missing in Athena: 9 orders
- Amount mismatches: 12 orders
- Total discrepancy value: $3,456.78

[Step 9: pandas_export] Exporting differences...
✓ Differences exported: .artifacts/sales_reconciliation_diff.csv

[Step 10: reconcile_counts] Generating reconciliation report...
✓ Report saved: .artifacts/sales_reconciliation_report.json
```

**Tools used:** `athena_start_query` → `athena_poll_query` → `athena_get_results` → `pandas_from_rows` → `jdbc_query_ro` → `pandas_from_rows` → `pandas_join` → `pandas_diff_frames` → `pandas_export` → `reconcile_counts` (10 tools)

---

### 🏗️ Service Health Investigation

**You:** "Investigate the 'payment-service': find it in Nomad, check Consul health, get logs, filter errors, correlate with events, and export report"

**AI Response:**
```
I'll investigate the payment-service health comprehensively.

[Step 1: nomad_list_jobs] Finding payment-service...
✓ Found: platform-payment-service-app (status: running)

[Step 2: nomad_get_job_status] Getting job details...
✓ Job running on node: node-abc-123
✓ Allocation: alloc-def-456

[Step 3: consul_get_service_health] Checking Consul health...
✓ Service health: passing (3/3 checks passing)
✓ Service IPs: 10.0.1.45:8080, 10.0.1.46:8080

[Step 4: nomad_get_allocation_logs] Retrieving logs...
✓ Retrieved last 200 log lines

[Step 5: pandas_from_rows] Storing logs as dataset...
✓ Dataset created: payment_service_logs

[Step 6: pandas_filter_query] Filtering error logs...
✓ Found 15 error entries in last hour

[Step 7: kafka_consume_batch] Consuming related events...
✓ Consumed 100 payment events from Kafka

[Step 8: event_correlate] Correlating logs with events...
✓ Correlated 12 error logs with payment events

[Step 9: pandas_groupby] Analyzing error patterns...
Error analysis:
- Database timeouts: 8 (53%)
- External API failures: 4 (27%)
- Validation errors: 3 (20%)

[Step 10: evidence_bundle_plus] Generating evidence bundle...
✓ Bundle created:
  - .artifacts/payment_service_investigation_stats.json
  - .artifacts/payment_service_error_logs.csv
  - .artifacts/payment_service_correlated_events.csv

Summary: Service is healthy but experiencing intermittent database timeouts affecting 8 transactions.
```

**Tools used:** `nomad_list_jobs` → `nomad_get_job_status` → `consul_get_service_health` → `nomad_get_allocation_logs` → `pandas_from_rows` → `pandas_filter_query` → `kafka_consume_batch` → `event_correlate` → `pandas_groupby` → `evidence_bundle_plus` (10 tools)

---

### ✅ Data Quality Validation Pipeline

**You:** "Validate Kafka events: consume batch, flatten nested JSON, validate against schema, check data quality, and generate report"

**AI Response:**
```
I'll run a complete data quality validation on Kafka events.

[Step 1: kafka_consume_batch] Consuming event batch...
✓ Consumed 500 messages from 'user-actions' topic

[Step 2: kafka_flatten] Flattening nested JSON structures...
✓ Flattened 500 records into 45 columns
✓ Dataset created: user_actions_flattened

[Step 3: schema_registry_get] Fetching validation schema...
✓ Retrieved JSONSchema: user-action-event v1.2

[Step 4: pandas_sample_random] Sampling for validation...
✓ Sampled 50 random records

[Step 5: event_validate] Validating against schema...
Validation results:
- Valid records: 47 (94%)
- Invalid records: 3 (6%)
  - Missing required field 'timestamp': 2
  - Invalid enum value 'action_type': 1

[Step 6: pandas_schema_check] Checking data types and constraints...
✓ Schema check passed: all required columns present
⚠️ Warning: 12% null values in 'metadata' column

[Step 7: pandas_count_distinct] Analyzing data quality...
Distinct value counts:
- user_id: 234 unique (expected: ~250)
- action_type: 8 distinct (expected: 10)
- session_id: 198 unique

[Step 8: pandas_filter_time_range] Filtering recent events...
✓ Filtered to last 24 hours: 487 events

[Step 9: great_expectations_check] Running GE validation suite...
✓ Expectations passed: 8/10
⚠️ Failed: 
  - expect_column_values_to_be_unique (user_id)
  - expect_column_values_to_not_be_null (metadata)

[Step 10: evidence_bundle_plus] Generating quality report...
✓ Quality report: .artifacts/data_quality_report_20251221.zip
```

**Tools used:** `kafka_consume_batch` → `kafka_flatten` → `schema_registry_get` → `pandas_sample_random` → `event_validate` → `pandas_schema_check` → `pandas_count_distinct` → `pandas_filter_time_range` → `great_expectations_check` → `evidence_bundle_plus` (10 tools)

---

### 📈 Time-Series Analysis & Forecasting

**You:** "Analyze sales trends: query Athena for last 6 months, filter by date range, group by week, sample for quality, create chart, and export"

**AI Response:**
```
I'll analyze sales trends over the last 6 months.

[Step 1: athena_start_query] Starting sales query...
✓ Query: SELECT * FROM sales WHERE date >= '2025-06-01'

[Step 2: athena_poll_query] Monitoring execution...
✓ Query running... (scanned 2.1 GB so far)

[Step 3: athena_get_results] Fetching results...
✓ Retrieved 45,678 sales records

[Step 4: pandas_from_rows] Storing in dataset...
✓ Dataset created: sales_6months

[Step 5: pandas_filter_time_range] Filtering date range...
✓ Filtered: 2025-06-01 to 2025-12-21 (45,234 records)

[Step 6: pandas_groupby] Grouping by week and region...
✓ Aggregated: 26 weeks × 4 regions = 104 groups

[Step 7: pandas_sample_stratified] Stratified sampling...
✓ Sampled 20 records per region for validation

[Step 8: pandas_describe] Statistical analysis...
Statistics:
- Total sales: $12,345,678
- Average per week: $474,833
- Trend: +5.2% week-over-week
- Peak week: Week 24 ($567,890)

[Step 9: dataset_to_chart] Creating time-series chart...
✓ Chart: .artifacts/sales_trends_6months.png

[Step 10: pandas_export] Exporting aggregated data...
✓ Exported: .artifacts/sales_by_week_region.csv (104 rows)
✓ Exported: .artifacts/sales_statistics.json
```

**Tools used:** `athena_start_query` → `athena_poll_query` → `athena_get_results` → `pandas_from_rows` → `pandas_filter_time_range` → `pandas_groupby` → `pandas_sample_stratified` → `pandas_describe` → `dataset_to_chart` → `pandas_export` (10 tools)

---

### 🔄 Cross-System Event Correlation

**You:** "Correlate user events across systems: get Kafka events, query database for user profiles, join datasets, deduplicate, correlate timestamps, and export"

**AI Response:**
```
I'll correlate user events across Kafka and database systems.

[Step 1: kafka_consume_batch] Consuming user events...
✓ Consumed 1,000 events from 'user-events' topic

[Step 2: kafka_flatten] Flattening event structure...
✓ Flattened to 1,000 records with 32 columns

[Step 3: jdbc_query_ro] Querying user profiles...
✓ Retrieved 850 user profiles from database

[Step 4: pandas_from_rows] Storing profiles...
✓ Dataset created: user_profiles

[Step 5: pandas_join] Joining events with profiles...
✓ Joined on user_id: 987 matched records

[Step 6: dedupe_by_id] Removing duplicate events...
✓ Deduplicated: 23 duplicates removed (964 unique)

[Step 7: event_correlate] Correlating by timestamp...
✓ Correlated events into 234 user sessions

[Step 8: pandas_groupby] Analyzing session patterns...
Session analysis:
- Average session duration: 12.5 minutes
- Events per session: 4.1
- Most active users: 12 users with 10+ events

[Step 9: pandas_filter_query] Filtering high-value sessions...
✓ Found 45 sessions with purchase events

[Step 10: pandas_export] Exporting correlated data...
✓ Exported: .artifacts/correlated_user_sessions.csv
✓ Exported: .artifacts/session_analysis.json
```

**Tools used:** `kafka_consume_batch` → `kafka_flatten` → `jdbc_query_ro` → `pandas_from_rows` → `pandas_join` → `dedupe_by_id` → `event_correlate` → `pandas_groupby` → `pandas_filter_query` → `pandas_export` (10 tools)

---

## 🧪 Testing

```bash
pytest
```

All tests use mocks/stubs. No real connections needed! ✅

---

## 🤝 Contributing

We ❤️ contributions! Here's how to help:

1. **🐛 Found a bug?** Open an issue
2. **💡 Have an idea?** Propose a new tool or feature
3. **🔧 Want to code?** Check out `TOOLS_ANALYSIS.md` for gaps
4. **📝 Docs unclear?** Improve them!

**Design Principles:**
- 🎯 **KISS**: One tool, one job
- 🔗 **Composable**: Tools work together seamlessly
- 🔒 **Safe**: Read-only by default, explicit opt-ins
- ⚡ **Fast**: Efficient operations, smart limits

---

## 📚 Learn More

- [Model Context Protocol](https://modelcontextprotocol.io) - The protocol we use
- [FastMCP](https://github.com/jlowin/fastmcp) - The framework we build on
- [Cursor](https://cursor.sh) - The IDE that makes this shine ✨

---

## 📄 License

MIT License - Use it, fork it, make it better! 🚀

---

**Made with ❤️ for data engineers who want to ship faster**

*Stop context-switching. Start shipping.* 🎯
