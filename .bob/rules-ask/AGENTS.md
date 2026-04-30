# Ask Mode Documentation Rules (Non-Obvious Only)

## Project Structure Context

### Dual Database Architecture
- HCD (Cassandra): Real-time operational data with 6-minute TTL
- Presto/Iceberg: Historical analytics with hourly partitioning
- Services communicate via HCD `services` table, not REST APIs

### Service Organization
- Root directory contains service scripts (`generate_traffic.py`, `hcd_to_presto.py`, etc.)
- `affiliate_common/`: Shared modules for services (database_connections, services_manager, schema_executor)
- `web/`: FastAPI web application with separate wrapper implementations
- Schema files at root: `hcd_schema.cql`, `presto_schema.sql`

### Wrapper Pattern Confusion
- Services use `affiliate_common.database_connections` (CassandraConnection, PrestoConnection)
- Web UI uses `web.cassandra_wrapper` and `web.presto_wrapper`
- These are DIFFERENT classes with similar interfaces - not the same code

## Data Flow Timing (Non-Obvious)

### Service Execution Schedule
- `generate_traffic.py`: Every 60 seconds, generates data for CURRENT minute
- `hcd_to_presto.py`: Wakes at 5 seconds into each minute, processes PREVIOUS minute
- This offset ensures data is available before ETL runs

### TTL and Retention
- HCD TTL: 6 minutes (360 seconds) - hardcoded in schema, NOT in env vars
- Attribution window: 90 minutes in-memory (CookieImpressionTracker)
- Query metrics: Limited to 50 most recent per service

## Query Metrics System

### Thread-Local Storage Pattern
- Web UI uses thread-local storage (`_request_queries`) for per-request tracking
- Must call `get_request_queries()` to retrieve AND clear metrics
- Services use instance-level storage, cleared after each iteration

### Deduplication Mechanism
- Both wrappers deduplicate similar queries using `normalize_query_for_deduplication()`
- Increments `repeat_count` instead of creating duplicate entries
- Normalized queries strip parameter values but keep structure

## Environment Setup Requirements

### Non-Standard Setup Steps
- `setup.sh` adds `ibm-lh-presto-svc` to `/etc/hosts` (required for Presto connection)
- Virtual env activation added to `.bashrc` automatically
- Services managed by systemd, not Python process managers
- Presto cert path hardcoded: `/certs/presto.crt`

## Hidden Implementation Details

### Presto Query Execution
- Presto doesn't use `?` placeholders like Cassandra
- `_format_presto_query()` manually replaces placeholders with escaped values
- Single quotes MUST be escaped: `param.replace("'", "''")`
- Wrapper creates Row objects with attribute access from tuples

### Batch Operations
- BatchStatement objects passed directly to `execute_query()`
- Must provide `representative_query` parameter for metrics capture
- Batch size limit: 10,000 records per chunk (hardcoded)

### Spark Integration
- Required for aggregation in `hcd_to_presto.py`
- Must disable arrow: `spark.sql.execution.arrow.pyspark.enabled=false`
- Aggregates by `(publishers_id, advertisers_id, cookie_id, bucket_date)`