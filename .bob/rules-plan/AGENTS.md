# Plan Mode Architecture Rules (Non-Obvious Only)

## System Architecture Constraints

### Service Communication Pattern
- Services communicate via HCD `services` table (not REST/message queue)
- `ServicesManager` polls table every iteration for dynamic config updates
- Settings stored as JSON in `settings` column, stats as timeseries in `stats` column
- Query metrics limited to 50 most recent to prevent table bloat

### Dual Wrapper Architecture
- Services use `affiliate_common.database_connections` (CassandraConnection, PrestoConnection)
- Web UI uses `web.cassandra_wrapper` and `web.presto_wrapper`
- These are DIFFERENT implementations with similar interfaces
- Cannot share connection instances between services and web UI

### Data Flow Synchronization
- `generate_traffic.py`: Runs every 60 seconds, generates data for CURRENT minute
- `hcd_to_presto.py`: Wakes at 5 seconds into each minute, processes PREVIOUS minute
- This timing offset is intentional - ensures data availability before ETL
- Both services process minute-aligned data (second=0, microsecond=0)

## Performance Constraints

### Memory Management
- `CookieImpressionTracker`: In-memory 90-minute sliding window
- Cleanup triggered every 1000 impressions to prevent bloat
- Query metrics deduplicated to reduce memory footprint
- Timeseries limited to 90 datapoints per metric

### Batch Size Limits
- Cassandra batch operations: 10,000 records per chunk (hardcoded)
- Presto batch inserts: 10,000 records per chunk (hardcoded)
- Query metrics: 50 most recent per service (hardcoded)
- Cannot be configured via environment variables

### TTL and Retention
- HCD TTL: 6 minutes (360 seconds) - hardcoded in schema
- Attribution window: 90 minutes in-memory only
- No long-term storage in HCD - data expires automatically
- Presto/Iceberg provides historical storage

## Hidden Dependencies

### Environment Setup Requirements
- `setup.sh` must add `ibm-lh-presto-svc` to `/etc/hosts`
- Presto cert path hardcoded: `/certs/presto.crt`
- Virtual env activation added to `.bashrc` by setup script
- Services managed by systemd (not Python process managers)

### Spark Integration Requirements
- Spark session required for aggregation in `hcd_to_presto.py`
- Must disable arrow: `spark.sql.execution.arrow.pyspark.enabled=false`
- Aggregation groups by `(publishers_id, advertisers_id, cookie_id, bucket_date)`
- Cannot use other aggregation frameworks

### Presto Query Limitations
- Presto doesn't use `?` placeholders like Cassandra
- Manual parameter substitution via `_format_presto_query()`
- Single quotes must be escaped: `param.replace("'", "''")`
- No true prepared statement support

## Architectural Decisions

### Query Metrics Deduplication
- Both wrappers deduplicate similar queries using `normalize_query_for_deduplication()`
- Increments `repeat_count` instead of creating duplicate entries
- Normalized queries strip parameter values but keep structure
- Reduces storage and improves performance

### Thread-Local Storage Pattern
- Web UI uses thread-local storage (`_request_queries`) for per-request tracking
- Services use instance-level storage, cleared after each iteration
- Different patterns for different use cases (web vs batch)
- Cannot mix patterns without breaking metrics

### Bucket Distribution Strategy
- Uses `hash(id) % AFFILIATE_JUNCTION_SALES_BUCKETS_COUNT` for write distribution
- Ensures even distribution across buckets
- Bucket count configurable via env var
- Critical for HCD performance at scale