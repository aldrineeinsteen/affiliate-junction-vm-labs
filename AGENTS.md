# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Critical Non-Obvious Patterns

### Database Connection Architecture
- **Dual wrapper pattern**: Services use `affiliate_common.database_connections` (CassandraConnection, PrestoConnection), web UI uses `web.cassandra_wrapper` and `web.presto_wrapper` - different classes with similar interfaces
- **Query metrics deduplication**: Both wrappers automatically deduplicate similar queries using `normalize_query_for_deduplication()` - increments `repeat_count` instead of creating duplicate metrics
- **Batch operations**: Pass BatchStatement object directly to `execute_query()` with `representative_query` parameter for proper metrics capture (see generate_traffic.py lines 621-651)

### Service Communication Pattern
- Services communicate via HCD `services` table (not REST/message queue)
- `ServicesManager` polls table every iteration for dynamic config updates
- Settings stored as JSON in `settings` column, stats as timeseries in `stats` column
- Query metrics limited to 50 most recent to prevent table bloat

### Data Flow Timing
- `generate_traffic.py`: Runs every 60 seconds, generates data for current minute
- `hcd_to_presto.py`: Runs at 5 seconds into each minute (processes previous minute's data)
- TTL is 6 minutes (360 seconds) in HCD, not configurable via env vars
- Bucket distribution uses `hash(id) % AFFILIATE_JUNCTION_SALES_BUCKETS_COUNT` for write distribution

### Attribution Window
- `CookieImpressionTracker` maintains 90-minute sliding window in memory (not in database)
- Conversions only generated for cookies with impressions in last 90 minutes
- Tracker cleanup happens every 1000 impressions to prevent memory bloat

### Presto Query Formatting
- Presto doesn't use `?` placeholders - `_format_presto_query()` manually replaces them
- Must escape single quotes in string parameters: `param.replace("'", "''")`
- Presto wrapper creates Row objects with attribute access from tuples

### Environment Setup
- Must run `setup.sh` which adds `ibm-lh-presto-svc` to `/etc/hosts`
- Presto cert path hardcoded: `/certs/presto.crt`
- Virtual env activation added to `.bashrc` by setup script
- Services enabled via systemd, not managed by Python

### Web UI Query Context
- Thread-local storage (`_request_queries`) tracks queries per HTTP request
- Must call `get_request_queries()` to retrieve and clear request-scoped metrics
- Query formatting uses `sqlparse` with specific options (see cassandra_wrapper.py lines 49-64)

### Spark Integration
- Spark session required for aggregation in `hcd_to_presto.py`
- Must disable arrow: `spark.sql.execution.arrow.pyspark.enabled=false`
- Aggregation groups by `(publishers_id, advertisers_id, cookie_id, bucket_date)`

## Commands

```bash
# Setup (run once)
./setup.sh

# Service management
sudo systemctl start|stop|restart generate_traffic
sudo systemctl start|stop|restart hcd_to_presto
sudo systemctl start|stop|restart presto_to_hcd
sudo systemctl start|stop|restart presto_insights
sudo systemctl start|stop|restart presto_cleanup
sudo systemctl start|stop|restart uvicorn

# View service logs
journalctl -u <service_name> -f

# Access HCD CQL console
./hcd-1.2.3/bin/hcd cqlsh 172.17.0.1 -u cassandra -p cassandra

# Web UI
http://localhost:10000
# Login: watsonx / watsonx.data
```

## File Organization
- `affiliate_common/`: Shared modules for services (database_connections, services_manager, schema_executor)
- `web/`: FastAPI web application with separate wrappers
- Service scripts at root: `generate_traffic.py`, `hcd_to_presto.py`, etc.
- Schema files: `hcd_schema.cql`, `presto_schema.sql`