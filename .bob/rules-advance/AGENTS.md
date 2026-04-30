# Advance Mode Rules (Non-Obvious Only)

## Database Query Execution Patterns

### Batch Operations Must Use Representative Query
When executing batch operations in services, pass the BatchStatement object directly with `representative_query` parameter:
```python
# CORRECT - captures metrics properly
cassandra_connection.execute_query(
    query=batch,  # BatchStatement object
    representative_query="INSERT INTO table (col1, col2) VALUES (?, ?)"
)

# WRONG - will not capture metrics correctly
session.execute(batch)
```

### Web UI Query Retrieval Pattern
Web UI must call `get_request_queries()` to retrieve AND clear request-scoped metrics:
```python
# In route handlers
cassandra_queries = cassandra_wrapper.get_request_queries()
presto_queries = presto_wrapper.get_request_queries()
# Queries are automatically cleared after retrieval
```

### Presto Parameter Substitution
Presto queries with parameters require manual string replacement (not true prepared statements):
```python
# Parameters are replaced by _format_presto_query()
# Single quotes MUST be escaped: param.replace("'", "''")
presto_wrapper.execute_query(
    "SELECT * FROM table WHERE name = ?",
    parameters=["O'Brien"]  # Will be escaped automatically
)
```

## Service Architecture Patterns

### Services Manager Updates
Services must update both timeseries stats AND query metrics:
```python
# Collect iteration stats
iteration_stats = self.collect_iteration_stats(...)

# Update timeseries (maintains 90 datapoints)
self.services_manager.update_timeseries_stats(iteration_stats)

# Update query metrics (limited to 50 most recent)
cassandra_metrics = self.cassandra_connection.get_query_metrics()
presto_metrics = self.presto_client.get_query_metrics()
self.services_manager.update_query_metrics(
    cassandra_metrics=cassandra_metrics,
    presto_metrics=presto_metrics
)

# MUST clear after storing
self.cassandra_connection.clear_query_metrics()
self.presto_client.clear_query_metrics()
```

### Timing Synchronization
- `generate_traffic.py`: Sleeps to maintain 60-second intervals
- `hcd_to_presto.py`: Calculates sleep to wake at 5 seconds into next minute
- Both process data for the PREVIOUS minute, not current

## Data Generation Patterns

### Cookie Selection Logic
Cookie selection uses probability-based cohort matching (see `get_cookie_for_publisher()`):
1. Check for fraud cookie association (always returns fraud cookie)
2. Apply probability thresholds for cohort matching
3. Fallback to random cookie selection

### Attribution Window Enforcement
Conversions only generated for cookies with recent impressions:
```python
# Check eligibility before generating conversion
eligible_cookies = self.cookie_tracker.get_eligible_cookies(now)
if cookie_id in eligible_cookies:
    # Generate conversion
```

## Environment and Configuration

### Hardcoded Paths
- Presto cert: `/certs/presto.crt` (not configurable)
- HCD binary: `./hcd-1.2.3/bin/hcd` (version in path)
- Schema files: `hcd_schema.cql`, `presto_schema.sql` (in script directory)

### Non-Configurable Values
- TTL: 6 minutes (360 seconds) - hardcoded in schema, not in env vars
- Query metrics limit: 50 (hardcoded in services_manager.py)
- Timeseries datapoints: 90 (hardcoded in services_manager.py)
- Cleanup threshold: 1000 impressions (hardcoded in CookieImpressionTracker)

## MCP and Browser Access
This mode has access to MCP tools and browser capabilities for enhanced functionality.