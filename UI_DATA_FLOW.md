# UI Data Flow and Component Integration

## Overview

This document explains how all components (HCD, Presto, Services, Web UI) connect and interact in the Affiliate Junction demo.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         USER BROWSER                                │
│                    http://<VM_IP>:10000                            │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ HTTP Requests
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    WEB UI (FastAPI/Uvicorn)                         │
│                         Port 10000                                  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  Routes:                                                      │  │
│  │  • /                    → Dashboard (index.html)             │  │
│  │  • /publisher/{id}      → Publisher view                     │  │
│  │  • /advertiser/{id}     → Advertiser view                    │  │
│  │  • /fraud               → Fraud detection view               │  │
│  │  • /services            → Services monitoring                │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  Database Wrappers:                                          │  │
│  │  • cassandra_wrapper    → HCD queries                        │  │
│  │  • presto_wrapper       → Presto queries                     │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────┬───────────────────────────────┬─────────────────────┘
              │                               │
              │ CQL Queries                   │ SQL Queries
              │ (Port 9042)                   │ (Port 31138/8443)
              ▼                               ▼
┌─────────────────────────┐    ┌──────────────────────────────────────┐
│   HCD (Cassandra)       │    │   Presto (watsonx.data)              │
│   Local on VM           │    │   Cloud or Local                     │
│   Port 9042             │    │   Port 31138 (Enterprise)            │
│                         │    │   Port 8443 (Developer Edition)      │
│  Tables:                │    │                                      │
│  • impression_tracking  │    │  Tables (Iceberg):                   │
│  • conversion_tracking  │    │  • impression_tracking               │
│  • services             │    │  • conversion_tracking               │
│  • publishers           │    │  • publishers                        │
│  • advertisers          │    │  • advertisers                       │
│  • fraud_cookies        │    │  • fraud_cookies                     │
│                         │    │                                      │
│  TTL: 6 minutes         │    │  Partitioned by bucket_date          │
└─────────▲───────────────┘    └──────────▲───────────────────────────┘
          │                               │
          │ Writes                        │ Writes (ETL)
          │                               │
┌─────────┴───────────────────────────────┴─────────────────────────┐
│                    BACKGROUND SERVICES                             │
│                    (Python + systemd)                              │
│                                                                    │
│  1. generate_traffic.service                                      │
│     • Generates synthetic affiliate data every 60 seconds         │
│     • Writes to HCD impression_tracking & conversion_tracking     │
│     • Simulates publishers, advertisers, cookies, fraud           │
│                                                                    │
│  2. hcd_to_presto.service                                         │
│     • Runs at :05 seconds of each minute                          │
│     • Reads previous minute's data from HCD                       │
│     • Aggregates using Spark                                      │
│     • Writes to Presto/Iceberg tables                             │
│     • Creates Presto schema if not exists                         │
│                                                                    │
│  3. presto_to_hcd.service                                         │
│     • Reads aggregated data from Presto                           │
│     • Writes summary stats back to HCD                            │
│     • Enables fast dashboard queries                              │
│                                                                    │
│  4. presto_insights.service                                       │
│     • Generates analytics insights                                │
│     • Fraud detection patterns                                    │
│     • Performance metrics                                         │
│                                                                    │
│  5. presto_cleanup.service                                        │
│     • Removes old partitions from Presto                          │
│     • Maintains data retention policies                           │
└───────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Through UI

### 1. User Accesses Dashboard (/)

**Request Flow:**
```
Browser → FastAPI (main.py) → index.html template
```

**Data Sources:**
- **HCD**: Real-time counts (last 6 minutes)
  - Total impressions
  - Total conversions
  - Active publishers/advertisers
  
- **Presto**: Historical analytics
  - Trend data (last 24 hours)
  - Top performers
  - Fraud patterns

**Code Path:**
```python
# web/main.py
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    # Check authentication
    user = get_current_user(request)
    
    # Query HCD for real-time data
    hcd_stats = cassandra_wrapper.execute_query_simple(
        "SELECT COUNT(*) FROM impression_tracking WHERE timestamp >= ?"
    )
    
    # Query Presto for analytics
    presto_stats = presto_wrapper.execute_query_simple(
        "SELECT SUM(impressions) FROM iceberg.affiliate_junction.impression_tracking 
         WHERE bucket_date >= CURRENT_DATE - INTERVAL '1' DAY"
    )
    
    # Render template with data
    return templates.TemplateResponse("index.html", {
        "request": request,
        "hcd_stats": hcd_stats,
        "presto_stats": presto_stats
    })
```

### 2. Publisher Dashboard (/publisher/{id})

**Request Flow:**
```
Browser → FastAPI → publishers.py → cassandra_wrapper + presto_wrapper
```

**Data Sources:**
- **HCD**: Real-time publisher metrics
  - Recent impressions (last 6 minutes)
  - Recent conversions
  - Current performance
  
- **Presto**: Historical publisher analytics
  - Daily trends
  - Top advertisers for this publisher
  - Revenue calculations
  - Fraud detection for publisher's traffic

**Code Path:**
```python
# web/publishers.py
def get_publisher_dashboard(publisher_id: str):
    # Real-time data from HCD
    recent_impressions = cassandra_wrapper.execute_query_simple(
        "SELECT * FROM impression_tracking 
         WHERE publishers_id = ? AND timestamp >= ?",
        parameters=[publisher_id, six_minutes_ago]
    )
    
    # Historical analytics from Presto
    trends = presto_wrapper.execute_query_simple(
        "SELECT DATE(bucket_date) as date, SUM(impressions) as total
         FROM iceberg.affiliate_junction.impression_tracking
         WHERE publishers_id = ?
         AND bucket_date >= CURRENT_DATE - INTERVAL '30' DAY
         GROUP BY DATE(bucket_date)
         ORDER BY date",
        parameters=[publisher_id]
    )
    
    return {
        "real_time": recent_impressions,
        "trends": trends
    }
```

### 3. Advertiser Dashboard (/advertiser/{id})

**Request Flow:**
```
Browser → FastAPI → advertisers.py → cassandra_wrapper + presto_wrapper
```

**Data Sources:**
- **HCD**: Real-time advertiser metrics
  - Recent conversions
  - Active campaigns
  
- **Presto**: Historical advertiser analytics
  - Conversion rates by publisher
  - ROI calculations
  - Campaign performance over time

### 4. Fraud Detection (/fraud)

**Request Flow:**
```
Browser → FastAPI → fraud operations → presto_wrapper (primarily)
```

**Data Sources:**
- **Presto**: Complex fraud analytics
  - Cross-publisher cookie patterns
  - Suspicious conversion rates
  - Cohort analysis
  - Time-based anomalies

**Why Presto?** Fraud detection requires:
- Complex JOINs across multiple tables
- Window functions for pattern detection
- Historical data analysis (beyond 6-minute TTL)

### 5. Services Monitoring (/services)

**Request Flow:**
```
Browser → FastAPI → HCD services table
```

**Data Source:**
- **HCD**: Services status and metrics
  - Service health
  - Query metrics (last 50 queries)
  - Timeseries stats (last 90 datapoints)

**Code Path:**
```python
# Services communicate via HCD 'services' table
# Each service updates its row with:
# - Status (running/stopped)
# - Settings (JSON config)
# - Stats (timeseries data)
# - Query metrics (recent database operations)

service_status = cassandra_wrapper.execute_query_simple(
    "SELECT * FROM services WHERE service_name = ?",
    parameters=["generate_traffic"]
)
```

---

## Query Transparency Feature

### Real-Time Query Viewer

Every page includes a **Query Panel** that shows:
- All HCD queries executed for that page
- All Presto queries executed for that page
- Query timing and parameters
- Success/failure status

**How It Works:**

```python
# web/cassandra_wrapper.py
class CassandraWrapper:
    def execute_query(self, query, parameters=None):
        # Execute query
        result = session.execute(query, parameters)
        
        # Capture metrics
        self._request_queries.append({
            "query_id": f"cql_{id}",
            "query_text": query,
            "parameters": parameters,
            "execution_time_ms": elapsed_time,
            "query_type": "HCD"
        })
        
        return result
    
    def get_request_queries(self):
        # Return and clear request-scoped queries
        queries = self._request_queries.copy()
        self._request_queries.clear()
        return queries

# web/main.py
@app.middleware("http")
async def query_metrics_middleware(request: Request, call_next):
    response = await call_next(request)
    
    # Get queries executed during this request
    cassandra_queries = cassandra_wrapper.get_request_queries()
    presto_queries = presto_wrapper.get_request_queries()
    
    # Attach to response for UI display
    response.headers["X-HCD-Query-Count"] = str(len(cassandra_queries))
    response.headers["X-Presto-Query-Count"] = str(len(presto_queries))
    
    return response
```

**UI Display:**
```javascript
// web/assets/js/partial-query-system.js
function updateQueryPanel() {
    // Fetch query metrics from API
    fetch('/api/query-metrics')
        .then(response => response.json())
        .then(data => {
            // Display HCD queries
            displayQueries(data.cassandra_queries, 'hcd-panel');
            
            // Display Presto queries
            displayQueries(data.presto_queries, 'presto-panel');
            
            // Update counters
            updateQueryCounters(data);
        });
}
```

---

## Configuration Flow

### How .env Connects Everything

```bash
# .env file (configured before setup.sh)

# HCD Configuration (Local)
HCD_HOST=172.17.0.1          # Docker bridge IP
HCD_PORT=9042                # Cassandra port
HCD_KEYSPACE=affiliate_junction

# Presto Configuration (Cloud or Local)
PRESTO_HOST=747e742b-xxx.lakehouse.ibmappdomain.cloud  # Your watsonx.data
PRESTO_PORT=31138                                       # Custom port
PRESTO_USER=ibmlhapikey_student_xxx@techzone.ibm.com  # API key user
PRESTO_PASSWD=your_api_key                             # API key
PRESTO_CATALOG=iceberg_data                            # Iceberg catalog
PRESTO_SCHEMA=affiliate_junction                       # Schema name

# Web UI Authentication
WEB_AUTH_USER=watsonx
WEB_AUTH_PASSWD=watsonx.data
```

**How Services Use It:**

```python
# All services and web UI load .env
from dotenv import load_dotenv
load_dotenv()

# HCD Connection
cassandra_connection = CassandraConnection(
    host=os.getenv('HCD_HOST'),
    port=int(os.getenv('HCD_PORT')),
    keyspace=os.getenv('HCD_KEYSPACE')
)

# Presto Connection
presto_connection = PrestoConnection(
    host=os.getenv('PRESTO_HOST'),
    port=int(os.getenv('PRESTO_PORT')),
    user=os.getenv('PRESTO_USER'),
    password=os.getenv('PRESTO_PASSWD'),
    catalog=os.getenv('PRESTO_CATALOG'),
    schema=os.getenv('PRESTO_SCHEMA'),
    cert_path='/certs/presto.crt'
)
```

---

## Summary: Complete Integration

1. **Background Services** generate and process data
   - `generate_traffic` → writes to HCD
   - `hcd_to_presto` → ETL from HCD to Presto
   - `presto_to_hcd` → summary stats back to HCD

2. **Web UI** queries both databases
   - HCD for real-time data (last 6 minutes)
   - Presto for historical analytics (days/weeks)

3. **Query Transparency** shows all database operations
   - Every query is captured and displayed
   - Demonstrates federated query patterns

4. **Configuration** ties it all together
   - `.env` file configures all connections
   - `/certs/presto.crt` enables SSL
   - Services communicate via HCD `services` table

5. **User Experience**
   - Login with watsonx/watsonx.data
   - View real-time + historical data
   - See actual queries powering each view
   - Monitor service health and performance

The beauty of this architecture is that it demonstrates **watsonx.data's federated capabilities** - seamlessly combining operational (HCD) and analytical (Presto) workloads in a single application.