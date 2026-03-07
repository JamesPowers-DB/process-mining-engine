# POS Process Mining Demo

End-to-end retail point-of-sale process mining demo built on Databricks.
Ingests synthetic POS data, mines a Direct-Follow Graph (DFG), and visualises
it as an interactive Dash application with filtering by store and channel.

```
UC Volume (NDJSON)
      │
      ▼  Auto Loader
Bronze: bronze_pos_transactions
      │
      ▼  parse JSON, watermark dedup
Silver: silver_transactions, silver_items, silver_incidents, silver_tender
      │
      ▼  union events → LEAD() pairs → group by (store, channel, A→B)
Gold:   gold_event_log, gold_dfg_edges, gold_dfg_nodes
      │
      ▼  SQL query (SDK Statement Exec API)
App:    Dash + dash-cytoscape DFG visualization
        Filters: store_id (multi), channel (multi), min frequency slider
```

---

## Architecture

| Layer  | Tables | Technology |
|--------|--------|------------|
| Bronze | `bronze_pos_transactions` | Auto Loader (cloudFiles, text/NDJSON), partitioned by `ingest_date` |
| Silver | `silver_transactions`, `silver_transaction_items`, `silver_transaction_incidents`, `silver_transaction_tender` | Streaming tables, watermark dedup |
| Gold   | `gold_event_log`, `gold_dfg_edges`, `gold_dfg_nodes` | Materialized views, complete refresh |
| App    | Dash + dash-cytoscape | Databricks Apps, gunicorn |

### Gold DFG Tables — Dimension Design

Both `gold_dfg_edges` and `gold_dfg_nodes` are segmented by **`store_id`** and **`channel`**, clustered for efficient filter pushdown:

```
gold_dfg_nodes: (activity_name, store_id, channel) → occurrence_count, case_count, start_count, end_count, avg_activity_duration_ms, activity_category
gold_dfg_edges: (source_activity, target_activity, store_id, channel) → edge_frequency, case_count, avg/min/max/p50/p95_transition_duration_ms
```

The app loads all rows once, then aggregates in Pandas based on active filters — no pipeline re-runs needed for filter changes.

### Activity Categories

| Category | Events | Color |
|----------|--------|-------|
| `TRANSACTION_LIFECYCLE` | STARTED, COMPLETED, VOIDED, SUSPENDED | Blue |
| `ITEM_SCAN` | SCANNED_BARCODE, QR_CODE, MANUAL_ENTRY, WEIGHED, LOOKED_UP, VOIDED, RETURNED | Green |
| `INCIDENT` | `<TYPE>_RAISED`, `<TYPE>_RESOLVED` (matched pairs for duration stats) | Red |
| `PAYMENT` | TENDER_APPLIED (one per tender line) | Purple |

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI (`databricks`) installed and authenticated
- A SQL Warehouse (copy its ID)
- `uv` or `pip` for local Python (optional — only needed to run the app locally)

---

## One-time Setup

### 1. Install the Databricks CLI

```bash
pip install databricks-cli
databricks configure --token   # enter host + PAT
```

### 2. Clone / navigate to this bundle

```bash
cd "POS Process Mining Demo"
```

### 3. Deploy the bundle (dev target)

```bash
databricks bundle deploy --target dev \
  -v catalog=pos_demo \
  -v schema=pos_mining \
  -v warehouse_id=<your-warehouse-id>
```

This creates:
- Unity Catalog: `pos_demo.pos_mining` (schema)
- UC Volume: `pos_demo.pos_mining.raw_pos_data`
- Lakeflow Pipeline: `pos-process-mining-demo`
- Databricks Job: `pos-data-generator`
- Databricks App: `pos-process-mining-app`

---

## Run the Demo

### Step 1 — Seed synthetic data

```bash
databricks bundle run --target dev pos_data_generator
```

Runtime: ~3-5 minutes. Generates **20,000 transactions** as NDJSON files in the
UC Volume `/Volumes/pos_demo/pos_mining/raw_pos_data/`.

### Step 2 — Trigger the pipeline

```bash
databricks bundle run --target dev pos_process_mining
```

Or from the Databricks UI: **Lakeflow Pipelines → pos-process-mining-demo → Start**.

The pipeline runs in 3 stages:
1. Bronze — Auto Loader ingests JSON files (~1 min)
2. Silver — Parse + flatten into 4 typed tables (~2 min)
3. Gold — Build event log + DFG (~3 min, complete refresh)

### Step 3 — Launch the Databricks App

```bash
databricks bundle run --target dev pos_process_mining_app
```

Or from the UI: **Apps → pos-process-mining-app → Open**.

---

## Using the App

The app loads all DFG data on startup (one SQL query), then all filtering is done client-side:

| Control | Behaviour |
|---------|-----------|
| **Store filter** | Multi-select; empty = all stores aggregated |
| **Channel filter** | Multi-select; empty = all channels aggregated |
| **Min Frequency slider** | Hides edges (and disconnected nodes) below threshold. 20 steps: 0, 1, 2, 4, 8 … 256k |
| **Node click** | Shows detail panel: occurrence count, case count, start/end counts, avg duration |

Aggregation across filtered dimensions: frequencies are **summed**; durations are **mean-of-means** (accurate when group sizes are similar).

---

## Daily Incremental Run

The pipeline is triggered-mode (not continuous). Schedule it via:

```yaml
# Add to resources/pipeline.yml under the pipeline definition:
triggers:
  - cron:
      quartz_cron_expression: "0 0 6 * * ?"   # 6 AM UTC daily
      timezone_id: "UTC"
```

Or run on-demand:
```bash
databricks bundle run pos_process_mining
```

**What happens:**
- Bronze: Auto Loader picks up only new files (checkpoint-based)
- Silver: Streaming tables process only new Bronze records
- Gold Event Log: Full recompute (materialized view)
- Gold DFG: Full recompute (complete refresh, segmented by store/channel)

---

## Full DFG Recompute After Schema Changes

If you modify Gold table schemas, reset the pipeline state:

```bash
# Option 1: Reset via CLI (drops Gold checkpoints, forces full recompute)
databricks pipelines reset --pipeline-id <pipeline-id>

# Option 2: If using bundle:
databricks bundle run pos_process_mining --full-refresh-selection gold_dfg_edges,gold_dfg_nodes
```

Gold tables have `"pipelines.reset.allowed": "true"` set, so a pipeline reset
is safe.

---

## Validation Queries

Run these in a SQL Warehouse or notebook against `pos_demo.pos_mining`:

### 1. Event log chronological ordering per case
```sql
-- Verify no case has out-of-order event_sequence vs event_ts
SELECT case_id, COUNT(*) AS violations
FROM (
  SELECT case_id, event_ts, event_sequence,
    LAG(event_ts) OVER (PARTITION BY case_id ORDER BY event_sequence) AS prev_ts
  FROM pos_demo.pos_mining.gold_event_log
)
WHERE event_ts < prev_ts
GROUP BY case_id
ORDER BY violations DESC;
-- Expected: 0 rows
```

### 2. Expected activity labels present
```sql
SELECT activity_name, activity_category, COUNT(*) AS occurrences
FROM pos_demo.pos_mining.gold_event_log
GROUP BY activity_name, activity_category
ORDER BY activity_category, activity_name;
```

### 3. DFG edge frequencies reconcile to event log
```sql
-- Each edge frequency = number of (case, consecutive-pair) observations
-- Cross-check: sum of all edge frequencies ≈ total events - number of cases
SELECT
  (SELECT SUM(edge_frequency) FROM pos_demo.pos_mining.gold_dfg_edges) AS total_edge_obs,
  (SELECT COUNT(*) FROM pos_demo.pos_mining.gold_event_log)
    - (SELECT COUNT(DISTINCT case_id) FROM pos_demo.pos_mining.gold_event_log) AS expected_obs;
-- Both columns should be equal
```

### 4. start_count / end_count sanity (aggregate across all dimensions)
```sql
-- Each case contributes exactly one start and one end
SELECT
  SUM(start_count) AS total_starts,
  SUM(end_count)   AS total_ends,
  (SELECT COUNT(DISTINCT case_id) FROM pos_demo.pos_mining.gold_event_log) AS total_cases
FROM (
  SELECT activity_name, SUM(start_count) AS start_count, SUM(end_count) AS end_count
  FROM pos_demo.pos_mining.gold_dfg_nodes
  GROUP BY activity_name
);
-- total_starts = total_ends = total_cases
```

### 5. DFG nodes by store and channel
```sql
-- Inspect dimension breakdown
SELECT store_id, channel, COUNT(DISTINCT activity_name) AS activities,
       SUM(occurrence_count) AS total_occurrences
FROM pos_demo.pos_mining.gold_dfg_nodes
GROUP BY store_id, channel
ORDER BY store_id, channel;
```

### 6. Top 10 process variants (most common activity sequences)
```sql
SELECT variant, COUNT(*) AS case_count
FROM (
  SELECT case_id,
    ARRAY_JOIN(COLLECT_LIST(activity_name), ' → ') AS variant
  FROM (
    SELECT case_id, activity_name, event_sequence
    FROM pos_demo.pos_mining.gold_event_log
    ORDER BY case_id, event_sequence
  )
  GROUP BY case_id
)
GROUP BY variant
ORDER BY case_count DESC
LIMIT 10;
```

---

## Running the App Locally (Optional)

```bash
cd app
pip install -r requirements.txt

export DATABRICKS_HOST="https://adb-XXXX.azuredatabricks.net"
export DATABRICKS_TOKEN="dapiXXXXXX"
export WAREHOUSE_ID="abc123def456"
export CATALOG="pos_demo"
export SCHEMA="pos_mining"

python app.py
# Open http://localhost:8000
```

---

## File Tree

```
POS Process Mining Demo/
├── databricks.yml                    # Bundle root: variables, targets
├── README.md                         # This file
├── resources/
│   └── pipeline.yml                  # Lakeflow pipeline + job + app definitions
├── src/
│   ├── pipeline/
│   │   ├── bronze.py                 # Auto Loader → bronze_pos_transactions
│   │   ├── silver_transactions.py    # Transaction headers (store_id, channel)
│   │   ├── silver_items.py           # Exploded item lines
│   │   ├── silver_incidents.py       # Exploded incident records
│   │   ├── silver_tender.py          # Exploded tender lines
│   │   ├── gold_event_log.py         # Unified process mining event log (4 categories)
│   │   └── gold_dfg.py               # DFG edges + nodes, segmented by store/channel
│   └── generator/
│       └── generate_pos_data.py      # Synthetic data notebook (20k transactions)
└── app/
    ├── app.py                        # Dash + dash-cytoscape visualization
    ├── requirements.txt
    └── app.yaml                      # Databricks App config
```
