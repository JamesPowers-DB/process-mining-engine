"""
gold_dfg.py — Gold Layer: Direct-Follow Graph (DFG) Algorithm
=============================================================
Applies the DFG algorithm to gold_event_log to produce two subject-agnostic
tables consumed by the Databricks App for visualization.

Algorithm
---------
For each case_id, sort events by (event_ts ASC, event_sequence ASC).
For each consecutive pair (A → B) within a case, record one transition
observation including the elapsed milliseconds between A and B.
Aggregate all observations across all cases.

Dimension breakdown
-------------------
Both tables are segmented by store_id and channel so the Dash application
can filter or aggregate across any combination without re-running the pipeline.
When the app queries with no filter it SUM/AVGs across all rows; when it
filters by store(s) or channel(s) it adds a WHERE clause.

gold_dfg_edges  — directional transitions with frequency and duration stats
gold_dfg_nodes  — per-activity occurrence stats including start/end counts

Both tables are COMPLETE REFRESH materialized views:
  - DFG is a global aggregate; partial updates would produce incorrect frequencies.
  - pipelines.reset.allowed=true permits resetting state when schema changes.
  - cluster_by=["store_id", "channel"] enables efficient filter pushdown.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window


# ── Percentile helper ─────────────────────────────────────────────────────────
def _pct(col_name: str, p: float) -> F.Column:
    """Return percentile_approx column expression."""
    return F.percentile_approx(F.col(col_name), p).alias(
        f"p{int(p*100)}_transition_duration_ms"
    )


# ── Case-level dimension lookup ───────────────────────────────────────────────

def _case_dims():
    """
    Return a (case_id, store_id, channel) lookup DataFrame from silver_transactions.

    All events in gold_event_log share the same case_id (= transaction_id).
    Joining this lookup onto the event log propagates store and channel to
    every event without those fields needing to be stored in the event log itself.
    """
    return dlt.read("silver_transactions").select(
        F.col("transaction_id").alias("case_id"),
        F.col("store_id"),
        F.col("channel"),
    )


# ── gold_dfg_edges ─────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_dfg_edges",
    comment=(
        "Direct-Follow Graph edges — one row per unique "
        "(store_id, channel, source_activity → target_activity) combination, "
        "aggregated across all cases. Complete refresh on every pipeline run. "
        "Cluster by store_id and channel for efficient filter pushdown."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true",
        "delta.enableChangeDataFeed": "true",
    },
    cluster_by=["store_id", "channel"],
)
def gold_dfg_edges():
    """
    Step 1: Enrich the event log with case-level store_id and channel via join
            with silver_transactions.
    Step 2: Use LEAD() to pair each event with its immediate successor within
            the same case, ordered by event_ts then event_sequence.
    Step 3: Filter out the terminal event in each case (LEAD returns null).
    Step 4: Aggregate edge observations into frequency + duration statistics,
            grouped by (store_id, channel, source_activity, target_activity).
    """
    event_log = dlt.read("gold_event_log")
    dims = _case_dims()

    # ── Step 1: Enrich events with case-level dimensions ──────────────────────
    event_log_enriched = event_log.join(dims, "case_id", "left")

    # ── Step 2: Annotate each event with its successor ────────────────────────
    w_case = Window.partitionBy("case_id").orderBy("event_ts", "event_sequence")

    edges_raw = event_log_enriched.select(
        F.col("case_id"),
        F.col("store_id"),
        F.col("channel"),
        F.col("activity_name").alias("source_activity"),
        F.col("event_ts").alias("source_ts"),
        F.lead(F.col("activity_name")).over(w_case).alias("target_activity"),
        F.lead(F.col("event_ts")).over(w_case).alias("target_ts"),
    )

    # ── Step 3: Drop terminal events (no successor) ───────────────────────────
    edges_obs = edges_raw.filter(F.col("target_activity").isNotNull()).select(
        F.col("case_id"),
        F.col("store_id"),
        F.col("channel"),
        F.col("source_activity"),
        F.col("target_activity"),
        # Duration in milliseconds; cast to long for arithmetic precision
        (
            F.col("target_ts").cast("long") * 1000
            - F.col("source_ts").cast("long") * 1000
        ).alias("duration_ms"),
    )

    # ── Step 4: Aggregate ─────────────────────────────────────────────────────
    return edges_obs.groupBy(
        "store_id", "channel", "source_activity", "target_activity"
    ).agg(
        F.count("*").alias("edge_frequency"),
        F.countDistinct("case_id").alias("case_count"),
        F.avg("duration_ms").alias("avg_transition_duration_ms"),
        F.min("duration_ms").alias("min_transition_duration_ms"),
        F.max("duration_ms").alias("max_transition_duration_ms"),
        F.percentile_approx("duration_ms", 0.50).alias("p50_transition_duration_ms"),
        F.percentile_approx("duration_ms", 0.95).alias("p95_transition_duration_ms"),
    )


# ── gold_dfg_nodes ─────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_dfg_nodes",
    comment=(
        "Direct-Follow Graph nodes — one row per unique "
        "(store_id, channel, activity_name) combination with occurrence "
        "statistics and start/end case counts. Complete refresh. "
        "Cluster by store_id and channel for efficient filter pushdown."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true",
        "delta.enableChangeDataFeed": "true",
    },
    cluster_by=["store_id", "channel"],
)
def gold_dfg_nodes():
    """
    Computes per-node statistics from gold_event_log, broken down by
    store_id and channel so the app can filter without pipeline re-runs.

    start_count: number of cases where this activity is event_sequence = 1
    end_count:   number of cases where this activity is the MAX event_sequence

    avg_activity_duration_ms: only meaningful for activities that appear as
    both _RAISED and _RESOLVED pairs (i.e., incidents). For all others, null.
    The self-duration is approximated by joining raised↔resolved events and
    computing the mean of their timestamp differences.
    """
    event_log = dlt.read("gold_event_log")
    dims = _case_dims()

    # ── Enrich events with case-level dimensions ──────────────────────────────
    event_log_enriched = event_log.join(dims, "case_id", "left")

    # ── Base occurrence stats ─────────────────────────────────────────────────
    base = event_log_enriched.groupBy(
        "activity_name", "activity_category", "store_id", "channel"
    ).agg(
        F.count("*").alias("occurrence_count"),
        F.countDistinct("case_id").alias("case_count"),
    )

    # ── Start counts: event_sequence = 1 per case ────────────────────────────
    starts = (
        event_log_enriched.filter(F.col("event_sequence") == 1)
        .groupBy("activity_name", "store_id", "channel")
        .agg(F.count("*").alias("start_count"))
    )

    # ── End counts: max event_sequence per case ───────────────────────────────
    max_seq_per_case = event_log_enriched.groupBy("case_id").agg(
        F.max("event_sequence").alias("max_seq")
    )
    ends = (
        event_log_enriched.join(max_seq_per_case, "case_id")
        .filter(F.col("event_sequence") == F.col("max_seq"))
        .groupBy("activity_name", "store_id", "channel")
        .agg(F.count("*").alias("end_count"))
    )

    # ── Incident self-duration (raised → resolved pairs) ─────────────────────
    # Match _RAISED events with their corresponding _RESOLVED events by case_id
    # and shared incident base name (strip suffix from activity_name).
    # store_id and channel come from the raised side (case-level attributes
    # are the same for both events within the same case).
    raised = (
        event_log_enriched.filter(F.col("activity_name").endswith("_RAISED"))
        .select(
            F.col("case_id"),
            F.col("store_id"),
            F.col("channel"),
            F.regexp_replace(F.col("activity_name"), "_RAISED$", "").alias("base_name"),
            F.col("event_ts").alias("raised_ts"),
        )
    )
    resolved = (
        event_log_enriched.filter(F.col("activity_name").endswith("_RESOLVED"))
        .select(
            F.col("case_id"),
            F.regexp_replace(F.col("activity_name"), "_RESOLVED$", "").alias("base_name"),
            F.col("event_ts").alias("resolved_ts"),
        )
    )
    incident_duration = (
        raised.join(resolved, ["case_id", "base_name"])
        .select(
            F.col("store_id"),
            F.col("channel"),
            # Attribute duration to the _RAISED activity name
            F.concat(F.col("base_name"), F.lit("_RAISED")).alias("activity_name"),
            (
                (F.col("resolved_ts").cast("long") - F.col("raised_ts").cast("long"))
                * 1000
            ).alias("duration_ms"),
        )
        .groupBy("store_id", "channel", "activity_name")
        .agg(F.avg("duration_ms").alias("avg_activity_duration_ms"))
    )

    # ── Assemble final node table ─────────────────────────────────────────────
    return (
        base
        .join(starts, ["activity_name", "store_id", "channel"], "left")
        .join(ends,   ["activity_name", "store_id", "channel"], "left")
        .join(incident_duration, ["activity_name", "store_id", "channel"], "left")
        .select(
            F.col("activity_name"),
            F.col("store_id"),
            F.col("channel"),
            F.col("occurrence_count"),
            F.col("case_count"),
            F.coalesce(F.col("start_count"), F.lit(0)).alias("start_count"),
            F.coalesce(F.col("end_count"),   F.lit(0)).alias("end_count"),
            F.col("avg_activity_duration_ms"),
            F.col("activity_category"),
        )
    )
