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

gold_dfg_edges  — directional transitions with frequency and duration stats
gold_dfg_nodes  — per-activity occurrence stats including start/end counts

Both tables are COMPLETE REFRESH materialized views:
  - DFG is a global aggregate; partial updates would produce incorrect frequencies.
  - pipelines.reset.allowed=true permits resetting state when needed.
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


# ── gold_dfg_edges ─────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_dfg_edges",
    comment=(
        "Direct-Follow Graph edges — one row per unique (source_activity → target_activity) "
        "pair aggregated across all cases. Complete refresh on every pipeline run."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
def gold_dfg_edges():
    """
    Step 1: Use LEAD() to pair each event with its immediate successor within
            the same case, ordered by event_ts then event_sequence.
    Step 2: Filter out the terminal event in each case (LEAD returns null).
    Step 3: Aggregate edge observations into frequency + duration statistics.
    """
    event_log = dlt.read("gold_event_log")

    # ── Step 1: Annotate each event with its successor ────────────────────────
    w_case = Window.partitionBy("case_id").orderBy("event_ts", "event_sequence")

    edges_raw = event_log.select(
        F.col("case_id"),
        F.col("activity_name").alias("source_activity"),
        F.col("event_ts").alias("source_ts"),
        F.col("event_sequence").alias("source_seq"),
        F.lead(F.col("activity_name")).over(w_case).alias("target_activity"),
        F.lead(F.col("event_ts")).over(w_case).alias("target_ts"),
    )

    # ── Step 2: Drop terminal events (no successor) ───────────────────────────
    edges_obs = edges_raw.filter(F.col("target_activity").isNotNull()).select(
        F.col("case_id"),
        F.col("source_activity"),
        F.col("target_activity"),
        # Duration in milliseconds; cast to long for arithmetic precision
        (
            F.col("target_ts").cast("long") * 1000
            - F.col("source_ts").cast("long") * 1000
        ).alias("duration_ms"),
    )

    # ── Step 3: Aggregate ─────────────────────────────────────────────────────
    return edges_obs.groupBy("source_activity", "target_activity").agg(
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
        "Direct-Follow Graph nodes — one row per unique activity_name with "
        "occurrence statistics and start/end case counts. Complete refresh."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
def gold_dfg_nodes():
    """
    Computes per-node statistics from gold_event_log.

    start_count: number of cases where this activity is event_sequence = 1
    end_count:   number of cases where this activity is the MAX event_sequence

    avg_activity_duration_ms: only meaningful for activities that appear as
    both _RAISED and _RESOLVED pairs (i.e., incidents). For all others, null.
    The self-duration is approximated by joining raised↔resolved events and
    computing the mean of their timestamp differences.
    """
    event_log = dlt.read("gold_event_log")

    # ── Base occurrence stats ─────────────────────────────────────────────────
    base = event_log.groupBy("activity_name", "activity_category").agg(
        F.count("*").alias("occurrence_count"),
        F.countDistinct("case_id").alias("case_count"),
    )

    # ── Start counts: event_sequence = 1 per case ────────────────────────────
    starts = (
        event_log.filter(F.col("event_sequence") == 1)
        .groupBy("activity_name")
        .agg(F.count("*").alias("start_count"))
    )

    # ── End counts: max event_sequence per case ───────────────────────────────
    max_seq_per_case = event_log.groupBy("case_id").agg(
        F.max("event_sequence").alias("max_seq")
    )
    ends = (
        event_log.join(max_seq_per_case, "case_id")
        .filter(F.col("event_sequence") == F.col("max_seq"))
        .groupBy("activity_name")
        .agg(F.count("*").alias("end_count"))
    )

    # ── Incident self-duration (raised → resolved pairs) ─────────────────────
    # Match _RAISED events with their corresponding _RESOLVED events by case_id
    # and shared incident base name (strip suffix from activity_name).
    raised = (
        event_log.filter(F.col("activity_name").endswith("_RAISED"))
        .select(
            F.col("case_id"),
            F.regexp_replace(F.col("activity_name"), "_RAISED$", "").alias("base_name"),
            F.col("event_ts").alias("raised_ts"),
        )
    )
    resolved = (
        event_log.filter(F.col("activity_name").endswith("_RESOLVED"))
        .select(
            F.col("case_id"),
            F.regexp_replace(F.col("activity_name"), "_RESOLVED$", "").alias("base_name"),
            F.col("event_ts").alias("resolved_ts"),
        )
    )
    incident_duration = (
        raised.join(resolved, ["case_id", "base_name"])
        .select(
            # Attribute duration to the _RAISED activity name
            F.concat(F.col("base_name"), F.lit("_RAISED")).alias("activity_name"),
            (
                (F.col("resolved_ts").cast("long") - F.col("raised_ts").cast("long"))
                * 1000
            ).alias("duration_ms"),
        )
        .groupBy("activity_name")
        .agg(F.avg("duration_ms").alias("avg_activity_duration_ms"))
    )

    # ── Assemble final node table ─────────────────────────────────────────────
    return (
        base.join(starts, "activity_name", "left")
        .join(ends, "activity_name", "left")
        .join(incident_duration, "activity_name", "left")
        .select(
            F.col("activity_name"),
            F.col("occurrence_count"),
            F.col("case_count"),
            F.coalesce(F.col("start_count"), F.lit(0)).alias("start_count"),
            F.coalesce(F.col("end_count"), F.lit(0)).alias("end_count"),
            F.col("avg_activity_duration_ms"),
            F.col("activity_category"),
        )
    )
