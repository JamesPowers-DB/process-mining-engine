"""
gold_event_log.py — Gold Layer: Unified Process Mining Event Log
================================================================
Assembles ALL timestamped sub-events from the four Silver tables into a
single, subject-agnostic event log suitable for any process mining algorithm.

Schema is deliberately domain-neutral:
  case_id, activity_name, event_ts, resource_id,
  activity_category, event_attributes, event_sequence

Activity derivation rules
--------------------------
FROM silver_transactions (2 events per transaction):
  TRANSACTION_STARTED   @ transaction_start_ts       (always)
  TRANSACTION_COMPLETED @ transaction_end_ts          (status = COMPLETED)
  TRANSACTION_VOIDED    @ transaction_end_ts          (status = VOIDED)
  TRANSACTION_SUSPENDED @ transaction_end_ts          (status = SUSPENDED)

FROM silver_transaction_items (1–2 events per item):
  ITEM_SCANNED_BARCODE   @ scan_ts  (scan_method = BARCODE,       is_void=false, is_return=false)
  ITEM_SCANNED_QR_CODE   @ scan_ts  (scan_method = QR_CODE,       is_void=false, is_return=false)
  ITEM_ENTERED_MANUALLY  @ scan_ts  (scan_method = MANUAL_ENTRY,  is_void=false, is_return=false)
  ITEM_WEIGHED           @ scan_ts  (scan_method = WEIGHT_PLU,    is_void=false, is_return=false)
  ITEM_LOOKED_UP         @ scan_ts  (scan_method = PLU_LOOKUP,    is_void=false, is_return=false)
  ITEM_VOIDED            @ scan_ts  (is_void = true)
  ITEM_RETURNED          @ scan_ts  (is_return = true)

FROM silver_transaction_incidents (2 events per incident):
  <incident_type>_RAISED   @ incident_ts
  <incident_type>_RESOLVED @ resolution_ts

FROM silver_transaction_tender (1 event per tender line):
  TENDER_APPLIED           @ tender_ts

Tie-breaking assumption
-----------------------
When two events share the same millisecond timestamp, ordering within the
case is resolved by activity_category priority:
  TRANSACTION_LIFECYCLE (0) > ITEM_SCAN (1) > INCIDENT (2) > PAYMENT (3)
This matches natural business sequencing: the transaction opens, items are
scanned, exceptions fire, and payment closes the session.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

# ── Activity category constants ───────────────────────────────────────────────
CAT_LIFECYCLE = "TRANSACTION_LIFECYCLE"
CAT_ITEM      = "ITEM_SCAN"
CAT_INCIDENT  = "INCIDENT"
CAT_PAYMENT   = "PAYMENT"

# Category tie-break priority (lower = earlier)
_CAT_PRIORITY = F.create_map(
    F.lit(CAT_LIFECYCLE), F.lit(0),
    F.lit(CAT_ITEM),      F.lit(1),
    F.lit(CAT_INCIDENT),  F.lit(2),
    F.lit(CAT_PAYMENT),   F.lit(3),
)

# ── Helper: build map<string,string> from kwargs ──────────────────────────────
def _attrs(**kv):
    """Return a Spark map literal from keyword args, skipping None values."""
    pairs = []
    for k, v in kv.items():
        pairs.extend([F.lit(k), v if isinstance(v, F.Column) else F.lit(v)])
    return F.create_map(*pairs)


# ── Event source builders ─────────────────────────────────────────────────────

def _transaction_events():
    """Two events per transaction: STARTED + terminal-status event."""
    tx = dlt.read("silver_transactions")

    # TRANSACTION_STARTED
    started = tx.select(
        F.col("transaction_id").alias("case_id"),
        F.lit("TRANSACTION_STARTED").alias("activity_name"),
        F.col("transaction_start_ts").alias("event_ts"),
        F.col("cashier_id").alias("resource_id"),
        F.lit(CAT_LIFECYCLE).alias("activity_category"),
        _attrs(
            store_id=F.col("store_id"),
            terminal_id=F.col("terminal_id"),
            channel=F.col("channel"),
            currency=F.col("currency"),
        ).alias("event_attributes"),
    )

    # TRANSACTION_<STATUS>  (COMPLETED | VOIDED | SUSPENDED)
    ended = tx.select(
        F.col("transaction_id").alias("case_id"),
        F.concat(F.lit("TRANSACTION_"), F.col("status")).alias("activity_name"),
        F.col("transaction_end_ts").alias("event_ts"),
        F.col("cashier_id").alias("resource_id"),
        F.lit(CAT_LIFECYCLE).alias("activity_category"),
        _attrs(
            store_id=F.col("store_id"),
            status=F.col("status"),
            total_amount=F.col("total_amount").cast("string"),
            item_count=F.col("item_count").cast("string"),
        ).alias("event_attributes"),
    )

    return started.unionByName(ended)


def _item_events():
    """
    One event per item line. Activity label is derived from scan_method,
    with ITEM_VOIDED and ITEM_RETURNED taking precedence over scan method.
    """
    items = dlt.read("silver_transaction_items")

    return items.select(
        F.col("transaction_id").alias("case_id"),
        F.when(F.col("is_void"), F.lit("ITEM_VOIDED"))
        .when(F.col("is_return"), F.lit("ITEM_RETURNED"))
        .when(F.col("scan_method") == "BARCODE", F.lit("ITEM_SCANNED_BARCODE"))
        .when(F.col("scan_method") == "QR_CODE", F.lit("ITEM_SCANNED_QR_CODE"))
        .when(F.col("scan_method") == "MANUAL_ENTRY", F.lit("ITEM_ENTERED_MANUALLY"))
        .when(F.col("scan_method") == "WEIGHT_PLU", F.lit("ITEM_WEIGHED"))
        .when(F.col("scan_method") == "PLU_LOOKUP", F.lit("ITEM_LOOKED_UP"))
        .otherwise(F.lit("ITEM_SCANNED_BARCODE"))  # safe default
        .alias("activity_name"),
        F.col("scan_ts").alias("event_ts"),
        F.lit(None).cast(T.StringType()).alias("resource_id"),
        F.lit(CAT_ITEM).alias("activity_category"),
        _attrs(
            sku=F.col("sku"),
            item_name=F.col("item_name"),
            department=F.col("department"),
            category=F.col("category"),
            scan_method=F.col("scan_method"),
            quantity=F.col("quantity").cast("string"),
            unit_price=F.col("unit_price").cast("string"),
            line_id=F.col("line_id"),
        ).alias("event_attributes"),
    )


def _incident_events():
    """
    Two events per incident: <TYPE>_RAISED at incident_ts and
    <TYPE>_RESOLVED at resolution_ts.
    """
    inc = dlt.read("silver_transaction_incidents")

    raised = inc.select(
        F.col("transaction_id").alias("case_id"),
        F.concat(F.col("incident_type"), F.lit("_RAISED")).alias("activity_name"),
        F.col("incident_ts").alias("event_ts"),
        F.lit(None).cast(T.StringType()).alias("resource_id"),
        F.lit(CAT_INCIDENT).alias("activity_category"),
        _attrs(
            incident_id=F.col("incident_id"),
            incident_type=F.col("incident_type"),
            related_line_id=F.col("related_line_id"),
        ).alias("event_attributes"),
    )

    resolved = inc.filter(F.col("resolution_ts").isNotNull()).select(
        F.col("transaction_id").alias("case_id"),
        F.concat(F.col("incident_type"), F.lit("_RESOLVED")).alias("activity_name"),
        F.col("resolution_ts").alias("event_ts"),
        F.col("resolved_by").alias("resource_id"),
        F.lit(CAT_INCIDENT).alias("activity_category"),
        _attrs(
            incident_id=F.col("incident_id"),
            incident_type=F.col("incident_type"),
            resolution=F.col("resolution"),
            duration_seconds=F.col("duration_seconds").cast("string"),
        ).alias("event_attributes"),
    )

    return raised.unionByName(resolved)


def _tender_events():
    """One TENDER_APPLIED event per tender line."""
    tender = dlt.read("silver_transaction_tender")

    return tender.select(
        F.col("transaction_id").alias("case_id"),
        F.lit("TENDER_APPLIED").alias("activity_name"),
        F.col("tender_ts").alias("event_ts"),
        F.lit(None).cast(T.StringType()).alias("resource_id"),
        F.lit(CAT_PAYMENT).alias("activity_category"),
        _attrs(
            tender_type=F.col("tender_type"),
            amount=F.col("amount").cast("string"),
            card_brand=F.col("card_brand"),
            tender_id=F.col("tender_id"),
        ).alias("event_attributes"),
    )


# ── Gold Event Log ────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_event_log",
    comment=(
        "Subject-agnostic unified event log for process mining. "
        "One row per discrete activity occurrence across all transaction sub-events. "
        "Materialized view — fully recomputed on each pipeline run to guarantee "
        "consistency between Silver and Gold."
    ),
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        # Allow full reset when schema changes require it
        "pipelines.reset.allowed": "true",
    },
)
def gold_event_log():
    """
    Materialized view (non-streaming return) that unions all four event
    sources and assigns event_sequence within each case.

    Watermark note (for streaming approach):
      If converted to a streaming table, add:
        .withWatermark("event_ts", "2 hours")
      on each source before union, using transaction_end_ts as the
      completeness signal — all events for a transaction arrive before
      transaction_end_ts + 2h.
    """
    # Union all event sources
    all_events = (
        _transaction_events()
        .unionByName(_item_events())
        .unionByName(_incident_events())
        .unionByName(_tender_events())
    )

    # Assign event_sequence within each case.
    # Tie-break: event_ts ASC → category priority ASC → activity_name ASC
    w = Window.partitionBy("case_id").orderBy(
        F.col("event_ts").asc(),
        _CAT_PRIORITY[F.col("activity_category")].asc(),
        F.col("activity_name").asc(),
    )

    return all_events.withColumn("event_sequence", F.row_number().over(w))
