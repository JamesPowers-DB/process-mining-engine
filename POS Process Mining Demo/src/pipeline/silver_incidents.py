"""
silver_incidents.py — Silver Layer: Transaction Incidents
=========================================================
Explodes the 'incidents' array from Bronze JSON records into individual
rows — one row per (transaction_id, incident_id).

Incidents represent deviations from the "happy path" and are the richest
source of process mining signal in the event log.

DQ Expectations (comment block — add before production):
---------------------------------------------------------
  @dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
  @dlt.expect_or_drop("valid_incident_id",    "incident_id IS NOT NULL")
  @dlt.expect_or_drop("valid_incident_ts",    "incident_ts IS NOT NULL")
  @dlt.expect("resolution_after_incident",
      "resolution_ts >= incident_ts OR resolution_ts IS NULL")
  @dlt.expect("non_negative_duration",
      "duration_seconds >= 0 OR duration_seconds IS NULL")
---------------------------------------------------------
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ── JSON schema: transaction_id + incidents array ─────────────────────────────
_INCIDENTS_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField(
            "incidents",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("incident_id", T.StringType()),
                        T.StructField("incident_ts", T.TimestampType()),
                        T.StructField("incident_type", T.StringType()),
                        T.StructField("related_line_id", T.StringType()),
                        T.StructField("resolution", T.StringType()),
                        T.StructField("resolution_ts", T.TimestampType()),
                        T.StructField("resolved_by", T.StringType()),
                        T.StructField("duration_seconds", T.IntegerType()),
                    ]
                )
            ),
        ),
    ]
)


@dlt.table(
    name="silver_transaction_incidents",
    comment=(
        "Exploded POS incidents — one row per (transaction_id, incident_id). "
        "Each incident produces two events in the Gold event log: _RAISED and _RESOLVED."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
def silver_transaction_incidents():
    """
    Streaming table: reads Bronze, parses the incidents array, and explodes it.

    Transactions with no incidents (empty or null array) produce zero rows —
    this is normal and expected for the majority of transactions.
    """
    return (
        dlt.read_stream("bronze_pos_transactions")
        .select(
            F.from_json(F.col("raw_json"), _INCIDENTS_SCHEMA).alias("d"),
            F.col("ingest_ts"),
        )
        # Skip transactions with no incidents
        .filter(F.col("d.incidents").isNotNull() & (F.size(F.col("d.incidents")) > 0))
        .select(
            F.col("d.transaction_id"),
            F.explode(F.col("d.incidents")).alias("inc"),
            F.col("ingest_ts"),
        )
        .select(
            F.col("transaction_id"),
            F.col("inc.incident_id"),
            F.col("inc.incident_ts"),
            F.col("inc.incident_type"),
            F.col("inc.related_line_id"),
            F.col("inc.resolution"),
            F.col("inc.resolution_ts"),
            F.col("inc.resolved_by"),
            F.col("inc.duration_seconds"),
            F.col("ingest_ts"),
        )
        .withWatermark("ingest_ts", "1 hour")
        .dropDuplicates(["transaction_id", "incident_id"])
    )
