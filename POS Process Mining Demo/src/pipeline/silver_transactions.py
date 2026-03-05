"""
silver_transactions.py — Silver Layer: Transaction Headers
==========================================================
Reads raw JSON strings from bronze_pos_transactions, parses the transaction
header fields (excludes nested arrays), and produces a clean, typed table.

One row per transaction_id.

DQ Expectations (comment block — add before production):
---------------------------------------------------------
  @dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
  @dlt.expect_or_drop("valid_start_ts",       "transaction_start_ts IS NOT NULL")
  @dlt.expect("positive_total",               "total_amount >= 0")
  @dlt.expect("end_after_start",
      "transaction_end_ts >= transaction_start_ts")
---------------------------------------------------------
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ── JSON schema for the transaction header fields only ────────────────────────
# Nested arrays (items, incidents, tender) are parsed in their own Silver files.
_HEADER_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField("store_id", T.StringType()),
        T.StructField("terminal_id", T.StringType()),
        T.StructField("cashier_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("channel", T.StringType()),
        T.StructField("transaction_start_ts", T.TimestampType()),
        T.StructField("transaction_end_ts", T.TimestampType()),
        T.StructField("status", T.StringType()),
        T.StructField("currency", T.StringType()),
        T.StructField("subtotal_amount", T.DecimalType(12, 2)),
        T.StructField("discount_amount", T.DecimalType(12, 2)),
        T.StructField("tax_amount", T.DecimalType(12, 2)),
        T.StructField("total_amount", T.DecimalType(12, 2)),
        T.StructField("item_count", T.IntegerType()),
    ]
)


@dlt.table(
    name="silver_transactions",
    comment=(
        "Standardized POS transaction headers — one row per transaction. "
        "Parsed from bronze_pos_transactions; nested arrays handled in separate tables."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
def silver_transactions():
    """
    Streaming table: incrementally processes new Bronze records.

    Deduplication strategy:
      withWatermark(1 hour) + dropDuplicates(transaction_id) removes late-arriving
      duplicates within a 1-hour window.  Records older than the watermark are
      assumed complete and not re-processed.
    """
    return (
        dlt.read_stream("bronze_pos_transactions")
        .select(
            F.from_json(F.col("raw_json"), _HEADER_SCHEMA).alias("d"),
            F.col("ingest_ts"),
            F.col("record_hash"),
        )
        .select(
            F.col("d.transaction_id"),
            F.col("d.store_id"),
            F.col("d.terminal_id"),
            F.col("d.cashier_id"),
            F.col("d.customer_id"),
            F.col("d.channel"),
            F.col("d.transaction_start_ts"),
            F.col("d.transaction_end_ts"),
            F.col("d.status"),
            F.col("d.currency"),
            F.col("d.subtotal_amount"),
            F.col("d.discount_amount"),
            F.col("d.tax_amount"),
            F.col("d.total_amount"),
            F.col("d.item_count"),
            F.col("ingest_ts"),
            F.col("record_hash"),
        )
        # Watermark on ingest_ts guards against late-arriving duplicates
        .withWatermark("ingest_ts", "1 hour")
        .dropDuplicates(["transaction_id"])
    )
