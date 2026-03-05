"""
silver_items.py — Silver Layer: Transaction Line Items
======================================================
Explodes the 'items' array from each raw Bronze JSON record into
individual rows — one row per (transaction_id, line_id).

DQ Expectations (comment block — add before production):
---------------------------------------------------------
  @dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
  @dlt.expect_or_drop("valid_line_id",        "line_id IS NOT NULL")
  @dlt.expect_or_drop("valid_sku",            "sku IS NOT NULL")
  @dlt.expect_or_drop("valid_scan_ts",        "scan_ts IS NOT NULL")
  @dlt.expect("non_negative_quantity",        "quantity >= 0")
  @dlt.expect("non_negative_unit_price",      "unit_price >= 0")
---------------------------------------------------------
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ── JSON schema: only the fields we need (transaction_id + items array) ────────
_ITEMS_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField(
            "items",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("line_id", T.StringType()),
                        T.StructField("sequence_number", T.IntegerType()),
                        T.StructField("scan_ts", T.TimestampType()),
                        T.StructField("sku", T.StringType()),
                        T.StructField("item_name", T.StringType()),
                        T.StructField("department", T.StringType()),
                        T.StructField("category", T.StringType()),
                        T.StructField("scan_method", T.StringType()),
                        T.StructField("quantity", T.DecimalType(10, 3)),
                        T.StructField("unit_price", T.DecimalType(12, 2)),
                        T.StructField("line_discount_amount", T.DecimalType(12, 2)),
                        T.StructField("extended_amount", T.DecimalType(12, 2)),
                        T.StructField("is_return", T.BooleanType()),
                        T.StructField("is_void", T.BooleanType()),
                    ]
                )
            ),
        ),
    ]
)


@dlt.table(
    name="silver_transaction_items",
    comment=(
        "Exploded POS line items — one row per (transaction_id, line_id). "
        "scan_method drives the activity label in the Gold event log."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
def silver_transaction_items():
    """
    Streaming table: reads Bronze, parses the items array, and explodes it.

    Deduplication on (transaction_id, line_id) within a 1-hour watermark window
    prevents duplicate item rows if the same Bronze record is reprocessed.
    """
    return (
        dlt.read_stream("bronze_pos_transactions")
        .select(
            F.from_json(F.col("raw_json"), _ITEMS_SCHEMA).alias("d"),
            F.col("ingest_ts"),
        )
        # Only process records that have at least one item
        .filter(F.col("d.items").isNotNull())
        .select(
            F.col("d.transaction_id"),
            F.explode(F.col("d.items")).alias("item"),
            F.col("ingest_ts"),
        )
        .select(
            F.col("transaction_id"),
            F.col("item.line_id"),
            F.col("item.sequence_number"),
            F.col("item.scan_ts"),
            F.col("item.sku"),
            F.col("item.item_name"),
            F.col("item.department"),
            F.col("item.category"),
            F.col("item.scan_method"),
            F.col("item.quantity"),
            F.col("item.unit_price"),
            F.col("item.line_discount_amount"),
            F.col("item.extended_amount"),
            F.col("item.is_return"),
            F.col("item.is_void"),
            F.col("ingest_ts"),
        )
        .withWatermark("ingest_ts", "1 hour")
        .dropDuplicates(["transaction_id", "line_id"])
    )
