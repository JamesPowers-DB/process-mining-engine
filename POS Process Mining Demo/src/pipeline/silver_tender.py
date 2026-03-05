"""
silver_tender.py — Silver Layer: Tender Lines
=============================================
Explodes the 'tender' array from Bronze JSON records into individual
rows — one row per (transaction_id, tender_id).

Split tender (multiple payment methods per transaction) is fully supported:
each tender line becomes its own row.

DQ Expectations (comment block — add before production):
---------------------------------------------------------
  @dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
  @dlt.expect_or_drop("valid_tender_id",      "tender_id IS NOT NULL")
  @dlt.expect_or_drop("valid_tender_ts",      "tender_ts IS NOT NULL")
  @dlt.expect_or_drop("valid_tender_type",    "tender_type IS NOT NULL")
  @dlt.expect("positive_amount",              "amount > 0")
---------------------------------------------------------
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ── JSON schema: transaction_id + tender array ────────────────────────────────
_TENDER_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField(
            "tender",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("tender_id", T.StringType()),
                        T.StructField("tender_ts", T.TimestampType()),
                        T.StructField("tender_type", T.StringType()),
                        T.StructField("amount", T.DecimalType(12, 2)),
                        T.StructField("authorization_number", T.StringType()),
                        T.StructField("card_brand", T.StringType()),
                        T.StructField("last4", T.StringType()),
                        T.StructField("coupon_code", T.StringType()),
                        T.StructField("change_amount", T.DecimalType(12, 2)),
                    ]
                )
            ),
        ),
    ]
)


@dlt.table(
    name="silver_transaction_tender",
    comment=(
        "Exploded POS tender lines — one row per (transaction_id, tender_id). "
        "Supports split tender (multiple rows per transaction)."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
def silver_transaction_tender():
    """
    Streaming table: reads Bronze, parses the tender array, and explodes it.

    VOIDED transactions may have zero tender rows — normal.
    SUSPENDED transactions may have no tender until later resumed and completed.
    """
    return (
        dlt.read_stream("bronze_pos_transactions")
        .select(
            F.from_json(F.col("raw_json"), _TENDER_SCHEMA).alias("d"),
            F.col("ingest_ts"),
        )
        .filter(F.col("d.tender").isNotNull() & (F.size(F.col("d.tender")) > 0))
        .select(
            F.col("d.transaction_id"),
            F.explode(F.col("d.tender")).alias("tnd"),
            F.col("ingest_ts"),
        )
        .select(
            F.col("transaction_id"),
            F.col("tnd.tender_id"),
            F.col("tnd.tender_ts"),
            F.col("tnd.tender_type"),
            F.col("tnd.amount"),
            F.col("tnd.authorization_number"),
            F.col("tnd.card_brand"),
            F.col("tnd.last4"),
            F.col("tnd.coupon_code"),
            F.col("tnd.change_amount"),
            F.col("ingest_ts"),
        )
        .withWatermark("ingest_ts", "1 hour")
        .dropDuplicates(["transaction_id", "tender_id"])
    )
