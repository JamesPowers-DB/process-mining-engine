"""
bronze.py — Bronze Layer: Raw POS Transaction Ingestion
========================================================
Reads line-delimited JSON files from a Unity Catalog Volume using Auto Loader
(cloudFiles). Each line is one complete POS transaction JSON object.

Adds standard Bronze metadata columns:
  - ingest_ts        : ingestion timestamp
  - source_system    : literal "POS_SYSTEM"
  - batch_id         : UUID per micro-batch (streaming) or run
  - record_hash      : SHA-256 of the raw JSON for deduplication
  - ingest_date      : date partition column
  - source_file_path : origin file path from Auto Loader metadata
  - source_file_ts   : file modification timestamp
"""

import dlt
from pyspark.sql import functions as F

# ── Pipeline parameter (set in pipeline.yml → configuration) ──────────────────
SOURCE_VOLUME_PATH = spark.conf.get(
    "pos.source_volume_path",
    "/Volumes/pos_demo/pos_mining/raw_pos_data",
)

# Schema checkpoint location — Auto Loader stores inferred schema evolution here.
# For text format this is unused, but required by the API.
SCHEMA_LOCATION = f"{SOURCE_VOLUME_PATH}/_autoloader_schema"


@dlt.table(
    name="bronze_pos_transactions",
    comment=(
        "Raw POS transaction JSON — one row per transaction artifact ingested from "
        "the UC Volume. No parsing or validation is applied at this layer."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
    partition_cols=["ingest_date"],
)
def bronze_pos_transactions():
    """
    Auto Loader streaming ingestion from UC Volume.

    cloudFiles.format=text reads one row per line, storing the entire
    line in the 'value' column. Each line in the source files is a
    complete, self-contained JSON object (NDJSON / JSON Lines format).

    _metadata columns (source_file_path, source_file_ts) are Auto Loader
    built-ins available when rescuedDataColumn or schemaEvolutionMode is used.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        # cloudFiles.schemaLocation is required by Auto Loader even for text format
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        # Emit one trigger per file to bound checkpoint size in demo runs
        .option("cloudFiles.maxFilesPerTrigger", "100")
        .load(SOURCE_VOLUME_PATH)
        .select(
            # ── Raw payload ───────────────────────────────────────────────────
            F.col("value").alias("raw_json"),
            # ── Bronze metadata ───────────────────────────────────────────────
            F.current_timestamp().alias("ingest_ts"),
            F.lit("POS_SYSTEM").alias("source_system"),
            F.expr("uuid()").alias("batch_id"),
            F.sha2(F.col("value"), 256).alias("record_hash"),
            F.to_date(F.current_timestamp()).alias("ingest_date"),
            # ── Auto Loader file provenance ───────────────────────────────────
            F.col("_metadata.file_path").alias("source_file_path"),
            F.col("_metadata.file_modification_time").alias("source_file_ts"),
        )
    )
