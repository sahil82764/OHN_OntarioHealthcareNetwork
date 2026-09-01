# Fabric notebook source
# METADATA ********************
# Bronze ingestion — metadata driven.
# Reads the landing zone for one source entity and appends to a Bronze Delta
# table with full audit columns. No business logic is applied here; the point
# of Bronze is to be a faithful, replayable record of what arrived.
#
# Parameters (set by the calling pipeline):
#   source_system : str   e.g. "EHR"
#   entity        : str   e.g. "patient"
#   load_date     : str   ISO date of the landing partition to process
# ****************************

# PARAMETERS CELL ********************
source_system = "EHR"
entity = "patient"
load_date = "2026-07-29"
# ***********************************

# MARKDOWN ********************
# ## Setup
# ****************************

# CELL ********************
%run 00_common_utils
# ****************************

# CELL ********************
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

batch_id = new_batch_id(source_system, entity)
bronze_table = f"lh_bronze.{source_system.lower()}_{entity.lower()}"

registry = (
    spark.table("lh_bronze.ctl_source_registry")
    .filter((F.col("source_system") == source_system) & (F.col("entity_name") == entity))
    .collect()
)
if not registry:
    raise ValueError(f"No registry entry for {source_system}.{entity}. Register the source before ingesting.")

cfg = registry[0]
file_format = cfg["file_format"]          # csv | json | parquet | hl7 | edi
landing_path = f"Files/landing/{source_system}/{entity}/ingest_date={load_date}/"
business_key = cfg["business_key"]         # comma-separated
has_header = bool(cfg["has_header"])
delimiter = cfg["delimiter"] or ","

print(f"batch_id={batch_id}  path={landing_path}  format={file_format}")

# MARKDOWN ********************
# ## Read landing files
#
# Everything is read as string. Type casting belongs in Silver, where a bad
# value can be quarantined with a rule id attached, instead of silently
# becoming null during a schema-inferred read.
# ****************************

# CELL ********************
def read_landing(fmt: str, path: str):
    if fmt == "csv":
        return (
            spark.read.option("header", has_header)
            .option("delimiter", delimiter)
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(None) if False else
            spark.read.option("header", has_header)
                 .option("delimiter", delimiter)
                 .option("inferSchema", "false")
                 .option("mode", "PERMISSIVE")
                 .csv(path)
        )
    if fmt == "json":
        return spark.read.option("multiLine", "true").json(path)
    if fmt == "parquet":
        return spark.read.parquet(path)
    if fmt == "hl7":
        # HL7 v2 arrives as pipe-delimited segments, one message per file.
        # Segment parsing happens in Silver; Bronze keeps the raw message.
        return (
            spark.read.text(path)
            .withColumn("_message_line", F.col("value"))
            .drop("value")
        )
    if fmt == "edi":
        return spark.read.text(path).withColumnRenamed("value", "_edi_line")
    raise ValueError(f"Unsupported format: {fmt}")


try:
    raw = read_landing(file_format, landing_path)
except AnalysisException:
    log_batch(batch_id, "bronze_ingest", source_system, bronze_table,
              "NO_DATA", 0, 0, f"No files at {landing_path}")
    print("No files found for this partition — exiting cleanly.")
    raw = None

# MARKDOWN ********************
# ## Add audit columns and append
# ****************************

# CELL ********************
if raw is not None:
    business_key_cols = [c.strip() for c in business_key.split(",") if c.strip() in raw.columns]

    enriched = (
        raw
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_load_date", F.lit(load_date).cast("date"))
    )

    # Hash over the payload only, so a re-delivered identical file is
    # detectable downstream without comparing every column.
    payload_cols = [c for c in raw.columns if not c.startswith("_")]
    enriched = row_hash(enriched, payload_cols)

    rows_read = enriched.count()

    # Duplicate-file guard: if this exact set of row hashes already exists for
    # this load date, the file was re-sent. Log it and skip rather than
    # doubling the row count.
    already_loaded = 0
    if spark.catalog.tableExists(bronze_table):
        existing = (
            spark.table(bronze_table)
            .filter(F.col("_load_date") == F.lit(load_date).cast("date"))
            .select("_row_hash")
        )
        already_loaded = enriched.join(existing, "_row_hash", "inner").count()

    if already_loaded == rows_read and rows_read > 0:
        log_batch(batch_id, "bronze_ingest", source_system, bronze_table,
                  "SKIPPED_DUPLICATE", rows_read, 0,
                  "All rows already present for this load date")
        print(f"Duplicate delivery detected ({rows_read} rows) — nothing appended.")
    else:
        (
            enriched.write.format("delta")
            .mode("append")
            .partitionBy("_load_date")
            .option("mergeSchema", "true")
            .saveAsTable(bronze_table)
        )
        log_batch(batch_id, "bronze_ingest", source_system, bronze_table,
                  "SUCCESS", rows_read, rows_read)
        print(f"Appended {rows_read} rows to {bronze_table}")

    if business_key_cols:
        max_ts_col = cfg["watermark_column"]
        if max_ts_col and max_ts_col in enriched.columns:
            new_wm = enriched.agg(F.max(F.col(max_ts_col)).alias("wm")).collect()[0]["wm"]
            if new_wm is not None:
                set_watermark(f"{source_system}.{entity}", str(new_wm), batch_id)

# MARKDOWN ********************
# ## Housekeeping
#
# Bronze tables are append-only and grow quickly. Compaction keeps file counts
# sane; the retention policy is enforced by a separate monthly maintenance job
# so that VACUUM never runs inside a load window.
# ****************************

# CELL ********************
if raw is not None and spark.catalog.tableExists(bronze_table):
    spark.sql(f"OPTIMIZE {bronze_table}")

mssparkutils.notebook.exit(batch_id)
