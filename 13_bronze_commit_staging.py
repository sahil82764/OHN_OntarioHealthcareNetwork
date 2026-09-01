# Fabric notebook source
# METADATA ********************
# Bronze — commit staged tables.
#
# The Copy activity lands raw rows in stg_<entity>. It cannot add audit
# columns, and Bronze rows without _batch_id cannot be re-run for a single
# window — any mistake would mean reloading everything. This notebook stamps
# the audit columns and appends staging into the real Bronze tables.
#
# It runs ONCE per pipeline, after the ForEach loop, and handles every staged
# entity in a single Spark session. The alternative — a notebook activity
# inside the loop — pays 10-30 seconds of session startup per entity, which
# across eleven tables is several minutes of doing nothing.
#
# Parameters (set by the pipeline):
#   batch_id : the batch to commit
#   entities : JSON array from the Lookup, so the notebook knows the
#              staging -> target mapping without querying the warehouse
# ****************************

# PARAMETERS CELL ********************
batch_id = "MANUAL_RUN"
entities = "[]"
# ***********************************

# CELL ********************
%run 00_common_utils
# ****************************

# CELL ********************
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

run_ts = datetime.now(timezone.utc)
entity_list = json.loads(entities) if isinstance(entities, str) else entities

if not entity_list:
    raise ValueError(
        "No entities passed. The pipeline must forward the Lookup output as "
        "the 'entities' parameter, otherwise this notebook has no idea which "
        "staging tables belong to this batch."
    )

print(f"Committing batch {batch_id} — {len(entity_list)} entities")

# MARKDOWN ********************
# ## Commit each staged table
#
# The audit columns are the point of this step:
#
#   _source_system  which system it came from
#   _batch_id       which run loaded it, so one window can be replayed
#   _ingest_ts      when it landed
#   _load_date      partition key
#   _row_hash       payload fingerprint, used by Silver to deduplicate
#
# The hash covers only the payload columns, never the audit columns. Hashing
# _ingest_ts would make every row unique on every run and defeat the
# deduplication it exists to enable.
# ****************************

# CELL ********************
results = []

for ent in entity_list:
    source_system = ent["source_system"]
    entity_name = ent["entity_name"]
    target_table = ent["target_table"]
    load_type = ent.get("load_type", "incremental")

    staging_table = f"lh_bronze.stg_{target_table}"
    bronze_table = f"lh_bronze.{target_table}"

    try:
        stg = spark.table(staging_table)
    except AnalysisException:
        # A copy that matched no rows may not create a staging table at all.
        # That is a normal incremental outcome, not a failure.
        print(f"  {source_system}.{entity_name:<30} no staging table — 0 rows")
        results.append({"entity": f"{source_system}.{entity_name}",
                        "rows": 0, "status": "NO_DATA"})
        continue

    row_count = stg.count()
    if row_count == 0:
        print(f"  {source_system}.{entity_name:<30} staging empty — 0 rows")
        spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
        results.append({"entity": f"{source_system}.{entity_name}",
                        "rows": 0, "status": "NO_DATA"})
        continue

    payload_cols = [c for c in stg.columns if not c.startswith("_")]

    enriched = (
        stg
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_entity_name", F.lit(entity_name))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_ts", F.lit(run_ts))
        .withColumn("_load_date", F.lit(run_ts.date()))
    )
    enriched = row_hash(enriched, payload_cols)

    # A full_snapshot entity re-reads every row each run, so appending would
    # duplicate the whole table daily. Replacing only this entity's rows for
    # today keeps Bronze append-only across days while staying idempotent
    # within a day — re-running the pipeline twice on the same date does not
    # double the row count.
    if load_type == "full_snapshot" and spark.catalog.tableExists(bronze_table):
        (
            enriched.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_load_date = '{run_ts.date()}'")
            .option("mergeSchema", "true")
            .partitionBy("_load_date")
            .saveAsTable(bronze_table)
        )
        mode_used = "replaceWhere"
    else:
        (
            enriched.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("_load_date")
            .saveAsTable(bronze_table)
        )
        mode_used = "append"

    spark.sql(f"DROP TABLE IF EXISTS {staging_table}")

    print(f"  {source_system}.{entity_name:<30} {row_count:>9,} rows  ({mode_used})")
    results.append({"entity": f"{source_system}.{entity_name}",
                    "rows": row_count, "status": "COMMITTED"})

# MARKDOWN ********************
# ## Verify before reporting success
#
# The pipeline advances watermarks based on this notebook succeeding. If it
# reports success without data actually reaching Bronze, the watermark moves
# past a window that was never loaded, and the gap is invisible.
# ****************************

# CELL ********************
committed = [r for r in results if r["status"] == "COMMITTED"]
no_data = [r for r in results if r["status"] == "NO_DATA"]
total_rows = sum(r["rows"] for r in results)

print(f"\nCommitted : {len(committed)} entities, {total_rows:,} rows")
print(f"No data   : {len(no_data)} entities")

if committed:
    missing = []
    for r in committed:
        ent = next(e for e in entity_list
                   if f"{e['source_system']}.{e['entity_name']}" == r["entity"])
        bronze_table = f"lh_bronze.{ent['target_table']}"
        in_bronze = (
            spark.table(bronze_table)
            .filter(F.col("_batch_id") == batch_id)
            .count()
        )
        if in_bronze != r["rows"]:
            missing.append(f"{r['entity']}: staged {r['rows']:,} but "
                           f"{in_bronze:,} in Bronze for this batch")
    if missing:
        raise RuntimeError(
            "Row counts do not reconcile between staging and Bronze:\n  "
            + "\n  ".join(missing))
    print("Reconciliation: staging and Bronze row counts match for every entity")

# Leftover staging tables mean an entity was copied but never listed in the
# entities parameter — its rows are sitting in staging, invisible, while the
# watermark is about to advance past them.
leftover = [t.tableName for t in spark.sql("SHOW TABLES IN lh_bronze").collect()
            if t.tableName.startswith("stg_")]
if leftover:
    raise RuntimeError(
        f"Staging tables still present after commit: {leftover}. These were "
        f"copied but not committed — check the entities parameter matches "
        f"the Lookup output.")

mssparkutils.notebook.exit(json.dumps({
    "batch_id": batch_id,
    "entities_committed": len(committed),
    "total_rows": total_rows,
}))
