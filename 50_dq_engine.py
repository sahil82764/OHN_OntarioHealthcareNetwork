# Fabric notebook source
# METADATA ********************
# Data quality engine — metadata driven.
#
# Rules live in governance.dim_dq_rule as SQL boolean expressions. Adding a
# rule is a data change, not a code change, so a data steward can extend
# coverage without a deployment.
#
# Parameters:
#   layer     : bronze | silver | gold
#   batch_id  : batch identifier from the calling pipeline
#   fail_fast : if true, raise on the first Error-severity breach
# ****************************

# PARAMETERS CELL ********************
layer = "silver"
batch_id = "manual_run"
fail_fast = True
# ***********************************

# CELL ********************
%run 00_common_utils
# ****************************

# CELL ********************
from datetime import datetime, timezone
from pyspark.sql import functions as F

run_ts = datetime.now(timezone.utc)
run_date_key = int(run_ts.strftime("%Y%m%d"))

rules = (
    spark.table("governance.dim_dq_rule")
    .filter((F.col("layer") == layer) & F.col("is_active"))
    .orderBy("target_table", "rule_id")
    .collect()
)
print(f"Executing {len(rules)} {layer} rules")

# MARKDOWN ********************
# ## Rule execution
#
# Each rule is a boolean expression evaluated per row. Rows where the
# expression is false or null fail. Null is treated as a failure deliberately:
# a rule that cannot be evaluated is not a rule that passed.
# ****************************

# CELL ********************
results = []
breaches = []

for r in rules:
    rule_id = r["rule_id"]
    table = r["target_table"]
    expr = r["rule_expression"]
    severity = r["severity"]
    threshold = float(r["pass_threshold_pct"])
    action = r["failure_action"]

    if not spark.catalog.tableExists(table):
        print(f"  {rule_id}: target {table} does not exist — skipped")
        continue

    df = spark.table(table)

    # Scope evaluation to the current batch where the table carries one, so a
    # rule does not re-scan seven years of history every night.
    if "_batch_id" in df.columns and batch_id != "manual_run":
        df = df.filter(F.col("_batch_id") == batch_id)
    elif "batch_id" in df.columns and batch_id != "manual_run":
        df = df.filter(F.col("batch_id") == batch_id)

    try:
        evaluated = df.withColumn("_dq_pass", F.expr(expr))
        evaluated = evaluated.withColumn("_dq_pass", F.coalesce(F.col("_dq_pass"), F.lit(False)))
        agg = evaluated.agg(
            F.count("*").alias("rows_evaluated"),
            F.sum(F.col("_dq_pass").cast("long")).alias("rows_passed"),
        ).collect()[0]
    except Exception as exc:  # noqa: BLE001 - rule text is user supplied
        print(f"  {rule_id}: expression failed to evaluate — {exc}")
        results.append((rule_id, table, 0, 0, 0, 0.0, True, str(exc)[:500]))
        breaches.append((rule_id, severity, "EXPRESSION_ERROR"))
        continue

    rows_evaluated = int(agg["rows_evaluated"] or 0)
    rows_passed = int(agg["rows_passed"] or 0)
    rows_failed = rows_evaluated - rows_passed
    pass_rate = 100.0 if rows_evaluated == 0 else round(rows_passed * 100.0 / rows_evaluated, 3)
    is_breach = pass_rate < threshold

    results.append((rule_id, table, rows_evaluated, rows_passed, rows_failed,
                    pass_rate, is_breach, None))

    status = "BREACH" if is_breach else "ok"
    print(f"  {rule_id} [{severity}] {table}: {pass_rate}% pass ({rows_failed} failed) — {status}")

    if is_breach:
        breaches.append((rule_id, severity, action))

        # Quarantine the failing rows so they can be inspected and replayed
        # once the source is corrected. Nothing is silently discarded.
        if action == "Quarantine" and rows_failed > 0:
            q_table = f"{table.split('.')[0]}.quarantine_{table.split('.')[-1]}"
            (
                evaluated.filter(~F.col("_dq_pass"))
                .withColumn("_dq_rule_id", F.lit(rule_id))
                .withColumn("_dq_run_ts", F.lit(run_ts))
                .withColumn("_dq_batch_id", F.lit(batch_id))
                .write.format("delta").mode("append").option("mergeSchema", "true")
                .saveAsTable(q_table)
            )
            print(f"     quarantined {rows_failed} rows to {q_table}")

# MARKDOWN ********************
# ## Persist results
# ****************************

# CELL ********************
if results:
    schema = """rule_id string, target_table string, rows_evaluated long, rows_passed long,
                rows_failed long, pass_rate_pct double, is_breach boolean, error_message string"""
    res_df = spark.createDataFrame(results, schema)

    rule_keys = spark.table("governance.dim_dq_rule").select("rule_id", "dq_rule_key")
    res_df = (
        res_df.join(rule_keys, "rule_id", "left")
        .withColumn("dq_rule_key", F.coalesce(F.col("dq_rule_key"), F.lit(UNKNOWN_KEY)))
        .withColumn("run_date_key", F.lit(run_date_key))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("run_ts", F.lit(run_ts))
    )
    res_df.write.format("delta").mode("append").saveAsTable("governance.fact_dq_result")

# MARKDOWN ********************
# ## Enforce
#
# Error-severity breaches with a FailBatch action stop the pipeline so bad
# data never reaches the semantic model. Warnings are recorded and surfaced on
# the data-quality report but do not block the load — blocking on every
# warning would train operators to ignore the alerts.
# ****************************

# CELL ********************
fatal = [b for b in breaches if b[1] == "Error" and b[2] in ("FailBatch", "EXPRESSION_ERROR")]
warnings = [b for b in breaches if b not in fatal]

summary = {
    "layer": layer,
    "batch_id": batch_id,
    "rules_executed": len(results),
    "breaches": len(breaches),
    "fatal": len(fatal),
    "warnings": len(warnings),
}
print(summary)

if fatal and fail_fast:
    raise RuntimeError(
        f"Data quality gate failed for {layer}. Fatal rules: {[b[0] for b in fatal]}. "
        f"See governance.fact_dq_result for batch {batch_id}."
    )

mssparkutils.notebook.exit(str(summary))
