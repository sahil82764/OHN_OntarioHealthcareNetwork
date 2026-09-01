# Fabric notebook source
# METADATA ********************
# Bronze — HL7 v2 ORU^R01 parser.
#
# Lab results arrive as batched HL7 v2.5 messages in ADLS Gen2, surfaced in
# OneLake through a shortcut (no copy). This notebook turns them into a Delta
# table.
#
# Why this cannot be a Copy activity: HL7 is not tabular. One file holds many
# messages, one message holds one order (OBR) and several results (OBX), and
# the fields are positional within pipe-delimited segments with a second
# level of ^-delimited components. The row count in the output has no
# relationship to the line count in the input.
# ****************************

# PARAMETERS CELL ********************
source_path = "Files/shortcuts/lab-blob/lab"   # OneLake shortcut to ADLS Gen2
load_date = "2026-08-01"
# ***********************************

# CELL ********************
%run 00_common_utils
# ****************************

# CELL ********************
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, StringType, StructField, StructType)

batch_id = new_batch_id("LIS", "lab_result")

# MARKDOWN ********************
# ## 1. Read whole files
#
# `wholetext` keeps each file as a single string. Reading line by line would
# lose the message boundaries entirely — HL7 segments are separated by a
# carriage return, and a message is a run of segments starting at MSH. Split
# the file into lines first and the OBX rows have no way back to their OBR.
# ****************************

# CELL ********************
raw = (
    spark.read.option("wholetext", True)
    .text(f"{source_path}/*/*/*/*.hl7")
    .withColumn("_source_file", F.input_file_name())
)

print(f"HL7 files read: {raw.count()}")

# MARKDOWN ********************
# ## 2. Parse
#
# A parser, not a regex. HL7 field access is positional and the escape rules
# matter: `\F\` is a literal pipe, `\S\` a literal caret. A regex that looks
# right will silently mangle any result containing an escaped delimiter, and
# those are exactly the free-text comments where it matters.
# ****************************

# CELL ********************
RESULT_SCHEMA = ArrayType(StructType([
    StructField("message_control_id", StringType()),
    StructField("sending_facility", StringType()),
    StructField("message_ts", StringType()),
    StructField("patient_id", StringType()),
    StructField("encounter_id", StringType()),
    StructField("department_id", StringType()),
    StructField("ordering_doctor_id", StringType()),
    StructField("placer_order_id", StringType()),
    StructField("order_ts", StringType()),
    StructField("collect_ts", StringType()),
    StructField("result_ts", StringType()),
    StructField("priority", StringType()),
    StructField("set_id", StringType()),
    StructField("value_type", StringType()),
    StructField("loinc_code", StringType()),
    StructField("result_value", StringType()),
    StructField("result_unit", StringType()),
    StructField("abnormal_flag", StringType()),
    StructField("result_status", StringType()),
    StructField("parse_error", StringType()),
]))


def parse_hl7_file(content):
    """Parse a batched HL7 file into flat result records.

    Returns one record per OBX. A message with no OBX still returns one
    record carrying a parse_error, so an order that produced no result is
    visible in Bronze rather than vanishing between the file and the table.
    """
    if not content:
        return []

    def unescape(v):
        if not v:
            return v
        return (v.replace("\\F\\", "|").replace("\\S\\", "^")
                 .replace("\\R\\", "~").replace("\\T\\", "&")
                 .replace("\\E\\", "\\"))

    def field(seg, idx):
        """1-based field access, matching how HL7 specs are written."""
        parts = seg.split("|")
        return unescape(parts[idx]) if idx < len(parts) else ""

    def comp(value, idx):
        """1-based component within a field."""
        parts = (value or "").split("^")
        return parts[idx - 1] if idx <= len(parts) else ""

    # Normalise line endings; real interfaces send \r, some senders \r\n,
    # and a file that has been through a text editor arrives with \n.
    text = content.replace("\r\n", "\r").replace("\n", "\r")
    segments = [s for s in text.split("\r") if s.strip()]

    # Split into messages on MSH boundaries. FHS/BHS/BTS/FTS are batch
    # envelope segments and carry no clinical content.
    messages, current = [], []
    for seg in segments:
        tag = seg[:3]
        if tag in ("FHS", "BHS", "BTS", "FTS"):
            continue
        if tag == "MSH":
            if current:
                messages.append(current)
            current = [seg]
        elif current:
            current.append(seg)
    if current:
        messages.append(current)

    out = []
    for msg in messages:
        ctx = {"message_control_id": "", "sending_facility": "", "message_ts": "",
               "patient_id": "", "encounter_id": "", "department_id": "",
               "ordering_doctor_id": "", "placer_order_id": "", "order_ts": "",
               "collect_ts": "", "result_ts": "", "priority": ""}
        obx_rows = []
        err = None

        try:
            for seg in msg:
                tag = seg[:3]
                if tag == "MSH":
                    ctx["sending_facility"] = field(seg, 3)
                    ctx["message_ts"] = field(seg, 6)
                    ctx["message_control_id"] = field(seg, 9)
                elif tag == "PID":
                    ctx["patient_id"] = comp(field(seg, 3), 1)
                elif tag == "PV1":
                    ctx["department_id"] = field(seg, 3)
                    ctx["ordering_doctor_id"] = field(seg, 8)
                    ctx["encounter_id"] = field(seg, 19)
                elif tag == "OBR":
                    ctx["placer_order_id"] = field(seg, 2)
                    ctx["priority"] = "STAT" if field(seg, 5) == "S" else "Routine"
                    ctx["order_ts"] = field(seg, 6)
                    ctx["collect_ts"] = field(seg, 7)
                    ctx["result_ts"] = field(seg, 22)
                elif tag == "OBX":
                    obx_rows.append({
                        "set_id": field(seg, 1),
                        "value_type": field(seg, 2),
                        "loinc_code": comp(field(seg, 3), 1),
                        "result_value": field(seg, 5),
                        "result_unit": field(seg, 6),
                        "abnormal_flag": field(seg, 8),
                        "result_status": field(seg, 11),
                        "obx_result_ts": field(seg, 14),
                    })
        except Exception as exc:  # noqa: BLE001
            err = f"{type(exc).__name__}: {exc}"[:300]

        if not obx_rows:
            out.append({**ctx, "set_id": None, "value_type": None,
                        "loinc_code": None, "result_value": None,
                        "result_unit": None, "abnormal_flag": None,
                        "result_status": None,
                        "parse_error": err or "No OBX segment in message"})
            continue

        for o in obx_rows:
            out.append({
                **ctx,
                "set_id": o["set_id"],
                "value_type": o["value_type"],
                "loinc_code": o["loinc_code"],
                "result_value": o["result_value"],
                "result_unit": o["result_unit"],
                "abnormal_flag": o["abnormal_flag"],
                "result_status": o["result_status"],
                # OBX-14 is more precise than OBR-22 when both are present:
                # the order finished when its last result finished, but each
                # individual result has its own timestamp.
                "result_ts": o["obx_result_ts"] or ctx["result_ts"],
                "parse_error": err,
            })
    return out


parse_udf = F.udf(parse_hl7_file, RESULT_SCHEMA)

# MARKDOWN ********************
# ## 3. Explode to one row per result
# ****************************

# CELL ********************
parsed = (
    raw.withColumn("_records", parse_udf(F.col("value")))
    .withColumn("_rec", F.explode_outer("_records"))
    .select("_source_file", "_rec.*")
)


def hl7_ts(col):
    """HL7 timestamps are YYYYMMDDHHMMSS, sometimes truncated to the day."""
    return F.coalesce(
        F.to_timestamp(col, "yyyyMMddHHmmss"),
        F.to_timestamp(col, "yyyyMMddHHmm"),
        F.to_timestamp(col, "yyyyMMdd"),
    )


bronze = (
    parsed
    .withColumn("order_ts", hl7_ts(F.col("order_ts")))
    .withColumn("collect_ts", hl7_ts(F.col("collect_ts")))
    .withColumn("result_ts", hl7_ts(F.col("result_ts")))
    .withColumn("message_ts", hl7_ts(F.col("message_ts")))
    .withColumn(
        "result_value_numeric",
        F.when(F.col("value_type") == "NM",
               F.col("result_value").cast("decimal(18,4)")))
    .withColumn(
        "result_value_text",
        F.when(F.col("value_type") != "NM", F.col("result_value")))
    .withColumn("is_critical",
                F.col("abnormal_flag").isin("HH", "LL", "AA"))
    .withColumn("_source_system", F.lit("LIS"))
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
    .withColumn("_load_date", F.lit(load_date).cast("date"))
)

bronze = row_hash(bronze, ["message_control_id", "placer_order_id",
                           "loinc_code", "set_id", "result_value"])

# MARKDOWN ********************
# ## 4. Quality gate before writing
#
# Parser failures must be loud. A silently empty result set from a bad
# delimiter assumption looks identical to a quiet day in the lab, and the
# difference only surfaces weeks later when someone notices the turnaround
# report is missing a hospital.
# ****************************

# CELL ********************
total = bronze.count()
errors = bronze.filter(F.col("parse_error").isNotNull()).count()
no_loinc = bronze.filter(F.col("loinc_code").isNull() | (F.col("loinc_code") == "")).count()
no_encounter = bronze.filter(F.col("encounter_id").isNull() | (F.col("encounter_id") == "")).count()

print(f"Parsed results      : {total:,}")
print(f"  with parse errors : {errors:,} ({100*errors/max(1,total):.2f}%)")
print(f"  missing LOINC     : {no_loinc:,} ({100*no_loinc/max(1,total):.2f}%)")
print(f"  missing encounter : {no_encounter:,} ({100*no_encounter/max(1,total):.2f}%)")

if total == 0:
    raise RuntimeError(
        f"No results parsed from {source_path}. Either the shortcut is empty "
        f"or the segment delimiter assumption is wrong — check a raw file "
        f"before assuming the source is quiet."
    )
if errors / total > 0.02:
    raise RuntimeError(
        f"{100*errors/total:.1f}% of messages failed to parse, above the 2% "
        f"tolerance. Inspect bronze.lis_lab_result where parse_error is not null."
    )

# MARKDOWN ********************
# ## 5. Write
# ****************************

# CELL ********************
(
    bronze.write.format("delta")
    .mode("append")
    .partitionBy("_load_date")
    .option("mergeSchema", "true")
    .saveAsTable("lh_bronze.lis_lab_result")
)

log_batch(batch_id, "bronze_parse_hl7", "LIS", "lh_bronze.lis_lab_result",
          "SUCCESS", raw.count(), total)

spark.sql("OPTIMIZE lh_bronze.lis_lab_result")
mssparkutils.notebook.exit(batch_id)
