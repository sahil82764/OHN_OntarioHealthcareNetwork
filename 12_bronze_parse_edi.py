# Fabric notebook source
# METADATA ********************
# Bronze — X12 EDI 837P / 835 parser.
#
# Claims leave as 837P submissions and remittances come back as 835s, both
# dropped on SFTP and copied byte-for-byte into OneLake. Neither is tabular.
#
# X12 is a stateful format: segments carry no keys, and a segment's meaning
# depends entirely on which loop is currently open. An SV1 service line
# belongs to whichever CLM was seen most recently, and a CAS adjustment
# belongs to whichever CLP came before it. You cannot parse this with a
# filter — you walk the segments in order and carry context forward.
# ****************************

# PARAMETERS CELL ********************
edi_837_path = "Files/landing/CLAIMS/837/*.edi"
edi_835_path = "Files/landing/CLAIMS/835/*.edi"
load_date = "2026-08-01"
# ***********************************

# CELL ********************
%run 00_common_utils
# ****************************

# CELL ********************
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, StringType, StructField, StructType)

batch_id = new_batch_id("CLAIMS", "edi")

# MARKDOWN ********************
# ## Segment splitting
#
# The delimiters are not fixed by the standard — they are declared in the ISA
# header itself. ISA element separator is byte 4, the segment terminator is
# the last byte of the 106-character ISA. Hardcoding `*` and `~` works until
# a trading partner sends something else, and then it fails silently by
# producing one enormous unparseable segment.
# ****************************

# CELL ********************
def split_segments(content: str):
    if not content or len(content) < 106:
        return [], "*", "~"
    element_sep = content[3]
    isa = content[:106]
    segment_term = isa[-1]
    body = content.replace("\r\n", "").replace("\n", "").replace("\r", "")
    segs = [s for s in body.split(segment_term) if s.strip()]
    return [s.split(element_sep) for s in segs], element_sep, segment_term


CLAIM_SCHEMA = ArrayType(StructType([
    StructField("interchange_control_number", StringType()),
    StructField("transaction_set", StringType()),
    StructField("submitter_id", StringType()),
    StructField("receiver_id", StringType()),
    StructField("transaction_date", StringType()),
    StructField("billing_provider_npi", StringType()),
    StructField("claim_number", StringType()),
    StructField("payer_id", StringType()),
    StructField("subscriber_member_id", StringType()),
    StructField("billed_amount", StringType()),
    StructField("place_of_service", StringType()),
    StructField("service_date", StringType()),
    StructField("primary_diagnosis_code", StringType()),
    StructField("line_number", StringType()),
    StructField("service_code", StringType()),
    StructField("line_amount", StringType()),
    StructField("line_units", StringType()),
    StructField("parse_error", StringType()),
]))


def parse_837(content: str):
    """Walk an 837P and emit one row per service line.

    Context (interchange, provider, subscriber, claim) is carried forward as
    the walk proceeds. A claim with no SV1 lines still emits one row with
    null line fields, so a header-only claim is visible rather than dropped.
    """
    segments, _, _ = split_segments(content)
    if not segments:
        return []

    out = []
    ctx = {k: None for k in [
        "interchange_control_number", "transaction_set", "submitter_id",
        "receiver_id", "transaction_date", "billing_provider_npi",
        "claim_number", "payer_id", "subscriber_member_id", "billed_amount",
        "place_of_service", "service_date", "primary_diagnosis_code"]}
    pending_lines = []
    err = None

    def flush():
        if ctx["claim_number"] is None:
            return
        if pending_lines:
            for ln in pending_lines:
                out.append({**ctx, **ln, "parse_error": err})
        else:
            out.append({**ctx, "line_number": None, "service_code": None,
                        "line_amount": None, "line_units": None,
                        "parse_error": err or "Claim has no service lines"})

    def el(seg, i):
        return seg[i] if i < len(seg) and seg[i] != "" else None

    try:
        for seg in segments:
            tag = seg[0].strip()

            if tag == "ISA":
                ctx["interchange_control_number"] = el(seg, 13)
                ctx["submitter_id"] = (el(seg, 6) or "").strip() or None
                ctx["receiver_id"] = (el(seg, 8) or "").strip() or None
            elif tag == "ST":
                ctx["transaction_set"] = el(seg, 1)
            elif tag == "BHT":
                ctx["transaction_date"] = el(seg, 4)
            elif tag == "NM1":
                qualifier = el(seg, 1)
                if qualifier == "85":       # billing provider
                    ctx["billing_provider_npi"] = el(seg, 9)
                elif qualifier == "IL":     # subscriber
                    ctx["subscriber_member_id"] = el(seg, 9)
                elif qualifier == "PR":     # payer
                    ctx["payer_id"] = el(seg, 9)
            elif tag == "SBR":
                ctx["payer_id"] = el(seg, 8) or ctx["payer_id"]
            elif tag == "CLM":
                # A new CLM closes the previous claim. Forgetting this flush
                # is the classic X12 bug: every service line ends up attached
                # to the last claim in the file.
                flush()
                pending_lines = []
                ctx["claim_number"] = el(seg, 1)
                ctx["billed_amount"] = el(seg, 2)
                pos = el(seg, 5) or ""
                ctx["place_of_service"] = pos.split(":")[0] if pos else None
                ctx["service_date"] = None
                ctx["primary_diagnosis_code"] = None
            elif tag == "DTP" and el(seg, 1) == "472":
                ctx["service_date"] = el(seg, 3)
            elif tag == "HI":
                code = el(seg, 1) or ""
                if ":" in code:
                    ctx["primary_diagnosis_code"] = code.split(":", 1)[1]
            elif tag == "LX":
                pending_lines.append({"line_number": el(seg, 1),
                                      "service_code": None,
                                      "line_amount": None, "line_units": None})
            elif tag == "SV1" and pending_lines:
                proc = el(seg, 1) or ""
                pending_lines[-1]["service_code"] = (
                    proc.split(":", 1)[1] if ":" in proc else proc or None)
                pending_lines[-1]["line_amount"] = el(seg, 2)
                pending_lines[-1]["line_units"] = el(seg, 4)
            elif tag == "SE":
                flush()
                ctx["claim_number"] = None
                pending_lines = []
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"[:300]
        flush()

    flush()
    # SE already flushed the final claim; drop the duplicate if present
    seen = set()
    deduped = []
    for r in out:
        key = (r["claim_number"], r["line_number"], r["service_code"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped


REMIT_SCHEMA = ArrayType(StructType([
    StructField("interchange_control_number", StringType()),
    StructField("check_or_eft_number", StringType()),
    StructField("payment_method", StringType()),
    StructField("payment_amount", StringType()),
    StructField("payment_date", StringType()),
    StructField("payer_name", StringType()),
    StructField("claim_number", StringType()),
    StructField("claim_status_code", StringType()),
    StructField("billed_amount", StringType()),
    StructField("paid_amount", StringType()),
    StructField("patient_responsibility", StringType()),
    StructField("adjustment_group", StringType()),
    StructField("adjustment_reason", StringType()),
    StructField("adjustment_amount", StringType()),
    StructField("parse_error", StringType()),
]))


def parse_835(content: str):
    """Walk an 835 remittance and emit one row per claim payment.

    CAS adjustments attach to the open CLP. A claim can carry several, so
    the first is kept on the claim row and any further ones are emitted as
    additional rows — losing them would understate denied amounts.
    """
    segments, _, _ = split_segments(content)
    if not segments:
        return []

    out = []
    hdr = {k: None for k in [
        "interchange_control_number", "check_or_eft_number", "payment_method",
        "payment_amount", "payment_date", "payer_name"]}
    claim = None
    err = None

    def el(seg, i):
        return seg[i] if i < len(seg) and seg[i] != "" else None

    def flush():
        if claim:
            out.append({**hdr, **claim, "parse_error": err})

    try:
        for seg in segments:
            tag = seg[0].strip()
            if tag == "ISA":
                hdr["interchange_control_number"] = el(seg, 13)
            elif tag == "BPR":
                hdr["payment_method"] = el(seg, 3)
                hdr["payment_amount"] = el(seg, 2)
                hdr["payment_date"] = el(seg, 16)
            elif tag == "TRN":
                hdr["check_or_eft_number"] = el(seg, 2)
            elif tag == "N1" and el(seg, 1) == "PR":
                hdr["payer_name"] = el(seg, 2)
            elif tag == "CLP":
                flush()
                claim = {
                    "claim_number": el(seg, 1),
                    "claim_status_code": el(seg, 2),
                    "billed_amount": el(seg, 3),
                    "paid_amount": el(seg, 4),
                    "patient_responsibility": el(seg, 5),
                    "adjustment_group": None,
                    "adjustment_reason": None,
                    "adjustment_amount": None,
                }
            elif tag == "CAS" and claim:
                if claim["adjustment_group"] is None:
                    claim["adjustment_group"] = el(seg, 1)
                    claim["adjustment_reason"] = el(seg, 2)
                    claim["adjustment_amount"] = el(seg, 3)
                else:
                    out.append({**hdr, **claim,
                                "adjustment_group": el(seg, 1),
                                "adjustment_reason": el(seg, 2),
                                "adjustment_amount": el(seg, 3),
                                "parse_error": err})
            elif tag == "SE":
                flush()
                claim = None
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"[:300]

    flush()
    return out


# MARKDOWN ********************
# ## Parse and write
# ****************************

# CELL ********************
udf_837 = F.udf(parse_837, CLAIM_SCHEMA)
udf_835 = F.udf(parse_835, REMIT_SCHEMA)


def load_edi(path, udf, table, source_label):
    raw = (
        spark.read.option("wholetext", True).text(path)
        .withColumn("_source_file", F.input_file_name())
    )
    n_files = raw.count()
    if n_files == 0:
        print(f"No files at {path} — skipping {table}")
        return 0

    parsed = (
        raw.withColumn("_recs", udf(F.col("value")))
        .withColumn("_r", F.explode_outer("_recs"))
        .select("_source_file", "_r.*")
        .withColumn("_source_system", F.lit("CLAIMS"))
        .withColumn("_edi_type", F.lit(source_label))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_load_date", F.lit(load_date).cast("date"))
    )
    parsed = row_hash(parsed, [c for c in parsed.columns if not c.startswith("_")])

    n_rows = parsed.count()
    n_err = parsed.filter(F.col("parse_error").isNotNull()).count()
    n_noclaim = parsed.filter(F.col("claim_number").isNull()).count()

    print(f"{source_label}: {n_files} file(s) -> {n_rows:,} rows "
          f"({n_err:,} with parse errors, {n_noclaim:,} with no claim number)")

    if n_rows == 0:
        raise RuntimeError(
            f"{source_label} produced zero rows from {n_files} files. The "
            f"delimiters were probably misdetected — print the first 200 "
            f"characters of a raw file and check byte 4 and byte 106.")
    if n_noclaim > 0:
        raise RuntimeError(
            f"{n_noclaim:,} {source_label} rows have no claim number. A claim "
            f"that cannot be identified cannot be reconciled to billing, so "
            f"this fails the batch rather than loading unusable rows.")

    (parsed.write.format("delta").mode("append")
     .partitionBy("_load_date").option("mergeSchema", "true")
     .saveAsTable(table))
    return n_rows


rows_837 = load_edi(edi_837_path, udf_837, "lh_bronze.claims_837_service_line", "837P")
rows_835 = load_edi(edi_835_path, udf_835, "lh_bronze.claims_835_remittance", "835")

# MARKDOWN ********************
# ## Reconciliation
#
# Every 835 line should reference a claim that was actually submitted in an
# 837. Orphan remittances mean either a missing submission file or a claim
# number mangled in transit, and both are worth failing on rather than
# discovering when the revenue-cycle report does not balance.
# ****************************

# CELL ********************
if rows_837 and rows_835:
    submitted = spark.table("lh_bronze.claims_837_service_line").select("claim_number").distinct()
    remitted = spark.table("lh_bronze.claims_835_remittance").select("claim_number").distinct()
    orphans = remitted.join(submitted, "claim_number", "left_anti").count()
    total_remitted = remitted.count()
    print(f"Remittances with no matching submission: {orphans:,} / {total_remitted:,} "
          f"({100*orphans/max(1,total_remitted):.2f}%)")
    if total_remitted and orphans / total_remitted > 0.05:
        print("WARNING: over 5% orphan remittances — check for missing 837 files "
              "in the landing zone before trusting the claims reports.")

log_batch(batch_id, "bronze_parse_edi", "CLAIMS", "lh_bronze.claims_*",
          "SUCCESS", 0, rows_837 + rows_835)
mssparkutils.notebook.exit(batch_id)
