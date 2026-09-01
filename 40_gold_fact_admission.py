# Fabric notebook source
# METADATA ********************
# Gold — fact_admission (accumulating snapshot).
#
# Satisfies BR-02 (admissions, discharges, readmissions) and BR-05 (ALOS).
# The admission row is created when the patient is admitted and updated as
# milestones complete, so open admissions are visible in the model rather than
# appearing only after discharge.
# ****************************

# CELL ********************
%run 00_common_utils
# ****************************

# CELL ********************
from pyspark.sql import functions as F, Window

batch_id = new_batch_id("GOLD", "fact_admission")
READMISSION_WINDOW_DAYS = 30

# MARKDOWN ********************
# ## 1. Source and reprocessing window
#
# Readmission is a backward-looking flag: a new admission today can change the
# is_readmission_30d value of nothing, but a *late-arriving* admission dated
# last week can change the flag on an admission already loaded. The reprocess
# window is therefore the incremental window plus the readmission window plus
# a grace period for late arrivals.
# ****************************

# CELL ********************
watermark = get_watermark("GOLD.fact_admission", "1900-01-01 00:00:00")
reprocess_from = F.date_sub(F.lit(watermark).cast("timestamp").cast("date"),
                            READMISSION_WINDOW_DAYS + 14)

adm_src = spark.table("lh_silver.admission").filter(
    F.col("admission_ts").cast("date") >= reprocess_from
)

# Patient identity comes through the MPI cross-reference, never the raw source
# id — that is what makes the readmission calculation work across facilities.
xref = spark.table("lh_silver.patient_xref").select(
    F.concat_ws("|", F.col("source_system"), F.col("source_patient_id")).alias("_uid"),
    "patient_golden_id",
)

adm = (
    adm_src
    .withColumn("_uid", F.concat_ws("|", F.col("source_system"), F.col("source_patient_id")))
    .join(xref, "_uid", "left")
    .drop("_uid")
)

unmatched = adm.filter(F.col("patient_golden_id").isNull()).count()
if unmatched:
    print(f"WARNING: {unmatched} admissions have no MPI match — routed to unknown patient member.")

# MARKDOWN ********************
# ## 2. Bed movements → transfers, ICU days, discharge bed
# ****************************

# CELL ********************
moves = spark.table("lh_silver.bed_assignment").filter(
    F.col("admission_id").isin([r["admission_id"] for r in adm.select("admission_id").distinct().collect()])
) if False else spark.table("lh_silver.bed_assignment").join(
    adm.select("admission_id").distinct(), "admission_id", "inner"
)

beds = spark.table("lh_gold.dim_bed").filter(F.col("is_current")).select(
    F.col("bed_id").alias("_bed_id"), "bed_key", "ward_type"
)

moves = moves.join(beds, moves["bed_id"] == F.col("_bed_id"), "left")

move_agg = moves.groupBy("admission_id").agg(
    (F.count("*") - 1).alias("transfer_count"),
    F.sum(
        F.when(F.col("ward_type") == "ICU",
               (F.col("assignment_end_ts").cast("long") - F.col("assignment_start_ts").cast("long")) / 86400.0)
        .otherwise(F.lit(0.0))
    ).alias("icu_days"),
)

first_bed = (
    moves.withColumn("_rn", F.row_number().over(
        Window.partitionBy("admission_id").orderBy(F.col("assignment_start_ts").asc())))
    .filter(F.col("_rn") == 1)
    .select("admission_id", F.col("bed_key").alias("first_bed_key"))
)

# MARKDOWN ********************
# ## 3. Length of stay
#
# A same-day admission and discharge counts as one day, not zero — that is the
# CIHI convention and it is what clinicians expect to see. Hours are kept
# separately for short-stay analysis where the day-count rounding matters.
# ****************************

# CELL ********************
adm = (
    adm
    .withColumn("length_of_stay_hours",
                F.when(F.col("discharge_ts").isNotNull(),
                       (F.col("discharge_ts").cast("long") - F.col("admission_ts").cast("long")) / 3600.0))
    .withColumn("length_of_stay_days",
                F.when(F.col("discharge_ts").isNull(), None)
                 .otherwise(F.greatest(F.datediff("discharge_ts", "admission_ts"), F.lit(1))))
    .withColumn("is_open", F.col("discharge_ts").isNull())
)

# Guard: a discharge before the admission is a source clock error. Null the
# derived measures and let DQ rule DQ-ADM-003 report it, rather than emitting
# a negative length of stay that would corrupt every ALOS average.
adm = adm.withColumn(
    "_los_invalid", F.col("discharge_ts").isNotNull() & (F.col("discharge_ts") < F.col("admission_ts"))
)
adm = (
    adm.withColumn("length_of_stay_days", F.when(F.col("_los_invalid"), None).otherwise(F.col("length_of_stay_days")))
       .withColumn("length_of_stay_hours", F.when(F.col("_los_invalid"), None).otherwise(F.col("length_of_stay_hours")))
)

# MARKDOWN ********************
# ## 4. Readmission
#
# Definition applied: an unplanned admission occurring within 30 days of a
# prior discharge for the same golden patient, excluding elective admissions,
# excluding inter-facility transfers, and excluding index admissions where the
# patient died. Each exclusion is applied explicitly so the definition can be
# audited against the ministry specification.
# ****************************

# CELL ********************
disposition = spark.table("lh_gold.dim_discharge_disposition").select(
    F.col("disposition_code").alias("_disp_code"), "is_transfer", "is_expired"
)
adm = adm.join(disposition, adm["discharge_disposition_code"] == F.col("_disp_code"), "left")

w_patient = Window.partitionBy("patient_golden_id").orderBy(F.col("admission_ts").asc())

adm = (
    adm
    .withColumn("_prior_discharge_ts", F.lag("discharge_ts").over(w_patient))
    .withColumn("_prior_was_transfer", F.coalesce(F.lag("is_transfer").over(w_patient), F.lit(False)))
    .withColumn("_prior_was_death", F.coalesce(F.lag("is_expired").over(w_patient), F.lit(False)))
    .withColumn("days_since_prior_discharge",
                F.when(F.col("_prior_discharge_ts").isNotNull(),
                       F.datediff("admission_ts", "_prior_discharge_ts")))
    .withColumn(
        "is_readmission_30d",
        F.coalesce(
            (F.col("days_since_prior_discharge").between(0, READMISSION_WINDOW_DAYS))
            & (F.col("admission_type_code") != F.lit("ELECTIVE"))
            & (~F.col("_prior_was_transfer"))
            & (~F.col("_prior_was_death")),
            F.lit(False),
        ),
    )
    .withColumn("is_index_admission", ~F.col("is_readmission_30d"))
)

# MARKDOWN ********************
# ## 5. Resolve dimension keys
#
# Patient, doctor, department and bed are SCD2, so the lookup is temporal: the
# version of the member in effect at admission time. Using the current version
# would attribute a 2024 admission to a doctor's 2026 department.
# ****************************

# CELL ********************
f = adm
f = lookup_dim_key(f, "lh_gold.dim_patient", "patient_golden_id", "patient_golden_id",
                   "patient_key", event_ts_col="admission_ts")
f = lookup_dim_key(f, "lh_gold.dim_doctor", "attending_physician_id", "doctor_id",
                   "doctor_key", event_ts_col="admission_ts", out_col="attending_doctor_key")
f = lookup_dim_key(f, "lh_gold.dim_department", "department_id", "department_id",
                   "department_key", event_ts_col="admission_ts")
f = lookup_dim_key(f, "lh_gold.dim_hospital", "hospital_id", "hospital_id",
                   "hospital_key", event_ts_col="admission_ts")
f = lookup_dim_key(f, "lh_gold.dim_admission_type", "admission_type_code",
                   "admission_type_code", "admission_type_key")
f = lookup_dim_key(f, "lh_gold.dim_discharge_disposition", "discharge_disposition_code",
                   "disposition_code", "discharge_disposition_key")
f = lookup_dim_key(f, "lh_gold.dim_diagnosis", "primary_diagnosis_code",
                   "diagnosis_code", "primary_diagnosis_key")
f = lookup_dim_key(f, "lh_gold.dim_insurance_provider", "payer_id", "payer_id",
                   "insurance_provider_key", event_ts_col="admission_ts")

f = f.join(move_agg, "admission_id", "left").join(first_bed, "admission_id", "left")

charges = (
    spark.table("lh_silver.invoice_line")
    .groupBy("encounter_id")
    .agg(F.sum("net_amount").alias("total_charges"))
)
f = f.join(charges, "encounter_id", "left")

# MARKDOWN ********************
# ## 6. Project to the fact grain
# ****************************

# CELL ********************
def date_key(col):
    return F.when(col.isNull(), F.lit(UNKNOWN_KEY)) \
            .otherwise(F.date_format(col, "yyyyMMdd").cast("int"))


def time_key(col):
    return F.when(col.isNull(), F.lit(UNKNOWN_KEY)) \
            .otherwise(F.hour(col) * 60 + F.minute(col))


fact = f.select(
    F.col("admission_id"),
    F.col("encounter_id"),
    F.col("patient_key"),
    F.col("attending_doctor_key"),
    F.col("department_key"),
    F.col("hospital_key"),
    F.coalesce(F.col("first_bed_key"), F.lit(UNKNOWN_KEY)).alias("bed_key"),
    F.col("admission_type_key"),
    F.col("discharge_disposition_key"),
    F.col("primary_diagnosis_key"),
    F.col("insurance_provider_key"),
    date_key(F.col("admission_ts")).alias("admission_date_key"),
    time_key(F.col("admission_ts")).alias("admission_time_key"),
    date_key(F.col("discharge_ts")).alias("discharge_date_key"),
    date_key(F.col("expected_discharge_ts")).alias("expected_discharge_date_key"),
    F.col("length_of_stay_days"),
    F.col("length_of_stay_hours"),
    F.coalesce(F.col("icu_days"), F.lit(0.0)).alias("icu_days"),
    F.coalesce(F.col("transfer_count"), F.lit(0)).alias("transfer_count"),
    F.col("days_since_prior_discharge"),
    F.coalesce(F.col("total_charges"), F.lit(0.0)).alias("total_charges"),
    F.col("is_readmission_30d"),
    F.col("is_index_admission"),
    F.col("is_open"),
    F.lit(batch_id).alias("batch_id"),
    F.current_timestamp().alias("loaded_ts"),
)

# MARKDOWN ********************
# ## 7. Accumulating-snapshot merge
#
# Match on the business key and update in place. An admission row is written
# on admission and revised on discharge, so the same admission never produces
# two fact rows.
# ****************************

# CELL ********************
rows_written = fact.count()

DeltaTable.forName(spark, "lh_gold.fact_admission").alias("t").merge(
    fact.alias("s"), "t.admission_id = s.admission_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

new_watermark = adm_src.agg(F.max("source_updated_ts").alias("wm")).collect()[0]["wm"]
if new_watermark:
    set_watermark("GOLD.fact_admission", str(new_watermark), batch_id)

spark.sql("OPTIMIZE lh_gold.fact_admission ZORDER BY (admission_date_key, hospital_key, patient_key)")

log_batch(batch_id, "gold_fact_admission", "GOLD", "lh_gold.fact_admission",
          "SUCCESS", adm_src.count(), rows_written)

print(f"fact_admission merged: {rows_written} rows")
mssparkutils.notebook.exit(batch_id)
