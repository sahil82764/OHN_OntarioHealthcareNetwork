# Fabric notebook source
# METADATA ********************
# Gold — conformed dimension load.
# SCD2 dimensions use the shared two-phase merge helper; SCD1 reference
# dimensions are overwritten. Unknown members are seeded once and never
# deleted, because facts depend on them.
# ****************************

# CELL ********************
%run 00_common_utils
# ****************************

# CELL ********************
from pyspark.sql import functions as F

batch_id = new_batch_id("GOLD", "dimensions")

# MARKDOWN ********************
# ## Seed special members
#
# Every dimension gets -1 unknown, -2 not applicable, -3 late arriving. Facts
# referencing a member that could not be resolved point here instead of being
# dropped, so row counts reconcile to source and the gap is visible in the
# data-quality report rather than hidden.
# ****************************

# CELL ********************
SPECIAL_MEMBERS = [
    (UNKNOWN_KEY, "UNKNOWN", "Unknown"),
    (NOT_APPLICABLE_KEY, "N/A", "Not applicable"),
    (LATE_ARRIVING_KEY, "LATE", "Late arriving member"),
]


def seed_special_members(table, key_col, bk_col, name_col):
    existing = spark.table(table).filter(F.col(key_col) < 0).count()
    if existing >= len(SPECIAL_MEMBERS):
        return
    rows = [(k, bk, nm) for k, bk, nm in SPECIAL_MEMBERS]
    df = spark.createDataFrame(rows, f"{key_col} long, {bk_col} string, {name_col} string")
    df = (
        df.withColumn("effective_from_ts", F.lit("1900-01-01").cast("timestamp"))
        .withColumn("effective_to_ts", F.lit(HIGH_DATE).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_row_hash", F.lit("SPECIAL"))
    )
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)


for tbl, k, bk, nm in [
    ("lh_gold.dim_patient", "patient_key", "patient_golden_id", "age_band"),
    ("lh_gold.dim_doctor", "doctor_key", "doctor_id", "doctor_display_name"),
    ("lh_gold.dim_hospital", "hospital_key", "hospital_id", "hospital_name"),
    ("lh_gold.dim_department", "department_key", "department_id", "department_name"),
    ("lh_gold.dim_bed", "bed_key", "bed_id", "ward_name"),
    ("lh_gold.dim_diagnosis", "diagnosis_key", "diagnosis_code", "diagnosis_description"),
]:
    seed_special_members(tbl, k, bk, nm)

# MARKDOWN ********************
# ## dim_patient (SCD2)
#
# Note what is *not* tracked as Type 2: match_confidence and
# source_record_count change every time the MPI runs, and versioning on them
# would create a new patient row nightly for no analytical benefit.
# ****************************

# CELL ********************
src_patient = (
    spark.table("lh_silver.patient_golden")
    .select(
        F.col("patient_golden_id"),
        F.col("source_patient_id"),
        F.col("hcn_token").alias("patient_token"),
        F.col("birth_year"),
        F.col("age_band"),
        F.col("sex"),
        F.col("fsa"),
        F.col("language").alias("preferred_language"),
        F.col("is_deceased"),
        F.col("source_record_count"),
        F.col("match_confidence"),
    )
    .withColumn("patient_token", F.coalesce(F.col("patient_token"), F.lit("NO_HCN")))
)

result = scd2_merge(
    source=src_patient,
    target_table="lh_gold.dim_patient",
    business_key="patient_golden_id",
    tracked_cols=["sex", "age_band", "fsa", "preferred_language", "is_deceased"],
    key_col="patient_key",
    batch_id=batch_id,
)
print(result)

# MARKDOWN ********************
# ## dim_doctor, dim_hospital, dim_department, dim_bed (SCD2)
# ****************************

# CELL ********************
scd2_merge(
    source=spark.table("lh_silver.doctor"),
    target_table="lh_gold.dim_doctor",
    business_key="doctor_id",
    tracked_cols=["specialty", "sub_specialty", "primary_department_id",
                  "employment_type", "fte", "is_active"],
    key_col="doctor_key",
    batch_id=batch_id,
)

scd2_merge(
    source=spark.table("lh_silver.hospital"),
    target_table="lh_gold.dim_hospital",
    business_key="hospital_id",
    tracked_cols=["hospital_name", "facility_type", "region", "licensed_beds",
                  "has_emergency_dept", "is_teaching_hospital"],
    key_col="hospital_key",
    batch_id=batch_id,
)

dept_src = lookup_dim_key(
    spark.table("lh_silver.department"),
    "lh_gold.dim_hospital", "hospital_id", "hospital_id", "hospital_key",
)
scd2_merge(
    source=dept_src,
    target_table="lh_gold.dim_department",
    business_key="department_id",
    tracked_cols=["department_name", "service_line", "hospital_key",
                  "cost_centre", "is_clinical", "is_inpatient_unit"],
    key_col="department_key",
    batch_id=batch_id,
)

bed_src = lookup_dim_key(
    lookup_dim_key(spark.table("lh_silver.bed"),
                   "lh_gold.dim_hospital", "hospital_id", "hospital_id", "hospital_key"),
    "lh_gold.dim_department", "department_id", "department_id", "department_key",
)
scd2_merge(
    source=bed_src,
    target_table="lh_gold.dim_bed",
    business_key="bed_id",
    tracked_cols=["room_number", "ward_name", "ward_type", "bed_type",
                  "hospital_key", "department_key", "is_isolation_capable", "is_active"],
    key_col="bed_key",
    batch_id=batch_id,
)

# MARKDOWN ********************
# ## SCD1 reference dimensions
#
# Clinical vocabularies are versioned upstream by the standards body. A code's
# description changing does not need a history row here — the code itself is
# the stable identity, and retired codes stay in the table so historical facts
# still resolve.
# ****************************

# CELL ********************
def scd1_upsert(source, table, key_col, business_key, tracked_cols):
    src = row_hash(source, tracked_cols)
    max_key = spark.table(table).agg(F.coalesce(F.max(key_col), F.lit(0)).alias("m")).collect()[0]["m"]
    existing = spark.table(table).select(F.col(business_key).alias("_bk"), F.col(key_col).alias("_k"))
    src = src.join(existing, src[business_key] == F.col("_bk"), "left")

    from pyspark.sql import Window as W
    new_rows = src.filter(F.col("_k").isNull())
    new_rows = new_rows.withColumn(
        key_col, F.row_number().over(W.orderBy(business_key)) + F.lit(max_key)
    ).drop("_bk", "_k")
    upd_rows = src.filter(F.col("_k").isNotNull()).withColumn(key_col, F.col("_k")).drop("_bk", "_k")

    payload = new_rows.unionByName(upd_rows)
    DeltaTable.forName(spark, table).alias("t").merge(
        payload.alias("s"), f"t.{business_key} = s.{business_key}"
    ).whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash").whenNotMatchedInsertAll().execute()


scd1_upsert(spark.table("lh_silver.ref_icd10ca"), "lh_gold.dim_diagnosis",
            "diagnosis_key", "diagnosis_code",
            ["diagnosis_description", "chapter", "category", "body_system", "is_chronic"])

scd1_upsert(spark.table("lh_silver.ref_loinc"), "lh_gold.dim_lab_test",
            "lab_test_key", "loinc_code",
            ["test_name", "panel_name", "specimen_type", "result_unit",
             "reference_low", "reference_high"])

scd1_upsert(spark.table("lh_silver.ref_medication"), "lh_gold.dim_medication",
            "medication_key", "din",
            ["generic_name", "brand_name", "atc_code", "atc_class", "dosage_form",
             "is_controlled_substance", "is_high_alert", "is_formulary"])

# MARKDOWN ********************
# ## Optimize for Direct Lake
#
# V-Order and ZORDER on the columns the semantic model filters and joins on.
# Direct Lake reads Delta files directly, so file layout is the model's
# performance profile.
# ****************************

# CELL ********************
for tbl, zcols in [
    ("lh_gold.dim_patient", "patient_golden_id, is_current"),
    ("lh_gold.dim_doctor", "doctor_id, is_current"),
    ("lh_gold.dim_department", "department_id, hospital_key"),
    ("lh_gold.dim_bed", "bed_id, hospital_key"),
]:
    spark.sql(f"OPTIMIZE {tbl} ZORDER BY ({zcols})")

log_batch(batch_id, "gold_dimensions", "GOLD", "lh_gold.dim_*", "SUCCESS")
mssparkutils.notebook.exit(batch_id)
