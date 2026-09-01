# Fabric notebook source
# METADATA ********************
# Silver — patient standardization and master patient index (MPI).
#
# Consolidates patient records from every source system into a single golden
# record per real person, and assigns a stable patient_golden_id that survives
# reprocessing. This is the notebook that satisfies BR-01 (consolidated
# patient view) and is the highest-risk transformation in the platform: a
# false merge joins two people's medical histories, a missed merge fragments
# one person's care record.
# ****************************

# CELL ********************
%run 00_common_utils
# ****************************

# CELL ********************
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType
from graphframes import GraphFrame  # available on the Fabric Spark runtime

batch_id = new_batch_id("MDM", "patient")
run_ts = datetime.now(timezone.utc)

HCN_SALT = mssparkutils.credentials.getSecret("https://kv-ohn-prod.vault.azure.net/", "hcn-token-salt")

AUTO_LINK_THRESHOLD = 14.0
REVIEW_THRESHOLD = 8.0

SOURCE_PRECEDENCE = {"EHR": 1, "SCHED": 2, "LIS": 3, "PHARM": 4, "FIN": 5, "CLAIMS": 6, "SURVEY": 7}

# MARKDOWN ********************
# ## 1. Collect patient records from every source
#
# Each source contributes a differently shaped patient record. They are
# normalised to a common contract here rather than in the matching logic, so
# adding a source means adding one select, not touching the scoring code.
# ****************************

# CELL ********************
def normalize(df, source_system, col_map):
    """Project a source patient table onto the common MPI contract."""
    return (
        df.select(
            F.lit(source_system).alias("source_system"),
            F.col(col_map["id"]).cast(StringType()).alias("source_patient_id"),
            std_string(F.col(col_map["given_name"])).alias("given_name"),
            std_string(F.col(col_map["family_name"])).alias("family_name"),
            F.to_date(F.col(col_map["birth_date"])).alias("birth_date"),
            std_string(F.col(col_map["sex"])).alias("sex_raw"),
            F.regexp_replace(F.col(col_map["hcn"]), r"\D", "").alias("hcn"),
            std_postal_code(F.col(col_map["postal_code"])).alias("postal_code"),
            std_phone(F.col(col_map["phone"])).alias("phone"),
            std_string(F.col(col_map["language"])).alias("language") if col_map.get("language") else F.lit(None).cast(StringType()).alias("language"),
            F.col(col_map["updated_ts"]).cast("timestamp").alias("source_updated_ts"),
        )
    )


ehr = normalize(
    spark.table("lh_bronze.ehr_patient"),
    "EHR",
    {"id": "patient_id", "given_name": "first_name", "family_name": "last_name",
     "birth_date": "date_of_birth", "sex": "gender", "hcn": "health_card_number",
     "postal_code": "postal_code", "phone": "primary_phone", "language": "preferred_language",
     "updated_ts": "last_modified_ts"},
)

sched = normalize(
    spark.table("lh_bronze.sched_patient"),
    "SCHED",
    {"id": "patient_ref", "given_name": "given", "family_name": "surname",
     "birth_date": "dob", "sex": "sex", "hcn": "health_card",
     "postal_code": "postal", "phone": "contact_phone", "updated_ts": "modified_at"},
)

fin = normalize(
    spark.table("lh_bronze.fin_patient_account"),
    "FIN",
    {"id": "account_holder_id", "given_name": "first_nm", "family_name": "last_nm",
     "birth_date": "birth_dt", "sex": "gender_cd", "hcn": "hc_number",
     "postal_code": "zip_postal", "phone": "phone_number", "updated_ts": "update_dt"},
)

records = ehr.unionByName(sched).unionByName(fin)

# MARKDOWN ********************
# ## 2. Standardize and validate
# ****************************

# CELL ********************
records = (
    records
    .withColumn("hcn", F.when(validate_ontario_hcn(F.col("hcn")), F.col("hcn")).otherwise(None))
    .withColumn("hcn_token", F.when(F.col("hcn").isNotNull(), tokenize("hcn", HCN_SALT)))
    .withColumn("fsa", F.substring(F.col("postal_code"), 1, 3))
    .withColumn("family_soundex", F.soundex(F.coalesce(F.col("family_name"), F.lit(""))))
    .withColumn("given_soundex", F.soundex(F.coalesce(F.col("given_name"), F.lit(""))))
    .withColumn("record_uid", F.concat_ws("|", F.col("source_system"), F.col("source_patient_id")))
    .withColumn("source_rank", F.create_map(
        *[x for k, v in SOURCE_PRECEDENCE.items() for x in (F.lit(k), F.lit(v))]
    )[F.col("source_system")])
)

records = map_code(records, "SEX", "EHR", "sex_raw", "sex").cache()

# Birth dates in the future or before 1900 are impossible; null them and let
# the DQ engine report them rather than letting them anchor a match.
records = records.withColumn(
    "birth_date",
    F.when(
        (F.col("birth_date") > F.current_date()) | (F.col("birth_date") < F.lit("1900-01-01").cast("date")),
        None,
    ).otherwise(F.col("birth_date")),
)

print(f"Candidate records: {records.count()}")

# MARKDOWN ********************
# ## 3. Blocking
#
# Comparing every record to every other record is O(n²) and unnecessary. Three
# blocking keys generate candidate pairs; a true duplicate only has to agree
# on one of them to be caught. The union of blocks gives high recall at a
# fraction of the cost.
# ****************************

# CELL ********************
def block(df, key_expr, block_name):
    return (
        df.filter(key_expr.isNotNull())
        .withColumn("block_key", F.concat_ws("#", F.lit(block_name), key_expr))
        .select("record_uid", "block_key")
    )


blocks = (
    block(records, F.col("hcn_token"), "HCN")
    .unionByName(block(records, F.concat_ws("#", F.col("family_soundex"), F.col("birth_date")), "NAME_DOB"))
    .unionByName(block(records, F.concat_ws("#", F.col("fsa"), F.year("birth_date"), F.substring("given_name", 1, 1)), "GEO_YOB"))
)

# Guard against pathological blocks (e.g. every record with a null-ish
# soundex) that would explode into millions of pairs.
block_sizes = blocks.groupBy("block_key").count()
usable_blocks = block_sizes.filter(F.col("count").between(2, 200)).select("block_key")
oversized = block_sizes.filter(F.col("count") > 200)
if oversized.count() > 0:
    oversized.write.format("delta").mode("overwrite").saveAsTable("lh_silver.mdm_oversized_blocks")
    print("Oversized blocks written for review — these are excluded from pairing.")

blocks = blocks.join(usable_blocks, "block_key")

left = blocks.withColumnRenamed("record_uid", "uid_a")
right = blocks.withColumnRenamed("record_uid", "uid_b")
pairs = (
    left.join(right, "block_key")
    .filter(F.col("uid_a") < F.col("uid_b"))
    .select("uid_a", "uid_b")
    .distinct()
)

print(f"Candidate pairs after blocking: {pairs.count()}")

# MARKDOWN ********************
# ## 4. Scoring
#
# Fellegi–Sunter style additive weights. Agreement on a strong identifier adds
# weight; disagreement subtracts it. Missing values contribute nothing, which
# is deliberate — an absent phone number is not evidence either way.
# ****************************

# CELL ********************
a = records.select([F.col(c).alias(f"a_{c}") for c in records.columns])
b = records.select([F.col(c).alias(f"b_{c}") for c in records.columns])

scored = (
    pairs
    .join(a, F.col("uid_a") == F.col("a_record_uid"))
    .join(b, F.col("uid_b") == F.col("b_record_uid"))
)

jw_family = F.expr("jaro_winkler(a_family_name, b_family_name)") if False else \
    (1 - F.levenshtein(F.col("a_family_name"), F.col("b_family_name")) /
     F.greatest(F.length("a_family_name"), F.length("b_family_name")))
jw_given = (1 - F.levenshtein(F.col("a_given_name"), F.col("b_given_name")) /
            F.greatest(F.length("a_given_name"), F.length("b_given_name")))

scored = (
    scored
    .withColumn("w_hcn", F.when(F.col("a_hcn_token").isNull() | F.col("b_hcn_token").isNull(), 0.0)
                          .when(F.col("a_hcn_token") == F.col("b_hcn_token"), 12.0).otherwise(-6.0))
    .withColumn("w_dob", F.when(F.col("a_birth_date").isNull() | F.col("b_birth_date").isNull(), 0.0)
                          .when(F.col("a_birth_date") == F.col("b_birth_date"), 6.0)
                          .when(F.abs(F.datediff("a_birth_date", "b_birth_date")) <= 31, 2.0)
                          .otherwise(-5.0))
    .withColumn("w_family", F.when(F.col("a_family_name").isNull() | F.col("b_family_name").isNull(), 0.0)
                             .when(jw_family >= 0.90, 4.0).otherwise(-3.0))
    .withColumn("w_given", F.when(F.col("a_given_name").isNull() | F.col("b_given_name").isNull(), 0.0)
                            .when(jw_given >= 0.88, 3.0).otherwise(-2.0))
    .withColumn("w_sex", F.when(F.col("a_sex").isin("UNKNOWN") | F.col("b_sex").isin("UNKNOWN"), 0.0)
                          .when(F.col("a_sex") == F.col("b_sex"), 1.0).otherwise(-3.0))
    .withColumn("w_postal", F.when(F.col("a_postal_code").isNull() | F.col("b_postal_code").isNull(), 0.0)
                             .when(F.col("a_postal_code") == F.col("b_postal_code"), 3.0)
                             .when(F.col("a_fsa") == F.col("b_fsa"), 1.0).otherwise(-1.0))
    .withColumn("w_phone", F.when(F.col("a_phone").isNull() | F.col("b_phone").isNull(), 0.0)
                            .when(F.col("a_phone") == F.col("b_phone"), 2.5).otherwise(-0.5))
    .withColumn("match_score",
                F.col("w_hcn") + F.col("w_dob") + F.col("w_family") +
                F.col("w_given") + F.col("w_sex") + F.col("w_postal") + F.col("w_phone"))
)

# MARKDOWN ********************
# ## 5. Apply steward overrides
#
# A steward's decision always beats the algorithm, and it must survive every
# future run — otherwise stewards re-do the same work every night.
# ****************************

# CELL ********************
overrides = spark.table("lh_silver.patient_match_override").select(
    F.col("uid_a").alias("o_a"), F.col("uid_b").alias("o_b"), F.col("decision")
)

scored = (
    scored.join(overrides,
                (F.col("uid_a") == F.col("o_a")) & (F.col("uid_b") == F.col("o_b")), "left")
    .withColumn("final_decision",
                F.when(F.col("decision") == "MATCH", F.lit("AUTO_LINK"))
                 .when(F.col("decision") == "NO_MATCH", F.lit("DISTINCT"))
                 .when(F.col("match_score") >= AUTO_LINK_THRESHOLD, F.lit("AUTO_LINK"))
                 .when(F.col("match_score") >= REVIEW_THRESHOLD, F.lit("REVIEW"))
                 .otherwise(F.lit("DISTINCT")))
    .drop("o_a", "o_b")
)

review_queue = scored.filter(F.col("final_decision") == "REVIEW").select(
    "uid_a", "uid_b", "match_score",
    "a_given_name", "a_family_name", "a_birth_date", "a_postal_code",
    "b_given_name", "b_family_name", "b_birth_date", "b_postal_code",
    F.lit(batch_id).alias("batch_id"), F.lit(run_ts).alias("queued_ts"),
    F.lit("PENDING").alias("review_status"),
)
review_queue.write.format("delta").mode("append").saveAsTable("lh_silver.patient_match_review")
print(f"Queued for steward review: {review_queue.count()}")

# MARKDOWN ********************
# ## 6. Cluster into golden identities
#
# Auto-linked pairs form the edges of a graph; each connected component is one
# person. This handles transitivity — if A matches B and B matches C, all
# three are the same person even when A and C were never directly compared.
# ****************************

# CELL ********************
edges = scored.filter(F.col("final_decision") == "AUTO_LINK").select(
    F.col("uid_a").alias("src"), F.col("uid_b").alias("dst")
)
vertices = records.select(F.col("record_uid").alias("id"))

spark.sparkContext.setCheckpointDir("Files/checkpoints/mdm")
components = GraphFrame(vertices, edges).connectedComponents().select("id", "component")

# MARKDOWN ********************
# ## 7. Assign stable golden IDs
#
# A newly discovered cluster gets a fresh UUID. A cluster that overlaps an
# existing one keeps the existing golden ID, so downstream keys never churn.
# When two previously separate clusters merge, the lower-precedence golden ID
# is retired into patient_xref_history so historical facts can be repointed.
# ****************************

# CELL ********************
existing_xref = spark.table("lh_silver.patient_xref").select("record_uid", "patient_golden_id")

clustered = components.withColumnRenamed("id", "record_uid").join(existing_xref, "record_uid", "left")

# One surviving golden id per component: the earliest existing one, else new.
w = Window.partitionBy("component").orderBy(F.col("patient_golden_id").asc_nulls_last())
resolved = (
    clustered
    .withColumn("surviving_golden_id", F.first("patient_golden_id", ignorenulls=True).over(w))
)

new_uuid = F.udf(lambda: str(uuid.uuid4()), StringType())
resolved = resolved.withColumn(
    "patient_golden_id_final",
    F.coalesce(F.col("surviving_golden_id"), new_uuid()),
)

# Detect retired ids so facts can be repointed rather than orphaned.
retired = (
    resolved.filter(F.col("patient_golden_id").isNotNull() &
                    (F.col("patient_golden_id") != F.col("patient_golden_id_final")))
    .select(
        F.col("patient_golden_id").alias("retired_golden_id"),
        F.col("patient_golden_id_final").alias("surviving_golden_id"),
        F.lit(batch_id).alias("batch_id"),
        F.lit(run_ts).alias("retired_ts"),
    ).distinct()
)
if retired.count() > 0:
    retired.write.format("delta").mode("append").saveAsTable("lh_silver.patient_xref_history")
    print(f"Golden IDs retired by cluster merge: {retired.count()}")

xref = resolved.select(
    "record_uid",
    F.col("patient_golden_id_final").alias("patient_golden_id"),
    F.lit(batch_id).alias("batch_id"),
    F.lit(run_ts).alias("updated_ts"),
)

DeltaTable.forName(spark, "lh_silver.patient_xref").alias("t").merge(
    xref.alias("s"), "t.record_uid = s.record_uid"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# MARKDOWN ********************
# ## 8. Survivorship — build the golden record
#
# Attribute-level survivorship. For each field the most recently updated
# non-null value wins; source precedence breaks ties. This beats picking one
# "best" source record wholesale, because the freshest address and the most
# reliable health card number often come from different systems.
# ****************************

# CELL ********************
enriched = records.join(xref.select("record_uid", "patient_golden_id"), "record_uid")


def survive(col_name):
    w_attr = (
        Window.partitionBy("patient_golden_id")
        .orderBy(
            F.when(F.col(col_name).isNull(), 1).otherwise(0),
            F.col("source_updated_ts").desc_nulls_last(),
            F.col("source_rank").asc(),
        )
    )
    return F.first(F.col(col_name), ignorenulls=True).over(w_attr).alias(col_name)


golden = (
    enriched.select(
        "patient_golden_id",
        survive("given_name"), survive("family_name"), survive("birth_date"),
        survive("sex"), survive("hcn_token"), survive("postal_code"),
        survive("fsa"), survive("phone"), survive("language"),
        F.first("source_patient_id").over(
            Window.partitionBy("patient_golden_id").orderBy(F.col("source_rank").asc())
        ).alias("source_patient_id"),
    )
    .dropDuplicates(["patient_golden_id"])
)

# Cluster confidence = weakest link in the cluster. A cluster held together by
# one marginal pair is reported as marginal, not as high confidence.
cluster_conf = (
    scored.filter(F.col("final_decision") == "AUTO_LINK")
    .join(xref.select(F.col("record_uid").alias("uid_a"), "patient_golden_id"), "uid_a")
    .groupBy("patient_golden_id")
    .agg(F.min("match_score").alias("match_confidence"))
)

record_counts = enriched.groupBy("patient_golden_id").agg(
    F.count("*").alias("source_record_count"),
    F.collect_set("source_system").alias("contributing_sources"),
)

golden = (
    golden.join(cluster_conf, "patient_golden_id", "left")
    .join(record_counts, "patient_golden_id", "left")
    .withColumn("match_confidence", F.coalesce(F.col("match_confidence"), F.lit(99.0)))
    .withColumn("age_band", age_band(F.col("birth_date")))
    .withColumn("birth_year", F.year("birth_date"))
    .withColumn("is_deceased", F.lit(False))
    .withColumn("_batch_id", F.lit(batch_id))
    .withColumn("_updated_ts", F.lit(run_ts))
)

golden.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("lh_silver.patient_golden")

# PII stays in a separate, restricted table. Gold never sees clear names.
golden.select("patient_golden_id", "given_name", "family_name", "birth_date",
              "postal_code", "phone", "_updated_ts") \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("lh_silver.patient_pii")

log_batch(batch_id, "silver_patient_mdm", "MDM", "lh_silver.patient_golden",
          "SUCCESS", records.count(), golden.count())

print(f"Golden patients: {golden.count()}  (from {records.count()} source records)")
mssparkutils.notebook.exit(batch_id)
