# Step 1 — Generate the data and get it into Fabric

Everything here runs before you write a single pipeline. By the end you will have a Fabric workspace, a lakehouse, and ~1.25M rows of synthetic source data sitting in the OneLake landing zone, ready for Bronze ingestion.

Estimated time: 45–90 minutes, most of it waiting on uploads.

---

## 1.1 Generate the data locally

You need Python 3.10 or newer. No packages to install — the generator is standard library only.

```bash
cd data-generator
python ohn_generator.py --patients 25000 --out ./landing
python validate_data.py --landing ./landing
```

Takes about 25 seconds and produces roughly 130 MB across 26 CSV files.

**Use 25,000 patients, not fewer.** Below about 9,000 the bed-sizing floor kicks in (explained in `_auto_bed_scale`) and bed occupancy lands near 30% instead of 85%, which makes the capacity report look like an empty hospital. If you need a smaller dataset for a first pipeline test, generate 2,000 into a separate folder and use it only for plumbing, then regenerate at full size.

### What you should see

`validate_data.py` must print **all checks passed**. It verifies two things:

- The deliberate defects are present at the configured rates. If they are not, your DQ engine has nothing to catch and the data-quality report is a flat 100% — which looks like a bug in the platform rather than clean data.
- The planted correlations survived generation. Higher-acuity ED patients are seen sooner, longer booking lead times raise no-show risk, denial rates vary by payer, longer ED waits produce lower satisfaction scores. These are the relationships your report pages exist to reveal. Without them, every scatter plot is a shapeless cloud.

If a check fails, the constants to adjust are at the top of `ohn_generator.py` — `DEFECTS` for defect rates, `BASE_*` for operational rates.

### Output layout

```
landing/
├── EHR/          patient, admission, bed_assignment, diagnosis, emergency_visit
├── SCHED/        patient, appointment, appointment_status_history
├── LIS/          lab_result
├── PHARM/        medication_order
├── CLAIMS/       claim_header, claim_line
├── FIN/          patient_account, invoice, invoice_line
├── HR/           doctor
├── FACIL/        hospital, department, bed
├── SURVEY/       survey_response
├── REF/          icd10ca, loinc, medication, payer, code_mapping
└── _truth/       patient_truth        ← do NOT upload as a source
```

Each entity sits in `<SOURCE>/<entity>/ingest_date=2026-08-01/<entity>_2026-08-01.csv`, which is exactly the partition layout `10_bronze_ingest.py` expects.

### About `_truth/patient_truth.csv`

This is the ground truth: which source records belong to the same real person. It is **not** a source system — no real hospital has this file, and the whole point of the MPI is to reconstruct it.

Upload it to a separate governance lakehouse, not to the landing zone. You will use it later to measure MDM precision and recall, which is some of the strongest testing evidence this project can produce: "the MPI correctly clustered 97.3% of source records, with 0.4% false merges." That is a far better claim than "the notebook ran."

---

## 1.2 Understand what you generated

Before uploading, it is worth knowing what is deliberately wrong with this data, because these are the problems the platform is built to solve.

**Patient identity is fragmented.** 25,000 real people appear as roughly 62,000 source records across three systems. The same person is `MRN…` in the EHR, `SCH…` in scheduling, and `ACC…` in finance, with:

- Nicknames in secondary systems (Robert in the EHR, Bob in scheduling)
- Transposition and substitution typos in names
- Health card numbers formatted three different ways, missing 9% of the time, and failing the check digit 2% of the time
- Birth dates in `YYYY-MM-DD`, `DD/MM/YYYY`, and `MM/DD/YYYY` depending on the system
- Stale addresses in secondary systems 20% of the time
- Sex coded as `M`/`F`, `MALE`/`FEMALE`, and `m`/`f` depending on the source

Plus about 4% of people are registered **twice within the EHR itself** — the classic duplicate-MRN problem, and the hardest case because deterministic matching on source id will never catch it.

**Roughly 1 in 200 rows has a data-quality defect**, each mapped to a specific rule in `config/dq_rules.csv`: discharges before admissions, CTAS scores outside 1–5, negative lab turnaround times, claims approved for more than they were billed, invoice lines that do not reconcile.

**The operational signals are real, not noise.** ED wait times respond to time of day, day of week, and triage acuity. Readmission risk rises with age, chronic diagnoses, long index stays, and discharge to home care. Satisfaction falls as ED waits rise. Denial rates differ by payer. These will show up as actual patterns in your reports.

---

## 1.3 Create the Fabric workspace

You need a Fabric capacity. A **Microsoft Fabric trial** (60 days, F64-equivalent) is enough for this entire project and is the right choice unless you already have capacity — Direct Lake and larger semantic models need more than the free Power BI tier gives you.

1. Go to `app.fabric.microsoft.com`
2. Account manager (top right) → **Start trial** if you do not have capacity
3. **Workspaces** → **New workspace**
4. Name it `OHN-Lakehouse-dev`
5. Under **Advanced**, set the licence mode to **Trial** or **Fabric capacity** — not Pro, or Direct Lake will not be available later

For now, one workspace is enough. The five-workspace topology in `01-solution-design.md` is what you grow into once the pipelines work; splitting on day one just means more places to look when something breaks.

## 1.4 Create the lakehouses

Inside `OHN-Lakehouse-dev`:

**New item** → **Lakehouse** → name it `lh_bronze`. Repeat for `lh_silver`, `lh_gold`, and `lh_governance`.

Leave schema support at the default unless you have a reason to change it. Note which you chose — it determines whether your tables are addressed as `lh_bronze.dbo.ehr_patient` or `lh_bronze.ehr_patient`, and the notebooks assume the latter.

## 1.5 Upload the data

Three options. Pick based on how much you are uploading.

### Option A — OneLake File Explorer (recommended)

Best for 130 MB. It syncs OneLake to Windows Explorer like OneDrive, so you drag the whole `landing` folder once and the structure is preserved.

1. Download OneLake File Explorer from Microsoft's download centre
2. Sign in with your Fabric account
3. Open File Explorer → **OneLake** → `OHN-Lakehouse-dev` → `lh_bronze` → `Files`
4. Copy your local `landing` folder in, minus `_truth`
5. Wait for the sync icons to turn to green checkmarks before moving on

### Option B — Browser upload

Works, but the UI uploads one folder level at a time and you have 26 entities. Use it only for a quick test with a small dataset.

In `lh_bronze` → **Files** → **…** → **Upload** → **Upload folder**.

### Option C — Generate directly into OneLake from a Fabric notebook

Skips the upload entirely. Useful if your local machine is slow or you want the generation reproducible inside Fabric.

1. In the workspace: **New item** → **Notebook**
2. Attach `lh_bronze` as the default lakehouse
3. Upload `ohn_generator.py` to `Files/scripts/` via the browser (it is one small file)
4. Run:

```python
import sys, subprocess
sys.path.insert(0, "/lakehouse/default/Files/scripts")

subprocess.run([
    sys.executable, "/lakehouse/default/Files/scripts/ohn_generator.py",
    "--patients", "25000",
    "--out", "/lakehouse/default/Files/landing"
], check=True)
```

Writing many small files through the notebook filesystem is slower than it looks — expect several minutes.

### Do not upload `_truth`

Put it in `lh_governance` instead:

```
lh_governance/Files/truth/patient_truth.csv
```

## 1.6 Verify the upload

Create a notebook, attach `lh_bronze`, and run:

```python
from notebookutils import mssparkutils

base = "Files/landing"
total_files = 0
for source in sorted(mssparkutils.fs.ls(base), key=lambda f: f.name):
    if not source.isDir:
        continue
    entities = mssparkutils.fs.ls(source.path)
    print(f"\n{source.name}")
    for e in sorted(entities, key=lambda f: f.name):
        parts = mssparkutils.fs.ls(e.path)
        n = sum(len(mssparkutils.fs.ls(p.path)) for p in parts if p.isDir)
        total_files += n
        print(f"   {e.name:<32} {len(parts)} partition(s), {n} file(s)")
print(f"\nTotal files: {total_files}")
```

You should see 25 entities (26 minus `_truth`) each with one partition and one file.

Then confirm the data actually reads, and that the defects made it through intact:

```python
from pyspark.sql import functions as F

df = spark.read.option("header", True).csv(
    "Files/landing/EHR/patient/ingest_date=2026-08-01/")

print(f"Rows: {df.count():,}")
df.show(5, truncate=False)

# The defects should be visible here — this is what the platform must handle
df.select(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("health_card_number") == "", 1).otherwise(0)).alias("no_hcn"),
    F.sum(F.when(F.col("date_of_birth") == "", 1).otherwise(0)).alias("no_dob"),
    F.countDistinct("gender").alias("distinct_sex_codes"),
).show()
```

`distinct_sex_codes` should be more than 3. That is the point — the raw feed uses codes the model does not, and `ref_code_mapping` is what reconciles them.

---

## Checkpoint

Before moving to Bronze ingestion, confirm:

- `validate_data.py` reports all checks passed
- `lh_bronze/Files/landing/` holds 25 entities
- `lh_governance/Files/truth/patient_truth.csv` exists and is **not** in the landing zone
- A Spark read against one landing folder returns rows
- Your workspace is on Trial or Fabric capacity, not Pro

---

## What comes next

**Step 2** builds the control framework — `ctl_source_registry`, `ctl_watermark`, `ctl_batch_log` — and runs `10_bronze_ingest.py` across all 25 entities to land them as Delta tables. The registry is what makes ingestion metadata-driven: adding a source becomes a row, not a new notebook.

**Step 3** is Silver, and the patient MPI is the piece to budget real time for. It is the hardest transformation in the project and the one that makes the consolidated patient view possible.

A note on sequencing: resist the temptation to build reports early. Every report depends on Gold, Gold depends on conformed dimensions, and the dimensions depend on patient identity being resolved. Get identity right first and the rest follows quickly; get it wrong and every downstream number is subtly incorrect in a way that is very hard to trace back.
