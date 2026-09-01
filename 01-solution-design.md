# Ontario Healthcare Network — Analytics Platform Solution Design

Version 1.0 · Microsoft Fabric

---

## 1. Business requirements summary

| ID | Requirement | Primary consumer | Supporting model objects |
|----|-------------|------------------|--------------------------|
| BR-01 | Single consolidated patient view across all facilities | Clinical ops, IT | `dim_patient` (SCD2 + MPI golden ID) |
| BR-02 | Track appointments, admissions, discharges, readmissions | Hospital admin | `fact_appointment`, `fact_admission` |
| BR-03 | Emergency-department wait-time analysis | ED leadership | `fact_emergency_visit` (milestone timestamps) |
| BR-04 | Bed and ward occupancy monitoring | Capacity planning | `fact_bed_occupancy_daily`, `dim_bed` |
| BR-05 | Average length of stay (ALOS) by service line | Executive | `fact_admission` |
| BR-06 | Cancellation and no-show analysis | Clinic managers | `fact_appointment` |
| BR-07 | Doctor and department workload | Department heads | `fact_appointment`, `fact_admission`, `dim_doctor` |
| BR-08 | Laboratory-test and medication activity monitoring | Lab / pharmacy | `fact_lab_result`, `fact_medication_order` |
| BR-09 | Insurance-claim status and approval rates | Revenue cycle | `fact_claim` |
| BR-10 | Billing and patient-satisfaction analysis | Finance, quality | `fact_billing_line`, `fact_satisfaction_survey` |
| BR-11 | Restricted access to confidential patient information | Governance | RLS, OLS, dynamic data masking, sensitivity labels |
| BR-12 | Healthcare data-quality monitoring | Data platform team | `fact_dq_result`, `dim_dq_rule` |

### Non-functional requirements

- Gold layer refreshed by 06:00 ET daily; ED and bed-occupancy facts refreshed hourly.
- Bronze retained 24 months; Silver and Gold retained 7 years (health-records retention).
- Full lineage from report visual to source column.
- All environments (dev / test / prod) deployed from Git via Fabric deployment pipelines.
- **Synthetic data only.** No real PHI enters any environment. Source generators use Synthea-style synthetic records.

---

## 2. Solution architecture

### 2.1 Workspace topology

| Workspace | Purpose | Items |
|-----------|---------|-------|
| `OHN-Ingestion-{env}` | Landing and orchestration | Pipelines, Dataflow Gen2, connection artifacts |
| `OHN-Lakehouse-{env}` | Medallion storage and transformation | `lh_bronze`, `lh_silver`, `lh_gold`, notebooks, Spark job definitions |
| `OHN-Warehouse-{env}` | SQL serving layer | `wh_ohn_analytics`, secured views, stored procedures |
| `OHN-Reporting-{env}` | Consumption | Semantic model, Power BI reports, dashboards |
| `OHN-Governance-{env}` | Platform operations | DQ results, audit log lakehouse, monitoring report |

`{env}` = `dev`, `test`, `prod`. Capacity: F64 for prod (Direct Lake, larger semantic models), F16 for lower environments.

### 2.2 Layer definitions

**Landing (`Files/landing/`)** — Raw files exactly as received, partitioned `source_system/entity/ingest_date=YYYY-MM-DD/`. Immutable. Formats: CSV, JSON, Parquet, HL7 v2 and FHIR JSON for clinical feeds.

**Bronze (`lh_bronze`, Delta)** — One table per source entity. Append-only, no business logic. Every row carries audit columns:

```
_source_system, _source_file, _ingest_ts, _batch_id, _row_hash, _is_current_file
```

**Silver (`lh_silver`, Delta)** — Cleansed, conformed, deduplicated. Applied here:
- Data-type standardization and null handling
- Reference-code standardization (sex, marital status, admission type, claim status) via `ref_code_mapping`
- Clinical vocabulary alignment: ICD-10-CA for diagnoses, LOINC for lab tests, DIN/ATC for medications, CCI for procedures
- Address and phone normalization
- **Patient MDM**: deterministic + probabilistic matching to produce `patient_golden_id`
- Confidential column protection (hashing and tokenization of direct identifiers)
- Deduplication using `_row_hash` and business keys
- Incremental merge on natural keys

**Gold (`lh_gold`, Delta)** — Kimball dimensional model. Conformed dimensions, SCD Type 2 where history matters, surrogate keys, fact tables at declared grain. Optimized with `OPTIMIZE`, `ZORDER`, V-Order enabled for Direct Lake.

**Warehouse (`wh_ohn_analytics`)** — T-SQL serving layer over shortcuts to Gold Delta tables. Hosts:
- Secured views with dynamic data masking for downstream SQL consumers
- Stored procedures for finance reconciliation
- The security predicate functions used by warehouse-level RLS

**Semantic model** — Direct Lake over Gold. Star schema, single-direction relationships, measure groups, RLS and OLS roles, calculation groups for time intelligence.

### 2.3 Ingestion patterns

| Source | Pattern | Frequency | Watermark |
|--------|---------|-----------|-----------|
| Hospital EHR / ADT (SQL Server) | Pipeline Copy activity, incremental | Hourly | `last_modified_ts` |
| Laboratory LIS (HL7 v2 / flat file) | Pipeline + Spark parser | Every 30 min | File arrival |
| Pharmacy system (REST API) | Dataflow Gen2 | Hourly | `updated_since` param |
| Appointment / scheduling (Azure SQL) | Pipeline, CDC-based | Every 15 min | Change tracking version |
| Insurance claims (SFTP EDI 837/835) | Pipeline binary copy + Spark EDI parser | Daily | File arrival |
| Billing / ERP (Oracle) | Pipeline, incremental | Daily | `gl_post_date` |
| HR / staffing (CSV export) | Dataflow Gen2 | Daily | Full snapshot |
| Satisfaction surveys (SaaS API) | Dataflow Gen2 | Daily | `response_date` |
| Reference data (ICD-10-CA, LOINC, DIN) | Manual upload + pipeline | Quarterly | Version number |

Control framework tables in `lh_bronze`:
- `ctl_source_registry` — one row per source entity with load type, watermark column, SLA
- `ctl_watermark` — current high-water mark per entity
- `ctl_batch_log` — batch id, start/end, row counts, status
- `ctl_error_log` — rejected rows with rule id and reason

### 2.4 Incremental processing

Bronze → Silver uses a watermark read from `ctl_watermark`, a Delta `MERGE` on the business key, and a post-load watermark update inside the same notebook transaction boundary. Silver → Gold uses:
- SCD Type 2 merge for `dim_patient`, `dim_doctor`, `dim_department`, `dim_hospital`, `dim_bed`
- SCD Type 1 overwrite for `dim_diagnosis`, `dim_lab_test`, `dim_medication`, `dim_insurance_provider`
- Insert-only append for transaction facts (`fact_lab_result`, `fact_billing_line`)
- Accumulating-snapshot merge for `fact_admission`, `fact_emergency_visit`, `fact_claim`
- Full daily rebuild of the rolling window for `fact_bed_occupancy_daily`

Late-arriving facts are handled by looking up the dimension row valid at the fact's event timestamp, falling back to an inferred member (`-1` unknown, `-2` not applicable, `-3` late arriving) rather than dropping the row.

---

## 3. Patient standardization and duplicate management

Duplicate patient records are the single largest data-quality risk in this platform. The MPI process runs in Silver and produces `patient_golden_id`.

**Stage 1 — Standardize.** Upper-case and strip punctuation from names, parse into given/family, standardize addresses to Canada Post format, normalize phone to E.164, validate and check-digit the Ontario health card number (HCN).

**Stage 2 — Blocking.** Candidate pairs generated on: (a) exact HCN, (b) soundex(family_name) + birth_date, (c) postal_code + birth_year + first initial. Blocking keeps the comparison space tractable.

**Stage 3 — Scoring.** Fellegi–Sunter style weighted score over comparison vectors:

| Attribute | Comparator | Match weight | Non-match weight |
|-----------|-----------|--------------|------------------|
| Health card number | exact | +12.0 | -6.0 |
| Birth date | exact / ±1 component | +6.0 / +2.0 | -5.0 |
| Family name | Jaro-Winkler ≥ 0.90 | +4.0 | -3.0 |
| Given name | Jaro-Winkler ≥ 0.88 | +3.0 | -2.0 |
| Sex | exact | +1.0 | -3.0 |
| Postal code | exact FSA+LDU / FSA only | +3.0 / +1.0 | -1.0 |
| Phone | exact | +2.5 | -0.5 |

**Stage 4 — Thresholds.** Score ≥ 14.0 → auto-link. Score 8.0–13.99 → manual review queue (`silver.patient_match_review`). Score < 8.0 → distinct patient.

**Stage 5 — Survivorship.** Golden record built attribute by attribute: most recently updated non-null value wins, with source-system precedence (EHR > scheduling > billing > survey) breaking ties. Golden ID is a stable UUID persisted in `silver.patient_xref` so it survives reprocessing.

**Stage 6 — Feedback.** Steward decisions in the review queue are written back to `silver.patient_match_override` and applied ahead of scoring on the next run, so manual work is never lost.

---

## 4. Security and access control

### 4.1 Data classification

| Class | Examples | Treatment |
|-------|----------|-----------|
| Direct identifier | Name, HCN, address, phone, email, MRN | Tokenized in Silver; SHA-256 with per-environment salt held in Key Vault. Clear values exist only in a restricted `silver.patient_pii` table. |
| Quasi-identifier | Birth date, postal code, sex | Birth date exposed as age band in Gold; postal code truncated to FSA (first 3 chars). |
| Sensitive clinical | Diagnosis, medication, lab result | Available to clinical roles; masked to `Restricted` for finance and executive roles via OLS. |
| Operational | Bed counts, wait times, department, claim status | Open to all authenticated report consumers. |

Microsoft Purview sensitivity labels applied at item level: `Highly Confidential — PHI` on `lh_silver` and `wh_ohn_analytics`, `Confidential` on the semantic model, and labels inherit into exported files.

### 4.2 Roles

| Role | Entra ID group | Row-level scope | Column scope |
|------|----------------|-----------------|--------------|
| Executive | `OHN-BI-Executive` | All hospitals | Aggregates only; clinical detail tables hidden by OLS |
| Hospital administrator | `OHN-BI-HospitalAdmin` | Own hospital | All operational, no direct identifiers |
| Department manager | `OHN-BI-DeptManager` | Own hospital + own department | Operational + clinical |
| Clinician | `OHN-BI-Clinician` | Own department, own patient panel | Full clinical incl. patient display name |
| Revenue cycle analyst | `OHN-BI-RevCycle` | All hospitals | Billing, claims; clinical detail hidden |
| Quality analyst | `OHN-BI-Quality` | All hospitals | De-identified clinical + satisfaction |
| Data steward | `OHN-Data-Steward` | All | Full, including MPI review queue |
| Platform engineer | `OHN-Data-Engineer` | All (dev/test); no prod PII read |

### 4.3 Enforcement points

- **Lakehouse / OneLake**: workspace roles plus OneLake data-access roles restricting `lh_silver` PII folders to stewards only.
- **Warehouse**: `GRANT`/`DENY` at object level, `MASKED WITH` on identifier columns, and security predicate functions filtering by `USER_NAME()` joined to `sec_user_hospital_map`.
- **Semantic model**: RLS roles with DAX filters on `dim_hospital` and `dim_department` driven by a `sec_user_scope` bridge table; OLS hiding `dim_diagnosis[diagnosis_description]` and `fact_lab_result[result_value]` from non-clinical roles.
- **Reports**: Apps with audience-based distribution so each role sees only relevant pages.

### 4.4 Auditing

Fabric activity logs and semantic-model query logs are ingested nightly into `lh_governance.audit_activity`. A monitoring report tracks access to PHI-classified items, failed authorization attempts, and export events.

---

## 5. Data quality framework

Rules are declared as data in `governance.dim_dq_rule` and executed by a generic notebook, so adding a rule requires no code change.

| Dimension | Example rule | Severity | Action |
|-----------|--------------|----------|--------|
| Completeness | `patient.birth_date` not null | Error | Quarantine row |
| Validity | Ontario HCN passes check digit | Error | Quarantine row |
| Validity | `admission.discharge_ts >= admission_ts` | Error | Quarantine row |
| Validity | Diagnosis code exists in ICD-10-CA reference | Warning | Route to unknown member |
| Uniqueness | One current row per `patient_golden_id` | Error | Fail batch |
| Consistency | Claim amount ≤ billed amount | Warning | Flag |
| Timeliness | Source file received within SLA window | Warning | Alert |
| Accuracy | ALOS within 3 SD of trailing 90-day mean | Warning | Alert |
| Referential | Every fact FK resolves to a dimension member | Error | Fail batch |

Each run writes to `governance.fact_dq_result` (rule id, table, batch id, rows evaluated, rows passed, rows failed, pass rate, severity, run timestamp). Error-severity failures above the configured tolerance raise a pipeline failure; the DQ report exposes trend, top failing rules, and quarantine volume.

Quarantined rows land in `lh_silver.quarantine_{entity}` with the failing rule id, and are reprocessed automatically once the underlying data is corrected at source.

---

## 6. Monitoring and operations

- **Pipeline monitoring**: Fabric Monitoring hub plus a custom `governance.fact_pipeline_run` table populated by each pipeline's final activity (pipeline name, run id, status, duration, rows read/written, error message).
- **Spark monitoring**: Notebook run metrics captured from the Spark application id; long-running or skewed stages flagged.
- **Semantic model refresh**: Direct Lake framing events and fallback-to-DirectQuery occurrences monitored; fallback is treated as a warning because it indicates the model exceeded guardrails.
- **Alerting**: Data Activator rules on `fact_pipeline_run` and `fact_dq_result` send Teams alerts to the platform channel on failure or DQ breach.
- **SLA dashboard**: freshness per source entity vs. contracted SLA, shown on the Data Quality report page.

---

## 7. Source control and CI/CD

- Fabric Git integration binds each workspace to a branch in Azure DevOps repo `ohn-fabric-analytics`.
- Branching: `feature/*` → `develop` (dev workspace) → `release/*` (test workspace) → `main` (prod workspace).
- Pull requests require one reviewer plus a passing build that lints notebooks, validates DDL, and runs `pytest` on the transformation helper library.
- Deployment pipelines promote items across environments with parameterized connection strings and workspace-scoped variable libraries.
- Notebooks are stored as `.py` percent-format files so diffs are reviewable.

---

## 8. Repository structure

```
ohn-fabric-analytics/
├── docs/                     architecture, requirements, STM, runbooks
├── notebooks/                PySpark transformation notebooks
├── pipelines/                pipeline and dataflow definitions (JSON)
├── warehouse/ddl/            T-SQL for warehouse objects and security
├── semantic-model/           TMDL / measures and RLS definitions
├── tests/                    unit and integration tests
├── config/                   source registry, DQ rules, code mappings
└── .azure-pipelines/         CI build and release definitions
```
