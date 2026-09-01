# Semantic Model and Reporting Design

Direct Lake semantic model over `lh_gold`. One model serves all thirteen report pages so measure definitions cannot drift between reports.

---

## 1. Relationships

All relationships are single-direction (dimension filters fact), one-to-many, with the dimension on the one side. Bi-directional filtering is not used anywhere; ambiguity is resolved with `CROSSFILTER` or `TREATAS` inside specific measures instead.

| From (fact) | To (dimension) | Active |
|---|---|---|
| `fact_admission[admission_date_key]` | `dim_date[date_key]` | Yes |
| `fact_admission[discharge_date_key]` | `dim_date[date_key]` | No — activated by `USERELATIONSHIP` |
| `fact_admission[expected_discharge_date_key]` | `dim_date[date_key]` | No |
| `fact_claim[submission_date_key]` | `dim_date[date_key]` | Yes |
| `fact_claim[adjudication_date_key]` | `dim_date[date_key]` | No |
| `fact_claim[payment_date_key]` | `dim_date[date_key]` | No |
| `fact_*[patient_key]` | `dim_patient[patient_key]` | Yes |
| `fact_*[hospital_key]` | `dim_hospital[hospital_key]` | Yes |
| `fact_*[department_key]` | `dim_department[department_key]` | Yes |
| `dim_department[hospital_key]` | `dim_hospital[hospital_key]` | Yes (snowflake, hidden) |

`dim_date` is marked as the date table on `calendar_date`.

**Direct Lake guardrails.** `dim_patient` is the largest dimension and the one most likely to trigger fallback to DirectQuery. Only current rows plus the seven-year history window are exposed through the model view; the full SCD2 history remains queryable in the warehouse for audit. Fallback behaviour is set to `DirectLakeOnly` in production so a guardrail breach surfaces as an error during testing rather than as a silent performance cliff for users.

---

## 2. Row-level security

`sec_user_scope` is a hidden bridge table refreshed nightly from Entra ID group membership: `user_principal_name`, `hospital_key`, `department_key`, `role_name`, `is_all_hospitals`.

| Role | DAX filter |
|---|---|
| `Executive` | `dim_hospital`: `TRUE()` — all facilities |
| `HospitalAdmin` | `dim_hospital`: `[hospital_key] IN SELECTCOLUMNS(FILTER(sec_user_scope, [user_principal_name] = USERPRINCIPALNAME()), "k", [hospital_key])` |
| `DeptManager` | Hospital filter as above, plus `dim_department`: `[department_key] IN SELECTCOLUMNS(FILTER(sec_user_scope, [user_principal_name] = USERPRINCIPALNAME()), "k", [department_key])` |
| `Clinician` | Department filter as above |
| `RevCycle` | `dim_hospital`: `TRUE()` |
| `Quality` | `dim_hospital`: `TRUE()` |

`sec_user_scope` itself carries the filter `[user_principal_name] = USERPRINCIPALNAME()` so a user cannot enumerate other users' scopes through the model.

## 3. Object-level security

| Object | Hidden from |
|---|---|
| `dim_patient[patient_token]` | All roles except `Clinician`, `DataSteward` |
| `dim_patient[birth_year]` | `Executive`, `RevCycle` |
| `dim_diagnosis[diagnosis_description]` | `RevCycle` |
| `fact_lab_result[result_value_numeric]`, `[result_value_text]` | `Executive`, `RevCycle` |
| `fact_medication_order` (whole table) | `Executive`, `RevCycle` |
| `fact_billing_line`, `fact_claim` | `Clinician` |

OLS returns an error rather than a blank when a hidden object is referenced, so every report page must be validated against every role. The role-validation matrix in `tests/rls_test_matrix.md` covers this.

---

## 4. Report pages

| # | Page | Audience | Key visuals |
|---|---|---|---|
| 1 | Hospital overview | Executive | Census, occupancy, ALOS, ED P90, readmission rate cards; admissions trend; facility comparison matrix |
| 2 | Admissions and readmissions | Hospital admin | Admissions by type and service line; readmission rate trend vs prior year; top diagnoses by readmission rate; readmission funnel by discharge disposition |
| 3 | Emergency department wait times | ED leadership | Median PIA and P90 ED LOS by hour of day and day of week; CTAS distribution; LWBS trend; target-attainment gauge |
| 4 | Bed occupancy | Capacity planning | Occupancy rate heatmap ward × day; midnight census trend; blocked-bed reason breakdown; turnover by ward |
| 5 | Average length of stay | Executive, admin | ALOS trend with outlier-trimmed comparison; ALOS by service line and diagnosis chapter; LOS distribution histogram |
| 6 | Appointment cancellations and no-shows | Clinic managers | No-show rate by department, day of week, and lead-time band; late-cancellation trend; repeat no-show patient cohort (de-identified) |
| 7 | Doctor utilisation | Department heads | Utilisation vs FTE scatter; booked vs available minutes; admissions per physician; panel size |
| 8 | Department performance | Department heads | Scorecard: volume, ALOS, readmission rate, satisfaction, cost per case; period-over-period deltas |
| 9 | Laboratory test activity | Lab management | Volume by panel; median turnaround vs STAT compliance; abnormal and critical result rates; ordering-pattern outliers by physician |
| 10 | Medication usage | Pharmacy | Orders and cost by ATC class; high-alert share; formulary compliance; order-to-dispense distribution |
| 11 | Insurance claims and billing | Revenue cycle | Approval rate by payer; denial reasons Pareto; days-to-adjudicate distribution; AR ageing; net collection rate |
| 12 | Patient satisfaction | Quality | NPS trend; domain scores by department; satisfaction vs ED wait time correlation; response rate |
| 13 | Data quality performance | Platform team | Pass rate by DQ dimension; breach trend; top failing rules; quarantine volume; pipeline success rate and freshness SLA |

Pages 1, 5, 8 and 13 are also published as a Power BI app with audience-based distribution so each Entra group sees only the pages relevant to it.

**Accessibility and design conventions.** Sequential colour ramps for magnitude, diverging only for variance-to-target. Every visual carries alt text. Conditional formatting never relies on colour alone — status is also encoded as an icon or label. Currency is shown in CAD with no decimal places on card visuals.
