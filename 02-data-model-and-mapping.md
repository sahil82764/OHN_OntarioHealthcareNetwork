# Data Model and Source-to-Target Mapping

---

## 1. Source-system overview

| Source | System | Entities | Interface | Load type |
|--------|--------|----------|-----------|-----------|
| `EHR` | Hospital EHR / ADT | patient, encounter, admission, discharge, bed_assignment, diagnosis, procedure | SQL Server | Incremental |
| `SCHED` | Appointment scheduling | appointment, appointment_status_history, clinic_slot | Azure SQL + change tracking | Incremental |
| `LIS` | Laboratory information system | lab_order, lab_result, specimen | HL7 v2 / flat file | Incremental |
| `PHARM` | Pharmacy | medication_order, dispense, formulary | REST API | Incremental |
| `CLAIMS` | Insurance platform | claim_header, claim_line, adjudication, payer | EDI 837/835 over SFTP | Daily |
| `FIN` | Billing / ERP | invoice, invoice_line, payment, adjustment | Oracle | Daily |
| `HR` | Staffing | doctor, staff, department_assignment, credential | CSV export | Daily snapshot |
| `SURVEY` | Patient satisfaction | survey_response, survey_question | SaaS API | Daily |
| `FACIL` | Facility management | hospital, department, ward, room, bed | CSV export | Daily snapshot |
| `REF` | Reference vocabularies | ICD-10-CA, LOINC, DIN/ATC, CCI | Manual upload | Quarterly |

---

## 2. Dimensional model

### 2.1 Dimensions

| Table | Grain | SCD | Key business attributes |
|-------|-------|-----|-------------------------|
| `dim_date` | One calendar day | n/a | date, fiscal period (Apr–Mar), day of week, is_weekend, is_holiday_on |
| `dim_time` | One minute of day | n/a | hour, minute, shift (day/evening/night) |
| `dim_patient` | Golden patient, versioned | Type 2 | golden_id, patient_token, age_band, sex, fsa, language, is_deceased |
| `dim_doctor` | Doctor, versioned | Type 2 | doctor_id, specialty, credential, primary_department, employment_type, fte |
| `dim_department` | Department, versioned | Type 2 | department_id, name, service_line, hospital_key, cost_centre |
| `dim_hospital` | Facility, versioned | Type 2 | hospital_id, name, region, LHIN, facility_type, licensed_beds |
| `dim_bed` | Bed, versioned | Type 2 | bed_id, room, ward, ward_type, bed_type, is_isolation_capable |
| `dim_diagnosis` | ICD-10-CA code | Type 1 | code, description, chapter, category, is_chronic, ccs_group |
| `dim_procedure` | CCI code | Type 1 | code, description, category |
| `dim_lab_test` | LOINC code | Type 1 | loinc_code, test_name, panel, specimen_type, unit, ref_low, ref_high |
| `dim_medication` | DIN | Type 1 | din, generic_name, brand_name, atc_class, form, is_controlled, is_high_alert |
| `dim_insurance_provider` | Payer | Type 2 | payer_id, name, payer_type (OHIP/private/self-pay), plan_tier |
| `dim_admission_type` | Admission type | Type 1 | code, description, is_emergency, is_elective |
| `dim_appointment_status` | Status | Type 1 | code, description, is_cancellation, is_no_show, is_completed |
| `dim_claim_status` | Status | Type 1 | code, description, is_terminal, is_approved, is_denied |
| `dim_discharge_disposition` | Disposition | Type 1 | code, description, is_home, is_transfer, is_expired, is_ama |
| `dim_dq_rule` | Rule | Type 1 | rule_id, dimension, target_table, severity, expression, threshold |

Every dimension carries: surrogate key `*_key` (BIGINT identity), business key, `effective_from_ts`, `effective_to_ts`, `is_current`, `row_hash`, `created_ts`, `updated_ts`. Every dimension is seeded with members `-1` unknown, `-2` not applicable, `-3` late arriving.

### 2.2 Facts

| Table | Grain | Type | Measures |
|-------|-------|------|----------|
| `fact_appointment` | One scheduled appointment | Transaction | scheduled_duration_min, actual_duration_min, wait_from_booking_days, lead_time_days, is_cancelled, is_no_show, is_completed, cancellation_notice_hours |
| `fact_admission` | One inpatient admission | Accumulating snapshot | length_of_stay_days, length_of_stay_hours, icu_days, is_readmission_30d, days_since_prior_discharge, transfer_count, total_charges |
| `fact_emergency_visit` | One ED visit | Accumulating snapshot | triage_wait_min, physician_initial_assessment_min, decision_to_admit_min, total_ed_los_min, ctas_level, left_without_being_seen |
| `fact_bed_occupancy_daily` | One bed × one day | Periodic snapshot | occupied_hours, available_hours, blocked_hours, is_occupied_at_midnight, turnover_count |
| `fact_lab_result` | One resulted test | Transaction | result_value_numeric, is_abnormal, is_critical, order_to_collect_min, collect_to_result_min, total_turnaround_min |
| `fact_medication_order` | One medication order line | Transaction | quantity_ordered, quantity_dispensed, days_supply, unit_cost, total_cost, is_high_alert, order_to_dispense_min |
| `fact_claim` | One claim | Accumulating snapshot | billed_amount, allowed_amount, approved_amount, paid_amount, patient_responsibility, denied_amount, days_to_adjudicate, is_approved, is_denied, resubmission_count |
| `fact_billing_line` | One invoice line | Transaction | charge_amount, discount_amount, tax_amount, net_amount, payment_amount, outstanding_amount |
| `fact_satisfaction_survey` | One survey response | Transaction | overall_score, wait_time_score, staff_courtesy_score, cleanliness_score, communication_score, would_recommend_flag, nps_category |
| `fact_dq_result` | One rule × table × batch | Transaction | rows_evaluated, rows_passed, rows_failed, pass_rate |
| `fact_pipeline_run` | One pipeline activity run | Transaction | duration_sec, rows_read, rows_written, is_success |
| `bridge_encounter_diagnosis` | Encounter × diagnosis | Bridge | diagnosis_rank, is_primary, diagnosis_type (admitting/discharge/comorbid) |

### 2.3 Fact-to-dimension conformance

| Dimension | appt | adm | ed | bed | lab | med | claim | bill | survey |
|-----------|:----:|:---:|:--:|:---:|:---:|:---:|:-----:|:----:|:------:|
| dim_date | ● | ● | ● | ● | ● | ● | ● | ● | ● |
| dim_time | ● | ● | ● | | ● | ● | | | |
| dim_patient | ● | ● | ● | ● | ● | ● | ● | ● | ● |
| dim_doctor | ● | ● | ● | | ● | ● | ● | ● | ● |
| dim_department | ● | ● | ● | ● | ● | ● | ● | ● | ● |
| dim_hospital | ● | ● | ● | ● | ● | ● | ● | ● | ● |
| dim_bed | | ● | ● | ● | | | | | |
| dim_diagnosis | | ● | ● | | | | ● | | |
| dim_lab_test | | | | | ● | | | | |
| dim_medication | | | | | | ● | | | |
| dim_insurance_provider | | ● | ● | | | | ● | ● | |
| dim_admission_type | | ● | ● | | | | | | |
| dim_appointment_status | ● | | | | | | | | |
| dim_claim_status | | | | | | | ● | ● | |
| dim_discharge_disposition | | ● | ● | | | | | | |

Role-playing dates are implemented as separate keys against the single `dim_date` (e.g. `admission_date_key`, `discharge_date_key`, `expected_discharge_date_key`) with inactive relationships activated by `USERELATIONSHIP` in measures.

---

## 3. Source-to-target mapping

### 3.1 `dim_patient` (SCD Type 2)

| Target column | Type | Source | Transformation |
|---|---|---|---|
| patient_key | BIGINT | — | Identity surrogate key |
| patient_golden_id | VARCHAR(36) | MPI | Output of `silver.patient_xref` survivorship |
| source_patient_id | VARCHAR(50) | EHR.patient.patient_id | Surviving source record id |
| patient_token | CHAR(64) | EHR.patient.health_card_number | `sha2(upper(trim(hcn)) \|\| salt, 256)`; clear value never lands in Gold |
| birth_year | INT | EHR.patient.date_of_birth | `year(dob)` |
| age_band | VARCHAR(10) | derived | 0-17, 18-34, 35-49, 50-64, 65-74, 75-84, 85+ |
| sex | VARCHAR(20) | EHR.patient.gender | Mapped via `ref_code_mapping` domain `SEX` to M/F/X/Unknown |
| fsa | CHAR(3) | EHR.patient.postal_code | `left(upper(replace(postal,' ','')),3)`; null if invalid |
| preferred_language | VARCHAR(30) | EHR.patient.language | ISO 639-1 to display name |
| is_deceased | BOOLEAN | EHR.patient.deceased_indicator | Y/1/TRUE → true |
| source_record_count | INT | MPI | Count of source records merged into the golden record |
| match_confidence | DECIMAL(5,2) | MPI | Minimum pairwise score within the cluster |
| effective_from_ts / effective_to_ts / is_current | — | derived | SCD2 mechanics; `effective_to_ts` = `9999-12-31` when current |

Type 2 change is triggered by a change in: sex, fsa, preferred_language, is_deceased, age_band. Changes to `match_confidence` or `source_record_count` update in place (Type 1 attributes on a Type 2 dimension).

### 3.2 `fact_admission`

| Target column | Source | Transformation |
|---|---|---|
| admission_id | EHR.admission.admission_id | Degenerate dimension, business key for merge |
| patient_key | via `silver.patient_xref` | Lookup `dim_patient` where `is_current` and admission_ts between effective dates |
| hospital_key / department_key / bed_key | EHR.admission | Lookup, `-1` if unresolved |
| attending_doctor_key | EHR.admission.attending_physician_id | Lookup `dim_doctor` |
| admission_date_key / admission_time_key | EHR.admission.admission_datetime | `date_format(ts,'yyyyMMdd')`, `hour*60+minute` |
| discharge_date_key | EHR.admission.discharge_datetime | `-1` while admission is open |
| admission_type_key | EHR.admission.admission_type | Mapped via `ref_code_mapping` domain `ADMIT_TYPE` |
| discharge_disposition_key | EHR.discharge.disposition_code | Domain `DISPOSITION` |
| primary_diagnosis_key | EHR.diagnosis where `is_primary` | If multiple, lowest `diagnosis_rank` wins |
| length_of_stay_days | derived | `datediff(discharge_dt, admission_dt)`; same-day stay counts as 1; null while open |
| length_of_stay_hours | derived | `(unix(discharge)-unix(admission))/3600.0` |
| icu_days | EHR.bed_assignment | Sum of days in beds where `ward_type='ICU'` |
| is_readmission_30d | derived | True when a prior discharge for the same `patient_golden_id` occurred within 30 days and the prior discharge was not a transfer and not `is_expired` |
| days_since_prior_discharge | derived | Window: `datediff(admission_dt, lag(discharge_dt) over (partition by golden_id order by admission_dt))` |
| transfer_count | EHR.bed_assignment | `count(*) - 1` per admission |
| total_charges | FIN.invoice_line | Sum of `net_amount` joined on encounter id; `0` if unbilled |
| is_open | derived | `discharge_datetime is null` |

**Readmission definition note.** The 30-day window is measured discharge-to-admission, excludes planned readmissions flagged by `admission_type='ELECTIVE'`, excludes transfers between OHN facilities, and excludes patients who died on the index admission. This definition is configurable in `config/business_rules.yaml`.

### 3.3 `fact_emergency_visit`

| Target column | Source | Transformation |
|---|---|---|
| ed_visit_id | EHR.encounter.encounter_id where `encounter_type='ED'` | Business key |
| arrival_ts / triage_ts / physician_seen_ts / disposition_ts / departure_ts | EHR.encounter milestones | Cast to timestamp; null-safe |
| ctas_level | EHR.encounter.triage_score | 1–5; out-of-range → null and DQ warning |
| triage_wait_min | derived | `(triage_ts - arrival_ts)/60`, floored at 0 |
| physician_initial_assessment_min | derived | `(physician_seen_ts - arrival_ts)/60` — the PIA metric reported to the ministry |
| decision_to_admit_min | derived | `(admit_decision_ts - physician_seen_ts)/60` |
| boarding_min | derived | `(bed_assigned_ts - admit_decision_ts)/60`, null if not admitted |
| total_ed_los_min | derived | `(departure_ts - arrival_ts)/60` |
| left_without_being_seen | derived | `physician_seen_ts is null and departure_ts is not null` |
| resulted_in_admission | derived | Exists matching row in `fact_admission` |

Negative intervals (clock-sequence errors at source) are set to null and logged as DQ rule `DQ-ED-002` rather than clamped, so wait-time averages are not silently biased.

### 3.4 `fact_claim`

| Target column | Source | Transformation |
|---|---|---|
| claim_id | CLAIMS.claim_header.claim_number | Business key |
| submission_date_key / adjudication_date_key / payment_date_key | CLAIMS milestones | Role-playing dates |
| insurance_provider_key | CLAIMS.claim_header.payer_id | Lookup |
| claim_status_key | CLAIMS.adjudication.status_code | Domain `CLAIM_STATUS`: SUBMITTED, PENDING, APPROVED, PARTIAL, DENIED, APPEALED, PAID, VOID |
| billed_amount / allowed_amount / approved_amount / paid_amount | CLAIMS + 835 remittance | `cast(... as decimal(18,2))`, sum of lines |
| denied_amount | derived | `billed_amount - approved_amount` when status in (DENIED, PARTIAL) else 0 |
| denial_reason_code | 835 CARC code | Mapped to grouped reason via `ref_code_mapping` domain `DENIAL_REASON` |
| days_to_adjudicate | derived | `datediff(adjudication_dt, submission_dt)` |
| resubmission_count | CLAIMS.claim_header.original_claim_number | Count of claims chained to the original |
| is_approved / is_denied | derived | From terminal status only, so pending claims do not depress the approval rate |

Approval rate is defined as approved claims ÷ adjudicated claims (terminal status), not ÷ all submitted claims.

### 3.5 `fact_bed_occupancy_daily`

Built by cross-joining `dim_bed` (current rows) with a rolling 90-day `dim_date` window and left-joining bed assignments, so unoccupied beds produce rows with zero occupancy rather than disappearing.

| Target column | Transformation |
|---|---|
| occupied_hours | Overlap in hours between each assignment interval and the calendar day, summed |
| available_hours | `24 - occupied_hours - blocked_hours` |
| blocked_hours | From `FACIL.bed_status` where status in (MAINTENANCE, INFECTION_CONTROL, STAFFING) |
| is_occupied_at_midnight | Assignment interval spans 23:59:59 of the day — the census-count basis |
| turnover_count | Count of assignment end events on that day |

Occupancy rate = `sum(occupied_hours) / sum(occupied_hours + available_hours)`, which correctly excludes blocked beds from the denominator.

### 3.6 `fact_appointment`

| Target column | Transformation |
|---|---|
| appointment_status_key | Final status from `SCHED.appointment_status_history`, latest by `status_ts` |
| is_no_show | Final status = `NO_SHOW` |
| is_cancelled | Final status in (`CANCELLED_PATIENT`, `CANCELLED_PROVIDER`, `CANCELLED_FACILITY`) |
| cancelled_by | Derived from which cancellation status was set |
| cancellation_notice_hours | `(scheduled_start_ts - cancellation_ts)/3600`; negative means cancelled after start time |
| lead_time_days | `datediff(scheduled_date, booking_date)` |
| actual_duration_min | `(checkout_ts - checkin_ts)/60`, null when not completed |
| wait_in_clinic_min | `(seen_ts - checkin_ts)/60` |

No-show rate = no-shows ÷ (completed + no-shows), excluding cancellations from the denominator. Cancellation rate is reported separately against all scheduled appointments.

### 3.7 Reference code standardization

All coded values pass through `config/ref_code_mapping.csv`:

```
domain,source_system,source_code,standard_code,standard_description,is_active
SEX,EHR,1,M,Male,true
SEX,EHR,2,F,Female,true
SEX,SCHED,MALE,M,Male,true
ADMIT_TYPE,EHR,E,EMERGENCY,Emergency,true
ADMIT_TYPE,EHR,U,URGENT,Urgent,true
CLAIM_STATUS,CLAIMS,A1,APPROVED,Approved in full,true
```

Unmapped source codes are routed to the unknown member and written to `governance.unmapped_codes` so stewards can extend the mapping without a code change.
