/* =====================================================================
   OHN Analytics Warehouse — Gold schema
   Target: Microsoft Fabric Warehouse (wh_ohn_analytics)
   Notes:  Fabric Warehouse T-SQL does not support PRIMARY KEY/FOREIGN KEY
           enforcement; constraints below are declared NOT ENFORCED to give
           the semantic model and query optimiser relationship metadata.
   ===================================================================== */

CREATE SCHEMA gold;
GO
CREATE SCHEMA sec;
GO
CREATE SCHEMA governance;
GO

/* ---------------------------------------------------------------- DATE */
CREATE TABLE gold.dim_date (
    date_key                INT             NOT NULL,
    calendar_date           DATE            NOT NULL,
    day_of_week             SMALLINT        NOT NULL,
    day_name                VARCHAR(10)     NOT NULL,
    is_weekend              BIT             NOT NULL,
    day_of_month            SMALLINT        NOT NULL,
    month_number            SMALLINT        NOT NULL,
    month_name              VARCHAR(12)     NOT NULL,
    month_year_label        VARCHAR(10)     NOT NULL,
    quarter_number          SMALLINT        NOT NULL,
    calendar_year           SMALLINT        NOT NULL,
    fiscal_year             SMALLINT        NOT NULL,   -- Apr 1 - Mar 31
    fiscal_quarter          SMALLINT        NOT NULL,
    fiscal_period           SMALLINT        NOT NULL,
    is_holiday              BIT             NOT NULL,
    holiday_name            VARCHAR(50)     NULL,
    week_start_date         DATE            NOT NULL,
    relative_day_offset     INT             NOT NULL
);
GO

CREATE TABLE gold.dim_time (
    time_key                INT             NOT NULL,   -- hour*60 + minute
    hour_24                 SMALLINT        NOT NULL,
    minute_of_hour          SMALLINT        NOT NULL,
    time_label              CHAR(5)         NOT NULL,
    hour_band               VARCHAR(12)     NOT NULL,
    shift_name              VARCHAR(10)     NOT NULL    -- Day / Evening / Night
);
GO

/* ------------------------------------------------------------- PATIENT */
CREATE TABLE gold.dim_patient (
    patient_key             BIGINT          NOT NULL,
    patient_golden_id       VARCHAR(36)     NOT NULL,
    source_patient_id       VARCHAR(50)     NULL,
    patient_token           CHAR(64)        NOT NULL,   -- SHA-256 of HCN
    birth_year              INT             NULL,
    age_band                VARCHAR(10)     NULL,
    sex                     VARCHAR(20)     NULL,
    fsa                     CHAR(3)         NULL,
    preferred_language      VARCHAR(30)     NULL,
    is_deceased             BIT             NOT NULL,
    source_record_count     INT             NOT NULL,
    match_confidence        DECIMAL(5,2)    NULL,
    effective_from_ts       DATETIME2(3)    NOT NULL,
    effective_to_ts         DATETIME2(3)    NOT NULL,
    is_current              BIT             NOT NULL,
    row_hash                CHAR(64)        NOT NULL,
    created_ts              DATETIME2(3)    NOT NULL,
    updated_ts              DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.dim_doctor (
    doctor_key              BIGINT          NOT NULL,
    doctor_id               VARCHAR(50)     NOT NULL,
    doctor_display_name     VARCHAR(120)    NOT NULL,
    specialty               VARCHAR(60)     NULL,
    sub_specialty           VARCHAR(60)     NULL,
    credential              VARCHAR(30)     NULL,
    primary_department_id   VARCHAR(50)     NULL,
    employment_type         VARCHAR(30)     NULL,       -- Staff / Locum / Resident
    fte                     DECIMAL(4,2)    NULL,
    hire_date               DATE            NULL,
    is_active               BIT             NOT NULL,
    effective_from_ts       DATETIME2(3)    NOT NULL,
    effective_to_ts         DATETIME2(3)    NOT NULL,
    is_current              BIT             NOT NULL,
    row_hash                CHAR(64)        NOT NULL
);
GO

CREATE TABLE gold.dim_hospital (
    hospital_key            BIGINT          NOT NULL,
    hospital_id             VARCHAR(50)     NOT NULL,
    hospital_name           VARCHAR(120)    NOT NULL,
    facility_type           VARCHAR(40)     NULL,       -- Acute / Community / Rehab / Clinic
    region                  VARCHAR(60)     NULL,
    health_region_code      VARCHAR(20)     NULL,
    city                    VARCHAR(60)     NULL,
    province                CHAR(2)         NULL,
    licensed_beds           INT             NULL,
    has_emergency_dept      BIT             NOT NULL,
    is_teaching_hospital    BIT             NOT NULL,
    effective_from_ts       DATETIME2(3)    NOT NULL,
    effective_to_ts         DATETIME2(3)    NOT NULL,
    is_current              BIT             NOT NULL,
    row_hash                CHAR(64)        NOT NULL
);
GO

CREATE TABLE gold.dim_department (
    department_key          BIGINT          NOT NULL,
    department_id           VARCHAR(50)     NOT NULL,
    department_name         VARCHAR(120)    NOT NULL,
    service_line            VARCHAR(60)     NULL,
    hospital_key            BIGINT          NOT NULL,
    cost_centre             VARCHAR(30)     NULL,
    is_clinical             BIT             NOT NULL,
    is_inpatient_unit       BIT             NOT NULL,
    effective_from_ts       DATETIME2(3)    NOT NULL,
    effective_to_ts         DATETIME2(3)    NOT NULL,
    is_current              BIT             NOT NULL,
    row_hash                CHAR(64)        NOT NULL
);
GO

CREATE TABLE gold.dim_bed (
    bed_key                 BIGINT          NOT NULL,
    bed_id                  VARCHAR(50)     NOT NULL,
    room_number             VARCHAR(20)     NULL,
    ward_name               VARCHAR(60)     NULL,
    ward_type               VARCHAR(40)     NULL,       -- ICU / Med-Surg / Maternity / ED / Rehab
    bed_type                VARCHAR(40)     NULL,
    hospital_key            BIGINT          NOT NULL,
    department_key          BIGINT          NOT NULL,
    is_isolation_capable    BIT             NOT NULL,
    is_active               BIT             NOT NULL,
    effective_from_ts       DATETIME2(3)    NOT NULL,
    effective_to_ts         DATETIME2(3)    NOT NULL,
    is_current              BIT             NOT NULL,
    row_hash                CHAR(64)        NOT NULL
);
GO

/* --------------------------------------------------- CLINICAL REFERENCE */
CREATE TABLE gold.dim_diagnosis (
    diagnosis_key           BIGINT          NOT NULL,
    diagnosis_code          VARCHAR(20)     NOT NULL,   -- ICD-10-CA
    diagnosis_description   VARCHAR(400)    NULL,
    chapter                 VARCHAR(120)    NULL,
    category                VARCHAR(120)    NULL,
    body_system             VARCHAR(60)     NULL,
    is_chronic              BIT             NOT NULL,
    is_notifiable           BIT             NOT NULL,
    row_hash                CHAR(64)        NOT NULL
);
GO

CREATE TABLE gold.dim_lab_test (
    lab_test_key            BIGINT          NOT NULL,
    loinc_code              VARCHAR(20)     NOT NULL,
    test_name               VARCHAR(200)    NOT NULL,
    panel_name              VARCHAR(120)    NULL,
    specimen_type           VARCHAR(60)     NULL,
    result_unit             VARCHAR(30)     NULL,
    reference_low           DECIMAL(18,4)   NULL,
    reference_high          DECIMAL(18,4)   NULL,
    is_stat_capable         BIT             NOT NULL,
    row_hash                CHAR(64)        NOT NULL
);
GO

CREATE TABLE gold.dim_medication (
    medication_key          BIGINT          NOT NULL,
    din                     VARCHAR(20)     NOT NULL,
    generic_name            VARCHAR(200)    NOT NULL,
    brand_name              VARCHAR(200)    NULL,
    atc_code                VARCHAR(20)     NULL,
    atc_class               VARCHAR(120)    NULL,
    dosage_form             VARCHAR(60)     NULL,
    route                   VARCHAR(40)     NULL,
    is_controlled_substance BIT             NOT NULL,
    is_high_alert           BIT             NOT NULL,
    is_formulary            BIT             NOT NULL,
    row_hash                CHAR(64)        NOT NULL
);
GO

CREATE TABLE gold.dim_insurance_provider (
    insurance_provider_key  BIGINT          NOT NULL,
    payer_id                VARCHAR(50)     NOT NULL,
    payer_name              VARCHAR(120)    NOT NULL,
    payer_type              VARCHAR(40)     NOT NULL,   -- Public / Private / Self-pay / WSIB
    plan_tier               VARCHAR(40)     NULL,
    effective_from_ts       DATETIME2(3)    NOT NULL,
    effective_to_ts         DATETIME2(3)    NOT NULL,
    is_current              BIT             NOT NULL,
    row_hash                CHAR(64)        NOT NULL
);
GO

/* ------------------------------------------------------ SMALL DIMENSIONS */
CREATE TABLE gold.dim_admission_type (
    admission_type_key      BIGINT      NOT NULL,
    admission_type_code     VARCHAR(20) NOT NULL,
    admission_type_desc     VARCHAR(60) NOT NULL,
    is_emergency            BIT         NOT NULL,
    is_elective             BIT         NOT NULL
);
GO

CREATE TABLE gold.dim_appointment_status (
    appointment_status_key  BIGINT      NOT NULL,
    status_code             VARCHAR(30) NOT NULL,
    status_desc             VARCHAR(60) NOT NULL,
    status_group            VARCHAR(30) NOT NULL,
    is_completed            BIT         NOT NULL,
    is_cancellation         BIT         NOT NULL,
    is_no_show              BIT         NOT NULL
);
GO

CREATE TABLE gold.dim_claim_status (
    claim_status_key        BIGINT      NOT NULL,
    status_code             VARCHAR(30) NOT NULL,
    status_desc             VARCHAR(60) NOT NULL,
    is_terminal             BIT         NOT NULL,
    is_approved             BIT         NOT NULL,
    is_denied               BIT         NOT NULL
);
GO

CREATE TABLE gold.dim_discharge_disposition (
    discharge_disposition_key BIGINT     NOT NULL,
    disposition_code        VARCHAR(20) NOT NULL,
    disposition_desc        VARCHAR(80) NOT NULL,
    is_home                 BIT         NOT NULL,
    is_transfer             BIT         NOT NULL,
    is_expired              BIT         NOT NULL,
    is_against_medical_advice BIT       NOT NULL
);
GO

/* ================================== FACTS ================================ */

CREATE TABLE gold.fact_appointment (
    appointment_key             BIGINT          NOT NULL,
    appointment_id              VARCHAR(50)     NOT NULL,
    patient_key                 BIGINT          NOT NULL,
    doctor_key                  BIGINT          NOT NULL,
    department_key              BIGINT          NOT NULL,
    hospital_key                BIGINT          NOT NULL,
    appointment_status_key      BIGINT          NOT NULL,
    booking_date_key            INT             NOT NULL,
    scheduled_date_key          INT             NOT NULL,
    scheduled_time_key          INT             NOT NULL,
    appointment_type            VARCHAR(40)     NULL,
    is_first_visit              BIT             NOT NULL,
    is_virtual                  BIT             NOT NULL,
    scheduled_duration_min      INT             NULL,
    actual_duration_min         INT             NULL,
    wait_in_clinic_min          INT             NULL,
    lead_time_days              INT             NULL,
    cancellation_notice_hours   DECIMAL(10,2)   NULL,
    cancelled_by                VARCHAR(20)     NULL,
    is_completed                BIT             NOT NULL,
    is_cancelled                BIT             NOT NULL,
    is_no_show                  BIT             NOT NULL,
    batch_id                    VARCHAR(50)     NOT NULL,
    loaded_ts                   DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.fact_admission (
    admission_key               BIGINT          NOT NULL,
    admission_id                VARCHAR(50)     NOT NULL,
    encounter_id                VARCHAR(50)     NULL,
    patient_key                 BIGINT          NOT NULL,
    attending_doctor_key        BIGINT          NOT NULL,
    department_key              BIGINT          NOT NULL,
    hospital_key                BIGINT          NOT NULL,
    bed_key                     BIGINT          NOT NULL,
    admission_type_key          BIGINT          NOT NULL,
    discharge_disposition_key   BIGINT          NOT NULL,
    primary_diagnosis_key       BIGINT          NOT NULL,
    insurance_provider_key      BIGINT          NOT NULL,
    admission_date_key          INT             NOT NULL,
    admission_time_key          INT             NOT NULL,
    discharge_date_key          INT             NOT NULL,
    expected_discharge_date_key INT             NOT NULL,
    length_of_stay_days         INT             NULL,
    length_of_stay_hours        DECIMAL(12,2)   NULL,
    icu_days                    DECIMAL(8,2)    NULL,
    transfer_count              INT             NOT NULL,
    days_since_prior_discharge  INT             NULL,
    total_charges               DECIMAL(18,2)   NULL,
    is_readmission_30d          BIT             NOT NULL,
    is_index_admission          BIT             NOT NULL,
    is_open                     BIT             NOT NULL,
    batch_id                    VARCHAR(50)     NOT NULL,
    loaded_ts                   DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.fact_emergency_visit (
    ed_visit_key                    BIGINT          NOT NULL,
    ed_visit_id                     VARCHAR(50)     NOT NULL,
    patient_key                     BIGINT          NOT NULL,
    hospital_key                    BIGINT          NOT NULL,
    department_key                  BIGINT          NOT NULL,
    triage_diagnosis_key            BIGINT          NOT NULL,
    arrival_date_key                INT             NOT NULL,
    arrival_time_key                INT             NOT NULL,
    departure_date_key              INT             NOT NULL,
    ctas_level                      SMALLINT        NULL,
    arrival_mode                    VARCHAR(30)     NULL,   -- Ambulance / Walk-in / Transfer
    triage_wait_min                 DECIMAL(10,2)   NULL,
    physician_initial_assessment_min DECIMAL(10,2)  NULL,
    decision_to_admit_min           DECIMAL(10,2)   NULL,
    boarding_min                    DECIMAL(10,2)   NULL,
    total_ed_los_min                DECIMAL(10,2)   NULL,
    left_without_being_seen         BIT             NOT NULL,
    resulted_in_admission           BIT             NOT NULL,
    batch_id                        VARCHAR(50)     NOT NULL,
    loaded_ts                       DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.fact_bed_occupancy_daily (
    bed_occupancy_key       BIGINT          NOT NULL,
    date_key                INT             NOT NULL,
    bed_key                 BIGINT          NOT NULL,
    hospital_key            BIGINT          NOT NULL,
    department_key          BIGINT          NOT NULL,
    occupied_hours          DECIMAL(6,2)    NOT NULL,
    available_hours         DECIMAL(6,2)    NOT NULL,
    blocked_hours           DECIMAL(6,2)    NOT NULL,
    is_occupied_at_midnight BIT             NOT NULL,
    turnover_count          INT             NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL,
    loaded_ts               DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.fact_lab_result (
    lab_result_key          BIGINT          NOT NULL,
    lab_result_id           VARCHAR(50)     NOT NULL,
    patient_key             BIGINT          NOT NULL,
    ordering_doctor_key     BIGINT          NOT NULL,
    department_key          BIGINT          NOT NULL,
    hospital_key            BIGINT          NOT NULL,
    lab_test_key            BIGINT          NOT NULL,
    order_date_key          INT             NOT NULL,
    order_time_key          INT             NOT NULL,
    result_date_key         INT             NOT NULL,
    encounter_id            VARCHAR(50)     NULL,
    priority                VARCHAR(20)     NULL,       -- Routine / STAT
    result_value_numeric    DECIMAL(18,4)   NULL,
    result_value_text       VARCHAR(200)    NULL,
    abnormal_flag           VARCHAR(10)     NULL,       -- H / L / HH / LL / N
    is_abnormal             BIT             NOT NULL,
    is_critical             BIT             NOT NULL,
    order_to_collect_min    DECIMAL(10,2)   NULL,
    collect_to_result_min   DECIMAL(10,2)   NULL,
    total_turnaround_min    DECIMAL(10,2)   NULL,
    batch_id                VARCHAR(50)     NOT NULL,
    loaded_ts               DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.fact_medication_order (
    medication_order_key    BIGINT          NOT NULL,
    medication_order_id     VARCHAR(50)     NOT NULL,
    patient_key             BIGINT          NOT NULL,
    prescribing_doctor_key  BIGINT          NOT NULL,
    department_key          BIGINT          NOT NULL,
    hospital_key            BIGINT          NOT NULL,
    medication_key          BIGINT          NOT NULL,
    order_date_key          INT             NOT NULL,
    order_time_key          INT             NOT NULL,
    dispense_date_key       INT             NOT NULL,
    encounter_id            VARCHAR(50)     NULL,
    dose_amount             DECIMAL(12,4)   NULL,
    dose_unit               VARCHAR(20)     NULL,
    frequency_code          VARCHAR(20)     NULL,
    quantity_ordered        DECIMAL(12,2)   NULL,
    quantity_dispensed      DECIMAL(12,2)   NULL,
    days_supply             INT             NULL,
    unit_cost               DECIMAL(12,4)   NULL,
    total_cost              DECIMAL(18,2)   NULL,
    order_to_dispense_min   DECIMAL(10,2)   NULL,
    is_high_alert           BIT             NOT NULL,
    is_discontinued         BIT             NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL,
    loaded_ts               DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.fact_claim (
    claim_key                   BIGINT          NOT NULL,
    claim_id                    VARCHAR(50)     NOT NULL,
    patient_key                 BIGINT          NOT NULL,
    provider_doctor_key         BIGINT          NOT NULL,
    department_key              BIGINT          NOT NULL,
    hospital_key                BIGINT          NOT NULL,
    insurance_provider_key      BIGINT          NOT NULL,
    claim_status_key            BIGINT          NOT NULL,
    primary_diagnosis_key       BIGINT          NOT NULL,
    service_date_key            INT             NOT NULL,
    submission_date_key         INT             NOT NULL,
    adjudication_date_key       INT             NOT NULL,
    payment_date_key            INT             NOT NULL,
    encounter_id                VARCHAR(50)     NULL,
    billed_amount               DECIMAL(18,2)   NOT NULL,
    allowed_amount              DECIMAL(18,2)   NULL,
    approved_amount             DECIMAL(18,2)   NULL,
    paid_amount                 DECIMAL(18,2)   NULL,
    denied_amount               DECIMAL(18,2)   NULL,
    patient_responsibility      DECIMAL(18,2)   NULL,
    denial_reason_code          VARCHAR(20)     NULL,
    denial_reason_group         VARCHAR(60)     NULL,
    days_to_adjudicate          INT             NULL,
    days_to_payment             INT             NULL,
    resubmission_count          INT             NOT NULL,
    is_adjudicated              BIT             NOT NULL,
    is_approved                 BIT             NOT NULL,
    is_denied                   BIT             NOT NULL,
    batch_id                    VARCHAR(50)     NOT NULL,
    loaded_ts                   DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.fact_billing_line (
    billing_line_key        BIGINT          NOT NULL,
    invoice_id              VARCHAR(50)     NOT NULL,
    invoice_line_number     INT             NOT NULL,
    patient_key             BIGINT          NOT NULL,
    department_key          BIGINT          NOT NULL,
    hospital_key            BIGINT          NOT NULL,
    insurance_provider_key  BIGINT          NOT NULL,
    invoice_date_key        INT             NOT NULL,
    service_date_key        INT             NOT NULL,
    encounter_id            VARCHAR(50)     NULL,
    service_code            VARCHAR(30)     NULL,
    service_category        VARCHAR(60)     NULL,
    quantity                DECIMAL(12,2)   NOT NULL,
    charge_amount           DECIMAL(18,2)   NOT NULL,
    discount_amount         DECIMAL(18,2)   NOT NULL,
    tax_amount              DECIMAL(18,2)   NOT NULL,
    net_amount              DECIMAL(18,2)   NOT NULL,
    payment_amount          DECIMAL(18,2)   NOT NULL,
    outstanding_amount      DECIMAL(18,2)   NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL,
    loaded_ts               DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.fact_satisfaction_survey (
    survey_key              BIGINT          NOT NULL,
    survey_response_id      VARCHAR(50)     NOT NULL,
    patient_key             BIGINT          NOT NULL,
    doctor_key              BIGINT          NOT NULL,
    department_key          BIGINT          NOT NULL,
    hospital_key            BIGINT          NOT NULL,
    response_date_key       INT             NOT NULL,
    service_date_key        INT             NOT NULL,
    encounter_type          VARCHAR(30)     NULL,
    overall_score           SMALLINT        NULL,
    wait_time_score         SMALLINT        NULL,
    staff_courtesy_score    SMALLINT        NULL,
    cleanliness_score       SMALLINT        NULL,
    communication_score     SMALLINT        NULL,
    pain_management_score   SMALLINT        NULL,
    would_recommend_score   SMALLINT        NULL,
    nps_category            VARCHAR(12)     NULL,       -- Promoter / Passive / Detractor
    has_free_text_comment   BIT             NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL,
    loaded_ts               DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE gold.bridge_encounter_diagnosis (
    encounter_id            VARCHAR(50)     NOT NULL,
    diagnosis_key           BIGINT          NOT NULL,
    patient_key             BIGINT          NOT NULL,
    diagnosis_date_key      INT             NOT NULL,
    diagnosis_rank          SMALLINT        NOT NULL,
    diagnosis_type          VARCHAR(20)     NOT NULL,   -- Admitting / Discharge / Comorbid
    is_primary              BIT             NOT NULL,
    is_present_on_admission BIT             NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL
);
GO

/* ------------------------------------------------------------ GOVERNANCE */
CREATE TABLE governance.dim_dq_rule (
    dq_rule_key             BIGINT          NOT NULL,
    rule_id                 VARCHAR(30)     NOT NULL,
    rule_name               VARCHAR(200)    NOT NULL,
    dq_dimension            VARCHAR(30)     NOT NULL,
    layer                   VARCHAR(10)     NOT NULL,
    target_table            VARCHAR(120)    NOT NULL,
    target_column           VARCHAR(120)    NULL,
    rule_expression         VARCHAR(2000)   NOT NULL,
    severity                VARCHAR(10)     NOT NULL,   -- Error / Warning / Info
    pass_threshold_pct      DECIMAL(5,2)    NOT NULL,
    failure_action          VARCHAR(20)     NOT NULL,   -- Quarantine / FailBatch / Flag
    is_active               BIT             NOT NULL
);
GO

CREATE TABLE governance.fact_dq_result (
    dq_result_key           BIGINT          NOT NULL,
    dq_rule_key             BIGINT          NOT NULL,
    run_date_key            INT             NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL,
    target_table            VARCHAR(120)    NOT NULL,
    rows_evaluated          BIGINT          NOT NULL,
    rows_passed             BIGINT          NOT NULL,
    rows_failed             BIGINT          NOT NULL,
    pass_rate_pct           DECIMAL(6,3)    NOT NULL,
    is_breach               BIT             NOT NULL,
    run_ts                  DATETIME2(3)    NOT NULL
);
GO

CREATE TABLE governance.fact_pipeline_run (
    pipeline_run_key        BIGINT          NOT NULL,
    run_id                  VARCHAR(80)     NOT NULL,
    pipeline_name           VARCHAR(120)    NOT NULL,
    activity_name           VARCHAR(120)    NULL,
    source_system           VARCHAR(30)     NULL,
    target_table            VARCHAR(120)    NULL,
    run_date_key            INT             NOT NULL,
    start_ts                DATETIME2(3)    NOT NULL,
    end_ts                  DATETIME2(3)    NULL,
    duration_sec            DECIMAL(12,2)   NULL,
    rows_read               BIGINT          NULL,
    rows_written            BIGINT          NULL,
    status                  VARCHAR(20)     NOT NULL,
    error_message           VARCHAR(4000)   NULL
);
GO

/* --------------------------------- NOT ENFORCED KEY METADATA ------------- */
ALTER TABLE gold.dim_date          ADD CONSTRAINT PK_dim_date          PRIMARY KEY NONCLUSTERED (date_key) NOT ENFORCED;
ALTER TABLE gold.dim_patient       ADD CONSTRAINT PK_dim_patient       PRIMARY KEY NONCLUSTERED (patient_key) NOT ENFORCED;
ALTER TABLE gold.dim_doctor        ADD CONSTRAINT PK_dim_doctor        PRIMARY KEY NONCLUSTERED (doctor_key) NOT ENFORCED;
ALTER TABLE gold.dim_hospital      ADD CONSTRAINT PK_dim_hospital      PRIMARY KEY NONCLUSTERED (hospital_key) NOT ENFORCED;
ALTER TABLE gold.dim_department    ADD CONSTRAINT PK_dim_department    PRIMARY KEY NONCLUSTERED (department_key) NOT ENFORCED;
ALTER TABLE gold.dim_bed           ADD CONSTRAINT PK_dim_bed           PRIMARY KEY NONCLUSTERED (bed_key) NOT ENFORCED;

ALTER TABLE gold.fact_admission ADD CONSTRAINT FK_adm_patient
    FOREIGN KEY (patient_key) REFERENCES gold.dim_patient(patient_key) NOT ENFORCED;
ALTER TABLE gold.fact_admission ADD CONSTRAINT FK_adm_admdate
    FOREIGN KEY (admission_date_key) REFERENCES gold.dim_date(date_key) NOT ENFORCED;
ALTER TABLE gold.fact_admission ADD CONSTRAINT FK_adm_hospital
    FOREIGN KEY (hospital_key) REFERENCES gold.dim_hospital(hospital_key) NOT ENFORCED;
GO
