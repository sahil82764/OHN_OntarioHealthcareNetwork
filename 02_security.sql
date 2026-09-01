/* =====================================================================
   OHN Analytics Warehouse — Security and access control
   Enforcement: object-level DENY, dynamic data masking, row-level security
   ===================================================================== */

/* ------------------------------- 1. User-to-scope mapping ---------------- */
/* Populated nightly from Entra ID group membership by the governance pipeline */

CREATE TABLE sec.user_hospital_scope (
    user_principal_name VARCHAR(200) NOT NULL,
    hospital_key        BIGINT       NOT NULL,
    granted_ts          DATETIME2(3) NOT NULL
);
GO

CREATE TABLE sec.user_department_scope (
    user_principal_name VARCHAR(200) NOT NULL,
    department_key      BIGINT       NOT NULL,
    granted_ts          DATETIME2(3) NOT NULL
);
GO

CREATE TABLE sec.user_role_assignment (
    user_principal_name VARCHAR(200) NOT NULL,
    role_name           VARCHAR(60)  NOT NULL,   -- Executive / HospitalAdmin / DeptManager / Clinician / RevCycle / Quality / Steward
    is_all_hospitals    BIT          NOT NULL,
    granted_ts          DATETIME2(3) NOT NULL
);
GO

/* ------------------------------- 2. Roles -------------------------------- */
CREATE ROLE r_executive;
CREATE ROLE r_hospital_admin;
CREATE ROLE r_dept_manager;
CREATE ROLE r_clinician;
CREATE ROLE r_revenue_cycle;
CREATE ROLE r_quality_analyst;
CREATE ROLE r_data_steward;
GO

/* ------------------------------- 3. Row-level security ------------------- */

CREATE FUNCTION sec.fn_hospital_predicate(@hospital_key BIGINT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS access_granted
    WHERE EXISTS (
            SELECT 1
            FROM sec.user_role_assignment ura
            WHERE ura.user_principal_name = USER_NAME()
              AND ura.is_all_hospitals = 1
          )
       OR EXISTS (
            SELECT 1
            FROM sec.user_hospital_scope uhs
            WHERE uhs.user_principal_name = USER_NAME()
              AND uhs.hospital_key = @hospital_key
          );
GO

CREATE FUNCTION sec.fn_department_predicate(@department_key BIGINT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS access_granted
    WHERE EXISTS (
            SELECT 1
            FROM sec.user_role_assignment ura
            WHERE ura.user_principal_name = USER_NAME()
              AND ura.role_name IN ('Executive','HospitalAdmin','RevCycle','Quality','Steward')
          )
       OR EXISTS (
            SELECT 1
            FROM sec.user_department_scope uds
            WHERE uds.user_principal_name = USER_NAME()
              AND uds.department_key = @department_key
          );
GO

CREATE SECURITY POLICY sec.pol_hospital_filter
    ADD FILTER PREDICATE sec.fn_hospital_predicate(hospital_key) ON gold.fact_admission,
    ADD FILTER PREDICATE sec.fn_hospital_predicate(hospital_key) ON gold.fact_appointment,
    ADD FILTER PREDICATE sec.fn_hospital_predicate(hospital_key) ON gold.fact_emergency_visit,
    ADD FILTER PREDICATE sec.fn_hospital_predicate(hospital_key) ON gold.fact_bed_occupancy_daily,
    ADD FILTER PREDICATE sec.fn_hospital_predicate(hospital_key) ON gold.fact_lab_result,
    ADD FILTER PREDICATE sec.fn_hospital_predicate(hospital_key) ON gold.fact_medication_order,
    ADD FILTER PREDICATE sec.fn_hospital_predicate(hospital_key) ON gold.fact_claim,
    ADD FILTER PREDICATE sec.fn_hospital_predicate(hospital_key) ON gold.fact_billing_line,
    ADD FILTER PREDICATE sec.fn_hospital_predicate(hospital_key) ON gold.fact_satisfaction_survey
    WITH (STATE = ON);
GO

CREATE SECURITY POLICY sec.pol_department_filter
    ADD FILTER PREDICATE sec.fn_department_predicate(department_key) ON gold.fact_lab_result,
    ADD FILTER PREDICATE sec.fn_department_predicate(department_key) ON gold.fact_medication_order
    WITH (STATE = ON);
GO

/* ------------------------------- 4. Column masking ----------------------- */
/* Gold already tokenises identifiers; masking is defence in depth for the
   few display attributes that remain. */

ALTER TABLE gold.dim_patient
    ALTER COLUMN patient_token
    ADD MASKED WITH (FUNCTION = 'partial(4, "XXXXXXXX", 0)');
GO

ALTER TABLE gold.dim_patient
    ALTER COLUMN birth_year
    ADD MASKED WITH (FUNCTION = 'default()');
GO

ALTER TABLE gold.dim_patient
    ALTER COLUMN fsa
    ADD MASKED WITH (FUNCTION = 'partial(1, "XX", 0)');
GO

ALTER TABLE gold.dim_doctor
    ALTER COLUMN doctor_display_name
    ADD MASKED WITH (FUNCTION = 'partial(2, "*****", 0)');
GO

GRANT UNMASK ON gold.dim_patient TO r_clinician, r_data_steward;
GRANT UNMASK ON gold.dim_doctor  TO r_clinician, r_dept_manager, r_hospital_admin, r_data_steward;
GO

/* ------------------------------- 5. Object grants ------------------------ */

/* Everyone with warehouse access reads conformed dimensions */
GRANT SELECT ON SCHEMA::gold TO r_executive, r_hospital_admin, r_dept_manager,
                                r_clinician, r_revenue_cycle, r_quality_analyst, r_data_steward;
GO

/* Clinical detail hidden from finance and executive roles */
DENY SELECT ON gold.fact_lab_result       TO r_revenue_cycle, r_executive;
DENY SELECT ON gold.fact_medication_order TO r_revenue_cycle, r_executive;
DENY SELECT ON gold.dim_diagnosis         TO r_revenue_cycle;
GO

/* Financial detail hidden from clinical roles */
DENY SELECT ON gold.fact_billing_line TO r_clinician;
DENY SELECT ON gold.fact_claim        TO r_clinician;
GO

/* Governance tables restricted to platform roles */
GRANT SELECT ON SCHEMA::governance TO r_data_steward, r_quality_analyst;
GO

/* Security metadata is never readable by report consumers */
DENY SELECT ON SCHEMA::sec TO r_executive, r_hospital_admin, r_dept_manager,
                              r_clinician, r_revenue_cycle, r_quality_analyst;
GO

/* ------------------------------- 6. De-identified view for research ------ */

CREATE VIEW gold.vw_admission_deidentified
AS
SELECT
    a.admission_key,
    p.age_band,
    p.sex,
    h.region                AS hospital_region,
    d.service_line,
    dx.chapter              AS diagnosis_chapter,
    at.admission_type_desc,
    a.length_of_stay_days,
    a.icu_days,
    a.is_readmission_30d,
    dt.calendar_year,
    dt.quarter_number
FROM gold.fact_admission a
JOIN gold.dim_patient        p  ON p.patient_key        = a.patient_key
JOIN gold.dim_hospital       h  ON h.hospital_key       = a.hospital_key
JOIN gold.dim_department     d  ON d.department_key     = a.department_key
JOIN gold.dim_diagnosis      dx ON dx.diagnosis_key     = a.primary_diagnosis_key
JOIN gold.dim_admission_type at ON at.admission_type_key= a.admission_type_key
JOIN gold.dim_date           dt ON dt.date_key          = a.admission_date_key;
GO

GRANT SELECT ON gold.vw_admission_deidentified TO r_quality_analyst, r_executive;
GO
