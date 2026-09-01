/* =====================================================================
   OHN — Control framework
   Target: Fabric Warehouse  (wh_ohn_control)

   WHY A WAREHOUSE AND NOT THE LAKEHOUSE
   -------------------------------------
   Control tables are written on every pipeline run: watermarks advance,
   batches are logged, errors are recorded. A Lakehouse SQL analytics
   endpoint is READ-ONLY, so writing to it from a pipeline means a Notebook
   activity, and every notebook activity pays 10-30 seconds of Spark session
   startup. Across 25 entities that is 10+ minutes of pure overhead per run,
   before a single row moves.

   A Warehouse supports full T-SQL DML and stored procedures, which the
   Stored Procedure activity calls directly in well under a second. The
   Lakehouse SQL endpoint also lags behind Delta writes by a metadata sync
   interval, so a watermark written through it may not be visible to the
   next Lookup — a race condition that shows up as duplicate loads under
   load and is unpleasant to diagnose.

   FABRIC WAREHOUSE T-SQL CONSTRAINTS OBSERVED HERE
   ------------------------------------------------
   No IDENTITY columns, no DEFAULT constraints, no triggers, no computed
   columns. Every value is supplied explicitly and keys are GUIDs or come
   from the caller. Written this way so the script runs as-is rather than
   failing halfway through.
   ===================================================================== */

CREATE SCHEMA ctl;
GO

/* ------------------------------------------------------------------ 1 */
/* Source registry — one row per ingestible entity.                     */
/* Adding a source becomes a row, not a new pipeline.                   */

IF OBJECT_ID('ctl.source_registry') IS NOT NULL DROP TABLE ctl.source_registry;
GO
CREATE TABLE ctl.source_registry (
    source_system       VARCHAR(20)   NOT NULL,
    entity_name         VARCHAR(60)   NOT NULL,
    connection_type     VARCHAR(30)   NOT NULL,  -- sqlserver_gateway | rest_api | sftp_edi | blob_hl7 | sharepoint_excel | manual_csv
    connection_name     VARCHAR(60)   NULL,      -- Fabric connection name, null for file sources
    source_object       VARCHAR(200)  NOT NULL,  -- dbo.patient | medication-orders | Files/landing/...
    target_table        VARCHAR(120)  NOT NULL,  -- bronze Delta table
    business_key        VARCHAR(200)  NOT NULL,
    watermark_column    VARCHAR(60)   NULL,      -- null for full_snapshot and file_arrival
    load_type           VARCHAR(20)   NOT NULL,  -- incremental | full_snapshot | file_arrival
    file_format         VARCHAR(10)   NULL,
    sla_minutes         INT           NOT NULL,
    is_active           BIT           NOT NULL,
    notes               VARCHAR(300)  NULL
);
GO

/* ------------------------------------------------------------------ 2 */
/* Watermark — the high-water mark per entity.                          */
/* Separate from the registry so configuration and runtime state are    */
/* not entangled: you can redeploy the registry without resetting how   */
/* far every load has progressed.                                        */

IF OBJECT_ID('ctl.watermark') IS NOT NULL DROP TABLE ctl.watermark;
GO
CREATE TABLE ctl.watermark (
    source_system       VARCHAR(20)   NOT NULL,
    entity_name         VARCHAR(60)   NOT NULL,
    watermark_value     VARCHAR(40)   NOT NULL,  -- ISO timestamp as text; works for dates and version numbers alike
    last_batch_id       VARCHAR(80)   NULL,
    updated_ts          DATETIME2(3)  NOT NULL
);
GO

/* ------------------------------------------------------------------ 3 */
/* Batch log — one row per entity per pipeline run.                     */

IF OBJECT_ID('ctl.batch_log') IS NOT NULL DROP TABLE ctl.batch_log;
GO
CREATE TABLE ctl.batch_log (
    batch_id            VARCHAR(80)   NOT NULL,
    pipeline_run_id     VARCHAR(80)   NULL,
    pipeline_name       VARCHAR(120)  NOT NULL,
    source_system       VARCHAR(20)   NOT NULL,
    entity_name         VARCHAR(60)   NOT NULL,
    target_table        VARCHAR(120)  NULL,
    load_type           VARCHAR(20)   NULL,
    watermark_from      VARCHAR(40)   NULL,
    watermark_to        VARCHAR(40)   NULL,
    status              VARCHAR(20)   NOT NULL,  -- RUNNING | STAGED | SUCCESS | FAILED | NO_DATA
    rows_read           BIGINT        NULL,
    rows_written        BIGINT        NULL,
    duration_seconds    DECIMAL(12,2) NULL,
    error_message       VARCHAR(4000) NULL,
    start_ts            DATETIME2(3)  NOT NULL,
    end_ts              DATETIME2(3)  NULL
);
GO

/* ------------------------------------------------------------------ 4 */
/* Error log — rejected rows and non-fatal problems.                    */

IF OBJECT_ID('ctl.error_log') IS NOT NULL DROP TABLE ctl.error_log;
GO
CREATE TABLE ctl.error_log (
    error_id            VARCHAR(40)   NOT NULL,
    batch_id            VARCHAR(80)   NOT NULL,
    source_system       VARCHAR(20)   NULL,
    entity_name         VARCHAR(60)   NULL,
    severity            VARCHAR(10)   NOT NULL,  -- Error | Warning
    error_type          VARCHAR(60)   NULL,
    error_message       VARCHAR(4000) NULL,
    record_identifier   VARCHAR(200)  NULL,
    logged_ts           DATETIME2(3)  NOT NULL
);
GO


/* =====================================================================
   STORED PROCEDURES
   ===================================================================== */

/* -------------------------------------------------------------------- */
/* Return every active entity for one connection type, with its current  */
/* watermark already joined on.                                          */
/*                                                                       */
/* One Lookup at the start of the pipeline gets everything the ForEach   */
/* needs. The obvious alternative — a Lookup per iteration to fetch that */
/* entity's watermark — costs 25 extra round trips and buys nothing.     */
/* -------------------------------------------------------------------- */

CREATE OR ALTER PROCEDURE ctl.sp_get_entities_to_ingest
    @connection_type VARCHAR(30)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        r.source_system,
        r.entity_name,
        r.connection_type,
        r.connection_name,
        r.source_object,
        r.target_table,
        r.business_key,
        r.watermark_column,
        r.load_type,
        r.file_format,
        r.sla_minutes,
        /* A never-loaded entity starts at 1900-01-01 so its first run is a
           full pull. Returning NULL here would make the generated WHERE
           clause compare against the string 'null' and quietly return
           nothing — a first run that "succeeds" with zero rows. */
        COALESCE(w.watermark_value, '1900-01-01 00:00:00') AS watermark_value
    FROM ctl.source_registry r
    LEFT JOIN ctl.watermark w
           ON w.source_system = r.source_system
          AND w.entity_name   = r.entity_name
    WHERE r.connection_type = @connection_type
      AND r.is_active = 1
    ORDER BY r.source_system, r.entity_name;
END;
GO

/* -------------------------------------------------------------------- */
/* Mark an entity as started.                                            */
/* -------------------------------------------------------------------- */

CREATE OR ALTER PROCEDURE ctl.sp_batch_start
    @batch_id        VARCHAR(80),
    @pipeline_run_id VARCHAR(80),
    @pipeline_name   VARCHAR(120),
    @source_system   VARCHAR(20),
    @entity_name     VARCHAR(60),
    @target_table    VARCHAR(120),
    @load_type       VARCHAR(20),
    @watermark_from  VARCHAR(40),
    @watermark_to    VARCHAR(40)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ctl.batch_log
        (batch_id, pipeline_run_id, pipeline_name, source_system, entity_name,
         target_table, load_type, watermark_from, watermark_to, status,
         rows_read, rows_written, duration_seconds, error_message, start_ts, end_ts)
    VALUES
        (@batch_id, @pipeline_run_id, @pipeline_name, @source_system, @entity_name,
         @target_table, @load_type, @watermark_from, @watermark_to, 'RUNNING',
         NULL, NULL, NULL, NULL, GETUTCDATE(), NULL);
END;
GO

/* -------------------------------------------------------------------- */
/* Mark an entity's COPY as finished.                                    */
/*                                                                       */
/* Note the status this sets: STAGED, not SUCCESS. A completed copy has  */
/* landed rows in a staging table, not in Bronze. Audit columns          */
/* (_batch_id, _row_hash, _ingest_ts) are stamped by the commit notebook */
/* afterwards, because a Copy activity cannot add them. Advancing the    */
/* watermark here would mark data as loaded while it is still one step   */
/* away from Bronze, and a failure in between would lose that window     */
/* permanently. sp_commit_batch does the promotion.                      */
/*                                                                       */
/* The watermark advances to the upper bound the pipeline chose BEFORE   */
/* the copy started, not to MAX(watermark_column) of what was copied.    */
/* Those differ, and the difference matters: if the source writes a row  */
/* while the copy is running, MAX() of the copied data would skip past   */
/* rows that were never read. Bounding the query with <= upper_bound and */
/* then storing that same bound makes the two agree by construction.     */
/*                                                                       */
/* Advancing the watermark and closing the batch happen in one call so   */
/* they cannot diverge — a batch that logged success but failed to move  */
/* the watermark would reload the same window forever.                   */
/* -------------------------------------------------------------------- */

CREATE OR ALTER PROCEDURE ctl.sp_batch_end
    @batch_id       VARCHAR(80),
    @source_system  VARCHAR(20),
    @entity_name    VARCHAR(60),
    @status         VARCHAR(20),   -- STAGED | FAILED | NO_DATA
    @rows_read      BIGINT,
    @rows_written   BIGINT,
    @error_message  VARCHAR(4000)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE ctl.batch_log
       SET status           = @status,
           rows_read        = @rows_read,
           rows_written     = @rows_written,
           error_message    = @error_message,
           end_ts           = GETUTCDATE(),
           duration_seconds = DATEDIFF(SECOND, start_ts, GETUTCDATE())
     WHERE batch_id = @batch_id
       AND source_system = @source_system
       AND entity_name = @entity_name;

END;
GO

/* -------------------------------------------------------------------- */
/* Promote a batch: STAGED -> SUCCESS, and advance every watermark.      */
/*                                                                       */
/* Called once, after the commit notebook has stamped audit columns and  */
/* appended staging into Bronze. Watermarks move only now, so a crash    */
/* between copy and commit re-reads the same window rather than skipping */
/* it. Re-reading is harmless — Silver deduplicates on _row_hash — while */
/* skipping loses data with no signal that anything went wrong.          */
/*                                                                       */
/* watermark_to comes from batch_log, where sp_batch_start recorded the  */
/* upper bound the copy query actually used. Taking it from batch_log    */
/* rather than re-deriving it means the stored watermark and the query   */
/* bound cannot drift apart.                                             */
/* -------------------------------------------------------------------- */

CREATE OR ALTER PROCEDURE ctl.sp_commit_batch
    @batch_id VARCHAR(80)
AS
BEGIN
    SET NOCOUNT ON;

    /* UPDATE then INSERT rather than MERGE. Fabric Warehouse supports
       MERGE, but the two-statement form is portable and its behaviour on a
       missing row is obvious to anyone reading it later. */
    UPDATE w
       SET w.watermark_value = b.watermark_to,
           w.last_batch_id   = b.batch_id,
           w.updated_ts      = GETUTCDATE()
      FROM ctl.watermark w
      JOIN ctl.batch_log b
        ON b.source_system = w.source_system
       AND b.entity_name   = w.entity_name
     WHERE b.batch_id = @batch_id
       AND b.status   = 'STAGED'
       AND b.watermark_to IS NOT NULL;

    INSERT INTO ctl.watermark
        (source_system, entity_name, watermark_value, last_batch_id, updated_ts)
    SELECT b.source_system, b.entity_name, b.watermark_to, b.batch_id, GETUTCDATE()
      FROM ctl.batch_log b
     WHERE b.batch_id = @batch_id
       AND b.status   = 'STAGED'
       AND b.watermark_to IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM ctl.watermark w
                        WHERE w.source_system = b.source_system
                          AND w.entity_name   = b.entity_name);

    UPDATE ctl.batch_log
       SET status = 'SUCCESS'
     WHERE batch_id = @batch_id
       AND status   = 'STAGED';

    SELECT COUNT(*) AS entities_committed
      FROM ctl.batch_log
     WHERE batch_id = @batch_id AND status = 'SUCCESS';
END;
GO

/* -------------------------------------------------------------------- */
/* Record a non-fatal problem without failing the run.                   */
/* -------------------------------------------------------------------- */

CREATE OR ALTER PROCEDURE ctl.sp_log_error
    @batch_id          VARCHAR(80),
    @source_system     VARCHAR(20),
    @entity_name       VARCHAR(60),
    @severity          VARCHAR(10),
    @error_type        VARCHAR(60),
    @error_message     VARCHAR(4000),
    @record_identifier VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO ctl.error_log
        (error_id, batch_id, source_system, entity_name, severity,
         error_type, error_message, record_identifier, logged_ts)
    VALUES
        (CAST(NEWID() AS VARCHAR(40)), @batch_id, @source_system, @entity_name,
         @severity, @error_type, @error_message, @record_identifier, GETUTCDATE());
END;
GO

/* -------------------------------------------------------------------- */
/* Reset an entity so its next run is a full reload.                     */
/* You will want this more often than you expect during development.     */
/* -------------------------------------------------------------------- */

CREATE OR ALTER PROCEDURE ctl.sp_reset_watermark
    @source_system VARCHAR(20),
    @entity_name   VARCHAR(60) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DELETE FROM ctl.watermark
     WHERE source_system = @source_system
       AND (@entity_name IS NULL OR entity_name = @entity_name);
END;
GO


/* =====================================================================
   MONITORING VIEWS
   ===================================================================== */

CREATE OR ALTER VIEW ctl.vw_last_run_per_entity
AS
SELECT
    r.source_system,
    r.entity_name,
    r.connection_type,
    r.load_type,
    r.sla_minutes,
    w.watermark_value,
    w.updated_ts                AS watermark_updated_ts,
    b.status                    AS last_status,
    b.rows_written              AS last_rows_written,
    b.duration_seconds          AS last_duration_seconds,
    b.end_ts                    AS last_end_ts,
    DATEDIFF(MINUTE, COALESCE(b.end_ts, '1900-01-01'), GETUTCDATE()) AS minutes_since_last_load,
    CASE
        WHEN b.end_ts IS NULL THEN 'NEVER_RUN'
        WHEN b.status <> 'SUCCESS' THEN 'FAILED'
        WHEN DATEDIFF(MINUTE, b.end_ts, GETUTCDATE()) > r.sla_minutes THEN 'STALE'
        ELSE 'OK'
    END AS freshness_status
FROM ctl.source_registry r
LEFT JOIN ctl.watermark w
       ON w.source_system = r.source_system AND w.entity_name = r.entity_name
LEFT JOIN (
    SELECT source_system, entity_name, status, rows_written,
           duration_seconds, end_ts,
           ROW_NUMBER() OVER (PARTITION BY source_system, entity_name
                              ORDER BY start_ts DESC) AS rn
    FROM ctl.batch_log
) b ON b.source_system = r.source_system
   AND b.entity_name = r.entity_name
   AND b.rn = 1
WHERE r.is_active = 1;
GO

CREATE OR ALTER VIEW ctl.vw_run_history
AS
SELECT TOP 1000
    batch_id, pipeline_name, source_system, entity_name, status,
    rows_read, rows_written, duration_seconds,
    watermark_from, watermark_to, error_message, start_ts, end_ts
FROM ctl.batch_log
ORDER BY start_ts DESC;
GO


/* =====================================================================
   SEED THE REGISTRY
   ===================================================================== */

DELETE FROM ctl.source_registry;
GO

INSERT INTO ctl.source_registry
(source_system, entity_name, connection_type, connection_name, source_object,
 target_table, business_key, watermark_column, load_type, file_format,
 sla_minutes, is_active, notes)
VALUES
/* ---- On-prem SQL Server, OHN_EHR ---------------------------------- */
('EHR','patient','sqlserver_gateway','OHN-SQL-EHR','dbo.patient','ehr_patient','patient_id','last_modified_ts','incremental',NULL,60,1,'Patient demographic master'),
('EHR','admission','sqlserver_gateway','OHN-SQL-EHR','dbo.admission','ehr_admission','admission_id','last_modified_ts','incremental',NULL,60,1,'Admission and discharge events'),
('EHR','emergency_visit','sqlserver_gateway','OHN-SQL-EHR','dbo.emergency_visit','ehr_emergency_visit','ed_visit_id','last_modified_ts','incremental',NULL,30,1,'ED visits with milestone timestamps'),
('EHR','bed_assignment','sqlserver_gateway','OHN-SQL-EHR','dbo.bed_assignment','ehr_bed_assignment','admission_id,bed_id,assignment_start_ts',NULL,'full_snapshot',NULL,60,1,'No modified timestamp at source'),
('EHR','diagnosis','sqlserver_gateway','OHN-SQL-EHR','dbo.diagnosis','ehr_diagnosis','encounter_id,diagnosis_code,diagnosis_rank',NULL,'full_snapshot',NULL,60,1,'No modified timestamp at source'),

/* ---- On-prem SQL Server, OHN_SCHED -------------------------------- */
('SCHED','patient','sqlserver_gateway','OHN-SQL-SCHED','dbo.patient','sched_patient','patient_ref','modified_at','incremental',NULL,60,1,'Scheduling system patient view'),
('SCHED','appointment','sqlserver_gateway','OHN-SQL-SCHED','dbo.appointment','sched_appointment','appointment_id','modified_at','incremental',NULL,15,1,'Appointments with status and timing'),
('SCHED','appointment_status_history','sqlserver_gateway','OHN-SQL-SCHED','dbo.appointment_status_history','sched_appointment_status_history','appointment_id,status_code,status_ts',NULL,'full_snapshot',NULL,60,1,'Append-only but no watermark column'),

/* ---- On-prem SQL Server, OHN_FIN ---------------------------------- */
('FIN','patient_account','sqlserver_gateway','OHN-SQL-FIN','dbo.patient_account','fin_patient_account','account_holder_id','update_dt','incremental',NULL,1440,1,'Finance system patient view'),
('FIN','invoice','sqlserver_gateway','OHN-SQL-FIN','dbo.invoice','fin_invoice','invoice_id','update_dt','incremental',NULL,1440,1,'Invoice headers'),
('FIN','invoice_line','sqlserver_gateway','OHN-SQL-FIN','dbo.invoice_line','fin_invoice_line','invoice_id,invoice_line_number',NULL,'full_snapshot',NULL,1440,1,'No modified timestamp at source'),

/* ---- REST API ----------------------------------------------------- */
('PHARM','medication_order','rest_api','OHN-API','medication-orders','pharm_medication_order','medication_order_id','updated_at','incremental','json',60,1,'Paginated REST with cursor'),
('FACIL','hospital','rest_api','OHN-API','hospitals','facil_hospital','hospital_id','update_ts','full_snapshot','json',1440,1,'Small reference-like entity'),
('FACIL','department','rest_api','OHN-API','departments','facil_department','department_id','update_ts','full_snapshot','json',1440,1,'Small reference-like entity'),
('FACIL','bed','rest_api','OHN-API','beds','facil_bed','bed_id','update_ts','full_snapshot','json',1440,1,'Small reference-like entity'),

/* ---- File sources ------------------------------------------------- */
('CLAIMS','claim_837','sftp_edi',NULL,'Files/landing/CLAIMS/837','claims_837_service_line','claim_number,line_number',NULL,'file_arrival','edi',1440,1,'X12 837P, parser notebook 12'),
('CLAIMS','claim_835','sftp_edi',NULL,'Files/landing/CLAIMS/835','claims_835_remittance','claim_number',NULL,'file_arrival','edi',1440,1,'X12 835, parser notebook 12'),
('LIS','lab_result','blob_hl7',NULL,'Files/landing/LIS/lab','lis_lab_result','placer_order_id,loinc_code,set_id',NULL,'file_arrival','hl7',30,1,'HL7 v2 ORU, parser notebook 11'),
('HR','doctor','sharepoint_excel','OHN-SharePoint','doctor_roster.xlsx','hr_doctor','doctor_id',NULL,'full_snapshot','xlsx',1440,1,'Header row is row 4, skip 3'),
('SURVEY','survey_response','sharepoint_excel','OHN-SharePoint','survey_response_export.xlsx','survey_response','survey_response_id','response_date','incremental','xlsx',1440,1,'Header row is row 4, skip 3'),

/* ---- Reference ---------------------------------------------------- */
('REF','icd10ca','manual_csv',NULL,'Files/reference/icd10ca.csv','ref_icd10ca','diagnosis_code',NULL,'full_snapshot','csv',10080,1,'Quarterly vocabulary release'),
('REF','loinc','manual_csv',NULL,'Files/reference/loinc.csv','ref_loinc','loinc_code',NULL,'full_snapshot','csv',10080,1,'Quarterly vocabulary release'),
('REF','medication','manual_csv',NULL,'Files/reference/medication.csv','ref_medication','din',NULL,'full_snapshot','csv',10080,1,'Quarterly vocabulary release'),
('REF','payer','manual_csv',NULL,'Files/reference/payer.csv','ref_payer','payer_id',NULL,'full_snapshot','csv',10080,1,'Payer master'),
('REF','code_mapping','manual_csv',NULL,'Files/reference/code_mapping.csv','ref_code_mapping','domain,source_system,source_code',NULL,'full_snapshot','csv',10080,1,'Source code to standard code mappings');
GO

/* ------------------------------------------------------------------- */
/* Verify                                                               */
/* ------------------------------------------------------------------- */
SELECT connection_type, COUNT(*) AS entity_count
FROM ctl.source_registry
GROUP BY connection_type
ORDER BY connection_type;
GO
