# PHASE 9 — Control framework and metadata-driven ingestion

You have one Copy activity moving `dbo.patient` into `lh_bronze`. Ten more SQL tables sit behind it. Building ten more Copy activities works, and also means every future change to the ingestion pattern is eleven edits.

This phase replaces that with **one** pipeline driven by a registry table. Adding a source becomes a row.

Time: about 2 hours, most of it in the pipeline UI.

---

## 9.1 — The shape

```
pl_ingest_sqlserver
│
├── Set variable    load_upper_bound = utcnow()
├── Set variable    batch_id
├── Lookup          ctl.sp_get_entities_to_ingest 'sqlserver_gateway'   → 11 rows
│
├── ForEach (batch 4)
│   ├── SP      sp_batch_start
│   └── Switch on connection_name
│       ├── OHN-SQL-EHR    → Copy → stg_*  → SP sp_batch_end (STAGED / FAILED)
│       ├── OHN-SQL-SCHED  → Copy → stg_*  → SP sp_batch_end
│       └── OHN-SQL-FIN    → Copy → stg_*  → SP sp_batch_end
│
├── Notebook        13_bronze_commit_staging   (once, all entities)
└── SP              sp_commit_batch            (advance all watermarks)
```

Four design decisions worth understanding before you click anything.

**Control tables live in a Warehouse, not the Lakehouse.** They are written on every run. A Lakehouse SQL analytics endpoint is read-only, so writing to it means a Notebook activity, and each one costs 10–30 seconds of Spark startup. Across 25 entities that is 10+ minutes of overhead before a row moves. A Warehouse supports stored procedures, which the Stored Procedure activity calls in under a second. The Lakehouse SQL endpoint also lags behind Delta writes by a metadata sync interval, so a watermark written through it may not be visible to the next Lookup — which appears as the same window loading twice, intermittently.

**Copy lands in staging, not Bronze.** A Copy activity cannot add columns. Bronze rows must carry `_batch_id`, `_row_hash`, `_ingest_ts` — without `_batch_id` you cannot replay a single window, and without `_row_hash` Silver cannot deduplicate. So Copy writes `stg_<entity>` and a notebook stamps and promotes.

**One commit notebook after the loop, not inside it.** All eleven entities are handled in a single Spark session.

**Watermarks advance only after the commit succeeds.** A crash between copy and commit re-reads the same window. Re-reading is harmless — Silver deduplicates on `_row_hash` — while skipping loses data silently.

---

## 9.2 — Create the control Warehouse (5 min)

`OHN-dev` → **New item** → **Warehouse** → `wh_ohn_control`.

---

## 9.3 — Run the control framework script (5 min)

Open `wh_ohn_control` → **New SQL query** → paste all of `warehouse/ddl/00_control_framework.sql` → **Run**.

**Expect:**

| connection_type | entity_count |
|---|---|
| blob_hl7 | 1 |
| manual_csv | 5 |
| rest_api | 4 |
| sftp_edi | 2 |
| sharepoint_excel | 2 |
| sqlserver_gateway | 11 |

Check the procedures:

```sql
SELECT name FROM sys.procedures WHERE schema_name(schema_id) = 'ctl' ORDER BY name;
```

Six: `sp_batch_end`, `sp_batch_start`, `sp_commit_batch`, `sp_get_entities_to_ingest`, `sp_log_error`, `sp_reset_watermark`.

Test the one the pipeline depends on:

```sql
EXEC ctl.sp_get_entities_to_ingest 'sqlserver_gateway';
```

**Expect:** 11 rows, every `watermark_value` reading `1900-01-01 00:00:00`. That default matters — returning NULL would make the generated WHERE clause compare against the string `'null'` and return nothing, so your first run would "succeed" having copied zero rows.

### Two things to notice in the registry

**Four SQL tables are `full_snapshot`, not `incremental`.** `bed_assignment`, `diagnosis`, `appointment_status_history`, and `invoice_line` have no modified-timestamp column at source. This is realistic — child tables often lack audit columns because the application only timestamps the parent. Options are a full reload each run, a join to the parent's timestamp, or asking the source team to add one. Full snapshot is honest at this size; at 50M rows it would not be, and that is a useful conversation to be able to have.

**`business_key` is sometimes composite** — `invoice_id,invoice_line_number`. Silver uses these for deduplication.

---

## 9.4 — Import the commit notebook (3 min)

`OHN-dev` → **New item** → **Notebook** → **Import** → `notebooks/13_bronze_commit_staging.py`.

Attach **`lh_bronze`** as the default lakehouse (left panel → **Add**).

`00_common_utils` must already be imported — the notebook calls `%run 00_common_utils` for `row_hash`.

---

## 9.5 — Pipeline shell (10 min)

**New item** → **Data pipeline** → `pl_ingest_sqlserver`.

### Variables

Left panel → **Variables** → add two, type String, both with empty defaults: `load_upper_bound`, `batch_id`.

### Set Upper Bound

**Set variable** activity, name `Set Upper Bound`.
- Name: `load_upper_bound`
- Value:

```
@formatDateTime(utcnow(), 'yyyy-MM-dd HH:mm:ss')
```

The explicit format matters. Raw `utcnow()` returns ISO-8601 with a `T` and fractional seconds, which SQL Server accepts inconsistently once embedded in a concatenated query string.

### Set Batch Id

**Set variable**, name `Set Batch Id`. Connect `Set Upper Bound` → `Set Batch Id`.
- Name: `batch_id`
- Value:

```
@concat('SQLSRV_', formatDateTime(utcnow(), 'yyyyMMddHHmmss'), '_', substring(pipeline().RunId, 0, 8))
```

### Get Entities

**Lookup** activity, name `Get Entities`. Connect from `Set Batch Id`.

- Connection: `wh_ohn_control`
- Use query: **Stored procedure**
- Name: `ctl.sp_get_entities_to_ingest`
- Import parameters → `connection_type` = `sqlserver_gateway`
- **First row only: UNCHECKED**

> Leaving "first row only" ticked is the most common mistake in this phase. The pipeline runs green, processes exactly one table, and looks like it worked.

**Save** and **Run**. The Lookup output should show `count: 11`.

---

## 9.6 — ForEach (5 min)

**ForEach** activity, name `For Each Entity`. Connect from `Get Entities`.

- Sequential: **unchecked**
- Batch count: **4**
- Items: `@activity('Get Entities').output.value`

Four parallel copies through one gateway is comfortable. The gateway is a single process on one machine, not a cluster — raise this only if you measure a gain.

---

## 9.7 — Inside the loop: log start (5 min)

Click the **pencil** on `For Each Entity`.

**Stored procedure** activity, name `Log Start`.
- Connection: `wh_ohn_control`
- Procedure: `ctl.sp_batch_start`
- **Import parameters**, then:

| Parameter | Value |
|---|---|
| `batch_id` | `@variables('batch_id')` |
| `pipeline_run_id` | `@pipeline().RunId` |
| `pipeline_name` | `@pipeline().Pipeline` |
| `source_system` | `@item().source_system` |
| `entity_name` | `@item().entity_name` |
| `target_table` | `@item().target_table` |
| `load_type` | `@item().load_type` |
| `watermark_from` | `@item().watermark_value` |
| `watermark_to` | `@variables('load_upper_bound')` |

---

## 9.8 — Switch on database (5 min)

**Switch** activity, name `Route By Database`. Connect from `Log Start`.

Expression: `@item().connection_name`

Add three cases, typed exactly as the registry stores them:
`OHN-SQL-EHR`, `OHN-SQL-SCHED`, `OHN-SQL-FIN`

Leave **Default** empty — an unrecognised connection should do nothing rather than route somewhere wrong.

Fabric Copy activities cannot parameterise which *connection* they use; it is bound at design time. Three databases means three Copy activities. The Switch keeps it at three rather than eleven, and adding a twelfth table to `OHN_EHR` still needs no pipeline change.

---

## 9.9 — Copy activity for OHN_EHR (15 min)

Pencil into case `OHN-SQL-EHR` → **Copy data**, name `Copy EHR`.

### Source

- Connection: `OHN-SQL-EHR`
- **Use query: Query**
- Query → **Add dynamic content**:

```
@concat(
  'SELECT * FROM ', item().source_object,
  if(equals(item().load_type, 'incremental'),
     concat(' WHERE ', item().watermark_column,
            ' > ''', item().watermark_value, '''',
            ' AND ', item().watermark_column,
            ' <= ''', variables('load_upper_bound'), ''''),
     '')
)
```

The doubled quotes are not a typo — `''` inside a pipeline expression produces one literal `'` in the SQL.

For `dbo.patient` on a first run this generates:

```sql
SELECT * FROM dbo.patient
WHERE last_modified_ts > '1900-01-01 00:00:00'
  AND last_modified_ts <= '2026-08-03 14:22:07'
```

For `dbo.bed_assignment` (full_snapshot, no watermark column) it generates `SELECT * FROM dbo.bed_assignment` with no WHERE clause. That is why the `if()` is there rather than a WHERE that assumes every entity has a watermark.

The upper bound is fixed once at the top of the pipeline and the watermark later advances to that same value. Advancing instead to `MAX(last_modified_ts)` of the copied rows would mean any row written during the copy falls outside what was read but inside what the watermark claims — silent data loss under concurrent writes.

### Destination

- Connection: `lh_bronze`
- Root folder: **Tables**
- Table name → dynamic content:

```
@concat('stg_', item().target_table)
```

- Table action: **Overwrite**

> Overwrite is correct **here** because this is staging — a scratch table for one entity for one run. Bronze itself is appended by the commit notebook. Pointing this at the Bronze table with Overwrite would make a five-minute incremental window delete two and a half years of history.

### Settings

- Retry: **3**, Retry interval: **30** seconds

---

## 9.10 — Close the copy (10 min)

Still inside case `OHN-SQL-EHR`.

**Stored procedure**, name `Log Staged`. Drag the **green** arrow from `Copy EHR`.

- Connection: `wh_ohn_control`, Procedure: `ctl.sp_batch_end`

| Parameter | Value |
|---|---|
| `batch_id` | `@variables('batch_id')` |
| `source_system` | `@item().source_system` |
| `entity_name` | `@item().entity_name` |
| `status` | `STAGED` |
| `rows_read` | `@activity('Copy EHR').output.rowsRead` |
| `rows_written` | `@activity('Copy EHR').output.rowsCopied` |
| `error_message` | *(empty)* |

**Stored procedure**, name `Log Failure`. Drag the **red** arrow from `Copy EHR`. Same procedure, with `status` = `FAILED`, `rows_read` and `rows_written` = `0`, and:

```
@activity('Copy EHR').output.errors[0].Message
```

`STAGED` is not `SUCCESS`. Only `sp_commit_batch` promotes a batch, after data has actually reached Bronze.

---

## 9.11 — The other two databases (10 min)

Cases `OHN-SQL-SCHED` and `OHN-SQL-FIN` are identical except for the Copy activity's **connection**, its **name** (`Copy SCHED`, `Copy FIN`), and the `@activity('...')` references in the two stored procedures.

Fastest route: select the three activities, **Ctrl+C**, click into the next case, **Ctrl+V**, then change the connection and names.

> `@activity('Copy EHR')` references do **not** update when you rename a pasted activity. If `rows_written` logs as null, this is why.

---

## 9.12 — Commit and promote (10 min)

Exit the ForEach (breadcrumb at the top).

### Commit notebook

**Notebook** activity, name `Commit Staging`. Connect `For Each Entity` → `Commit Staging` (green arrow).

- Notebook: `13_bronze_commit_staging`
- **Base parameters** → add two:

| Name | Type | Value |
|---|---|---|
| `batch_id` | String | `@variables('batch_id')` |
| `entities` | String | `@string(activity('Get Entities').output.value)` |

`@string(...)` matters — the notebook expects a JSON string and parses it. Passing the array object directly gives you a Python representation with single quotes that `json.loads` rejects.

### Promote

**Stored procedure**, name `Commit Batch`. Connect `Commit Staging` → `Commit Batch` (green arrow).

- Connection: `wh_ohn_control`
- Procedure: `ctl.sp_commit_batch`
- `batch_id` = `@variables('batch_id')`

Green arrow only. If the commit notebook fails, watermarks must not advance.

**Save.**

---

## 9.13 — Run it (10 min)

Clear the table your manual Copy activity created, or you get duplicates:

```python
spark.sql("DROP TABLE IF EXISTS lh_bronze.ehr_patient")
```

**Run.** Expect 4–8 minutes.

### Verify the control tables

```sql
SELECT source_system, entity_name, status, rows_written, duration_seconds
FROM ctl.batch_log
ORDER BY start_ts DESC;
```

11 rows, all `SUCCESS` (not `STAGED` — if they are still `STAGED`, `sp_commit_batch` did not run).

Rough expected volumes at 25,000 patients:

| Entity | Rows |
|---|---|
| ehr_patient | ~26,000 |
| ehr_admission | ~12,000 |
| ehr_emergency_visit | ~19,000 |
| ehr_bed_assignment | ~15,000 |
| ehr_diagnosis | ~38,000 |
| sched_patient | ~20,000 |
| sched_appointment | ~155,000 |
| sched_appointment_status_history | ~310,000 |
| fin_patient_account | ~16,000 |
| fin_invoice | ~12,000 |
| fin_invoice_line | ~54,000 |

```sql
SELECT * FROM ctl.watermark ORDER BY source_system, entity_name;
```

Seven rows — only `incremental` entities get a watermark. The four `full_snapshot` ones do not, which is correct.

### Verify the data and its audit columns

```python
df = spark.table("lh_bronze.ehr_patient")
print(f"{df.count():,} rows")
df.select("patient_id", "_source_system", "_batch_id", "_load_date",
          "_row_hash").show(3, truncate=False)

# no staging tables should survive
print([t.tableName for t in spark.sql("SHOW TABLES IN lh_bronze").collect()
       if t.tableName.startswith("stg_")])
```

`_batch_id` populated and an empty staging list means the commit worked.

---

## 9.14 — Prove the incremental logic (10 min)

This is the test most people skip, and the one that matters.

**Run the pipeline again immediately.**

```sql
SELECT entity_name, load_type, rows_written
FROM ctl.batch_log
WHERE batch_id = (SELECT MAX(batch_id) FROM ctl.batch_log)
ORDER BY entity_name;
```

The seven `incremental` entities should show **0 rows** — nothing changed at the source. The four `full_snapshot` entities reload in full, which is expected, and the commit notebook's `replaceWhere` means they do not double.

If incremental entities reload everything, the watermark is not advancing. Check `ctl.watermark` has rows and that `sp_commit_batch` ran.

### Now change the source

```bash
docker exec ohn-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "YourPassword" -C -d OHN_EHR \
  -Q "UPDATE TOP (25) dbo.patient SET last_modified_ts = GETUTCDATE(), preferred_language = 'French';"
```

Run a third time. **Expect `ehr_patient` to write exactly 25 rows.** Nothing else changes.

That is the phase demonstrated end to end: eleven tables, one pipeline, incremental by watermark, audit columns intact, full run history. It is the best thing to be able to show and explain from this project.

---

## 9.15 — Freshness (2 min)

```sql
SELECT source_system, entity_name, freshness_status,
       minutes_since_last_load, sla_minutes, last_rows_written
FROM ctl.vw_last_run_per_entity
ORDER BY CASE freshness_status
           WHEN 'FAILED' THEN 1 WHEN 'NEVER_RUN' THEN 2
           WHEN 'STALE' THEN 3 ELSE 4 END,
         source_system, entity_name;
```

The 14 non-SQL entities show `NEVER_RUN` — correct, those pipelines do not exist yet. This view becomes the SLA panel on the data-quality report page.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| Lookup returns 1 row | "First row only" still ticked |
| `Incorrect syntax near ''` | Quote escaping in the query expression — use `''` not `'` |
| Notebook: "No entities passed" | `entities` parameter missing `@string(...)` |
| Notebook: "Staging tables still present" | An entity was copied but not in the entities list |
| Notebook: row counts do not reconcile | Destination was Bronze instead of `stg_` |
| Status stuck at STAGED | `sp_commit_batch` did not run — check the green arrow from `Commit Staging` |
| Second run reloads everything | Watermark not advancing; check `ctl.watermark` |
| `rows_written` is null | `@activity('Copy EHR')` still points at the pre-rename name |
| Gateway timeout | `sched_appointment_status_history` is largest — drop batch count to 2 |
| Rows double each run | Destination table action is Append instead of Overwrite on the staging table |

To start over on one entity:

```sql
EXEC ctl.sp_reset_watermark 'EHR', 'patient';
```

---

## What you have now

```
25 entities registered → 11 loading through one metadata-driven pipeline
  → staged, stamped with audit columns, committed to Bronze
  → watermarks advancing only after commit → every run logged → freshness measurable
```

## Next

**Phase 10** — the remaining four connection types, reusing these same control procedures: a REST Copy activity with pagination for the API, binary copy plus the two parser notebooks for EDI and HL7, a Dataflow Gen2 for Excel, and a file copy for reference data.

Then **Phase 11 — Silver and the patient MPI**, where this stops being plumbing.
