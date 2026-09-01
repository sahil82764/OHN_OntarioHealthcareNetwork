# QUICKSTART — run this in order

Every command below assumes you are in the project root (the folder containing `README.md`) unless it says otherwise.

Total time to "data flowing into Fabric": about 3 hours, most of it waiting on installs and uploads.

**If you only have an afternoon, do Phases 1, 2, 4, 5, 6 and skip Phase 3 (SQL Server) and Phase 7 (gateway).** You will have a working multi-source pipeline with the API and file sources. Add SQL Server after.

---

# PHASE 0 — Install what you need

| Tool | Needed for | Check it works |
|---|---|---|
| Python 3.10+ | Phases 1, 2, 4 | `python --version` |
| Docker Desktop | Phase 3 (SQL Server) | `docker --version` |
| A Fabric workspace | Phases 5+ | app.fabric.microsoft.com loads |

Nothing to `pip install`. The generator, splitter, and API all use the standard library only.

If `python` gives "command not found", try `python3` and use that everywhere below.

---

# PHASE 1 — Generate the data  (~2 minutes)

```bash
cd data-generator
python ohn_generator.py --patients 25000 --out ./landing
```

**Expect:** a table of 26 entities and about 1.25 million rows total, ending with `Written to .../landing`.

Now check it is usable:

```bash
python validate_data.py --landing ./landing
```

**Expect:** `All checks passed — data is ready to load into Fabric.`

If anything says FAIL, stop and paste it to me. Do not continue with data that failed validation — the defects and correlations it checks are what make every later report work.

```bash
cd ..
```

### What you just made

`data-generator/landing/` holds 26 CSVs shaped like raw source extracts. This is the *raw material*. It is not what you upload to Fabric.

---

# PHASE 2 — Split it into five source systems  (~1 minute)

```bash
cd sources
python split_sources.py --landing ../data-generator/landing --out . --clean
cd ..
```

**Expect:** six numbered sections printing row counts, ending with `Written to .../sources`.

### What you just made

```
sources/
├── 01-sqlserver/    SQL scripts + CSVs + docker-compose   → Phase 3
├── 02-api/          JSON payloads + the API server        → Phase 4
├── 03-sftp/         EDI claim files                       → Phase 6
├── 04-blob/         HL7 lab message files                 → Phase 6
├── 05-sharepoint/   two Excel workbooks                   → Phase 6
└── 06-reference/    lookup CSVs                           → Phase 6
```

**From here on you use `sources/`, not `data-generator/landing/`.** That is the single most important thing to understand about the layout, and it is what was unclear in my last message. `landing/` was an intermediate artifact.

---

# PHASE 3 — Stand up SQL Server  (~20 minutes)

Skip this on a first pass if you want to move fast.

```bash
cd sources/01-sqlserver
cp .env.example .env
```

Open `.env` and change the password. SQL Server 2022 rejects anything under 8 characters or missing mixed case, a digit, and a symbol — and it fails with a cryptic error rather than telling you the password is the problem.

```bash
./setup.sh
```

On Windows without a bash shell, run the four steps manually:

```powershell
docker compose up -d
# wait ~40 seconds for the instance to start, then:
docker exec ohn-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourPassword" -C -i /scripts/01_create_databases.sql
docker exec ohn-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourPassword" -C -i /scripts/02_create_tables.sql
docker exec ohn-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourPassword" -C -i /scripts/03_load_data.sql
docker exec ohn-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourPassword" -C -i /scripts/04_create_reader_login.sql
```

**Expect:** row counts for tables across `OHN_EHR`, `OHN_SCHED`, and `OHN_FIN`.

**Verify:**

```bash
docker exec ohn-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "YourPassword" -C \
  -Q "SELECT COUNT(*) FROM OHN_EHR.dbo.patient"
```

Should return roughly 26,000.

```bash
cd ../..
```

---

# PHASE 4 — Start the API  (~2 minutes)

Open a **second terminal** and leave it running.

```bash
cd sources/02-api
python api_server.py --data ./data --port 8000
```

**Expect:** `loaded medication-orders  ~100,000 records` and `listening on http://0.0.0.0:8000`.

**Verify** in your first terminal:

```bash
curl http://localhost:8000/health
```

Should return `{"status": "ok", ...}`.

Get a token and pull a page:

```bash
curl -s -X POST http://localhost:8000/oauth/token \
  -d 'grant_type=client_credentials&client_id=fabric&client_secret=fabric-dev-secret'
```

Copy the `access_token` value, then:

```bash
curl -s -H "Authorization: Bearer PASTE_TOKEN_HERE" \
  "http://localhost:8000/api/v1/medication-orders?limit=2"
```

Should return two medication orders plus a `pagination` block.

**Leave this terminal running for the rest of the project.**

---

# PHASE 5 — Create the Fabric workspace  (~15 minutes)

1. Go to **app.fabric.microsoft.com**
2. Top right → account manager → **Start trial** if you have no capacity
3. **Workspaces** → **New workspace** → name it `OHN-dev`
4. Expand **Advanced** → licence mode **Trial** or **Fabric capacity**

> Not Pro. Direct Lake will not be available later, and you will not find out until you are building the semantic model.

5. In `OHN-dev`: **New item** → **Lakehouse** → `lh_bronze`
6. Repeat for `lh_silver`, `lh_gold`, `lh_governance`

**Verify:** four lakehouses listed in the workspace.

---

# PHASE 6 — Upload the file-based sources  (~20 minutes)

Easiest route is **OneLake File Explorer** (download from Microsoft, sign in, and OneLake appears in Windows Explorer like OneDrive). Drag folders in and it syncs.

Upload into `lh_bronze` → `Files`:

| From | To |
|---|---|
| `sources/03-sftp/` | `Files/landing/CLAIMS/` |
| `sources/04-blob/lab/` | `Files/landing/LIS/lab/` |
| `sources/06-reference/` | `Files/reference/` |

Upload into `lh_governance` → `Files`:

| From | To |
|---|---|
| `data-generator/landing/_truth/` | `Files/truth/` |

> `_truth` is the answer key for patient matching. It is not a source system — no hospital has this file. It goes in governance so you can later measure how well your MPI did.

The two Excel files in `sources/05-sharepoint/` go to a SharePoint document library or OneDrive, not OneLake — they are ingested with Dataflow Gen2 in a later phase. You can park them for now.

**Verify** — new notebook in `OHN-dev`, attach `lh_bronze`, run:

```python
from notebookutils import mssparkutils
for f in mssparkutils.fs.ls("Files/landing"):
    print(f.name, f.isDir)
```

You should see `CLAIMS` and `LIS`.

---

# PHASE 7 — Connect Fabric to SQL Server  (~30 minutes)

Only if you did Phase 3.

1. Download and install the **on-premises data gateway** — standard mode, **not** personal mode. Personal mode cannot be used by Fabric pipelines.
2. Register it against your tenant.
3. In Fabric: **Settings** (gear) → **Manage connections and gateways** → **New**
   - Gateway cluster: yours
   - Connection type: **SQL Server**
   - Server: `localhost,1433`
   - Database: `OHN_EHR`
   - Authentication: **Basic**, username `fabric_reader`, password from `04_create_reader_login.sql`
   - Tick **trust server certificate** (the dev cert is self-signed)
4. **Create**, then repeat for `OHN_SCHED` and `OHN_FIN`.

**Verify:** each connection shows a green status.

If it fails: the gateway must reach SQL Server on *its own* machine. Same machine as Docker → `localhost,1433` is right. Different machine → use the Docker host's LAN IP and confirm port 1433 is open.

---

# PHASE 8 — Load your first table  (~20 minutes)

Do not build the whole orchestrator yet. Prove one path end to end first.

1. In `OHN-dev`: **New item** → **Data pipeline** → `pl_test_ehr_patient`
2. Add a **Copy data** activity
3. Source: your `OHN_EHR` connection, table `dbo.patient`
4. Destination: `lh_bronze`, table `ehr_patient`
5. **Run**

**Verify** in a notebook:

```python
df = spark.table("lh_bronze.ehr_patient")
print(f"{df.count():,} rows")
df.show(5)
```

Roughly 26,000 rows means the gateway, the connection, and the lakehouse all work together. That is the milestone worth stopping at.

---

# Where you are now

```
Generated data → split into 5 sources → SQL Server running
    → API running → Fabric workspace + 4 lakehouses
    → file sources uploaded → gateway connected → one table loaded
```

# What comes next

In order:

1. **Control framework** — create `ctl_source_registry`, `ctl_watermark`, `ctl_batch_log` in `lh_bronze` and seed the registry from `config/ctl_source_registry.csv`. This is what makes ingestion metadata-driven instead of one pipeline per table.
2. **Bronze ingestion** — `notebooks/10_bronze_ingest.py` for the file sources, a parameterised Copy activity loop for SQL Server, a REST Copy activity for the API.
3. **Parsers** — `notebooks/11_bronze_parse_hl7.py` and `12_bronze_parse_edi.py` for the lab and claims files.
4. **Silver + MPI** — `notebooks/20_silver_patient_mdm.py`. The hard part, and the interesting part.

---

# If something breaks

Tell me the phase number and paste the error. The most common ones:

| Symptom | Cause |
|---|---|
| `validate_data.py` reports FAIL | Usually too few patients. Use 25,000. |
| `split_sources.py` says "not found" | Wrong `--landing` path. It needs the folder containing `EHR/`, `SCHED/`, etc. |
| SQL Server container exits immediately | Weak password in `.env`. |
| Gateway connection test fails | Gateway is in personal mode, or `localhost` is wrong from the gateway's machine. |
| API returns 401 | Token expired — they last 15 minutes. Get a new one. |
