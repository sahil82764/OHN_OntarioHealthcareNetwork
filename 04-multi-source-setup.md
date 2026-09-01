# Step 2 — Five source systems and their Fabric connectors

The generated data is now split across five systems that each present a different ingestion problem. This is the part of the project that demonstrates real capability: anyone can point a Copy activity at a CSV folder, but a gateway connection, a paginated API with expiring tokens, and two non-tabular formats are what a hospital data platform actually faces.

Run the split first:

```bash
cd sources
python split_sources.py --landing ../data-generator/landing --out . --clean
```

---

## What went where, and why

| # | Source | Entities | Format | Fabric connector | The problem it creates |
|---|--------|----------|--------|------------------|------------------------|
| 1 | On-prem SQL Server | patient, admission, bed_assignment, diagnosis, emergency_visit, appointment, invoice… | 3 relational databases | Copy activity + on-premises data gateway | Network boundary, watermark state, schema drift |
| 2 | REST API | medication_order, hospital, department, bed | Paginated JSON | Copy activity, REST source | Expiring tokens, cursors, 429s, partial failure |
| 3 | SFTP drop | claim_header, claim_line | X12 EDI 837P / 835 | Binary copy + Spark parser | Not tabular; stateful segment grammar |
| 4 | Azure Blob | lab_result | HL7 v2 ORU^R01 | OneLake shortcut + Spark parser | Nested repeating segments, one file : many messages |
| 5 | SharePoint | doctor, survey_response | Excel | Dataflow Gen2 | Human-maintained: title rows, type drift |
| — | Reference | ICD-10-CA, LOINC, DIN, code mappings | CSV | Manual upload | Slowly changing, version-controlled |

Note the deliberate consequence: **patient identity is now genuinely fragmented across three transport mechanisms**. The same person is a row in `OHN_EHR.patient`, a row in `OHN_SCHED.patient`, and a row in `OHN_FIN.patient_account`. Your MPI has to reconcile them across systems that never share a key — which is exactly the situation it was designed for, and which the single-folder version of this project never actually tested.

---

## Source 1 — On-prem SQL Server

### Stand it up

```bash
cd sources/01-sqlserver
cp .env.example .env      # edit the password first
./setup.sh
```

This runs SQL Server 2022 in Docker, creates `OHN_EHR`, `OHN_SCHED`, and `OHN_FIN`, creates the tables with explicit column types, bulk-loads the CSVs, indexes the watermark columns, and creates a read-only `fabric_reader` login.

Three details worth noticing in the generated DDL:

**Types are declared, not inferred.** `health_card_number` is `VARCHAR(20)`. Let SQL Server or a Copy activity infer it and you get a `BIGINT` that has silently eaten every leading zero, and no error anywhere.

**Watermark columns are indexed.** Every incremental run filters on `last_modified_ts`. Without the index that is a full scan of a 200k-row table on every pull, several times an hour.

**Dates stay as `VARCHAR`.** The generator emits three different date formats across the three systems on purpose. Parsing them at the source would discard the very inconsistency your Silver layer is meant to resolve.

### Connect Fabric to it

An on-prem source needs the **on-premises data gateway** — Fabric cannot reach `localhost` on your machine.

1. Download and install the standard gateway (not personal mode — personal mode cannot be used by Fabric pipelines).
2. Register it against your tenant and give it a recognisable name.
3. In Fabric: **Settings → Manage connections and gateways → New**
   - Gateway cluster: yours
   - Connection type: SQL Server
   - Server: `localhost,1433` (as seen *from the gateway machine*)
   - Database: `OHN_EHR` — create one connection per database
   - Authentication: Basic, `fabric_reader`
   - Encryption: enable, and tick trust server certificate for the self-signed dev cert

If the gateway runs on the same machine as Docker, `localhost,1433` is correct. If it runs elsewhere, use the Docker host's LAN address and confirm port 1433 is reachable.

### The incremental pattern

In your pipeline, per table:

1. **Lookup** — read the last watermark: `SELECT MAX(watermark_value) FROM ctl_watermark WHERE entity_name = 'EHR.patient'`
2. **Copy activity** — source query:
   ```sql
   SELECT * FROM dbo.patient
   WHERE last_modified_ts > '@{activity('GetWatermark').output.firstRow.wm}'
     AND last_modified_ts <= '@{pipeline().TriggerTime}'
   ```
3. **Stored procedure / notebook** — write the new watermark only after the copy succeeds

The upper bound matters. Without `<= TriggerTime`, rows written to the source *during* the copy are read in this run but the watermark advances past them, and they are never re-read. That is a silent data-loss bug that only shows up under load.

---

## Source 2 — REST API

### Start it

```bash
cd sources/02-api
python api_server.py --data ./data --port 8000
```

Or containerised: `docker compose up -d`.

Verify:
```bash
curl -s -X POST localhost:8000/oauth/token \
  -d 'grant_type=client_credentials&client_id=fabric&client_secret=fabric-dev-secret'
```

The API implements OAuth2 client credentials with 15-minute tokens, keyset cursor pagination, `updated_since` filtering, and rate limiting at 120 requests per minute with `Retry-After`.

**Keyset, not offset, pagination** is deliberate. `OFFSET` shifts under concurrent writes and the pipeline skips rows without ever raising an error. The cursor encodes the last `(timestamp, id)` seen, so pages are stable.

### Expose it to Fabric

Fabric cannot reach your laptop. Three options, in order of preference for a portfolio project:

- **Azure Container Apps** — build the Dockerfile, push to ACR, deploy. Gives a real public HTTPS endpoint, costs very little, and looks like an actual integration.
- **Dev tunnel or ngrok** — fastest path, fine for a demo, but the URL rotates.
- **The on-prem gateway** — the same gateway from Source 1 can proxy a local HTTP endpoint. No public exposure, which is the most defensible choice if anyone asks about security.

### Copy activity configuration

Source type **REST**, base URL `https://<your-host>/api/v1/`.

Authentication: the connector has no client-credentials grant, so acquire the token in a **Web activity** first and pass it through:

```
POST https://<host>/oauth/token
Body: grant_type=client_credentials&client_id=fabric&client_secret=@{...}
```

Store the secret in **Azure Key Vault** and reference it. Do not paste it into the pipeline JSON — that JSON goes into Git.

> **Security note — Key Vault skipped for this build.** This project runs the client-credentials secret (`fabric-dev-secret`) as a local development default instead of provisioning Azure Key Vault. That's a deliberate simplification for a local, no-public-exposure demo (API reachable only through the on-prem gateway proxy, never over the public internet) — not an oversight. It is stated here explicitly rather than left as a silent gap: a production build would put this secret in Key Vault and reference it from a Fabric Key Vault connection, exactly as described above, never pasted into pipeline JSON.

Then in the Copy activity:
- Relative URL: `medication-orders?limit=1000&updated_since=@{variables('watermark')}`
- Additional header: `Authorization: Bearer @{activity('GetToken').output.access_token}`
- **Pagination rule**: `AbsoluteUrl` → `$.pagination.next_url`
- Retry: 3 attempts, 30-second interval — needed for the 429s and, if you enable chaos mode, the 503s

Test your retry policy honestly:

```bash
python api_server.py --data ./data --port 8000 --chaos 0.08
```

8% of requests then fail or stall. A pipeline that has never seen a 503 has not been tested against one.

---

## Source 3 — SFTP, X12 EDI

The splitter writes `outbound/` (837P claim submissions) and `inbound/` (835 remittances).

Host them on any SFTP server — a container running `atmoz/sftp` is enough, or upload to Blob Storage and use the Blob connector if you would rather not run one.

Ingestion is two stages:

1. **Copy activity, binary mode**, SFTP → `Files/landing/CLAIMS/837/` and `/835/`. Binary, because parsing at copy time would mean writing an EDI parser in a mapping expression.
2. **Notebook** `12_bronze_parse_edi.py` walks the segments.

### Why this needs a parser

X12 segments carry no keys. An `SV1` service line belongs to whichever `CLM` was seen most recently; a `CAS` adjustment belongs to the open `CLP`. Filter rather than walk, and every service line in the file attaches to the last claim — the output looks plausible and is completely wrong.

The delimiters are declared in the `ISA` header itself, not fixed by the standard: byte 4 is the element separator, byte 106 the segment terminator. The parser reads them rather than assuming `*` and `~`.

The parser is verified against the source CSVs: 100% of claim numbers resolve, zero billed-amount mismatches, and zero line-count mismatches — that last check is what proves lines attached to the right claim.

---

## Source 4 — Azure Blob, HL7 v2

Upload `sources/04-blob/lab/` to an ADLS Gen2 container preserving the `YYYY/MM/DD` structure, then create a **OneLake shortcut** rather than copying:

`lh_bronze` → **New shortcut** → **ADLS Gen2** → target `Files/shortcuts/lab-blob`

A shortcut means no copy, no duplicate storage, and no staleness. It is the right answer whenever the source is already in a format OneLake can read and you own the container.

Then run `11_bronze_parse_hl7.py`.

### Why this needs a parser

Read the file as lines and you lose message boundaries — segments are `\r`-separated and a message is a run of segments starting at `MSH`. Once split, an `OBX` result has no way back to its `OBR` order.

About 20% of messages carry more than one `OBX` (a chemistry panel, a CBC). A parser that assumes one result per message silently drops a fifth of your lab data, and the turnaround report looks fine.

Field access is positional and 1-based: `OBX-5` is the value, `OBX-8` the abnormal flag, `OBX-11` the status, `OBX-14` the timestamp. One extra delimiter shifts everything right and the flags come back empty — which is exactly the bug that showed up the first time the writer was built, caught only because the parser was tested against real output rather than assumed correct.

---

## Source 5 — SharePoint, Excel

Upload `doctor_roster.xlsx` and `survey_response_export.xlsx` to a SharePoint document library or OneDrive.

Ingest with **Dataflow Gen2** → SharePoint folder connector → destination `lh_bronze`.

These workbooks have a title block in rows 1–3 and headers in row 4, because that is what human-maintained spreadsheets look like. Power Query will confidently take row 1 as the header. In the query: **Remove Top Rows → 3**, then **Use First Row as Headers**.

Also set explicit column types. Excel stores numeric-looking text as numbers, so a doctor ID like `00123` arrives as `123`. Everything downstream then fails to join, quietly.

---

## Reference data

Upload `sources/06-reference/*.csv` to `lh_bronze/Files/reference/`. Small, slowly changing, and version-controlled in Git alongside the code — a diff on `code_mapping.csv` should be reviewable.

---

## Update the source registry

Metadata-driven ingestion needs the registry to know how each entity arrives. `ctl_source_registry` gains a `connection_type` column:

| source_system | entity | connection_type | load_type | watermark_column |
|---|---|---|---|---|
| EHR | patient | sqlserver_gateway | incremental | last_modified_ts |
| SCHED | appointment | sqlserver_gateway | incremental | modified_at |
| FIN | invoice | sqlserver_gateway | incremental | update_dt |
| PHARM | medication_order | rest_api | incremental | updated_at |
| FACIL | bed | rest_api | full_snapshot | update_ts |
| CLAIMS | claim_837 | sftp_edi | file_arrival | — |
| LIS | lab_result | blob_hl7 | file_arrival | — |
| HR | doctor | sharepoint_excel | full_snapshot | — |
| SURVEY | survey_response | sharepoint_excel | incremental | response_date |

The orchestrator branches on `connection_type` with a Switch activity, so adding a source stays a configuration change.

---

## Checkpoint

- `docker exec ohn-sqlserver ... SELECT COUNT(*) FROM OHN_EHR.dbo.patient` returns rows
- `curl` gets a token and a page of medication orders
- Gateway connection tests green in Fabric for all three databases
- OneLake shortcut lists HL7 files
- Both parser notebooks run and their reconciliation checks pass

---

## One honest caveat

Five sources is more setup than a single folder, and if your goal is a working pipeline by the weekend, three is enough — SQL Server, the API, and one file-based source. The Excel and SFTP sources add breadth rather than depth.

Where the real difficulty lies is Source 1 plus the MPI. Reconciling patient identity across three databases that share no key is the hard problem, and it is the one worth being able to talk about.
