# Ontario Healthcare Network — Fabric Analytics Platform

Governed healthcare analytics platform on Microsoft Fabric, consolidating patient, appointment, admission, laboratory, medication, billing, insurance, doctor, department, and hospital data into a trusted dimensional model.

> **New here? Start with [QUICKSTART.md](QUICKSTART.md).** It is an ordered runbook with exact commands. Everything else is reference.

> **Data privacy.** Synthetic data only. No real patient or confidential information is used at any stage.

---

## The four commands that get you started

```bash
cd data-generator
python ohn_generator.py --patients 25000 --out ./landing
python validate_data.py --landing ./landing

cd ../sources
python split_sources.py --landing ../data-generator/landing --out . --clean
```

`data-generator/landing/` is **intermediate** — raw material for the splitter. After the split you work entirely from `sources/`, which is what feeds Fabric, SQL Server, and the API.

---

## File index

### Start here
| File | What it is |
|---|---|
| `QUICKSTART.md` | Ordered runbook, 8 phases, exact commands and expected output |
| `README.md` | This index |

### Generate and split the data
| File | What it is |
|---|---|
| `data-generator/ohn_generator.py` | Creates 25 source entities (~1.25M rows) with deliberate defects and planted correlations. Standard library only |
| `data-generator/validate_data.py` | Verifies the defects and correlations survived generation. Must pass before you load anything |
| `sources/split_sources.py` | Splits the flat CSVs into five source systems in their native formats |

### The five source systems
| File | What it is |
|---|---|
| `sources/01-sqlserver/setup.sh` | Stands up SQL Server 2022 in Docker, creates 3 databases, bulk-loads, creates a read-only login |
| `sources/01-sqlserver/.env.example` | Copy to `.env` and set a strong password before running |
| `sources/02-api/api_server.py` | REST API with OAuth2, cursor pagination, rate limiting, optional chaos mode. No dependencies |
| `sources/02-api/README.md` | How to run and test the API |
| `sources/02-api/Dockerfile` · `docker-compose.yml` | Containerised API |

The SQL scripts, EDI files, HL7 files, Excel workbooks, and reference CSVs are **generated** by `split_sources.py` into `sources/01-sqlserver/` … `06-reference/`.

### Fabric notebooks
| File | Layer | What it does |
|---|---|---|
| `notebooks/00_common_utils.py` | shared | Hashing, watermarks, SCD2 merge, temporal key lookup. `%run` this from the others |
| `notebooks/10_bronze_ingest.py` | Bronze | Metadata-driven landing → Delta, with duplicate-file detection |
| `notebooks/11_bronze_parse_hl7.py` | Bronze | HL7 v2 ORU^R01 parser. Verified against generated messages |
| `notebooks/12_bronze_parse_edi.py` | Bronze | X12 837P/835 parser with claim reconciliation. Verified against source CSVs |
| `notebooks/20_silver_patient_mdm.py` | Silver | Patient standardization and duplicate resolution. The hard part |
| `notebooks/30_gold_dimensions.py` | Gold | SCD1 and SCD2 conformed dimensions |
| `notebooks/40_gold_fact_admission.py` | Gold | Admission fact with length-of-stay and readmission logic |
| `notebooks/50_dq_engine.py` | all | Metadata-driven data quality engine with quarantine |

### Warehouse and semantic model
| File | What it is |
|---|---|
| `warehouse/ddl/01_gold_schema.sql` | Dimensions, facts, governance tables |
| `warehouse/ddl/02_security.sql` | RLS predicates, masking, roles, grants, de-identified view |
| `semantic-model/measures.dax` | All measures, with the denominator choices documented inline |
| `semantic-model/model-design.md` | Relationships, RLS/OLS roles, 13 report page specs |

### Configuration
| File | What it is |
|---|---|
| `config/ctl_source_registry.csv` | One row per source entity — drives metadata-driven ingestion |
| `config/dq_rules.csv` | 36 seed data-quality rules |
| `config/ref_code_mapping.csv` | Source code → standard code mappings |

### Documentation and tests
| File | What it is |
|---|---|
| `docs/01-solution-design.md` | Requirements, architecture, MDM design, security, DQ framework, CI/CD |
| `docs/02-data-model-and-mapping.md` | Dimensional model and source-to-target mapping |
| `docs/03-step1-data-and-fabric-setup.md` | Data generation and Fabric setup in depth |
| `docs/04-multi-source-setup.md` | Five sources and their Fabric connector configuration |
| `tests/test_transformations.py` | Unit tests for standardization and business rules |

---

## Build order

1. **Data** — generate, validate, split (QUICKSTART Phases 1–2)
2. **Sources** — SQL Server, API, file uploads (Phases 3–6)
3. **Fabric** — workspace, lakehouses, gateway, first Copy activity (Phases 5, 7–8)
4. **Control framework** — `ctl_source_registry`, `ctl_watermark`, `ctl_batch_log`
5. **Bronze** — ingestion plus the HL7 and EDI parsers
6. **Silver** — reference standardization, then the patient MPI
7. **Gold** — dimensions before facts
8. **Semantic model and reports**

Resist building reports early. Every report depends on Gold, Gold depends on conformed dimensions, and dimensions depend on patient identity being resolved. Get identity right first.

---

## Design decisions worth knowing

**Patient identity is resolved once, in Silver.** Every fact resolves its patient through `patient_xref`, never a source system's own id. This is what makes cross-facility readmission and the consolidated patient view work at all — and with three separate source databases sharing no key, it is a real problem rather than a formality.

**Metric denominators are opinionated and documented.** Readmission rate divides by index admissions; no-show rate excludes cancellations; claim approval rate divides by adjudicated claims; occupancy rate excludes blocked beds. Stated next to each measure so disagreement becomes a conversation about definition.

**Bad data is quarantined, not dropped.** Rows failing an Error-severity rule land in a quarantine table with the failing rule id and replay once corrected upstream. Row counts always reconcile to source.

**Unresolved keys point at an unknown member.** Facts are never dropped for a missing lookup; the gap shows up as a DQ measure instead of a quiet undercount.

**Direct identifiers never reach Gold.** Health card numbers are tokenized in Silver with a Key Vault salt. Clear values exist only in `lh_silver.patient_pii`, restricted to stewards at the OneLake data-access-role level.

**Key Vault is skipped for the API client secret, on purpose.** The REST API's OAuth secret runs as a local dev default (`fabric-dev-secret`) instead of an Azure Key Vault-backed one — defensible for a demo reachable only through the on-prem gateway, never publicly exposed. See `docs/04-multi-source-setup.md` for the production alternative.

**The synthetic data is deliberately imperfect.** Clean data would make the MPI match nothing, every DQ rule pass at 100%, and every report a flat line — the platform would look like it works while proving nothing.
