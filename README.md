# Qualifyze Data Engineer — Technical Case

## Overview

This pipeline ingests public compliance data from EudraGMDP — the European Medicines Agency's public database of GMP certificates and non-compliance reports — and integrates it with Qualifyze's internal site registry. The goal is to enrich internal site records with publicly available compliance information, making it accessible to Qualifyze customers.

EudraGMDP is the first of multiple planned public sources. The architecture is designed from the start to accommodate additional sources without structural changes.

---

## Architecture

The pipeline follows a **medallion architecture** with four layers: raw, bronze, silver, and gold.

```
EudraGMDP Excel export (local file)
            │
            ▼
    [ Python: file loader ]
    generate ingestion_batch_id (UUID)
    append all rows + batch_id to raw_eudragmdp
            │
            ▼
    ┌───────────────────────────────────┐
    │  raw_eudragmdp                    │
    │  append-only · full history       │
    │  source of truth for reprocessing │
    └───────────────────────────────────┘
            │
            ▼
    [ dbt run --vars batch_id ]
    reads only current batch from raw
            │
            ├──────────────────────────────────┐
            ▼                                  ▼
  bronze_eudragmdp                      bronze_rejected
  cast types · validate                 missing required fields
  append-only · full history            append-only · full history
  +ingestion_batch_id · +ingested_at    +ingestion_batch_id
            │
            ▼
    [ dbt models ]
    reads ALL bronze history
            │
            ├────────────────────────────────────┐
            ▼                                    ▼
  silver_eudragmdp_sites        silver_compliance_documents
  rebuild each run              rebuild each run
  deduplicated on               deduplicated on eudragmdp_doc_ref
  oms_location_id               max last_updated_date per cert
  one row per site              complete picture across all runs
            │
            ▼
    [ dbt models + ref_accepted_document_types seed ]
            │
            ├────────────────────────────┬──────────────────────┐
            ▼                            ▼                      ▼
  gold_master_site     gold_site_compliance_view      gold_ncr_alerts
  rebuild each run     rebuild each run               append-only + update
                       filtered by accepted           new NCRs inserted
                       doc types · customer-facing    status updated on notify
```

### Why raw is append-only and the source of truth

Raw is the absolute source of truth. Every ingestion appends rows with an `ingestion_batch_id` — nothing is ever deleted. If any downstream model fails or produces incorrect results, the entire pipeline can be rerun from raw without re-fetching the source file.

### Why bronze is also append-only

Bronze adds structural validation and type casting on top of raw. Being append-only means bronze accumulates validated, correctly typed data across all runs. Silver can always be rebuilt from the full bronze history to produce a complete picture of all known certificates — not just those from the latest run.

Example: if run 1 ingests January certificates and run 2 ingests February certificates, silver after run 2 contains all certificates from both runs. A certificate present in January but absent from the February file is not lost — it remains in bronze from run 1 and therefore in silver.

### Why silver rebuilds from all of bronze

Silver reads the entire bronze history and deduplicates:
- `silver_eudragmdp_sites` — one row per site, keeping the most recent version by `last_updated_date`
- `silver_compliance_documents` — one row per certificate, keeping the most recent version by `last_updated_date`

This produces a complete, current, deduplicated view of all known data regardless of how many runs have occurred or which certificates appeared in which run.

### Why document type filtering belongs in gold

Silver is a complete, generic, reusable layer. Business decisions about which document types are valid belong in the `ref_accepted_document_types` seed table, owned by the business team. When the accepted list changes, only gold is rerun.

### Why a common compliance document structure in silver

Qualifyze ingests compliance data from multiple sources. Silver normalises each source into a common schema so gold can union them without knowing source-specific details. Adding a new source requires only a new raw ingest and a new silver model — gold is never touched.

---

## Ingestion Batch ID and Source Tracking

Each source provider has its own independent Python script instance. When a new file arrives from EudraGMDP, its script runs independently from any FDA or other provider script. Each run generates its own UUID and identifies its source:

```python
import uuid
batch_id = str(uuid.uuid4())
source = 'eudragmdp'
```

Both `batch_id` and `source` are added to every row appended to `raw_eudragmdp`. The `batch_id` is passed to dbt as a variable. Bronze filters raw on both columns:

```bash
dbt run --vars '{"batch_id": "abc-123-...", "source": "eudragmdp"}'
```

```sql
-- bronze_eudragmdp
select ...
from raw_eudragmdp
where ingestion_batch_id = '{{ var("batch_id") }}'
and source = '{{ var("source") }}'
```

`source` is added for traceability — it makes it immediately clear which pipeline produced each row in raw and bronze, useful for debugging and auditing in a multi-source system.

Silver does not filter by batch — it reads all of bronze to build a complete picture across all runs.

---

## Data Model

### Raw

**`raw_eudragmdp`** — append-only store of every ingested file. One row per certificate per ingestion run. Never deleted. Source of truth for reprocessing. Dates stored as TEXT exactly as they arrive from Excel.

| Column | Type | Notes |
|--------|------|-------|
| ingestion_batch_id | TEXT | UUID generated per run by Python |
| source | TEXT | 'eudragmdp' — identifies the source pipeline |
| certificate_number | TEXT | |
| eudragmdp_doc_ref | TEXT | |
| document_type | TEXT | as-is from source |
| mia_number | TEXT | nullable |
| oms_organisation_id | TEXT | |
| oms_location_id | TEXT | |
| site_name | TEXT | |
| address_1 | TEXT | |
| address_2 | TEXT | nullable |
| address_3 | TEXT | nullable |
| city | TEXT | |
| postcode | TEXT | nullable |
| country | TEXT | |
| duns_number | TEXT | nullable |
| site_nca_reference | TEXT | nullable |
| inspection_end_date | TEXT | raw TEXT, cast in bronze |
| issue_date | TEXT | raw TEXT, cast in bronze |
| last_updated_date | TEXT | raw TEXT, cast in bronze |

### Bronze

**`bronze_eudragmdp`** — append-only store of structurally validated, correctly typed rows. Reads from raw filtered by current `batch_id`. Accumulates across all runs — silver rebuilds from the full history.

| Column | Type | Notes |
|--------|------|-------|
| ingestion_batch_id | TEXT | from --vars batch_id |
| ingested_at | TIMESTAMP | current_timestamp, same for all rows in run |
| source_file | TEXT | filename |
| certificate_number | TEXT | |
| eudragmdp_doc_ref | TEXT | natural key |
| document_type | TEXT | stored as-is, no filtering at this layer |
| mia_number | TEXT | nullable |
| oms_organisation_id | TEXT | |
| oms_location_id | TEXT | natural key for site |
| site_name | TEXT | |
| address_1 | TEXT | |
| address_2 | TEXT | nullable |
| address_3 | TEXT | nullable |
| city | TEXT | |
| postcode | TEXT | nullable |
| country | TEXT | |
| duns_number | TEXT | nullable |
| site_nca_reference | TEXT | nullable |
| inspection_end_date | DATE | cast from TEXT in raw |
| issue_date | DATE | cast from TEXT in raw |
| last_updated_date | DATE | cast from TEXT in raw |

**`bronze_rejected`** — append-only store of rows that failed structural validation.

| Column | Type | Notes |
|--------|------|-------|
| ingestion_batch_id | TEXT | same UUID as the run |
| raw_data | TEXT | full row as JSON |
| rejection_reason | TEXT | list of missing or unparseable fields |
| rejected_at | TIMESTAMP | |
| source_file | TEXT | |

### Silver

Silver models rebuild completely on every run, reading the full bronze history and deduplicating to produce a complete, current view of all known data.

**`silver_eudragmdp_sites`** — one row per unique physical site. Deduplicated on `oms_location_id`, keeping the most recent row by `last_updated_date`.

| Column | Type | Notes |
|--------|------|-------|
| oms_location_id | TEXT | PK, natural key from EudraGMDP |
| oms_organisation_id | TEXT | |
| site_name | TEXT | |
| address_1 | TEXT | |
| address_2 | TEXT | nullable |
| address_3 | TEXT | nullable |
| city | TEXT | |
| country | TEXT | |
| qualifyze_site_id | TEXT | FK to internal registry, nullable until matched |
| match_confidence | TEXT | exact / fuzzy / unmatched |

**`silver_compliance_documents`** — one row per certificate. Deduplicated on `eudragmdp_doc_ref`, keeping the most recent version by `last_updated_date`. Complete picture across all runs — certificates from earlier runs are preserved even if absent from the latest file.

| Column | Type | Notes |
|--------|------|-------|
| source | TEXT | 'eudragmdp' |
| source_document_id | TEXT | eudragmdp_doc_ref |
| qualifyze_site_id | TEXT | FK to master_site, nullable |
| oms_location_id | TEXT | FK to silver_eudragmdp_sites |
| document_type | TEXT | all types, no filtering at this layer |
| inspection_date | DATE | |
| issue_date | DATE | |
| last_updated_date | DATE | |
| severity | TEXT | nullable |
| outcome | TEXT | nullable |

### Reference Data

**`ref_accepted_document_types`** — dbt seed file owned by the business team. Controls which document types are surfaced to customers in gold. Currently seeded with GMPC and GMPNC. When a new type is validated, it is added here and gold is rerun — no other reprocessing needed.

| Column | Type | Notes |
|--------|------|-------|
| document_type | TEXT | e.g. GMPC, GMPNC |
| added_at | DATE | |
| added_by | TEXT | |

### Gold

**`gold_master_site`** — canonical site entity. Rebuild each run. In this exercise populated from mock internal data.

| Column | Type | Notes |
|--------|------|-------|
| qualifyze_site_id | TEXT | PK |
| canonical_name | TEXT | |
| canonical_address | TEXT | |
| city | TEXT | |
| country | TEXT | |

**`gold_site_compliance_view`** — customer-facing view. Rebuild each run. All sites from `gold_master_site` are always visible to customers regardless of which certification providers have data for them. EudraGMDP compliance data is attached where a match exists, NULL where it does not. As new sources are onboarded (FDA, internal audits, other registries), their silver models are unioned into the same view — customers always see a complete picture of their site across all providers.

The join chain is: `gold_master_site` → `silver_eudragmdp_sites` (on `qualifyze_site_id`, populated by matching) → `silver_compliance_documents` (on `oms_location_id`). Filtered to accepted document types via `ref_accepted_document_types`.

| Column | Type | Notes |
|--------|------|-------|
| qualifyze_site_id | TEXT | always present — from gold_master_site |
| canonical_name | TEXT | always present |
| country | TEXT | always present |
| source | TEXT | nullable — NULL if no EudraGMDP match |
| source_document_id | TEXT | nullable |
| document_type | TEXT | accepted types only, nullable |
| inspection_date | DATE | nullable |
| issue_date | DATE | nullable |
| severity | TEXT | nullable |
| outcome | TEXT | nullable |

**`gold_ncr_alerts`** — append-only with updates. New NCRs are inserted. `notified_at` and `notification_status` are updated when notification is sent. Never rebuilt — preserves full alert history.

| Column | Type | Notes |
|--------|------|-------|
| source_document_id | TEXT | PK |
| qualifyze_site_id | TEXT | |
| document_type | TEXT | GMPNC |
| detected_at | TIMESTAMP | first run where this NCR appeared |
| notified_at | TIMESTAMP | nullable until notification sent |
| notification_status | TEXT | pending / sent / failed |

---

## Monitoring Unknown Document Types

Unknown document types pass through raw and bronze but are filtered out of gold. A monitoring query surfaces them for the data curation team:

```sql
select distinct document_type, count(*) as occurrences
from silver_compliance_documents
where document_type not in (
    select document_type from ref_accepted_document_types
)
group by document_type
```

When a type is validated, it is added to `ref_accepted_document_types` and gold is rerun. No other changes needed.

---

## Matching Logic

EudraGMDP identifies sites using `oms_location_id`. Qualifyze's internal registry uses `qualifyze_site_id`. These are different identifiers for potentially the same physical sites.

The matching runs in two passes:

**Pass 1 — exact match** on `oms_location_id` against the internal registry.

**Pass 2 — fuzzy match** on `site_name` + `country` using string similarity. Results above a defined threshold are marked `match_confidence = fuzzy` and flagged for human review.

Sites with no match are inserted with `qualifyze_site_id = NULL` and `match_confidence = unmatched`. Creation of new internal site records is a business decision outside the scope of this pipeline.

---

## NCR Alerting (Part 2)

Non-Compliance Reports (document_type = GMPNC) are detected by comparing silver compliance documents against `gold_ncr_alerts`. A document whose `source_document_id` does not exist in `gold_ncr_alerts` is treated as a new NCR, inserted with `notification_status = pending`, and a notification is dispatched to stakeholders.

NCR alerting is a natural extension of the same pipeline, running as a final step after gold models complete.

---

## Assumptions

1. **Source file is provided locally.** Header at row 5. In production this would be replaced by automated fetching from EudraGMDP. A `fetch_source(url)` stub is included.

2. **Raw and bronze are append-only.** Nothing is ever deleted. Raw is the source of truth for reprocessing. Bronze accumulates validated data across all runs, enabling silver to always produce a complete picture.

3. **Silver rebuilds from the full bronze history on every run.** This ensures certificates from earlier runs are never lost, even if absent from the latest file.

4. **A `source` column is added to every raw row alongside `batch_id`.** This identifies which pipeline ingested the row (e.g. `'eudragmdp'`). Bronze filters on both `batch_id` and `source` to prevent cross-source contamination in multi-source scenarios.

5. **No warnings are raised for any missing fields.** Fields consistently sparse in the source (`Postcode` 0/45, `Address 4` 0/45, `DUNS Number` 1/45, `Address 3` 5/45, `MIA Number` 32/45) are treated as optional by design based on observed data patterns. Fields required for structural validity route the row silently to `bronze_rejected`. The pipeline never raises partial warnings on individual rows — a row is either valid or rejected.

6. **The definition of required vs optional fields is an open task** to be defined together with the business and data curation team. The current set of required fields (`oms_location_id`, `eudragmdp_doc_ref`, `site_name`, `country`, `document_type`, `inspection_end_date`, `issue_date`) was chosen based on what is necessary for identification and basic downstream utility. Fields that appear sparse in the sample may carry more business value than they appear — this should be validated with domain experts before finalising.

7. **Document type filtering is a business decision.** Raw, bronze, and silver accept all document types. Gold filters via `ref_accepted_document_types`. Currently seeded with GMPC and GMPNC. When a new type is validated, gold is rerun — no other reprocessing needed.

8. **Hard stop on infrastructure errors only.** The pipeline stops if the source file is missing, the file structure has changed incompatibly, or the database is inaccessible. Data anomalies never stop the pipeline.

9. **All sites in `gold_master_site` are always visible to customers.** EudraGMDP compliance data is attached where a match exists. Sites with no match show NULL compliance columns — they are not hidden.

10. **Matching uses a mock internal registry** of 10 sites. In production this would query Qualifyze's actual internal site registry.

11. **Creation of new internal site records is out of scope.** When no match is found, `qualifyze_site_id` is left NULL. This is a business decision.

12. **Orchestration is a single Python script for this exercise.** `run_pipeline.py` accepts `--source` and `--file` arguments making it reusable across sources. In production, ingest and bronze would run as independent Dagster jobs per source, while silver and gold would run as shared jobs triggered after all sources complete — avoiding redundant rebuilds.

13. **SQLite is used as the database.** In production this would be PostgreSQL. Migration is a connection string change.

14. **EudraGMDP is the first of multiple sources.** Adding a new source requires only a new raw ingest and a new silver model.

15. **NCR change detection is based on first appearance.** Updates to existing NCRs are not tracked in this version.

---

## Setup and Run

**Requirements:** Python 3.10+, dbt-core, dbt-sqlite

```bash
pip install -r requirements.txt
python run_pipeline.py --source eudragmdp --file data/raw/searchGMPCExport_5451317683799439071.xls
```

The pipeline will:
1. Generate `ingestion_batch_id` (UUID)
2. Read Excel from the provided `--file` path (header at row 5)
3. Append all rows + `batch_id` + `source` to `raw_eudragmdp`
4. Run dbt bronze — filter current batch and source from raw, cast types, validate
5. Run dbt silver — rebuild from full bronze history across all sources, deduplicate
6. Run matching against mock internal registry
7. Run dbt gold — rebuild compliance view, detect new NCRs

Results are written to `data/qualifyze.db` (SQLite).

**Adding a new source** (e.g. FDA) requires only:
```bash
python run_pipeline.py --source fda --file data/raw/fda_export.xlsx
```
Silver and gold automatically include the new source on the next run.

---

## Production Orchestration Design

In this exercise, `run_pipeline.py` runs everything end to end in a single script — sufficient for demonstration with one source. In production with Dagster, the pipeline would be split into independent jobs:

```
Per-source jobs (one per provider):
  EudraGMDP job:  ingest → dbt bronze_eudragmdp
  FDA job:        ingest → dbt bronze_fda
  ...

Shared jobs (run after all sources complete):
  Silver job:     dbt silver_eudragmdp_sites
                  dbt silver_compliance_documents
                  (triggered after any bronze job, or on a fixed schedule)

  Gold job:       dbt gold_site_compliance_view
                  dbt gold_ncr_alerts
                  (triggered after silver completes)
```

This separation means:
- Each source ingests independently without waiting for others
- Silver and gold are not rebuilt redundantly for every source — they run once after all sources have completed
- Adding a new source means adding one new Dagster job for its bronze layer — silver and gold jobs are untouched

---

## Improvements and Scale

**Automated source fetching.** Replace local file with scheduled fetching from EudraGMDP using `last_updated_date` as watermark.

**Config-driven ingestion.** Currently source and file path are passed as CLI arguments. A config file per source (YAML) would allow each source to define its own header row, column mapping, and file pattern — enabling zero-code onboarding of new sources.

Example:
```yaml
# config/eudragmdp.yml
source: eudragmdp
file_pattern: "data/raw/searchGMDP*.xls"
header_row: 4
column_mapping:
  "OMS Location Identifier": oms_location_id
  ...
```

**Incremental silver.** Currently silver rebuilds from all of bronze on every run. At scale, an incremental approach processing only new or changed bronze rows would reduce compute time significantly.

**Raw and bronze retention policy.** Both layers grow indefinitely. A retention policy would be needed at scale to control storage costs, though current data volumes make this a future concern.

**Change history (SCD Type 2).** Track how site data changes over time — useful for compliance audits.

**Distinction between rejection categories.** `bronze_rejected` currently treats all missing required fields equally. Future improvement: distinguish between fields critical for identification and fields critical for business utility.

**Source metadata.** A `source_metadata` JSON column in `silver_compliance_documents` would preserve source-specific fields without polluting the common schema.

**PostgreSQL and Dagster.** Production deployment would replace SQLite with PostgreSQL and the Python script with the Dagster orchestration design described above.

**dbt tests in CI.** dbt tests run automatically on every push to a feature branch via GitHub Actions. Merges to main are blocked if any test fails. This was standard practice in previous roles and ensures data quality regressions are caught before reaching production.

**Fuzzy matching threshold tuning.** Calibrate threshold against confirmed matches and mismatches as data grows.

**NCR update tracking.** Detect updates to existing NCRs using `last_updated_date` as change indicator.

---

## Future: Site Identity and Curation

One of the core challenges at Qualifyze — as became clear from understanding the business — is that the same physical site can arrive from different sources with slightly different names, addresses, or identifiers. EudraGMDP might call it "Goodwill Pharma Plc.", while an internal audit record says "Goodwill Pharmaceutical PLC", and an FDA record says "Goodwill Pharma". These are the same place, but the pipeline has no way to know that automatically.

The current matching logic handles the simple case — exact match on a known identifier, fuzzy match on name and country. But at scale, with many sources and millions of records, a more structured approach is needed.

**The problem in plain terms:**

When a new site record arrives, there are four possible situations:
- It is clearly the same site as one we already know — same company, minor spelling difference
- It is the same company but a different physical location — a branch or subsidiary
- It looks similar but is actually a different company entirely
- We genuinely cannot tell without more context

Each situation requires a different action, and not all of them can be decided by an algorithm alone.

**A possible approach:**

Rather than trying to match everything automatically and risking incorrect merges, new site records could go through a two-step process:

First, an automated similarity check runs when a new record arrives — comparing it against known sites using name similarity, address proximity, and country. Records that look like a likely match are flagged for human review. Records that are clearly new pass directly into the normal pipeline.

Second, a small review queue holds the flagged records. A person — ideally someone with domain knowledge of the pharma industry — looks at each flagged record and makes one of four decisions: confirm it is a duplicate and merge it, link it as a related site (branch of a known company), confirm it is a separate entity, or defer it if more information is needed.

Once reviewed, the decision is recorded and the record either merges into an existing site or enters the pipeline as a new one.

**Why this matters for the current pipeline:**

The `match_confidence` column already exists in `silver_eudragmdp_sites` — records marked `fuzzy` or `unmatched` are exactly the candidates that would feed this review queue. The infrastructure is partly there; what is missing is the queue itself and the review workflow.

This would be a natural next step after the current pipeline is stable, and something to define together with the data curation and product teams — since the decision of what counts as "the same site" is ultimately a business question, not a technical one.

