# CI Failure Atlas Design

Status: current architecture snapshot  
Last updated: 2026-03-16

## Purpose

CI Failure Atlas ingests CI run/failure data, builds semantic failure clusters, and serves two operator-facing outputs:

- a static triage site (weekly + global triage reports), and
- a local review app for human Phase3 linking.

The current implementation is optimized for local operation and fast iteration while preserving deterministic outputs and a clean path to a database-backed architecture.

## Semantic Phase Definitions

The semantic pipeline is split into three phases:

1. **Phase1 (normalize + test-scoped clustering)**
   - Builds enriched failure evidence from ingested facts.
   - Normalizes failure text and classifies failures into deterministic test-scoped clusters.
   - Produces semantic artifacts centered on `phase1_workset`, `test_clusters`, and `review_queue`.

2. **Phase2 (global merge)**
   - Merges phase1 test-scoped clusters into global failure signatures.
   - Produces `global_clusters` as the main static triage input.

3. **Phase3 (human linking and reconciliation)**
   - Human-in-the-loop linking of semantically equivalent phase2 signatures.
   - Uses stable row-level anchors (`environment + run_url + row_id`) and durable link state.
   - Applied as a materialized view in the review app and during site build.

## Current Architecture Snapshot

The system has three active planes:

1. **Controllers (ingestion + derived facts)**
   - `source.sippy.runs`
   - `source.sippy.tests-daily`
   - `source.github.pull-requests`
   - `source.prow.failures`
   - `facts.runs`
   - `facts.raw-failures`
   - `metrics.rollup.daily`
   - Runtime entrypoint: `cfa run`

2. **Semantic materialization**
   - Triggered by `cfa report site build` (or reused via `--from-existing`)
   - Phase1 and phase2 execute in memory during site build for performance
   - Core semantic artifacts persisted by default:
     - `phase1_workset.ndjson`
     - `test_clusters.ndjson`
     - `review_queue.ndjson`
     - `global_clusters.ndjson`
     - `window_metadata.json`

3. **Product surfaces**
   - Static site:
     - Weekly report (`weekly-metrics.html`)
     - Global signature triage (`global-signature-triage.html`)
   - Dynamic review app:
     - `cfa report review` for Phase3 manual linking and cross-week propagation

## Storage Model (Current)

Storage is file-based NDJSON behind interfaces in `pkg/store/contracts` and implemented in `pkg/store/ndjson`.

Primary layout:

```text
data/
  facts/
    runs.ndjson
    artifact_failures.ndjson
    raw_failures.ndjson
    metrics_daily.ndjson
    test_metadata_daily.ndjson
    pull_requests.ndjson
  semantic/<week>/
    phase1_workset.ndjson
    test_clusters.ndjson
    review_queue.ndjson
    global_clusters.ndjson
    window_metadata.json
  state/
    checkpoints.ndjson
    dead_letters.ndjson
    phase3/
      links.ndjson
      issues.ndjson
      events.ndjson
```

## Current Command Surface

- `cfa run`  
  Runs all controllers continuously.

- `cfa report site build`  
  Builds semantic outputs (unless `--from-existing`) and generates weekly/global site pages.

- `cfa report site run`  
  Serves generated static site locally.

- `cfa report site push`  
  Uploads site content to Azure static website storage.

- `cfa report review`  
  Runs the local Phase3 review application.

## Key Decisions (Current)

1. **Core product scope is intentionally narrow**: controllers + site weekly/global triage + review app.
2. **Single history knob**: `history.weeks` is used consistently across ingestion/report history semantics.
3. **Phase3 source of truth is durable links** (`state/phase3/links.ndjson`), not pre-collapsed weekly assets.
4. **Row-level anchors are mandatory** for Phase3 durability (`environment + run_url + row_id`).
5. **Phase3 aggregation is a materialized view**:
   - at runtime in review app,
   - at build-time when generating site reports.
6. **Performance-first pipeline**:
   - in-memory phase1->phase2 in site build,
   - bulk reads and in-memory indexing in hot report paths,
   - reduced default semantic artifact persistence.
7. **Storage remains abstracted by contracts**, preserving portability to future backends.

## Future Architecture: Go Web App + PostgreSQL

### Target State

- A single Go web application serving both:
  - review workflows (human-in-the-loop linking and curation),
  - triage/report views.
- PostgreSQL as primary store for facts, semantic data, and Phase3 state.
- NDJSON retained only as optional import/export or migration tooling.

### Steps Already Taken Toward This Direction

1. **Store abstraction is in place** (`pkg/store/contracts`) and already exercised by controllers/reports.
2. **Hot-path performance work is backend-agnostic**:
   - N+1 reads removed in semantic/report paths,
   - bulk preload/index patterns used in reports,
   - in-memory semantic phase transitions in site build.
3. **Semantic and Phase3 contracts are explicit and durable**, reducing coupling to NDJSON file mechanics.
4. **Command surface trimmed to core operations**, reducing migration scope and maintenance load.

### Next Steps (Concise)

1. **Implement PostgreSQL store adapter** matching `pkg/store/contracts`.
2. **Define relational schema + migrations** for facts, semantic artifacts, and Phase3 state.
3. **Add parity tests** (NDJSON vs PostgreSQL) on key read/write/report paths.
4. **Introduce service-layer APIs** for review and triage data access.
5. **Move review UI to the Go web app runtime** and deprecate standalone local-only patterns.
6. **Move static site generation to DB-backed reads** (or server-rendered equivalent), then phase out NDJSON as primary runtime storage.
