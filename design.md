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
   - Core semantic datasets persisted by default in PostgreSQL semantic namespaces:
     - `phase1_workset`
     - `test_clusters`
     - `review_queue`
     - `global_clusters`

3. **Product surfaces**
   - Static site:
     - Weekly report (`weekly-metrics.html`)
     - Global signature triage (`global-signature-triage.html`)
   - Dynamic review app:
     - `cfa report review` for Phase3 manual linking and cross-week propagation

## Storage Model (Current)

Storage is PostgreSQL-only behind interfaces in `pkg/store/contracts` and implemented in `pkg/store/postgres`.

Current runtime uses:

- normalized facts/state tables (`cfa_runs`, `cfa_raw_failures`, `cfa_metrics_daily`, ...)
- semantic/Phase3 tables with typed keys + JSONB payload
- semantic namespace isolation via `semantic_subdir` (week-scoped partitions)

NDJSON is no longer a runtime store and is kept only as an import format for migration tooling.

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
3. **Phase3 source of truth is durable links** (`cfa_phase3_links`), not pre-collapsed weekly assets.
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
5. **PostgreSQL Step 1 is implemented**:
   - mixed-schema migrations (normalized facts/state + typed-key JSONB semantic/Phase3 tables),
   - implemented currently used store methods,
   - NDJSON/PostgreSQL parity tests for implemented store methods,
   - command-path smoke tests for postgres-enabled `run`, `run-once`, `sync-once`, `workflow phase1`, and `workflow phase2`.

### Step 2 Refactor Design (Remove Phase1/Test-Cluster Persistence Dependency)

Goal: make `phase1_workset` and `test_clusters` purely internal pipeline data, not required persisted assets.

1. **Runtime data flow**
   - Keep phase1 outputs (`workset`, `normalized`, `assignments`, `test_clusters`) in memory for phase transitions.
   - Persist only user-facing semantic outputs required by current product surfaces:
     - `global_clusters`
     - `review_queue`
     - `window_metadata` (or equivalent metadata row)
     - Phase3 state (`issues`, `links`, `events`).

2. **Execution-path changes**
   - `workflow phase1` becomes an internal computation stage in build/review-oriented flows.
   - `workflow phase2` consumes in-memory phase1 outputs where possible (instead of re-reading `test_clusters` from store).
   - Report and review generation read global-level semantic data + Phase3 links as the source of truth.

3. **Debuggability without default persistence**
   - Keep optional debug persistence for phase1 internals behind an explicit opt-in flag.
   - Default mode remains minimal persistence to reduce IO/storage churn and simplify backend migration.

4. **Acceptance criteria**
   - Site build and review app behavior remain unchanged for operators.
   - No required reads of `phase1_workset`/`test_clusters` in normal runtime paths.
   - Parity tests continue to pass for persisted datasets.

### Step 2 Contract + Schema Trim Plan (Minimal Semantic Persistence)

1. **Contract trim (target)**
   - Keep facts/state methods unchanged.
   - Keep semantic/Phase3 methods needed by product surfaces:
     - `Upsert/ListGlobalClusters`
     - `Upsert/ListReviewQueue`
     - `Upsert/ListPhase3Issues`
     - `Upsert/ListPhase3Links`
     - `DeletePhase3Links`
     - `Append/ListPhase3Events`
   - De-scope phase1 persistence methods from primary contract surface (or move to debug-only extension interface).

2. **Schema trim (target)**
   - Keep:
     - `cfa_sem_global_clusters`
     - `cfa_sem_review_queue`
     - `cfa_phase3_issues`
     - `cfa_phase3_links`
     - `cfa_phase3_events`
   - Transition phase1-oriented semantic tables (`cfa_sem_phase1_workset`, `cfa_sem_test_clusters`) to optional/debug lifecycle, then remove after migration window.

3. **Migration sequence**
   - Introduce code paths that no longer depend on phase1 persisted tables.
   - Mark phase1 persistence methods as deprecated in contracts.
   - Remove writes first, then remove reads, then apply schema-drop migration for deprecated tables.
   - Keep explicit rollback window with compatibility checks before irreversible drops.

### Next Steps (Concise)

1. **Execute Step 2 refactor**: remove hard dependency on persisted `phase1_workset`/`test_clusters`.
2. **Trim store contracts + schema** to minimal persisted semantic model.
3. **Introduce service-layer APIs** for review and triage data access.
4. **Move review UI to the Go web app runtime** and deprecate standalone local-only patterns.
5. **Move static site generation to DB-backed reads** (or server-rendered equivalent), then phase out NDJSON as primary runtime storage.
