# CI Failure Atlas Design

Status: current architecture snapshot  
Last updated: 2026-04-01

## Purpose

CI Failure Atlas ingests CI run/failure data, materializes semantic failure clusters by week, and serves operator-facing triage and review workflows.

The important architectural point is that the Go app + PostgreSQL runtime is no longer the target state. It is the current state.

## System Overview

The runtime has three main planes:

1. **Controllers**
   - Ingest and derive facts into PostgreSQL.
   - Main entrypoint: `cfa run`
   - Supporting debug helpers: `cfa run-once`, `cfa sync-once`

2. **Semantic materialization**
   - Builds one semantic week from facts already stored in PostgreSQL.
   - Main entrypoint: `cfa semantic materialize`
   - Phase1 and phase2 execute in memory; persisted outputs are the user-facing week datasets.

3. **Product surfaces**
   - `cfa app` serves weekly/triage/review views from PostgreSQL.
   - `cfa app export-site` renders static HTML from the same PostgreSQL-backed data as a compatibility/export path.

## Codebase Map

The architecture maps to the repository roughly like this:

- CLI bootstrap and command surface:
  - `cmd/main.go`
  - `pkg/cli`
- Controllers and source ingestion:
  - `pkg/run`
  - `pkg/controllers`
  - `pkg/source`
- Semantic materialization and read models:
  - `pkg/semantic/engine`
  - `pkg/semantic/workflow`
  - `pkg/semantic/query`
  - `pkg/semantic/history`
- Product-facing HTTP and HTML/report surfaces:
  - `pkg/frontend`
  - `pkg/report`
- Runtime storage contract and PostgreSQL implementation:
  - `pkg/store/contracts`
  - `pkg/store/postgres`
- Deployment and publishing artifacts:
  - `deploy/`
  - `Dockerfile`
  - `infra/azure/`

## Semantic Pipeline

The semantic workflow is still logically split into three phases:

1. **Phase1: normalize + test-scoped clustering**
   - Build enriched failure evidence from facts.
   - Normalize failure text.
   - Classify failures into deterministic test-scoped clusters.

2. **Phase2: global merge**
   - Merge test-scoped clusters into global failure signatures.
   - Produce the signature rows used by weekly/triage and review flows.

3. **Phase3: human linking and reconciliation**
   - Operators link semantically equivalent signatures in the review UI.
   - Durable row-level anchors remain `environment + run_url + row_id`.
   - Stored Phase3 state is reapplied both in the live app and in exported static reports.

## Terminology And Search Notes

User-facing docs and UI now use "triage" for the signature triage surface.

Some internal files, symbols, or helper names may still retain older `global` terminology from the earlier evolution of the codebase. That does not usually indicate a different product surface. When searching the repo, check both `triage` and `global` unless you are specifically working on phase2 global-signature merge semantics.

## Storage Model

PostgreSQL is the active runtime store behind `pkg/store/contracts` and `pkg/store/postgres`.

The current persisted model is:

- facts/state tables such as `cfa_runs`, `cfa_raw_failures`, `cfa_metrics_daily`, and related checkpoints/metadata
- semantic week tables for:
  - `cfa_sem_global_clusters`
  - `cfa_sem_review_queue`
- Phase3 state tables for:
  - `cfa_phase3_issues`
  - `cfa_phase3_links`
  - `cfa_phase3_events`

NDJSON is no longer part of the runtime architecture. It remains only as a legacy import format for `cfa migrate import-legacy-data`.

## Semantic Week Contract

The semantic partitioning contract is now explicit:

- one stored semantic partition equals one UTC week
- a week is keyed by a Sunday-starting `YYYY-MM-DD`
- materialization replaces the full stored week, not partial per-environment slices
- history/navigation in the app and reports is based on these stored weeks

This removes the old ambiguity around generic semantic subdirectories or ad hoc materialization windows.

## Local Runtime Model

Local operation defaults to embedded PostgreSQL with initialization and migrations enabled. That is a development convenience, not the architecture itself.

In practice:

- `cfa run`, `cfa semantic materialize`, and `cfa app` all operate against PostgreSQL
- embedded Postgres is the default local transport
- switching to remote PostgreSQL is a configuration detail via `--storage.postgres.*`, not a different runtime model

## Product Surfaces

### Unified app

`cfa app` is the primary operator surface:

- weekly report view
- signature triage view
- Phase3 review/linking workflow
- cross-week history lookups based on stored semantic weeks

### Static export

`cfa app export-site` remains intentionally supported because current publishing still relies on uploading static HTML.

Its role is now narrow:

- read already-materialized PostgreSQL data
- render weekly/triage/archive HTML
- hand off publishing to Azure CLI or another external script

It is not the architectural center of the system and it is not responsible for running semantic materialization.

## Current Command Surface

Primary commands:

- `cfa run`
- `cfa semantic materialize`
- `cfa app`
- `cfa app export-site`

Secondary maintenance/debug commands:

- `cfa run-once`
- `cfa sync-once`
- `cfa migrate import-legacy-data`

## Key Design Decisions

1. **App + DB is the primary runtime**
   - Reports, review, and exported HTML all read from PostgreSQL-backed state.

2. **Semantic weeks are canonical**
   - The UI, materialization contract, and storage schema all agree on Sunday-starting week partitions.

3. **Only user-facing semantic outputs are persisted by default**
   - Phase1 internals remain in-memory unless future debugging needs justify additional persistence.

4. **Phase3 source of truth is durable link state**
   - The review workflow stores stable issue/link/event records rather than collapsing decisions into transient export artifacts.

5. **Static export is compatibility, not primary architecture**
   - It exists to bridge current hosting, not to define the long-term runtime shape.

## Developer Orientation

For a new coding session, the default validation loop is:

- `make check` for broad repo validation
- `go test ./pkg/semantic/...` for semantic/materialization work
- `go test ./pkg/frontend/... ./pkg/report/...` for app/report work
- `go test ./pkg/store/postgres/...` for store or migration work

For manual smoke testing, use the main runtime commands described in `README.md`. Agent-oriented repo guidance lives in `AGENTS.md`.

## Next Milestone

The major remaining phase is hosted operation.

That work includes:

- deploying the Go app in a hosted environment
- running against managed PostgreSQL
- scheduling controllers and semantic materialization/backfill
- adding auth, deployment automation, backups, and runbooks
- deciding when storage-account-hosted static export can become optional instead of primary

The architecture refactor is largely complete; the next work is operationalization.
