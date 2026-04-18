# CI Failure Atlas Design

Status: current architecture snapshot  
Last updated: 2026-04-18

## Purpose

CI Failure Atlas ingests CI run/failure data, materializes semantic failure clusters by week, and serves operator-facing report, triage, runs, and review workflows.

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
   - `cfa app` serves report/triage/runs/review views from PostgreSQL.
   - Azure Storage can still host a tiny redirect page that points users at the hosted app URL.

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
   - Produce the signature rows used by report/triage and review flows.

3. **Phase3: human linking and reconciliation**
   - Operators link semantically equivalent signatures in the review UI.
   - Durable row-level anchors remain `environment + run_url + row_id`.
   - Stored Phase3 state is reapplied across the live app surfaces and shared report renderers.

## Terminology And Search Notes

User-facing docs and UI now use "triage" for the signature triage surface.

Some internal files, symbols, or helper names may still retain older `global` terminology from the earlier evolution of the codebase. That does not usually indicate a different product surface. When searching the repo, check both `triage` and `global` unless you are specifically working on phase2 global-signature merge semantics.

## UI Terminology

The following user-facing terms are used consistently across all report and triage views. When adding or editing UI labels, tooltips, or copy, use these terms — not the internal names from the storage model or semantic pipeline.

### Core Concepts

| User-facing term | Replaces | Definition |
|---|---|---|
| **Failure pattern** | signature, failure signature, semantic cluster | A recurring, normalized form of a CI failure extracted from raw logs. |
| **Occurrences** | support count, matched failure support, total signature support | The number of individual CI failures matching a given pattern within the selected window. |
| **Runs affected** | jobs affected | The number of distinct job runs exhibiting a given failure pattern. |
| **Run impact** | impact | Percentage of all job runs in the environment affected by this pattern (`runs affected / total runs`). |
| **Flake signal** | flake score (0–14) | Heuristic indicator of whether a failure looks like an ongoing flake rather than a PR-introduced regression. Displayed as **Low / Medium / High** (color-coded); numeric score stays in the hover tooltip. |
| **After last push of merged PR** | after last push, PostGoodCount | Runs that occurred after the last push of a PR that eventually merged. High count = strong flake signal. Used as an input to flake signal scoring; no dedicated main-table column. |
| **Failed at** | lane | Where in the job run the failure occurred: `provision`, `e2e`, or `other`. |
| **Also in** | seen in | Other environments where the same failure pattern was also detected during the selected window. |

### "Failed at" Values

| Value | Meaning |
|---|---|
| `provision` | Failure during environment setup. DEV only — each DEV presubmit deploys its own ephemeral Azure environment before running tests. |
| `e2e` | Failure during the test suite execution step. Applies to all environments. |
| `other` | Failure due to CI infrastructure issues, image build problems, etc. CFA does not extract failure patterns for these. Replaces `unknown`. |

### View Names (Navigation)

| Label | Route | Notes |
|---|---|---|
| Last 7 Days | `/report` (rolling) | Rolling 7-day report window |
| Weekly Report | `/report` (week-anchored) | Per-week semantic report |
| Failure Patterns | `/triage` | Windowed signature triage view |
| Run Log | `/runs` | Day-scoped run history |

### What Not to Expose in User-Facing Views

- Raw flake score number (0–14) as the primary cell value — use the High/Medium/Low label; keep the score in the tooltip.
- Full flake signal scoring breakdown in the expanded detail row — the label alone is sufficient there; the breakdown is in the column hover tooltip.
- Bad PR score and reasons in the expanded detail row — this is already factored into the flake signal.
- Quality flag badges (`context type stub leaked`, `source deserialization/no-output error`, `struct/object fragment`, etc.) — internal diagnostics only.
- `Phase3 cluster` column header — use `Linked group ID` if the column must appear.
- `Triage threshold` summary card — remove entirely.
- Internal names such as `signature`, `support`, `lane`, `matched failure support` — use the table above.

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
- history/navigation in the app is composed from these stored weeks

This removes the old ambiguity around generic semantic subdirectories or ad hoc materialization windows.

### Presentation windows

User-facing report surfaces now resolve a presentation window independently from the persisted semantic partitioning:

- `/report` accepts either `week=YYYY-MM-DD` or `start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
- `/` redirects to a rolling 7-day `/report` window
- `/triage` and `/runs` use the same shared window resolver for navigation and range normalization

The important invariant is that this does **not** change semantic storage:

- weekly semantic materialization remains the only canonical persisted semantic partition
- the app loads every stored semantic week that intersects the requested UTC date window
- cross-week operator views are composed in memory at query/render time

Cross-week signature identity follows explicit rules:

- use Phase3 issue ID as the strongest cross-week identity when present
- otherwise fall back to `environment + canonical phrase + search query`

The field contract also stays explicit:

- window-local fields are recomputed across the requested range, for example jobs affected, impact, seen-in, references, and samples
- week-anchored heuristics such as flake score, trend, and after-last-push stay anchored to an explicit contributing semantic week rather than pretending a multi-week window is itself a stored semantic partition

The non-preferred alternative is to add larger persisted semantic partitions such as monthly materializations. That remains intentionally out of scope because it would introduce a second canonical semantic granularity, complicate identity/link semantics, and still leave boundary problems at larger partition edges.

## Local Runtime Model

Local operation defaults to embedded PostgreSQL with initialization and migrations enabled. That is a development convenience, not the architecture itself.

In practice:

- `cfa run`, `cfa semantic materialize`, and `cfa app` all operate against PostgreSQL
- embedded Postgres is the default local transport
- switching to remote PostgreSQL is a configuration detail via `--storage.postgres.*`, not a different runtime model

## Product Surfaces

### Unified app

`cfa app` is the primary operator surface:

- report view (`/report`) with classic week-shaped and arbitrary-window modes
- signature triage view
- day-scoped run history view (`/runs`, `/api/runs/day`)
- Phase3 review/linking workflow
- cross-week history lookups based on stored semantic weeks

### Day run history view and current fact gap

The run-history surface is intentionally day-scoped and run-centric:

- it loads the requested UTC day of `RunRecord` + `RawFailureRecord` facts
- resolves the requested day through the shared presentation-window rules used by the other report surfaces
- enriches each raw failure row by matching its `signature_id` against the contributing stored semantic clusters for that day

This gives a Prow-like operator view, but it is not yet a full Prow-history data model.

Current gap to keep explicit:

- `RunRecord` currently stores `run_url`, `job_name`, PR metadata, `failed`, and `occurred_at`
- it does not yet include richer run-history fields such as duration/build metadata or more detailed terminal run state
- some raw failures can still reference runs whose `RunRecord` needs backfill/lookup, so the first version of the run page is informative but not a complete fact model

### Storage-account redirect

The old static export flow has been removed.

The remaining Azure Storage publishing step is intentionally narrow:

- generate a tiny `index.html`/`404.html` redirect page
- upload that redirect page to the storage account's static website container
- hand users off to the hosted app URL

It is not part of semantic materialization or report rendering; it only preserves an existing access path while hosted operation is being hardened.

## Current Command Surface

Primary commands:

- `cfa run`
- `cfa semantic materialize`
- `cfa app`

Secondary maintenance/debug commands:

- `cfa run-once`
- `cfa sync-once`
- `cfa migrate import-legacy-data`

## Key Design Decisions

1. **App + DB is the primary runtime**
   - Reports, triage, runs, and review all read from PostgreSQL-backed state.

2. **Semantic weeks are canonical**
   - The UI, materialization contract, and storage schema all agree on Sunday-starting week partitions.

3. **Only user-facing semantic outputs are persisted by default**
   - Phase1 internals remain in-memory unless future debugging needs justify additional persistence.

4. **Phase3 source of truth is durable link state**
   - The review workflow stores stable issue/link/event records rather than collapsing decisions into transient export artifacts.

5. **Storage-account publishing is redirect-only compatibility**
   - It preserves an existing entrypoint without duplicating the report surface outside the live app.

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
- operating the storage-account redirect and hosted app path together until the redirect is no longer needed

The architecture refactor is largely complete; the next work is operationalization.
