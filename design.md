# CI Failure Atlas Design

Status: Draft (accepted baseline)  
Last updated: 2026-03-06

## 1) Context

`CI Failure Atlas` extracts CI metadata and artifacts to produce:

1. Reporting outputs for Tiger Team triage and stability improvements.
2. Metrics outputs that gain value as historical trends accumulate.

The project delivered early value through workflow-driven, script-assisted iteration.  
As logic evolved, semantic behavior became duplicated across:

- Go pipeline and guardrails,
- Python assets,
- and tests.

This duplication increases implementation and maintenance cost per improvement cycle.

## 2) Design Goals

1. Keep reporting value high and iteration fast.
2. Build a durable data plane for freshness and trend metrics.
3. Remove semantic logic duplication and converge on a single production semantic core.
4. Mirror `release-dashboard/backend` CLI + controller shape to minimize future migration friction.
5. Keep storage abstracted while using NDJSON as the first concrete storage adapter.
6. Keep packages reusable by others (no `internal/` package boundary).

## 3) Explicit Decisions

1. **No `internal/` usage** in the new architecture. Public package layout only.
2. **Controller and CLI shape mirrors** `/home/rvazquez/projects/sdp-pipelines/release-dashboard/backend`.
3. **No legacy command wrapper requirement** (clean command surface is acceptable).
4. **Pass1 remains conceptually the data-fetch and raw-fact build phase** in the new shape.
5. **NDJSON remains the primary storage backend for now**, behind storage interfaces.
6. **Facts and metrics records are environment-scoped** (`environment` is required).

## 4) Architecture Overview

The system is split into three planes:

1. **Data Plane (continuous freshness)**
   - Controller-driven acquisition and normalization.
   - Incremental updates with checkpoints.

2. **Semantic Plane (deterministic enrichment + clustering)**
   - Shared production semantic core in Go.
   - Rule-driven behavior with explicit validation and canary gates.
   - Workflow-assisted improvement cycles remain supported.

3. **Product Plane**
   - Reports and metrics materializations from the same curated facts.

High-level flow:

`sources (Sippy/Prow)` -> `raw facts (pass1 boundary)` -> `semantic outputs` -> `reports + metrics`

## 5) CLI Shape (Cobra)

The root CLI shape mirrors `release-dashboard`:

- Root command with contextual logger in `PersistentPreRun`.
- Global `--verbosity/-v`.
- Subcommand factories in `cmd/`.
- Option lifecycle pattern: `Raw -> Validate -> Complete -> Run`.

### 5.1 Controller Runtime Commands

- `cfa run`
  - Start all enabled controllers in long-running mode.
- `cfa run-once --controllers.name <name> --controllers.key <key>`
  - Run one controller reconcile for one key.
- `cfa sync-once --controllers.name <name>`
  - Run one full keyspace sync for one controller.

Shared source-selection flag:

- `--source.envs` (multi-value, allowed: `dev,int,stg,prod`)

### 5.2 Product and Workflow Commands

- `cfa report summary`
- `cfa report test-summary`
- `cfa workflow phase1`
- `cfa workflow phase2`
- `cfa workflow validate`
- `cfa workflow canary`
- `cfa workflow promote-rules`
- `cfa metrics rollup-daily --date <YYYY-MM-DD>`
- `cfa metrics trend --window <7d|30d|90d>`

## 6) Controller Runtime Pattern

Controllers implement:

```go
type Controller interface {
    Run(ctx context.Context, threadiness int)
    RunOnce(ctx context.Context, key string) error
    SyncOnce(ctx context.Context) error
}
```

Runtime semantics mirror `release-dashboard`:

- typed workqueue
- rate-limited retries
- periodic enqueue with jitter
- worker goroutines (`wait.UntilWithContext`)
- graceful shutdown via context cancellation
- crash protection (`utilruntime.HandleCrash`)

## 7) V1 Controller Registry

1. `source.sippy.runs`
   - key: `environment|run_url`
   - responsibility: discover runs from Sippy, filter to the environment's canonical job name, persist failed run metadata, and persist hourly run-count baselines (`total/success/failure`) for metric denominators.

2. `source.prow.failures`
   - key: `environment|run_url`
   - responsibility: fetch deterministic JUnit artifacts from environment-scoped known paths (no directory crawling) and extract failure rows.

3. `facts.raw-failures`
   - key: `environment|run_url`
   - responsibility: normalize/fingerprint/enrich rows into canonical raw failure facts.
   - this is the controllerized **pass1 boundary**.

4. `metrics.rollup.daily`
   - key: `YYYY-MM-DD`
   - responsibility: compute daily aggregate metrics.

## 8) Pass1 in New Shape

Pass1 remains a first-class boundary.  
It still means:

- source metadata and artifact ingestion,
- deterministic normalization,
- signature/fingerprint creation,
- enriched raw failure facts for downstream semantic and reporting/metrics layers.

Current scope note:

- Raw failure facts are built from extracted test failure rows only (deterministic junit artifacts).
- Runs without usable test-failure artifacts are not expanded into separate infra-failure facts in v1.

The difference is execution style:

- old shape: one batch command,
- new shape: controllerized data plane with one-shot (`sync-once`) and continuous (`run`) modes.

## 9) Package Layout (No internal)

```text
cmd/
  main.go
  run.go
  report.go
  workflow.go
  metrics.go

pkg/
  controllers/
    interface.go
    source_sippy_runs_controller.go
    source_prow_failures_controller.go
    facts_raw_failures_controller.go
    metrics_rollup_daily_controller.go
  run/
    options.go
  run_once/
    options.go
  sync_once/
    options.go
  source/
    sippy/
    prowartifacts/
  facts/
    model/
    normalize/
    pipeline/
  semantic/
    contracts/
    rules/
    engine/
    validate/
    canary/
  report/
    summary/
    testsummary/
  metrics/
    rollup/
    trend/
  store/
    contracts/
    ndjson/
```

## 10) Storage Model

Storage remains abstracted with NDJSON as first adapter:

- interfaces in `pkg/store/contracts`
- implementation in `pkg/store/ndjson`

Initial NDJSON layout:

```text
data/
  facts/
    runs.ndjson
    artifact_failures.ndjson
    raw_failures.ndjson
    metrics_daily.ndjson
  semantic/
    test_clusters.ndjson
    global_clusters.ndjson
    review_queue.ndjson
    phase1_workset.ndjson
  state/
    checkpoints.ndjson
    dead_letters.ndjson
  reports/
    triage-summary.md
    test-failure-summary.md
```

NDJSON adapter requirements:

- deterministic ordering for reviewability and reproducibility
- idempotent upsert semantics for fact records
- safe writer locking for concurrent controller updates
- compaction support for append-heavy streams

Artifact identity note:

- `artifact_row_id` is the occurrence identity for one testcase failure row.
- `signature_id` is the grouping fingerprint (`sha256(normalized failure text)`), so multiple artifact rows may intentionally share the same signature.
- In `facts.raw-failures`, `row_id` is sourced from `artifact_row_id`, `test_name`/`test_suite` are carried forward from `artifact_failures`, and `occurred_at` is sourced from the matching `runs` metadata when available.

## 11) Semantic Core Migration Strategy

### Stage A: Contract Freeze

- Define canonical schemas in `pkg/semantic/contracts`.
- Ensure all workflow and report commands consume these contracts only.

### Stage B: Shared Production Engine

- Move evidence extraction, provider anchoring, clustering, search phrase resolution, and review reasoning into `pkg/semantic/engine`.
- Make this the sole production semantic implementation.

### Stage C: Rules Lifecycle

- Introduce rule bundles with versioning (`rules_version`).
- Gate promotions with:
  - validation invariants,
  - canary scoring,
  - corpus replay delta checks.

### Stage D: Workflow Integration

- Workflow commands run the shared engine.
- Python assets remain available for experimentation, but promoted behavior must land as rule updates and/or Go semantic core changes.

## 12) Metrics End-State

Target metric families:

1. **Spread / concentration**
   - top-N share, entropy/HHI, affected tests per cluster.
2. **Novelty**
   - newly seen signatures/canonical phrases/tests per window.
3. **Regression risk**
   - post-good-commit rates and trend.
4. **Churn / stability**
   - split-merge churn by period and by rule version.
5. **Review burden**
   - low-confidence and ambiguous-provider queue trends.

These are generated from fresh facts + semantic outputs, not from ad-hoc snapshots.

Current v1 daily rollup metrics:

- `run_count`, `failure_count`, `failure_rate` (from `runs`)
- `failure_row_count` and lane breakdowns (from `raw_failures`)

## 13) Initial Implementation Slices

1. CLI and options scaffolding
   - `cmd/*`, `pkg/run*`, `pkg/controllers/interface.go`.
2. Storage contracts + NDJSON adapter
   - facts/checkpoints/deadletters stores.
3. Controllerize pass1 boundary
   - `source.sippy.runs`, `source.prow.failures`, `facts.raw-failures`.
4. Keep existing report commands working on unchanged output contracts.
5. Semantic core extraction into `pkg/semantic`.
6. Metrics daily rollups and trend commands.

## 14) Non-Goals (V1)

- No immediate requirement to move away from NDJSON.
- No requirement to preserve old command aliases/wrappers.
- No mandatory full rewrite of all existing semantic behavior in one step.

---

This document captures the accepted baseline design and is the source for implementation planning.

TODO: externalize environment-scoped static maps (Sippy job-name map and deterministic JUnit artifact-path map) to CLI/config file inputs.
