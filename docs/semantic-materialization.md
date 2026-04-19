# Semantic Materialization

Status: current workflow detail  
Last updated: 2026-04-19

## Purpose

Semantic materialization turns raw CI failure facts already stored in PostgreSQL into one week of operator-facing failure patterns plus a review queue.

The materialization command does **not** fetch source data directly. Controllers and ingestion jobs are responsible for populating facts first; semantic materialization only reads those facts, derives semantic clusters, and replaces the stored semantic week.

## Materialization Unit

The canonical unit is one Sunday-starting UTC week:

- key format: `YYYY-MM-DD`
- time range: `[week_start, week_start + 7d)`
- scope: all supported environments together (`dev`, `int`, `stg`, `prod`)
- replacement model: full week replacement, not partial per-environment updates

The operational entrypoints are:

```bash
make semantic-materialize SEMANTIC_WEEK=2026-04-12
make semantic-backfill SEMANTIC_WEEKS=8
```

`semantic-backfill` just loops over week starts and invokes materialization once per week.

## Inputs

Semantic materialization reads facts that have already been derived into PostgreSQL, primarily:

- `RunRecord` rows for run-level metadata
- `RawFailureRecord` rows for artifact-backed failures
- derived PR and post-good context attached during enrichment

The enrichment step filters down to the requested materialization window and excludes facts that should not participate in semantic clustering, such as:

- failures outside the week window
- failures without usable artifact-backed evidence
- invalid or malformed rows that cannot be normalized safely

## End-To-End Flow

The code path is `pkg/semantic/workflow/MaterializeWeek()`. It normalizes the requested week, runs phase1 and phase2, then persists the week with `ReplaceMaterializedWeek()`.

### 1. Enrich semantic input

`semanticinput.BuildEnrichedFailures()` reads the relevant facts for the requested week and produces enriched failure rows with:

- environment
- lane / failed-at classification
- job and test identifiers
- run URL and timestamps
- PR metadata
- `post_good_commit` context
- raw and normalized failure text

This is the last step before the semantic pipeline becomes fully in-memory.

### 2. Build the phase1 workset

`phase1.BuildWorkset()` converts enriched failures into deterministic workset rows. Each row keeps the provenance needed later for:

- exact drilldown back to the raw failure
- row-level traceability
- deterministic ordering and clustering
- review and sample generation

Important fields at this stage include:

- `environment`
- `row_id`
- `group_key`
- `lane`
- `job_name`
- `test_name`
- `run_url`
- `occurred_at`
- `signature_id`
- raw/normalized text

`signature_id` is still preserved here, but it is no longer the semantic identity driver in the current model.

### 3. Normalize and extract failure-pattern text

`phase1.Normalize()` is the most quality-sensitive step in the workflow. It extracts the semantic text that later drives clustering.

For each row it derives:

- `canonical_evidence_phrase`
- `search_query_phrase`
- provider/genericity hints
- a deterministic `phase1_key`

The current `v2` model treats this extracted text as the semantic identity driver. That means this step needs to balance two failure modes:

- **too generic**: unrelated failures overmerge into one failure pattern
- **too specific/noisy**: equivalent failures undermerge into separate failure patterns

The quality goal is not just “stable text.” The goal is to surface the most useful failure clue from the raw error, especially for nested or provider-structured failures.

### 4. Classify into test-scoped phase1 clusters

`phase1.Classify()` groups rows within a constrained test scope. In practice this means:

- rows are not merged globally yet
- clustering happens inside deterministic grouping boundaries
- the classifier emits local-cluster assignments plus reasons/confidence

This phase is intentionally conservative. It exists to keep unrelated tests or scopes from collapsing together before the later cross-test merge step.

### 5. Compile phase1 outputs

`phase1.Compile()` turns assignments into:

- `TestClusterRecord` rows
- phase1 review queue items

The compiled test cluster chooses representative text/search phrases and carries supporting references forward. Those references are the bridge to later raw-failure samples and review anchors.

## Phase2 Merge

`phase2.Merge()` merges test-scoped phase1 clusters into environment-scoped failure patterns.

Outputs:

- `FailurePatternRecord` rows
- merged/updated review queue items

The major design rule in the current workflow is:

- merge identity is driven by extracted failure-pattern text
- `signature_id` is provenance, not the primary semantic identity

That means phase2 is supposed to collapse the same issue across multiple tests when the extracted semantic text says they are the same thing, while keeping distinct issues apart even if they share a coarse upstream signature.

## Legacy Phase3 State Is Deprecated

Older builds supported manual Phase3 linking stored in separate tables such as:

- `cfa_phase3_issues`
- `cfa_phase3_links`
- `cfa_phase3_events`

That state is now deprecated and is no longer applied in the main read path.
Current schema migrations also drop those legacy tables.

Current runtime behavior is:

- materialization writes phase2 failure patterns and the review queue
- report, failure-patterns, and run-log views read only the stored phase2 week data
- pre-migration databases may still contain legacy Phase3 rows until migrations run
- semantic-quality review should happen by improving extraction/merge logic and rematerializing weeks, not by layering manual links on top

## Stored Outputs

Materialization persists one `storecontracts.MaterializedWeek`, which currently contains:

- `FailurePatterns`
- `ReviewQueue`

The PostgreSQL store replaces the target week transactionally:

1. delete prior failure-pattern rows for that semantic week
2. delete prior review-queue rows for that semantic week
3. insert the new phase2 failure patterns
4. insert the new review queue

Phase1 intermediates are not persisted by default.

## Semantic Schema Versions

The current contract is semantic schema `v2`.

Why it exists:

- semantic identity changed materially
- historical weeks may still contain older `v1` rows
- combining incompatible weeks would corrupt cross-week views

Current behavior:

- materialized rows carry `schema_version`
- one week must be internally consistent
- the app only loads current-schema weeks into history/windowed views
- legacy `v1` weeks must be rematerialized/backfilled before they participate in navigation or history

## Failure-Pattern Identity Rules

The most important design rules in the current model are:

1. `failure_pattern` text is the semantic identity driver.
2. `signature_id` is provenance/debug context only.
3. Durable raw-to-semantic joins use stored row/run anchors.
4. Cross-week composition only combines schema-compatible weeks.

In practical terms:

- if a pattern is overmerged, the usual root cause is that extraction was too generic
- if a pattern is undermerged, the usual root cause is that extraction preserved too much noise
- if a pattern label is low quality, the root cause is often that extraction missed the best nested failure clue

## Relationship To Read Models

Materialization produces weekly semantic outputs. The app then builds user-facing read models from those stored weeks.

Important downstream behaviors:

- report/failure-patterns/run-log views can compose across multiple semantic weeks
- window-local values such as runs affected, impact, seen-in, references, and samples are recomputed for the requested window
- historical comparisons only use schema-compatible weeks

## Common Quality Problems

The workflow tends to regress in a few recognizable ways.

### Overmerged patterns

Symptoms:

- one failure pattern has samples that clearly describe different root causes
- `affected_runs` is suspiciously high relative to what a targeted CI log search suggests
- representative text is generic enough to hide important distinctions

Likely causes:

- phase1 extracted text is too generic
- phase2 merge key is too broad for a generic phrase
- provider/resource/detail clues were discarded too early

### Undermerged patterns

Symptoms:

- several patterns differ only by noisy tokens, resource names, timestamps, IDs, or other low-value detail
- multiple patterns clearly describe the same issue when viewed in CI search

Likely causes:

- phase1 preserved instance-specific noise in the canonical phrase
- normalization failed to collapse equivalent provider-structured variants
- representative text differs cosmetically while the underlying failure is the same

### Low-quality patterns

Symptoms:

- the phrase is technically stable but not useful for an operator
- the phrase keeps a wrapper error instead of the real nested cause
- the phrase is too vague to search effectively

Likely causes:

- nested detail extraction stopped too early
- the canonical phrase prefers a transport/wrapper error over the actual failure
- the search phrase does not preserve the best searchable clue

## Review And Improvement Loop

The most useful validation loop is:

1. materialize or backfill the target weeks
2. run the local app
3. fetch failure-pattern data from the local API
4. optionally fetch semantic review signals from `/api/review/signals/week?week=<semantic-week>` to prioritize suspicious clusters
5. compare suspicious patterns against concrete raw samples and CI search results
6. decide whether the problem is:
   - extraction quality
   - merge identity
   - review-signal heuristics or readmodel composition
7. add or update targeted regression tests before rerunning materialization

For practical review guidance, see `docs/semantic-materialization-review-agent-prompt.md`.

## Useful Commands

```bash
# Materialize one week
make semantic-materialize SEMANTIC_WEEK=2026-04-12

# Backfill multiple weeks
make semantic-backfill SEMANTIC_WEEKS=8

# Run the local app
make app

# Focused validation for semantic changes
go test ./pkg/semantic/...
go test ./pkg/frontend/...
```

## External Validation

When validating whether a pattern is overmerged, undermerged, or labeled poorly, compare the local API output with concrete CI evidence from [Search OpenShift CI](https://search.dptools.openshift.org/). That comparison is often the fastest way to tell whether CFA’s extracted `failure_pattern` text matches the real issue boundary operators see in logs.
