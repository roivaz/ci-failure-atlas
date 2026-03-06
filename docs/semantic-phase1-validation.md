# Semantic Phase1 Validation

## Validation Run

- Command: `go test ./...`
- Result: pass (all packages green, including new semantic/store/workflow tests).

- Command: `go run cmd/main.go workflow phase1 --storage.ndjson.data-dir data`
- Result: pass
- Runtime summary:
  - `raw_rows=294`
  - `runs=152`
  - `workset_rows=294`
  - `normalized_rows=294`
  - `assignments=294`
  - `test_clusters=75`
  - `review_items=11`

## Artifacts Produced

`workflow phase1` now writes deterministic semantic outputs under `data/semantic/`:

- `phase1_workset.ndjson`
- `phase1_normalized.ndjson`
- `phase1_assignments.ndjson`
- `test_clusters.ndjson`
- `review_queue.ndjson`

## Phase2 Follow-up Gaps

- No global/phase2 clustering yet (`workflow phase2`, `workflow validate`, `workflow canary` still pending).
- No rules bundle/version promotion flow yet (phase1 is currently code-driven parity behavior).
- Search phrase recovery is fail-open for provenance safety, but still heuristic and should be evaluated with canary scoring once phase2 exists.
- Reports/trend commands are not yet wired to consume new semantic outputs end-to-end.
