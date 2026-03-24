# Semantic Build Validation

## Validation Run

- Command: `go test ./...`
- Result: pass.

- Command: `go run cmd/main.go report site build --storage.ndjson.data-dir data --history.weeks 1`
- Result: pass.

## Default Semantic Artifacts Produced

The site build path writes deterministic semantic outputs under `data/semantic/<week>/`:

- `phase1_workset.ndjson`
- `test_clusters.ndjson`
- `review_queue.ndjson`
- `global_clusters.ndjson`
- `window_metadata.json`

## Notes

- Intermediate phase1 artifacts (`phase1_normalized`, `phase1_assignments`) are not persisted by default.
