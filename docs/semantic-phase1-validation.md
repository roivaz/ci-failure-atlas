# Semantic Build Validation

## Validation Run

- Command: `go test ./...`
- Result: pass.

- Command: `go run cmd/main.go report site build --history.weeks 1`
- Result: pass.

## Default Semantic Datasets Produced

The site build path writes deterministic semantic outputs into PostgreSQL semantic partitions (`semantic_subdir`), including:

- `phase1_workset`
- `test_clusters`
- `review_queue`
- `global_clusters`

## Notes

- Intermediate phase1 artifacts (`phase1_normalized`, `phase1_assignments`) are not persisted by default.
