# CI Failure Atlas

CI Failure Atlas ingests CI failure data, builds semantic clustering outputs, and provides:

- a static triage site (weekly + global triage reports), and
- a local review app for human Phase3 linking.

## What This Project Does

The project combines ingestion, semantic processing, and reporting into one operational flow:

`controllers` -> `semantic phases (phase1/phase2/phase3)` -> `review + static triage site`

### 1) Ingestion via controllers

Controllers continuously ingest and derive facts into PostgreSQL facts/state tables:

- source ingestion (Sippy, Prow, GitHub),
- run/raw-failure fact materialization,
- daily metric rollups.

### 2) Semantic workflow

Semantic outputs are generated during `report site build` (or reused from existing snapshots):

- **Phase1**: normalize and classify failures into deterministic test-scoped clusters.
- **Phase2**: merge phase1 outputs into global failure signatures.
- **Phase3**: human-in-the-loop linking in `report review` to group equivalent signatures using durable row-level anchors.

The review app is the operator interface for Phase3 linking and cross-week reconciliation.

### 3) Static site for reporting

The static site is the reporting surface used for triage:

- weekly report (`weekly-metrics.html`)
- global signature triage (`global-signature-triage.html`)

## Prerequisites

- Go 1.25+
- Access to Sippy/Prow/GitHub APIs used by controllers
- (Optional, for report site push) Azure CLI authenticated to the target subscription

## Build and Run

### Run all controllers

This is the data ingestion and derived-facts pipeline.

```bash
go run cmd/main.go run \
  --source.envs dev,int,stg,prod \
  --history.weeks 4
```

Common options:

- `--storage.postgres.embedded.data-dir` (default: `data/postgres`)
- `--source.envs` (default: `dev`)
- `--history.weeks` (default: `4`)

### Build the report site

Builds semantic outputs (if needed) and generates static weekly/global triage pages:

```bash
go run cmd/main.go report site build \
  --site.root site \
  --source.envs dev,int,stg,prod \
  --history.weeks 4 \
  --start-date 2026-03-15
```

Use existing semantic snapshots without rerunning semantic build:

```bash
go run cmd/main.go report site build \
  --site.root site \
  --from-existing
```

### Serve the site locally

```bash
go run cmd/main.go report site run \
  --site.root site \
  --site.listen 127.0.0.1:8080
```

### Push the site to Azure static website storage

```bash
go run cmd/main.go report site push \
  --site.root site \
  --site.storage-account <storage-account-name> \
  --site.auth-mode login \
  --site.container '$web'
```

Notes:

- `--site.storage-account` is required.
- `report site push` rebuilds indexes/reports from existing semantic snapshots before upload.

### Run the review app

Starts the local Phase3 linking UI:

```bash
go run cmd/main.go report review \
  --storage.semantic-subdir 2026-03-15 \
  --history.weeks 4 \
  --site.listen 127.0.0.1:8081
```

## Typical Workflow

1. Run controllers (`cfa run`) to refresh facts/state in PostgreSQL.
2. Build site (`cfa report site build`) to generate semantic snapshots and static reports.
3. Open review app (`cfa report review`) to link signatures (Phase3).
4. Rebuild site (usually `--from-existing`) to reflect latest Phase3 links in static reports.
5. Push site (`cfa report site push`) when ready to publish.

## Repository Notes

- Primary architectural snapshot: `design.md`
- Additional semantic notes: `docs/`
