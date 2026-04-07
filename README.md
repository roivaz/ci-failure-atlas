# CI Failure Atlas

CI Failure Atlas is a PostgreSQL-backed Go application for ingesting ARO CI data, materializing weekly semantic failure clusters, and serving operator-facing weekly/global/review views.

The app+DB runtime is now the primary architecture. Static-site export still exists as an intentional compatibility path while hosted operation is being designed.

## Current Architecture

- `cfa run` continuously ingests Sippy, Prow, and GitHub data and derives normalized facts into PostgreSQL.
- `cfa semantic materialize` builds one semantic week from those facts and replaces that stored week in PostgreSQL.
- `cfa app` serves the unified weekly, global, and review UI from PostgreSQL.
- `cfa app export-site` renders static HTML from existing PostgreSQL data only; it does not run semantic materialization.

Local development defaults to embedded PostgreSQL with initialization and migrations enabled. Remote PostgreSQL is supported through the usual `--storage.postgres.*` flags.

## Prerequisites

- Go 1.25+
- Access to the Sippy, Prow, and GitHub APIs used by the controllers
- Optional Azure CLI access if you want to publish exported static HTML

## Core Workflow

### 1. Ingest facts

```bash
go run cmd/main.go run \
  --source.envs dev,int,stg,prod \
  --history.weeks 4
```

This runs the controller set continuously and keeps facts/state tables up to date in PostgreSQL.

### 2. Materialize a semantic week

```bash
go run cmd/main.go semantic materialize \
  --week 2026-03-29
```

A semantic week is always a Sunday-starting UTC week in `YYYY-MM-DD` form. Materialization replaces the full stored week across all supported environments.

If `--week` is omitted, the command defaults to the current UTC week start. For bulk refreshes, the `Makefile` also includes:

```bash
make semantic-backfill SEMANTIC_WEEKS=8
```

### 3. Run the app

```bash
go run cmd/main.go app \
  --week 2026-03-22 \
  --app.listen 127.0.0.1:8082 \
  --history.weeks 4
```

Open `http://127.0.0.1:8082/` for the weekly/global app or `http://127.0.0.1:8082/review/` for Phase3 review/linking.

### 4. Export static HTML

```bash
go run cmd/main.go app export-site \
  --site.root site \
  --history.weeks 4
```

`app export-site` is export-only. Run `cfa semantic materialize` first, then export the already-materialized weeks from PostgreSQL.

## Publish the Exported Site

Publishing is intentionally outside `cfa`. Use Azure CLI or another small script. Example:

```bash
az storage blob upload-batch \
  --destination '$web' \
  --source site \
  --account-name <storage-account-name> \
  --auth-mode login \
  --overwrite
```

To preview locally before uploading:

```bash
python -m http.server 8080 --directory site
```

## Semantic Week Contract

- Semantic partitions are PostgreSQL week partitions, not free-form subdirectories.
- A stored week must be a Sunday-starting `YYYY-MM-DD`.
- The review app, weekly report, and history lookups all navigate across those stored weeks.
- Partial per-environment materialization is intentionally not supported.

## Other Commands

The main runtime commands above are the normal operator surface. There are also targeted maintenance/debug helpers:

- `cfa run-once`
- `cfa sync-once`
- `cfa migrate import-legacy-data`

## Next Milestone

The remaining big phase is hosted operation rather than more architectural refactoring. That work includes:

- running the Go app against managed PostgreSQL instead of local embedded defaults
- scheduling controllers and semantic materialization/backfill
- establishing auth, deployment, backups, and operational runbooks
- eventually retiring storage-account-hosted static export as the primary access path

## Reference

- Architecture notes: `design.md`
