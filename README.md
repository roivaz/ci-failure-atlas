# CI Failure Atlas

CI Failure Atlas is a PostgreSQL-backed Go application for ingesting ARO CI data, materializing weekly semantic failure clusters, and serving operator-facing report/triage/runs/review views.

The app+DB runtime is the primary architecture. Dynamic HTML is served directly from PostgreSQL-backed state.

## Current Architecture

- `cfa run` continuously ingests Sippy, Prow, and GitHub data and derives normalized facts into PostgreSQL.
- `cfa semantic materialize` builds one semantic week from those facts and replaces that stored week in PostgreSQL.
- `cfa app` serves the unified report, triage, runs, and review UI from PostgreSQL.

Local development defaults to embedded PostgreSQL with initialization and migrations enabled. Remote PostgreSQL is supported through the usual `--storage.postgres.*` flags.

## Repository Guide

- `cmd/main.go` bootstraps the Cobra CLI.
- `pkg/cli` defines the command surface and shared PostgreSQL setup.
- `pkg/run`, `pkg/controllers`, and `pkg/source` implement continuous ingestion and source clients.
- `pkg/semantic` owns phase1/2/3 processing, week materialization, and history/query helpers.
- `pkg/frontend` serves the unified report/triage/runs/review app and API surface.
- `pkg/report` renders HTML/report outputs for the app surface.
- `pkg/store/contracts` defines the store interfaces; `pkg/store/postgres` implements the active runtime store, migrations, and init/bootstrap helpers.
- `deploy/` contains the standalone Helm chart for Postgres, the app, controllers, and cronjobs.
- `Dockerfile` builds the container image for the Go application.
- `infra/azure/` contains Azure infrastructure related to the storage-account redirect and current deployment experiments.
- `.cursor/skills/` contains project-local skills for triage/review workflows.

Search note: user-facing docs now say "triage", but some internal files and symbols still use older `global` names. When navigating the repo, check both terms unless you are specifically working on phase2 global-signature semantics.

## Prerequisites

- Go 1.25+
- Access to the Sippy, Prow, and GitHub APIs used by the controllers
- Optional Azure CLI access if you want to upload the storage-account redirect page

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

Open `http://127.0.0.1:8082/` for the rolling 7-day report, `http://127.0.0.1:8082/report` for the report surface, or `http://127.0.0.1:8082/review/` for Phase3 review/linking.

Key app routes:

- `/` renders the rolling 7-day report window
- `/report?week=YYYY-MM-DD` renders the classic week-shaped report view
- `/report?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD` renders an arbitrary UTC report window
- `/triage?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD` renders the triage window view

The day-scoped run history surface is:

- HTML: `/runs?date=YYYY-MM-DD&env=dev`
- JSON: `/api/runs/day?date=YYYY-MM-DD&env=dev`

It renders one row per run for that day and enriches attached raw failures with semantic signatures from the latest contributing stored semantic snapshot for the matched signature.

Current limitation: this is intentionally not yet a full Prow-history clone. `RunRecord` currently carries `run_url`, `job_name`, PR metadata, `failed`, and `occurred_at`, but not richer build/duration metadata, and some raw failures can still reference runs that need run-record backfill.

### 4. Refresh local embedded PostgreSQL from a remote dump

If you want to test against fresh production-like data locally, the `Makefile` includes Docker-backed PostgreSQL client helpers. This avoids relying on whatever `pg_dump` or `psql` version is installed on your workstation.

Typical flow:

1. Port-forward the remote PostgreSQL instance to localhost with `kubectl port-forward`.
2. Dump the remote database through that forwarded port.
3. Start the local app in another terminal so embedded PostgreSQL is up.
4. Restore the dump into the local embedded database.

Example:

```bash
# Terminal 1: remote port-forward
kubectl port-forward <pod-or-service> 5432:5432

# Terminal 2: dump the remote database through localhost
make db-dump-remote \
  REMOTE_PGUSER=<remote-user> \
  REMOTE_PGPASSWORD=<remote-password> \
  REMOTE_PGDATABASE=<remote-database> \
  DB_DUMP_FILE=.work/cfa-prod.sql

# Terminal 3: start the local app (this starts embedded PostgreSQL by default)
make app

# Terminal 4: restore the dump into the local embedded database
make db-restore-local DB_DUMP_FILE=.work/cfa-prod.sql
```

Notes:

- `db-dump-remote` uses the `postgres:18.3` Docker image and writes plain SQL with `--clean --if-exists --no-owner --no-privileges`.
- `db-restore-local` restores that SQL into the local database with `psql -v ON_ERROR_STOP=1`.
- Remote dump credentials are required explicitly: `REMOTE_PGUSER`, `REMOTE_PGPASSWORD`, and `REMOTE_PGDATABASE`.
- Local restore defaults to `127.0.0.1:5432` and `postgres/postgres`, but `LOCAL_PGHOST`, `LOCAL_PGPORT`, `LOCAL_PGUSER`, `LOCAL_PGPASSWORD`, and `LOCAL_PGDATABASE` can be overridden if needed.
- For safety, `db-restore-local` only allows localhost targets.

### 5. Upload the storage redirect page

```bash
make site-upload \
  AZ_STORAGE_ACCOUNT=<storage-account-name> \
  SITE_ROOT=site
```

This generates a minimal `index.html`/`404.html` redirect page under `SITE_ROOT` and uploads it to the storage account's static website container.

The redirect target defaults to `https://cihealth.tools.hcpsvc.osadev.cloud/` and can be overridden with `SITE_REDIRECT_URL=...`.

## Redirect Page Details

To preview the generated redirect locally before uploading:

```bash
python -m http.server 8080 --directory site
```

## Deployment Artifacts

- `deploy/` is the current standalone Kubernetes packaging surface for the app runtime.
- `Dockerfile` is the image build entrypoint used by the deployment flow.
- `infra/azure/report-static-website-storage.bicep` provisions Azure Storage for the redirect-page website container.
- Hosted app operation, auth, backups, and broader operational automation are still evolving; these artifacts help with current deployment experiments but do not yet define a finished production platform.

## Semantic Week Contract

- Semantic partitions are PostgreSQL week partitions, not free-form subdirectories.
- A stored week must be a Sunday-starting `YYYY-MM-DD`.
- The review app and windowed report/triage/runs surfaces compose over those stored weeks.
- Partial per-environment materialization is intentionally not supported.

## Validation And Developer Loop

Default validation after code changes:

```bash
make check
```

Useful focused loops:

- `go test ./pkg/semantic/...` for phase1/2/3 or materialization changes
- `go test ./pkg/frontend/... ./pkg/report/...` for UI, API, and report rendering changes
- `go test ./pkg/store/postgres/...` for schema, migration, or query-layer changes

Useful local smoke commands:

```bash
make run-controllers CONTROLLER_ENVS=dev,int,stg,prod
make semantic-materialize SEMANTIC_WEEK=2026-03-29
make app APP_WEEK=2026-03-22
make db-dump-remote REMOTE_PGUSER=<remote-user> REMOTE_PGPASSWORD=<remote-password> REMOTE_PGDATABASE=<remote-database> DB_DUMP_FILE=.work/cfa-prod.sql
make db-restore-local DB_DUMP_FILE=.work/cfa-prod.sql
make site-upload AZ_STORAGE_ACCOUNT=<storage-account-name> SITE_ROOT=site
```

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
- keeping the storage-account redirect and hosted app deployment paths operational

## Reference

- Architecture notes: `design.md`
- Agent-oriented working notes: `AGENTS.md`
