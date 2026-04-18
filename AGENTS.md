# CI Failure Atlas Agent Notes

## Start Here

- Read `README.md` for the operator/developer workflow.
- Read `design.md` for the architecture and semantic/storage invariants.
- Treat the PostgreSQL-backed app+DB runtime as the current architecture, not a future target.
- Treat embedded PostgreSQL as a local-development convenience, not a separate architecture.

## Repo Map

- `cmd/main.go`: CLI bootstrap
- `pkg/cli`: command wiring and shared option binding
- `pkg/run`, `pkg/controllers`, `pkg/source`: continuous ingestion runtime and source clients
- `pkg/semantic/...`: phase1/2/3 engines, materialization workflow, history/query helpers
- `pkg/frontend/...`: HTTP server, readmodel helpers, shared UI, and the report/failure-patterns/run-log/review surface packages
- `pkg/store/contracts`, `pkg/store/postgres`: store abstraction, PostgreSQL runtime, migrations, init/bootstrap
- `deploy/`: standalone Helm chart for Postgres, app, controllers, and cronjobs
- `Dockerfile`: container image build
- `infra/azure/`: Azure static-site storage infrastructure
- `.cursor/skills/`: project-local skills for review/failure-pattern workflows

## Invariants

- Semantic weeks are Sunday-starting UTC weeks keyed by `YYYY-MM-DD`.
- Materialization replaces a full stored semantic week; partial per-environment semantic partitions are not supported.
- `cfa app export-site` reads existing PostgreSQL data only; it does not run semantic materialization.
- User-facing docs say "failure patterns" and "run log", but some internal files and symbols still use older phase-oriented `global` names.

## Validation

- Default repo validation: `make check`
- Semantic/materialization changes: `go test ./pkg/semantic/...`
- App/report changes: `go test ./pkg/frontend/...`
- Store or migration changes: `go test ./pkg/store/postgres/...`
- Useful smoke commands: `make app`, `make semantic-materialize`, `make run-controllers`

## Current Ops State

- `deploy/` and `Dockerfile` support the current deployment experiments.
- Static HTML export plus Azure Storage publishing remain supported compatibility paths.
- Hosted app operation, auth, backups, and full runbooks are still evolving.
