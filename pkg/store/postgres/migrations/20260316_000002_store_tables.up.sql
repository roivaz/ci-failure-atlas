-- Step-1 mixed schema:
-- - facts/state datasets normalized
-- - semantic/phase3 datasets use typed keys + JSONB payload

CREATE TABLE IF NOT EXISTS cfa_runs (
  environment TEXT NOT NULL,
  run_url TEXT NOT NULL,
  job_name TEXT NOT NULL DEFAULT '',
  pr_number INTEGER NOT NULL DEFAULT 0,
  pr_state TEXT NOT NULL DEFAULT '',
  pr_sha TEXT NOT NULL DEFAULT '',
  final_merged_sha TEXT NOT NULL DEFAULT '',
  merged_pr BOOLEAN NOT NULL DEFAULT FALSE,
  post_good_commit BOOLEAN NOT NULL DEFAULT FALSE,
  failed BOOLEAN NOT NULL DEFAULT FALSE,
  occurred_at TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (environment, run_url)
);
CREATE INDEX IF NOT EXISTS cfa_runs_environment_occurred_at_idx ON cfa_runs (environment, occurred_at);

CREATE TABLE IF NOT EXISTS cfa_pull_requests (
  pr_number INTEGER PRIMARY KEY,
  state TEXT NOT NULL DEFAULT '',
  merged BOOLEAN NOT NULL DEFAULT FALSE,
  head_sha TEXT NOT NULL DEFAULT '',
  merge_commit_sha TEXT NOT NULL DEFAULT '',
  merged_at TEXT NOT NULL DEFAULT '',
  closed_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  last_checked_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS cfa_artifact_failures (
  environment TEXT NOT NULL,
  artifact_row_id TEXT NOT NULL,
  run_url TEXT NOT NULL DEFAULT '',
  test_name TEXT NOT NULL DEFAULT '',
  test_suite TEXT NOT NULL DEFAULT '',
  signature_id TEXT NOT NULL DEFAULT '',
  failure_text TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (environment, artifact_row_id)
);
CREATE INDEX IF NOT EXISTS cfa_artifact_failures_environment_run_idx ON cfa_artifact_failures (environment, run_url);

CREATE TABLE IF NOT EXISTS cfa_raw_failures (
  environment TEXT NOT NULL,
  row_id TEXT NOT NULL,
  run_url TEXT NOT NULL DEFAULT '',
  non_artifact_backed BOOLEAN NOT NULL DEFAULT FALSE,
  test_name TEXT NOT NULL DEFAULT '',
  test_suite TEXT NOT NULL DEFAULT '',
  signature_id TEXT NOT NULL DEFAULT '',
  occurred_at TEXT NOT NULL DEFAULT '',
  raw_text TEXT NOT NULL DEFAULT '',
  normalized_text TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (environment, row_id)
);
CREATE INDEX IF NOT EXISTS cfa_raw_failures_environment_run_idx ON cfa_raw_failures (environment, run_url);
CREATE INDEX IF NOT EXISTS cfa_raw_failures_environment_occurred_at_idx ON cfa_raw_failures (environment, occurred_at);

CREATE TABLE IF NOT EXISTS cfa_metrics_daily (
  environment TEXT NOT NULL,
  date TEXT NOT NULL,
  metric TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY (environment, date, metric)
);
CREATE INDEX IF NOT EXISTS cfa_metrics_daily_date_idx ON cfa_metrics_daily (date);

CREATE TABLE IF NOT EXISTS cfa_test_metadata_daily (
  environment TEXT NOT NULL,
  date TEXT NOT NULL,
  release TEXT NOT NULL DEFAULT '',
  period TEXT NOT NULL DEFAULT '',
  test_name TEXT NOT NULL DEFAULT '',
  test_suite TEXT NOT NULL DEFAULT '',
  current_pass_percentage DOUBLE PRECISION NOT NULL DEFAULT 0,
  current_runs INTEGER NOT NULL DEFAULT 0,
  previous_pass_percentage DOUBLE PRECISION NOT NULL DEFAULT 0,
  previous_runs INTEGER NOT NULL DEFAULT 0,
  net_improvement DOUBLE PRECISION NOT NULL DEFAULT 0,
  ingested_at TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (environment, date, period, test_suite, test_name)
);
CREATE INDEX IF NOT EXISTS cfa_test_metadata_daily_environment_date_idx ON cfa_test_metadata_daily (environment, date);

CREATE TABLE IF NOT EXISTS cfa_checkpoints (
  name TEXT PRIMARY KEY,
  value TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS cfa_dead_letters (
  id BIGSERIAL PRIMARY KEY,
  controller TEXT NOT NULL,
  key TEXT NOT NULL,
  error TEXT NOT NULL,
  failed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cfa_dead_letters_failed_at_idx ON cfa_dead_letters (failed_at);

CREATE TABLE IF NOT EXISTS cfa_sem_phase1_workset (
  semantic_subdir TEXT NOT NULL DEFAULT '',
  environment TEXT NOT NULL,
  row_id TEXT NOT NULL,
  lane TEXT NOT NULL DEFAULT '',
  job_name TEXT NOT NULL DEFAULT '',
  test_name TEXT NOT NULL DEFAULT '',
  occurred_at TEXT NOT NULL DEFAULT '',
  run_url TEXT NOT NULL DEFAULT '',
  signature_id TEXT NOT NULL DEFAULT '',
  payload JSONB NOT NULL,
  PRIMARY KEY (semantic_subdir, environment, row_id)
);
CREATE INDEX IF NOT EXISTS cfa_sem_phase1_workset_order_idx ON cfa_sem_phase1_workset (semantic_subdir, environment, lane, job_name, test_name, occurred_at, run_url, signature_id, row_id);

CREATE TABLE IF NOT EXISTS cfa_sem_test_clusters (
  semantic_subdir TEXT NOT NULL DEFAULT '',
  environment TEXT NOT NULL,
  phase1_cluster_id TEXT NOT NULL,
  lane TEXT NOT NULL DEFAULT '',
  job_name TEXT NOT NULL DEFAULT '',
  test_name TEXT NOT NULL DEFAULT '',
  support_count INTEGER NOT NULL DEFAULT 0,
  payload JSONB NOT NULL,
  PRIMARY KEY (semantic_subdir, environment, phase1_cluster_id)
);
CREATE INDEX IF NOT EXISTS cfa_sem_test_clusters_order_idx ON cfa_sem_test_clusters (semantic_subdir, environment, lane, job_name, test_name, support_count, phase1_cluster_id);

CREATE TABLE IF NOT EXISTS cfa_sem_global_clusters (
  semantic_subdir TEXT NOT NULL DEFAULT '',
  environment TEXT NOT NULL,
  phase2_cluster_id TEXT NOT NULL,
  support_count INTEGER NOT NULL DEFAULT 0,
  contributing_tests_count INTEGER NOT NULL DEFAULT 0,
  payload JSONB NOT NULL,
  PRIMARY KEY (semantic_subdir, environment, phase2_cluster_id)
);
CREATE INDEX IF NOT EXISTS cfa_sem_global_clusters_order_idx ON cfa_sem_global_clusters (semantic_subdir, support_count, contributing_tests_count, environment, phase2_cluster_id);

CREATE TABLE IF NOT EXISTS cfa_sem_review_queue (
  semantic_subdir TEXT NOT NULL DEFAULT '',
  environment TEXT NOT NULL,
  review_item_id TEXT NOT NULL,
  phase TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  payload JSONB NOT NULL,
  PRIMARY KEY (semantic_subdir, environment, review_item_id)
);
CREATE INDEX IF NOT EXISTS cfa_sem_review_queue_order_idx ON cfa_sem_review_queue (semantic_subdir, environment, phase, reason, review_item_id);

CREATE TABLE IF NOT EXISTS cfa_phase3_issues (
  issue_id TEXT PRIMARY KEY,
  payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS cfa_phase3_links (
  environment TEXT NOT NULL,
  run_url TEXT NOT NULL,
  row_id TEXT NOT NULL,
  issue_id TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT '',
  payload JSONB NOT NULL,
  PRIMARY KEY (environment, run_url, row_id)
);
CREATE INDEX IF NOT EXISTS cfa_phase3_links_issue_id_idx ON cfa_phase3_links (issue_id);

CREATE TABLE IF NOT EXISTS cfa_phase3_events (
  event_id TEXT PRIMARY KEY,
  at TEXT NOT NULL DEFAULT '',
  payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS cfa_phase3_events_at_idx ON cfa_phase3_events (at);
