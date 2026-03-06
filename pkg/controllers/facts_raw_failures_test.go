package controllers

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestFactsRawFailuresSyncOnceUsesRunOccurredAt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/5000/job/123456"
	occurredAt := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      runURL,
			JobName:     "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			OccurredAt:  occurredAt,
		},
	}); err != nil {
		t.Fatalf("upsert runs: %v", err)
	}

	if err := store.UpsertArtifactFailures(ctx, []contracts.ArtifactFailureRecord{
		{
			Environment:   "dev",
			ArtifactRowID: "dev-row-1",
			RunURL:        runURL,
			TestName:      "test-a",
			TestSuite:     "suite-a",
			SignatureID:   "sig-old-1",
			FailureText:   "Request failed for https://example.com/abc with id 123e4567-e89b-12d3-a456-426614174000 at 2026-03-06T12:34:56Z",
		},
		{
			Environment:   "dev",
			ArtifactRowID: "dev-row-2",
			RunURL:        runURL,
			TestName:      "test-b",
			TestSuite:     "suite-a",
			SignatureID:   "sig-old-2",
			FailureText:   "cluster=foo-bar resourcegroup=my-rg nodepool=my-pool",
		},
	}); err != nil {
		t.Fatalf("upsert artifact failures: %v", err)
	}

	controller, err := newFactsRawFailuresController(logr.Discard(), Dependencies{Store: store})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	rows := mustReadRawFailureRows(t, filepath.Join(dataDir, "facts", "raw_failures.ndjson"))
	if len(rows) != 2 {
		t.Fatalf("unexpected raw failure row count: got=%d want=2", len(rows))
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].RowID < rows[j].RowID })

	if rows[0].RowID != "dev-row-1" || rows[1].RowID != "dev-row-2" {
		t.Fatalf("expected raw row IDs to mirror artifact_row_id: rows=%+v", rows)
	}
	if rows[0].TestName != "test-a" || rows[0].TestSuite != "suite-a" {
		t.Fatalf("expected first row test metadata from artifact failures, got row=%+v", rows[0])
	}
	if rows[1].TestName != "test-b" || rows[1].TestSuite != "suite-a" {
		t.Fatalf("expected second row test metadata from artifact failures, got row=%+v", rows[1])
	}
	for _, row := range rows {
		if row.OccurredAt != occurredAt {
			t.Fatalf("expected occurred_at=%q from run metadata, got row=%+v", occurredAt, row)
		}
		if row.MergedPR {
			t.Fatalf("expected merged_pr=false for non-enriched run, got row=%+v", row)
		}
		if row.PostGoodCommitFailures != 0 {
			t.Fatalf("expected post_good_commit_failures=0 for non-post-good run, got row=%+v", row)
		}
		if row.SignatureID != sha256Hex(row.NormalizedText) {
			t.Fatalf("expected signature_id=sha256(normalized_text), got row=%+v", row)
		}
	}
	if !strings.Contains(rows[0].NormalizedText, "<url>") || !strings.Contains(rows[0].NormalizedText, "<uuid>") || !strings.Contains(rows[0].NormalizedText, "<ts>") {
		t.Fatalf("expected normalized text placeholders in first row, got=%q", rows[0].NormalizedText)
	}

	keys, err := store.ListRawFailureRunKeys(ctx)
	if err != nil {
		t.Fatalf("list raw failure run keys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "dev|"+runURL {
		t.Fatalf("unexpected raw failure run keys: %v", keys)
	}
}

func TestFactsRawFailuresRunOnceWithoutRunMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/5001/job/223344"
	if err := store.UpsertArtifactFailures(ctx, []contracts.ArtifactFailureRecord{
		{
			Environment:   "dev",
			ArtifactRowID: "dev-row-9",
			RunURL:        runURL,
			TestName:      "test-without-run",
			TestSuite:     "suite-z",
			SignatureID:   "sig-old-z",
			FailureText:   "context deadline exceeded",
		},
	}); err != nil {
		t.Fatalf("upsert artifact failures: %v", err)
	}

	controller, err := newFactsRawFailuresController(logr.Discard(), Dependencies{Store: store})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.RunOnce(ctx, "dev|"+runURL); err != nil {
		t.Fatalf("run once: %v", err)
	}

	rows := mustReadRawFailureRows(t, filepath.Join(dataDir, "facts", "raw_failures.ndjson"))
	if len(rows) != 1 {
		t.Fatalf("unexpected raw failure row count: got=%d want=1", len(rows))
	}
	if rows[0].TestName != "test-without-run" || rows[0].TestSuite != "suite-z" {
		t.Fatalf("expected test metadata to be propagated, got row=%+v", rows[0])
	}
	if rows[0].OccurredAt != "" {
		t.Fatalf("expected empty occurred_at when run metadata is missing, got=%q", rows[0].OccurredAt)
	}
}

func TestFactsRawFailuresRunOnceSkipsAlreadyMaterializedRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/6001/job/998877"
	if err := store.UpsertArtifactFailures(ctx, []contracts.ArtifactFailureRecord{
		{
			Environment:   "dev",
			ArtifactRowID: "dev-row-existing",
			RunURL:        runURL,
			TestName:      "test-existing",
			TestSuite:     "suite-existing",
			SignatureID:   "sig-artifact",
			FailureText:   "new failure text that should not overwrite",
		},
	}); err != nil {
		t.Fatalf("upsert artifact failures: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "dev-row-existing",
			RunURL:         runURL,
			TestName:       "test-existing",
			TestSuite:      "suite-existing",
			SignatureID:    "sig-existing",
			OccurredAt:     "2026-03-06T00:00:00Z",
			RawText:        "already materialized",
			NormalizedText: "already materialized",
		},
	}); err != nil {
		t.Fatalf("upsert existing raw failure: %v", err)
	}

	controller, err := newFactsRawFailuresController(logr.Discard(), Dependencies{Store: store})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.RunOnce(ctx, "dev|"+runURL); err != nil {
		t.Fatalf("run once: %v", err)
	}

	rows := mustReadRawFailureRows(t, filepath.Join(dataDir, "facts", "raw_failures.ndjson"))
	if len(rows) != 1 {
		t.Fatalf("unexpected raw failure row count: got=%d want=1", len(rows))
	}
	if rows[0].RawText != "already materialized" || rows[0].NormalizedText != "already materialized" || rows[0].SignatureID != "sig-existing" {
		t.Fatalf("expected existing raw row to be preserved when skipping rematerialization, got row=%+v", rows[0])
	}
}

func TestFactsRawFailuresRunOnceRefreshesPRMergeSignals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/7001/job/121212"
	occurredAt := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         runURL,
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       7001,
			PRSHA:          "sha-7001",
			FinalMergedSHA: "",
			MergedPR:       false,
			PostGoodCommit: false,
			OccurredAt:     occurredAt,
		},
	}); err != nil {
		t.Fatalf("upsert initial run metadata: %v", err)
	}
	if err := store.UpsertArtifactFailures(ctx, []contracts.ArtifactFailureRecord{
		{
			Environment:   "dev",
			ArtifactRowID: "dev-row-merge",
			RunURL:        runURL,
			TestName:      "test-merge",
			TestSuite:     "suite-merge",
			SignatureID:   "sig-merge",
			FailureText:   "merge-signal failure text",
		},
	}); err != nil {
		t.Fatalf("upsert artifact failures: %v", err)
	}

	controller, err := newFactsRawFailuresController(logr.Discard(), Dependencies{Store: store})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}
	if err := controller.RunOnce(ctx, "dev|"+runURL); err != nil {
		t.Fatalf("first run once: %v", err)
	}

	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         runURL,
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       7001,
			PRSHA:          "sha-7001",
			FinalMergedSHA: "sha-7001",
			MergedPR:       true,
			PostGoodCommit: true,
			OccurredAt:     occurredAt,
		},
	}); err != nil {
		t.Fatalf("upsert updated run metadata: %v", err)
	}
	if err := controller.RunOnce(ctx, "dev|"+runURL); err != nil {
		t.Fatalf("second run once: %v", err)
	}

	rows := mustReadRawFailureRows(t, filepath.Join(dataDir, "facts", "raw_failures.ndjson"))
	if len(rows) != 1 {
		t.Fatalf("unexpected raw failure row count: got=%d want=1", len(rows))
	}
	if !rows[0].MergedPR || rows[0].PostGoodCommitFailures != 1 {
		t.Fatalf("expected refreshed merge signal fields (merged_pr=true, post_good_commit_failures=1), got row=%+v", rows[0])
	}
}

func TestFactsRawFailuresSyncOnceSkipsUnresolvedPROlderThanRetryWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/8001/job/898989"
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         runURL,
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       8001,
			PRSHA:          "sha-8001",
			FinalMergedSHA: "",
			MergedPR:       false,
			PostGoodCommit: false,
			OccurredAt:     time.Now().UTC().Add(-10 * 24 * time.Hour).Format(time.RFC3339),
		},
	}); err != nil {
		t.Fatalf("upsert unresolved run metadata: %v", err)
	}
	if err := store.UpsertArtifactFailures(ctx, []contracts.ArtifactFailureRecord{
		{
			Environment:   "dev",
			ArtifactRowID: "dev-row-old-unresolved",
			RunURL:        runURL,
			TestName:      "test-old-unresolved",
			TestSuite:     "suite-old-unresolved",
			SignatureID:   "sig-old-unresolved",
			FailureText:   "this should be skipped by unresolved retry cutoff",
		},
	}); err != nil {
		t.Fatalf("upsert artifact failure: %v", err)
	}

	controller, err := newFactsRawFailuresController(logr.Discard(), Dependencies{Store: store})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}
	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	keys, err := store.ListRawFailureRunKeys(ctx)
	if err != nil {
		t.Fatalf("list raw failure run keys: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected unresolved old PR run to be skipped, got keys=%v", keys)
	}
}

func mustReadRawFailureRows(t *testing.T, path string) []contracts.RawFailureRecord {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open raw failures file %q: %v", path, err)
	}
	defer f.Close()

	rows := make([]contracts.RawFailureRecord, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row contracts.RawFailureRecord
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("decode raw failure row %q: %v", line, err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan raw failures file: %v", err)
	}
	return rows
}
