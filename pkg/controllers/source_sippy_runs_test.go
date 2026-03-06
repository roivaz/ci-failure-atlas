package controllers

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	sippysource "ci-failure-atlas/pkg/source/sippy"
	"ci-failure-atlas/pkg/sourceoptions"
	"ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestSourceSippyRunsSyncOnceUpsertsFailedRunsAndCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	source := mustCompleteSourceOptions(t, []string{"dev"})
	fakeClient := &fakeSippyClient{
		runs: []sippysource.JobRun{
			{
				RunURL:    "https://prow.ci.openshift.org/view/gs/test-platform-results/job-a/1",
				JobName:   "job-a",
				StartedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
				Failed:    true,
			},
			{
				RunURL:    "https://prow.ci.openshift.org/view/gs/test-platform-results/job-b/2",
				JobName:   "job-b",
				StartedAt: time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC),
				Failed:    false,
			},
		},
	}

	controller, err := newSourceSippyRunsController(logr.Discard(), Dependencies{
		Store:  store,
		Source: source,
	}, fakeClient)
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	if len(fakeClient.calls) != 1 {
		t.Fatalf("expected 1 sippy call, got %d", len(fakeClient.calls))
	}
	if fakeClient.calls[0].Release != "Presubmits" {
		t.Fatalf("unexpected release: got=%q want=%q", fakeClient.calls[0].Release, "Presubmits")
	}

	keys, err := store.ListRunKeys(ctx)
	if err != nil {
		t.Fatalf("list run keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("unexpected run keys length: got=%d want=%d keys=%v", len(keys), 1, keys)
	}
	if keys[0] != "dev|https://prow.ci.openshift.org/view/gs/test-platform-results/job-a/1" {
		t.Fatalf("unexpected run key: %q", keys[0])
	}

	hours, err := store.ListRunCountHourlyHours(ctx)
	if err != nil {
		t.Fatalf("list run count hourly hours: %v", err)
	}
	wantHours := []string{"2026-01-02T03:00:00Z", "2026-01-02T04:00:00Z"}
	if !reflect.DeepEqual(hours, wantHours) {
		t.Fatalf("unexpected run count hourly hour list: got=%v want=%v", hours, wantHours)
	}

	runCountRows := mustReadRunCountHourlyRows(t, filepath.Join(dataDir, "facts", "run_counts_hourly.ndjson"))
	if len(runCountRows) != 2 {
		t.Fatalf("unexpected run count row count: got=%d want=2", len(runCountRows))
	}
	for _, row := range runCountRows {
		switch row.Hour {
		case "2026-01-02T03:00:00Z":
			if row.TotalRuns != 1 || row.FailedRuns != 1 || row.SuccessfulRuns != 0 {
				t.Fatalf("unexpected counters for hour %s: %+v", row.Hour, row)
			}
		case "2026-01-02T04:00:00Z":
			if row.TotalRuns != 1 || row.FailedRuns != 0 || row.SuccessfulRuns != 1 {
				t.Fatalf("unexpected counters for hour %s: %+v", row.Hour, row)
			}
		default:
			t.Fatalf("unexpected hour in run counts: %s", row.Hour)
		}
	}

	checkpoint, found, err := store.GetCheckpoint(ctx, checkpointNameForEnvironment("dev"))
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if !found {
		t.Fatalf("expected checkpoint to be present")
	}
	if checkpoint.Value == "" {
		t.Fatalf("expected non-empty checkpoint value")
	}
	checkpointTime, ok := parseTimestamp(checkpoint.Value)
	if !ok {
		t.Fatalf("expected checkpoint value to be a timestamp, got=%q", checkpoint.Value)
	}
	wantCheckpointTime := time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC)
	if !checkpointTime.Equal(wantCheckpointTime) {
		t.Fatalf("unexpected checkpoint time: got=%s want=%s", checkpointTime.Format(time.RFC3339Nano), wantCheckpointTime.Format(time.RFC3339Nano))
	}
}

func TestSourceSippyRunsSyncOnceUsesCheckpointAsSince(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	checkpointTime := time.Date(2026, 2, 3, 1, 2, 3, 456000000, time.UTC)
	if err := store.UpsertCheckpoints(ctx, []contracts.CheckpointRecord{
		{
			Name:      checkpointNameForEnvironment("dev"),
			Value:     checkpointTime.Format(time.RFC3339Nano),
			UpdatedAt: checkpointTime.Format(time.RFC3339Nano),
		},
	}); err != nil {
		t.Fatalf("upsert checkpoint: %v", err)
	}

	fakeClient := &fakeSippyClient{}
	controller, err := newSourceSippyRunsController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptions(t, []string{"dev"}),
	}, fakeClient)
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	if len(fakeClient.calls) != 1 {
		t.Fatalf("expected 1 sippy call, got %d", len(fakeClient.calls))
	}
	wantSince := checkpointTime.Add(-sourceSippyRunsReplayWindow).Truncate(time.Hour)
	if !fakeClient.calls[0].Since.Equal(wantSince) {
		t.Fatalf("unexpected since timestamp: got=%s want=%s", fakeClient.calls[0].Since.Format(time.RFC3339Nano), wantSince.Format(time.RFC3339Nano))
	}
}

type fakeSippyClient struct {
	runs  []sippysource.JobRun
	err   error
	calls []sippysource.ListJobRunsOptions
}

func (f *fakeSippyClient) ListJobRuns(_ context.Context, opts sippysource.ListJobRunsOptions) ([]sippysource.JobRun, error) {
	f.calls = append(f.calls, opts)
	if f.err != nil {
		return nil, f.err
	}
	out := make([]sippysource.JobRun, len(f.runs))
	copy(out, f.runs)
	return out, nil
}

func mustCompleteSourceOptions(t *testing.T, envs []string) *sourceoptions.Options {
	t.Helper()

	raw := sourceoptions.DefaultOptions()
	raw.Environments = append([]string(nil), envs...)
	raw.SippyReleaseInt = "INT"
	raw.SippyReleaseStg = "STG"
	raw.SippyReleaseProd = "PROD"
	raw.SippyLookback = "24h"

	validated, err := raw.Validate()
	if err != nil {
		t.Fatalf("validate source options: %v", err)
	}

	completed, err := validated.Complete(context.Background())
	if err != nil {
		t.Fatalf("complete source options: %v", err)
	}
	return completed
}

func mustReadRunCountHourlyRows(t *testing.T, path string) []contracts.RunCountHourlyRecord {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open run counts file %q: %v", path, err)
	}
	defer f.Close()

	rows := make([]contracts.RunCountHourlyRecord, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row contracts.RunCountHourlyRecord
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("decode run count hourly row %q: %v", line, err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan run counts file %q: %v", path, err)
	}
	return rows
}
