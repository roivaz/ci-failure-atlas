package controllers

import (
	"context"
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
	devJobName, err := sippyJobNameForEnvironment("dev")
	if err != nil {
		t.Fatalf("resolve dev job name: %v", err)
	}
	fakeClient := &fakeSippyClient{
		runs: []sippysource.JobRun{
			{
				RunURL:    "https://prow.ci.openshift.org/view/gs/test-platform-results/job-a/1",
				JobName:   devJobName,
				PRNumber:  4242,
				PRSHA:     "sha-4242",
				StartedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
				Failed:    true,
			},
			{
				RunURL:    "https://prow.ci.openshift.org/view/gs/test-platform-results/job-b/2",
				JobName:   devJobName,
				PRNumber:  4243,
				PRSHA:     "sha-4243",
				StartedAt: time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC),
				Failed:    false,
			},
			{
				RunURL:    "https://prow.ci.openshift.org/view/gs/test-platform-results/job-c/3",
				JobName:   "some-other-job",
				StartedAt: time.Date(2026, 1, 2, 5, 4, 5, 0, time.UTC),
				Failed:    true,
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
	if fakeClient.calls[0].JobName != devJobName {
		t.Fatalf("unexpected job filter: got=%q want=%q", fakeClient.calls[0].JobName, devJobName)
	}
	if fakeClient.calls[0].Org == "" || fakeClient.calls[0].Repo == "" {
		t.Fatalf("expected dev runs query to include org/repo filter, got=%+v", fakeClient.calls[0])
	}
	if len(fakeClient.pullRequestCalls) != 0 {
		t.Fatalf("expected 0 pull-request lookup calls, got %d", len(fakeClient.pullRequestCalls))
	}

	keys, err := store.ListRunKeys(ctx)
	if err != nil {
		t.Fatalf("list run keys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("unexpected run keys length: got=%d want=%d keys=%v", len(keys), 2, keys)
	}
	run, found, err := store.GetRun(ctx, "dev", "https://prow.ci.openshift.org/view/gs/test-platform-results/job-a/1")
	if err != nil {
		t.Fatalf("get run metadata: %v", err)
	}
	if !found {
		t.Fatalf("expected run metadata for dev/job-a/1")
	}
	if run.MergedPR || run.PostGoodCommit || run.FinalMergedSHA != "" || run.PRState != "" {
		t.Fatalf("unexpected run merge fields before facts.runs materialization: %+v", run)
	}
	if !run.Failed {
		t.Fatalf("expected job-a run to be marked failed")
	}
	runB, found, err := store.GetRun(ctx, "dev", "https://prow.ci.openshift.org/view/gs/test-platform-results/job-b/2")
	if err != nil {
		t.Fatalf("get run metadata for job-b: %v", err)
	}
	if !found {
		t.Fatalf("expected run metadata for dev/job-b/2")
	}
	if runB.Failed {
		t.Fatalf("expected job-b run to be marked successful")
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

func TestSourceSippyRunsSyncOncePreservesExistingSignalsOnReplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/job-unresolved/1"
	occurredAt := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         runURL,
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       5001,
			PRState:        "closed",
			PRSHA:          "sha-unresolved",
			FinalMergedSHA: "sha-unresolved",
			MergedPR:       true,
			PostGoodCommit: true,
			OccurredAt:     occurredAt,
		},
	}); err != nil {
		t.Fatalf("seed existing run: %v", err)
	}

	fakeClient := &fakeSippyClient{
		runs: []sippysource.JobRun{
			{
				RunURL:    runURL,
				JobName:   "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
				PRNumber:  5001,
				PRSHA:     "sha-unresolved",
				StartedAt: time.Now().UTC(),
				Failed:    true,
			},
		},
	}
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

	run, found, err := store.GetRun(ctx, "dev", runURL)
	if err != nil {
		t.Fatalf("get run metadata: %v", err)
	}
	if !found {
		t.Fatalf("expected replayed run to exist")
	}
	if !run.MergedPR || run.FinalMergedSHA != "sha-unresolved" || !run.PostGoodCommit || run.PRState != "closed" {
		t.Fatalf("expected existing merge signals to be preserved by source.sippy.runs replay, got=%+v", run)
	}
}

func TestSourceSippyRunsSyncOncePeriodicEnvSkipsPRLookupAndMarksPostGood(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	intJobName, err := sippyJobNameForEnvironment("int")
	if err != nil {
		t.Fatalf("resolve int job name: %v", err)
	}

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/job-int/1"
	fakeClient := &fakeSippyClient{
		runs: []sippysource.JobRun{
			{
				RunURL:    runURL,
				JobName:   intJobName,
				PRNumber:  7777, // Intentionally set to prove no PR lookup is attempted for periodic envs.
				PRSHA:     "sha-7777",
				StartedAt: time.Date(2026, 1, 2, 6, 4, 5, 0, time.UTC),
				Failed:    true,
			},
		},
		pullRequests: []sippysource.PullRequest{
			{Number: 7777, SHA: "sha-7777", MergedAt: time.Date(2026, 1, 2, 7, 0, 0, 0, time.UTC)},
		},
	}

	controller, err := newSourceSippyRunsController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptions(t, []string{"int"}),
	}, fakeClient)
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	if len(fakeClient.pullRequestCalls) != 0 {
		t.Fatalf("expected 0 pull-request lookup calls for periodic env, got %d", len(fakeClient.pullRequestCalls))
	}
	if len(fakeClient.calls) != 1 {
		t.Fatalf("expected 1 ListJobRuns call for periodic env, got %d", len(fakeClient.calls))
	}
	if fakeClient.calls[0].JobName != intJobName {
		t.Fatalf("unexpected job filter for periodic env: got=%q want=%q", fakeClient.calls[0].JobName, intJobName)
	}
	if fakeClient.calls[0].Org != "" || fakeClient.calls[0].Repo != "" {
		t.Fatalf("expected periodic env runs query to omit org/repo filter, got=%+v", fakeClient.calls[0])
	}

	run, found, err := store.GetRun(ctx, "int", runURL)
	if err != nil {
		t.Fatalf("get run metadata: %v", err)
	}
	if !found {
		t.Fatalf("expected run metadata for int/job-int/1")
	}
	if !run.PostGoodCommit {
		t.Fatalf("expected periodic env runs to be marked post-good, got=%+v", run)
	}
	if !run.MergedPR {
		t.Fatalf("expected periodic env runs to be treated as merged-code signal, got=%+v", run)
	}
	if run.FinalMergedSHA != "" {
		t.Fatalf("expected no merged SHA lookup for periodic env, got=%q", run.FinalMergedSHA)
	}
}

type fakeSippyClient struct {
	runs             []sippysource.JobRun
	err              error
	calls            []sippysource.ListJobRunsOptions
	pullRequests     []sippysource.PullRequest
	pullRequestsErr  error
	pullRequestCalls []sippysource.ListPullRequestsOptions
	tests            []sippysource.TestSummary
	testsErr         error
	testCalls        []sippysource.ListTestsOptions
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

func (f *fakeSippyClient) ListPullRequests(_ context.Context, opts sippysource.ListPullRequestsOptions) ([]sippysource.PullRequest, error) {
	f.pullRequestCalls = append(f.pullRequestCalls, opts)
	if f.pullRequestsErr != nil {
		return nil, f.pullRequestsErr
	}
	out := make([]sippysource.PullRequest, len(f.pullRequests))
	copy(out, f.pullRequests)
	return out, nil
}

func (f *fakeSippyClient) ListTests(_ context.Context, opts sippysource.ListTestsOptions) ([]sippysource.TestSummary, error) {
	f.testCalls = append(f.testCalls, opts)
	if f.testsErr != nil {
		return nil, f.testsErr
	}
	out := make([]sippysource.TestSummary, len(f.tests))
	copy(out, f.tests)
	return out, nil
}

func mustCompleteSourceOptions(t *testing.T, envs []string) *sourceoptions.Options {
	t.Helper()

	raw := sourceoptions.DefaultOptions()
	raw.Environments = append([]string(nil), envs...)
	raw.SippyReleaseInt = "INT"
	raw.SippyReleaseStg = "STG"
	raw.SippyReleaseProd = "PROD"
	raw.HistoryHorizonWeeks = 2

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
