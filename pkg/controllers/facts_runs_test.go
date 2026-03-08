package controllers

import (
	"context"
	"testing"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestFactsRunsSyncOnceMaterializesMergedAndPostGoodSignals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/1234/job/111"
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         runURL,
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       1234,
			PRState:        "",
			PRSHA:          "head-sha-1234",
			FinalMergedSHA: "",
			MergedPR:       false,
			PostGoodCommit: false,
			OccurredAt:     "2026-03-07T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if err := store.UpsertPullRequests(ctx, []contracts.PullRequestRecord{
		{
			PRNumber:       1234,
			State:          "closed",
			Merged:         true,
			HeadSHA:        "head-sha-1234",
			MergeCommitSHA: "merge-sha-1234",
			MergedAt:       "2026-03-07T11:00:00Z",
			ClosedAt:       "2026-03-07T11:00:00Z",
			UpdatedAt:      "2026-03-07T11:00:00Z",
			LastCheckedAt:  "2026-03-07T11:05:00Z",
		},
	}); err != nil {
		t.Fatalf("seed pull request facts: %v", err)
	}

	controller, err := newFactsRunsController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptions(t, []string{"dev"}),
	})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	row, found, err := store.GetRun(ctx, "dev", runURL)
	if err != nil {
		t.Fatalf("get reconciled run: %v", err)
	}
	if !found {
		t.Fatalf("expected reconciled run to exist")
	}
	if row.PRState != "closed" {
		t.Fatalf("unexpected pr_state: got=%q want=%q", row.PRState, "closed")
	}
	if !row.MergedPR || !row.PostGoodCommit || row.FinalMergedSHA != "head-sha-1234" {
		t.Fatalf("unexpected merged/post-good reconciliation: %+v", row)
	}
}

func TestFactsRunsSyncOnceStopsPostGoodForClosedNotMergedPR(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/777/job/222"
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         runURL,
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       777,
			PRState:        "",
			PRSHA:          "head-sha-777",
			FinalMergedSHA: "head-sha-777",
			MergedPR:       true,
			PostGoodCommit: true,
			OccurredAt:     "2026-03-07T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if err := store.UpsertPullRequests(ctx, []contracts.PullRequestRecord{
		{
			PRNumber:      777,
			State:         "closed",
			Merged:        false,
			HeadSHA:       "head-sha-777",
			UpdatedAt:     "2026-03-07T11:00:00Z",
			LastCheckedAt: "2026-03-07T11:05:00Z",
		},
	}); err != nil {
		t.Fatalf("seed pull request facts: %v", err)
	}

	controller, err := newFactsRunsController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptions(t, []string{"dev"}),
	})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	row, found, err := store.GetRun(ctx, "dev", runURL)
	if err != nil {
		t.Fatalf("get reconciled run: %v", err)
	}
	if !found {
		t.Fatalf("expected reconciled run to exist")
	}
	if row.PRState != "closed" {
		t.Fatalf("unexpected pr_state: got=%q want=%q", row.PRState, "closed")
	}
	if row.MergedPR || row.PostGoodCommit || row.FinalMergedSHA != "" {
		t.Fatalf("expected closed-unmerged PR to disable merged/post-good signals, got=%+v", row)
	}
}

func TestFactsRunsSyncOncePeriodicEnvDefaultsToMergedPostGood(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic/job/333"
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment:    "int",
			RunURL:         runURL,
			JobName:        "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel",
			PRNumber:       0,
			PRSHA:          "",
			FinalMergedSHA: "",
			MergedPR:       false,
			PostGoodCommit: false,
			OccurredAt:     "2026-03-07T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	controller, err := newFactsRunsController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptions(t, []string{"int"}),
	})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	row, found, err := store.GetRun(ctx, "int", runURL)
	if err != nil {
		t.Fatalf("get reconciled run: %v", err)
	}
	if !found {
		t.Fatalf("expected reconciled run to exist")
	}
	if !row.MergedPR || !row.PostGoodCommit {
		t.Fatalf("expected periodic run to default to merged/post-good=true, got=%+v", row)
	}
}

func TestFactsRunsSyncOnceFiltersConfiguredEnvironments(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	devRunURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/2222/job/999"
	intRunURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic/job/888"
	occurredAt := "2026-03-07T10:00:00Z"
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         devRunURL,
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       0,
			MergedPR:       true,
			PostGoodCommit: true,
			OccurredAt:     occurredAt,
		},
		{
			Environment:    "int",
			RunURL:         intRunURL,
			JobName:        "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel",
			PRNumber:       0,
			MergedPR:       false,
			PostGoodCommit: false,
			OccurredAt:     occurredAt,
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}

	controller, err := newFactsRunsController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptions(t, []string{"int"}),
	})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}
	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	devRow, found, err := store.GetRun(ctx, "dev", devRunURL)
	if err != nil {
		t.Fatalf("get dev run: %v", err)
	}
	if !found {
		t.Fatalf("expected dev run to exist")
	}
	if !devRow.MergedPR || !devRow.PostGoodCommit {
		t.Fatalf("expected dev run to remain unchanged when env is filtered out, got=%+v", devRow)
	}

	intRow, found, err := store.GetRun(ctx, "int", intRunURL)
	if err != nil {
		t.Fatalf("get int run: %v", err)
	}
	if !found {
		t.Fatalf("expected int run to exist")
	}
	if !intRow.MergedPR || !intRow.PostGoodCommit {
		t.Fatalf("expected int run to be reconciled, got=%+v", intRow)
	}
}
