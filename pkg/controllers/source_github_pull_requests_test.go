package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"

	githubsource "ci-failure-atlas/pkg/source/githubpullrequests"
	"ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestSourceGitHubPullRequestsSyncOnceUpsertsRowsAndCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	checkpoint := time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)
	if err := store.UpsertCheckpoints(ctx, []contracts.CheckpointRecord{
		{
			Name:      sourceGitHubPullRequestsCheckpointName(),
			Value:     checkpoint.Format(time.RFC3339Nano),
			UpdatedAt: checkpoint.Format(time.RFC3339Nano),
		},
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	fakeClient := &fakeGitHubPullRequestsClient{
		pages: map[int]fakeGitHubPullRequestsPage{
			1: {
				rows: []githubsource.PullRequest{
					{
						Number:         1001,
						State:          "open",
						Merged:         false,
						HeadSHA:        "head-1001",
						MergeCommitSHA: "",
						UpdatedAt:      time.Date(2026, 3, 7, 10, 0, 0, 0, time.UTC),
					},
					{
						Number:         1000,
						State:          "closed",
						Merged:         false,
						HeadSHA:        "head-1000",
						MergeCommitSHA: "",
						UpdatedAt:      checkpoint,
					},
				},
				rate: githubsource.RateLimit{Limit: 60, Remaining: 40},
			},
		},
	}

	controller, err := newSourceGitHubPullRequestsController(logr.Discard(), Dependencies{
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
		t.Fatalf("expected 1 GitHub page call, got %d", len(fakeClient.calls))
	}
	if fakeClient.calls[0].Owner != "Azure" || fakeClient.calls[0].Repo != "ARO-HCP" {
		t.Fatalf("unexpected owner/repo options: %+v", fakeClient.calls[0])
	}

	rows, err := store.ListPullRequests(ctx)
	if err != nil {
		t.Fatalf("list pull requests: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected pull request row count: got=%d want=1", len(rows))
	}
	if rows[0].PRNumber != 1001 || rows[0].State != "open" || rows[0].HeadSHA != "head-1001" {
		t.Fatalf("unexpected stored pull request row: %+v", rows[0])
	}

	nextCheckpoint, found, err := store.GetCheckpoint(ctx, sourceGitHubPullRequestsCheckpointName())
	if err != nil {
		t.Fatalf("get updated checkpoint: %v", err)
	}
	if !found {
		t.Fatalf("expected checkpoint to exist")
	}
	ts, ok := parseTimestamp(nextCheckpoint.Value)
	if !ok {
		t.Fatalf("expected checkpoint value timestamp, got=%q", nextCheckpoint.Value)
	}
	want := time.Date(2026, 3, 7, 10, 0, 0, 0, time.UTC)
	if !ts.Equal(want) {
		t.Fatalf("unexpected checkpoint value: got=%s want=%s", ts.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestSourceGitHubPullRequestsSyncOnceStopsWhenCheckpointReached(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	checkpoint := time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)
	if err := store.UpsertCheckpoints(ctx, []contracts.CheckpointRecord{
		{
			Name:      sourceGitHubPullRequestsCheckpointName(),
			Value:     checkpoint.Format(time.RFC3339Nano),
			UpdatedAt: checkpoint.Format(time.RFC3339Nano),
		},
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	fakeClient := &fakeGitHubPullRequestsClient{
		pages: map[int]fakeGitHubPullRequestsPage{
			1: {
				rows: []githubsource.PullRequest{
					{
						Number:    2000,
						State:     "closed",
						Merged:    true,
						HeadSHA:   "head-2000",
						UpdatedAt: checkpoint.Add(-1 * time.Minute),
					},
				},
				hasNext: true,
			},
			2: {
				rows: []githubsource.PullRequest{
					{Number: 1999, State: "open", HeadSHA: "head-1999", UpdatedAt: checkpoint.Add(-2 * time.Minute)},
				},
			},
		},
	}

	controller, err := newSourceGitHubPullRequestsController(logr.Discard(), Dependencies{
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
		t.Fatalf("expected controller to stop after first page when checkpoint reached, got calls=%d", len(fakeClient.calls))
	}
	rows, err := store.ListPullRequests(ctx)
	if err != nil {
		t.Fatalf("list pull requests: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows upserted when all rows are <= checkpoint, got=%d", len(rows))
	}
}

func TestSourceGitHubPullRequestsSyncOnceSkipsWhenPREnvironmentDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	fakeClient := &fakeGitHubPullRequestsClient{
		pages: map[int]fakeGitHubPullRequestsPage{
			1: {
				rows: []githubsource.PullRequest{
					{
						Number:    3001,
						State:     "open",
						HeadSHA:   "head-3001",
						UpdatedAt: time.Date(2026, 3, 8, 8, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	}

	controller, err := newSourceGitHubPullRequestsController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptions(t, []string{"int"}),
	}, fakeClient)
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}
	if len(fakeClient.calls) != 0 {
		t.Fatalf("expected no GitHub API calls when PR lookup env is disabled, got=%d", len(fakeClient.calls))
	}
	rows, err := store.ListPullRequests(ctx)
	if err != nil {
		t.Fatalf("list pull requests: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no pull request rows when sync is skipped, got=%d", len(rows))
	}
}

type fakeGitHubPullRequestsPage struct {
	rows    []githubsource.PullRequest
	rate    githubsource.RateLimit
	hasNext bool
	err     error
}

type fakeGitHubPullRequestsClient struct {
	pages map[int]fakeGitHubPullRequestsPage
	calls []githubsource.ListPullRequestsPageOptions
}

func (f *fakeGitHubPullRequestsClient) ListPullRequestsPage(_ context.Context, opts githubsource.ListPullRequestsPageOptions) ([]githubsource.PullRequest, githubsource.RateLimit, bool, error) {
	f.calls = append(f.calls, opts)
	page := f.pages[opts.Page]
	if page.err != nil {
		return nil, githubsource.RateLimit{}, false, page.err
	}
	out := make([]githubsource.PullRequest, len(page.rows))
	copy(out, page.rows)
	return out, page.rate, page.hasNext, nil
}
