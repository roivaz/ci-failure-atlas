package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"

	githubsource "ci-failure-atlas/pkg/source/githubpullrequests"
	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	sourceGitHubPullRequestsReconcileInterval = 60 * time.Minute
	sourceGitHubPullRequestsDefaultPerPage    = 100
	sourceGitHubPullRequestsMaxPagesPerSync   = 20
	sourceGitHubPullRequestsMaxRequests       = 20
)

type sourceGitHubPullRequestsController struct {
	logger            logr.Logger
	reconcileInterval time.Duration
	queue             workqueue.TypedRateLimitingInterface[string]
	prLookupEnabled   bool

	store        contracts.Store
	githubClient githubsource.Client
	deps         Dependencies
}

var _ Controller = (*sourceGitHubPullRequestsController)(nil)

func NewSourceGitHubPullRequests(logger logr.Logger, deps Dependencies) (Controller, error) {
	return newSourceGitHubPullRequestsController(logger, deps, nil)
}

func newSourceGitHubPullRequestsController(
	logger logr.Logger,
	deps Dependencies,
	client githubsource.Client,
) (*sourceGitHubPullRequestsController, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("source.github.pull-requests: store dependency is required")
	}
	if deps.Source == nil {
		return nil, fmt.Errorf("source.github.pull-requests: source options dependency is required")
	}
	if len(deps.Source.Environments) == 0 {
		return nil, fmt.Errorf("source.github.pull-requests: no source environments configured")
	}
	if strings.TrimSpace(deps.Source.SippyOrg) == "" {
		return nil, fmt.Errorf("source.github.pull-requests: source org is required")
	}
	if strings.TrimSpace(deps.Source.SippyRepo) == "" {
		return nil, fmt.Errorf("source.github.pull-requests: source repo is required")
	}

	validEnvironmentCount := 0
	prLookupEnabled := false
	for _, env := range deps.Source.Environments {
		normalized := normalizeEnvironment(env)
		if normalized == "" {
			continue
		}
		validEnvironmentCount++
		if supportsPRLookupForEnvironment(normalized) {
			prLookupEnabled = true
		}
	}
	if validEnvironmentCount == 0 {
		return nil, fmt.Errorf("source.github.pull-requests: no valid source environments configured")
	}

	if client == nil {
		client = githubsource.NewHTTPClient("")
	}

	return &sourceGitHubPullRequestsController{
		logger: logger.WithValues("controller", SourceGitHubPullRequestsControllerName),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: SourceGitHubPullRequestsControllerName,
			},
		),
		reconcileInterval: sourceGitHubPullRequestsReconcileInterval,
		prLookupEnabled:   prLookupEnabled,
		store:             deps.Store,
		githubClient:      client,
		deps:              deps,
	}, nil
}

func (c *sourceGitHubPullRequestsController) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	if threadiness <= 0 {
		threadiness = 1
	}

	c.logger.Info("Starting controller.", "threads", threadiness)
	for i := 0; i < threadiness; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	go wait.JitterUntilWithContext(ctx, c.queueMetadata, c.reconcileInterval, 0.1, true)
	c.logger.Info("Started workers.")
	<-ctx.Done()
	c.logger.Info("Shutting down controller.")
}

func (c *sourceGitHubPullRequestsController) RunOnce(ctx context.Context, key string) error {
	c.logger.Info("Reconciling one key.", "key", key)
	return c.processKey(ctx, key)
}

func (c *sourceGitHubPullRequestsController) SyncOnce(ctx context.Context) error {
	keys, err := c.listKeys(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := c.processKey(ctx, key); err != nil {
			return fmt.Errorf("failed processing key %q: %w", key, err)
		}
	}
	c.logger.Info("Completed one full sync.", "keys", len(keys))
	return nil
}

func (c *sourceGitHubPullRequestsController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *sourceGitHubPullRequestsController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.processKey(ctx, key); err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, fmt.Errorf("failed processing key %q", key), "Error syncing; requeuing for later retry", "controller", SourceGitHubPullRequestsControllerName, "key", key)
	c.queue.AddRateLimited(key)
	return true
}

func (c *sourceGitHubPullRequestsController) queueMetadata(ctx context.Context) {
	keys, err := c.listKeys(ctx)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed listing keys for periodic enqueue", "controller", SourceGitHubPullRequestsControllerName)
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.queue.Add(key)
	}
}

func (c *sourceGitHubPullRequestsController) listKeys(_ context.Context) ([]string, error) {
	if !c.prLookupEnabled {
		return []string{}, nil
	}
	return []string{"sync"}, nil
}

func (c *sourceGitHubPullRequestsController) processKey(ctx context.Context, _ string) error {
	checkpoint, err := c.getCheckpointTime(ctx)
	if err != nil {
		return err
	}
	maxUpdatedSeen := checkpoint

	now := time.Now().UTC()
	page := 1
	pagesRead := 0
	requestsMade := 0
	reachedCheckpoint := false
	hasNextPage := true
	latestRate := githubsource.RateLimit{}

	out := make([]contracts.PullRequestRecord, 0, 256)
	for hasNextPage && pagesRead < sourceGitHubPullRequestsMaxPagesPerSync && requestsMade < sourceGitHubPullRequestsMaxRequests {
		rows, rate, hasNext, err := c.githubClient.ListPullRequestsPage(ctx, githubsource.ListPullRequestsPageOptions{
			Owner:     c.deps.Source.SippyOrg,
			Repo:      c.deps.Source.SippyRepo,
			State:     "all",
			Sort:      "updated",
			Direction: "desc",
			PerPage:   sourceGitHubPullRequestsDefaultPerPage,
			Page:      page,
		})
		if err != nil {
			return fmt.Errorf("list github pull requests page=%d: %w", page, err)
		}

		pagesRead++
		requestsMade++
		latestRate = rate
		hasNextPage = hasNext

		for _, row := range rows {
			if !row.UpdatedAt.IsZero() && row.UpdatedAt.After(maxUpdatedSeen) {
				maxUpdatedSeen = row.UpdatedAt
			}

			if !checkpoint.IsZero() && !row.UpdatedAt.IsZero() && (row.UpdatedAt.Equal(checkpoint) || row.UpdatedAt.Before(checkpoint)) {
				reachedCheckpoint = true
				continue
			}

			out = append(out, contracts.PullRequestRecord{
				PRNumber:       row.Number,
				State:          row.State,
				Merged:         row.Merged,
				HeadSHA:        row.HeadSHA,
				MergeCommitSHA: row.MergeCommitSHA,
				MergedAt:       formatTimeRFC3339(row.MergedAt),
				ClosedAt:       formatTimeRFC3339(row.ClosedAt),
				UpdatedAt:      formatTimeRFC3339(row.UpdatedAt),
				LastCheckedAt:  now.Format(time.RFC3339Nano),
			})
		}

		if reachedCheckpoint {
			break
		}
		page++
	}

	if len(out) > 0 {
		if err := c.store.UpsertPullRequests(ctx, out); err != nil {
			return fmt.Errorf("upsert %d pull request rows: %w", len(out), err)
		}
	}

	nextCheckpoint := checkpoint
	if maxUpdatedSeen.After(nextCheckpoint) {
		nextCheckpoint = maxUpdatedSeen
	}
	if !nextCheckpoint.IsZero() {
		if err := c.store.UpsertCheckpoints(ctx, []contracts.CheckpointRecord{
			{
				Name:      sourceGitHubPullRequestsCheckpointName(),
				Value:     nextCheckpoint.Format(time.RFC3339Nano),
				UpdatedAt: now.Format(time.RFC3339Nano),
			},
		}); err != nil {
			return fmt.Errorf("update github pull request checkpoint: %w", err)
		}
	}

	c.logger.Info(
		"Synced GitHub pull requests.",
		"repo", c.deps.Source.SippyOrg+"/"+c.deps.Source.SippyRepo,
		"upserted_rows", len(out),
		"checkpoint_reached", reachedCheckpoint,
		"pages_read", pagesRead,
		"requests_made", requestsMade,
		"rate_limit_remaining", latestRate.Remaining,
		"rate_limit_limit", latestRate.Limit,
		"rate_limit_reset", formatTimeRFC3339(latestRate.ResetAt),
	)
	return nil
}

func (c *sourceGitHubPullRequestsController) getCheckpointTime(ctx context.Context) (time.Time, error) {
	checkpoint, found, err := c.store.GetCheckpoint(ctx, sourceGitHubPullRequestsCheckpointName())
	if err != nil {
		return time.Time{}, fmt.Errorf("get checkpoint %q: %w", sourceGitHubPullRequestsCheckpointName(), err)
	}
	if !found {
		return time.Time{}, nil
	}
	if parsed, ok := parseTimestamp(checkpoint.Value); ok {
		return parsed.UTC(), nil
	}
	c.logger.Info("Checkpoint timestamp is invalid; ignoring saved value.", "name", sourceGitHubPullRequestsCheckpointName(), "value", checkpoint.Value)
	return time.Time{}, nil
}

func sourceGitHubPullRequestsCheckpointName() string {
	return SourceGitHubPullRequestsControllerName + ".updated_at"
}

func formatTimeRFC3339(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}
