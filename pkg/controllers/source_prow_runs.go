package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"

	sourceoptions "ci-failure-atlas/pkg/source/options"
	"ci-failure-atlas/pkg/source/prowartifacts"
	"ci-failure-atlas/pkg/source/prowjobs"
	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	sourceProwRunsReconcileInterval = 2 * time.Minute
	sourceProwRunsReplayWindow      = 30 * time.Minute
)

type sourceProwRunsController struct {
	logger            logr.Logger
	reconcileInterval time.Duration
	queue             workqueue.TypedRateLimitingInterface[string]

	store      contracts.Store
	prowClient prowjobs.Client
	deps       Dependencies
}

var _ Controller = (*sourceProwRunsController)(nil)

func NewSourceProwRuns(logger logr.Logger, deps Dependencies) (Controller, error) {
	return newSourceProwRunsController(logger, deps, nil)
}

func newSourceProwRunsController(logger logr.Logger, deps Dependencies, client prowjobs.Client) (*sourceProwRunsController, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("source.prow.runs: store dependency is required")
	}
	if deps.Source == nil {
		return nil, fmt.Errorf("source.prow.runs: source options dependency is required")
	}
	if len(deps.Source.Environments) == 0 {
		return nil, fmt.Errorf("source.prow.runs: no source environments configured")
	}
	if strings.TrimSpace(deps.Source.ProwBaseURL) == "" {
		return nil, fmt.Errorf("source.prow.runs: prow base URL is required")
	}

	for _, env := range deps.Source.Environments {
		if _, ok := sourceoptions.ProwJobNameForEnvironment(env); !ok {
			return nil, fmt.Errorf("source.prow.runs: missing prow job mapping for environment %q", normalizeEnvironment(env))
		}
	}

	if client == nil {
		client = prowjobs.NewHTTPClient(deps.Source.ProwBaseURL)
	}

	return &sourceProwRunsController{
		logger: logger.WithValues("controller", SourceProwRunsControllerName),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: SourceProwRunsControllerName,
			},
		),
		reconcileInterval: sourceProwRunsReconcileInterval,
		store:             deps.Store,
		prowClient:        client,
		deps:              deps,
	}, nil
}

func (c *sourceProwRunsController) Run(ctx context.Context, threadiness int) {
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

func (c *sourceProwRunsController) RunOnce(ctx context.Context, key string) error {
	c.logger.Info("Reconciling one key.", "key", key)
	return c.processKey(ctx, key)
}

func (c *sourceProwRunsController) SyncOnce(ctx context.Context) error {
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

func (c *sourceProwRunsController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *sourceProwRunsController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.processKey(ctx, key); err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, fmt.Errorf("failed processing key %q", key), "Error syncing; requeuing for later retry", "controller", SourceProwRunsControllerName, "key", key)
	c.queue.AddRateLimited(key)
	return true
}

func (c *sourceProwRunsController) queueMetadata(ctx context.Context) {
	keys, err := c.listKeys(ctx)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed listing keys for periodic enqueue", "controller", SourceProwRunsControllerName)
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.queue.Add(key)
	}
}

func (c *sourceProwRunsController) listKeys(_ context.Context) ([]string, error) {
	keys := make([]string, 0, len(c.deps.Source.Environments))
	for _, env := range c.deps.Source.Environments {
		normalized := normalizeEnvironment(env)
		if normalized == "" {
			continue
		}
		keys = append(keys, normalized)
	}
	return keys, nil
}

func (c *sourceProwRunsController) processKey(ctx context.Context, key string) error {
	environment := normalizeEnvironment(key)
	if environment == "" {
		return fmt.Errorf("empty key")
	}
	return c.syncEnvironment(ctx, environment)
}

func (c *sourceProwRunsController) syncEnvironment(ctx context.Context, environment string) error {
	jobName, ok := sourceoptions.ProwJobNameForEnvironment(environment)
	if !ok {
		return fmt.Errorf("missing prow job mapping for environment %q", normalizeEnvironment(environment))
	}

	checkpointTime, err := c.getCheckpointTime(ctx, environment)
	if err != nil {
		return err
	}
	since := c.resolveSince(checkpointTime)

	allJobs, err := c.prowClient.ListJobs(ctx)
	if err != nil {
		return fmt.Errorf("list prow jobs for environment %q: %w", environment, err)
	}

	jobs := filterCompletedJobsByNameAndSince(allJobs, jobName, since)

	now := time.Now().UTC()
	runRecords := make([]contracts.RunRecord, 0, len(jobs))
	for _, job := range jobs {
		record, ok := mapProwJobToRunRecord(c.deps.Source.ProwBaseURL, environment, job)
		if !ok {
			continue
		}
		existing, found, err := c.store.GetRun(ctx, environment, record.RunURL)
		if err != nil {
			return fmt.Errorf("get existing run record for environment=%q run_url=%q: %w", environment, record.RunURL, err)
		}
		record = mergeRunRecordFromProw(existing, found, record)
		runRecords = append(runRecords, record)
	}
	runRecords = dedupeRunRecords(runRecords)

	if len(runRecords) > 0 {
		if err := c.store.UpsertRuns(ctx, runRecords); err != nil {
			return fmt.Errorf("upsert %d prow run records for environment %q: %w", len(runRecords), environment, err)
		}
	}

	nextCheckpoint := computeNextProwRunsCheckpoint(checkpointTime, jobs)
	checkpoint := contracts.CheckpointRecord{
		Name:      prowRunsCheckpointNameForEnvironment(environment),
		Value:     nextCheckpoint.Format(time.RFC3339Nano),
		UpdatedAt: now.Format(time.RFC3339Nano),
	}
	if err := c.store.UpsertCheckpoints(ctx, []contracts.CheckpointRecord{checkpoint}); err != nil {
		return fmt.Errorf("update prow runs checkpoint for environment %q: %w", environment, err)
	}

	c.logger.Info(
		"Synced completed Prow runs for environment.",
		"environment", environment,
		"job_name", jobName,
		"fetched_total", len(allJobs),
		"matched_completed", len(jobs),
		"upserted_runs", len(runRecords),
		"since_completion", since.Format(time.RFC3339),
	)
	return nil
}

func (c *sourceProwRunsController) getCheckpointTime(ctx context.Context, environment string) (time.Time, error) {
	checkpoint, found, err := c.store.GetCheckpoint(ctx, prowRunsCheckpointNameForEnvironment(environment))
	if err != nil {
		return time.Time{}, fmt.Errorf("get prow runs checkpoint for environment %q: %w", environment, err)
	}
	if !found {
		return time.Time{}, nil
	}
	if parsed, ok := parseTimestamp(checkpoint.Value); ok {
		return parsed.UTC(), nil
	}
	c.logger.Info("Prow runs checkpoint timestamp is invalid; ignoring saved value.", "environment", environment, "value", checkpoint.Value)
	return time.Time{}, nil
}

func (c *sourceProwRunsController) resolveSince(lastCheckpoint time.Time) time.Time {
	floor := time.Now().UTC().Add(-c.deps.Source.ProwRecentWindow)
	if lastCheckpoint.IsZero() {
		return floor
	}
	since := lastCheckpoint.UTC().Add(-sourceProwRunsReplayWindow)
	if since.Before(floor) {
		return floor
	}
	return since
}

func mapProwJobToRunRecord(prowBaseURL string, environment string, job prowjobs.Job) (contracts.RunRecord, bool) {
	jobName := strings.TrimSpace(job.Spec.Job)
	if jobName == "" {
		return contracts.RunRecord{}, false
	}
	if !prowjobs.IsTerminalState(job.Status.State) || job.Status.CompletionTime.IsZero() || job.Status.StartTime.IsZero() {
		return contracts.RunRecord{}, false
	}

	runURL, err := prowartifacts.CanonicalRunURL(prowBaseURL, job.Status.URL)
	if err != nil {
		return contracts.RunRecord{}, false
	}

	record := contracts.RunRecord{
		Environment: normalizeEnvironment(environment),
		RunURL:      runURL,
		JobName:     jobName,
		Failed:      prowjobs.FailedFromState(job.Status.State),
		OccurredAt:  job.Status.StartTime.UTC().Format(time.RFC3339Nano),
	}

	if job.Spec.Refs != nil && len(job.Spec.Refs.Pulls) > 0 {
		record.PRNumber = job.Spec.Refs.Pulls[0].Number
		record.PRSHA = strings.TrimSpace(job.Spec.Refs.Pulls[0].SHA)
	}

	if !sourceoptions.SupportsPRLookupForEnvironment(environment) {
		record.MergedPR = true
		record.PostGoodCommit = true
	}

	return record, true
}

func filterCompletedJobsByNameAndSince(jobs []prowjobs.Job, jobName string, since time.Time) []prowjobs.Job {
	normalizedJobName := strings.TrimSpace(jobName)
	if normalizedJobName == "" {
		return []prowjobs.Job{}
	}

	filtered := make([]prowjobs.Job, 0, len(jobs))
	for _, job := range jobs {
		if strings.TrimSpace(job.Spec.Job) != normalizedJobName {
			continue
		}
		if !prowjobs.IsTerminalState(job.Status.State) || job.Status.CompletionTime.IsZero() {
			continue
		}
		if job.Status.CompletionTime.UTC().Before(since.UTC()) {
			continue
		}
		filtered = append(filtered, job)
	}
	return filtered
}

func computeNextProwRunsCheckpoint(previous time.Time, jobs []prowjobs.Job) time.Time {
	next := previous.UTC()
	for _, job := range jobs {
		if job.Status.CompletionTime.IsZero() {
			continue
		}
		completedAt := job.Status.CompletionTime.UTC()
		if completedAt.After(next) {
			next = completedAt
		}
	}
	if next.IsZero() {
		next = time.Now().UTC()
	}
	return next
}

func prowRunsCheckpointNameForEnvironment(environment string) string {
	return SourceProwRunsControllerName + "." + normalizeEnvironment(environment)
}
