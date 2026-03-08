package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	sippysource "ci-failure-atlas/pkg/source/sippy"
	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	sourceSippyRunsReconcileInterval = 30 * time.Minute
	sourceSippyRunsDefaultPageSize   = 1000
	sourceSippyRunsReplayWindow      = 3 * time.Hour
)

// TODO: Wire this map via CLI flags/config file (same for deterministic JUnit path mapping).
var sippyJobNameByEnvironment = map[string]string{
	"dev":  "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
	"int":  "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel",
	"stg":  "periodic-ci-Azure-ARO-HCP-main-periodic-stage-e2e-parallel",
	"prod": "periodic-ci-Azure-ARO-HCP-main-periodic-prod-e2e-parallel",
}

type sourceSippyRunsController struct {
	logger            logr.Logger
	reconcileInterval time.Duration
	queue             workqueue.TypedRateLimitingInterface[string]

	store       contracts.Store
	sippyClient sippysource.Client
	deps        Dependencies
}

var _ Controller = (*sourceSippyRunsController)(nil)

func NewSourceSippyRuns(logger logr.Logger, deps Dependencies) (Controller, error) {
	return newSourceSippyRunsController(logger, deps, nil)
}

func newSourceSippyRunsController(logger logr.Logger, deps Dependencies, client sippysource.Client) (*sourceSippyRunsController, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("source.sippy.runs: store dependency is required")
	}
	if deps.Source == nil {
		return nil, fmt.Errorf("source.sippy.runs: source options dependency is required")
	}
	if len(deps.Source.Environments) == 0 {
		return nil, fmt.Errorf("source.sippy.runs: no source environments configured")
	}

	for _, env := range deps.Source.Environments {
		if strings.TrimSpace(deps.Source.SippyReleaseByEnv[env]) == "" {
			return nil, fmt.Errorf("source.sippy.runs: missing sippy release mapping for environment %q", env)
		}
		if _, err := sippyJobNameForEnvironment(env); err != nil {
			return nil, fmt.Errorf("source.sippy.runs: %w", err)
		}
	}

	if client == nil {
		client = sippysource.NewHTTPClient(deps.Source.SippyBaseURL)
	}

	return &sourceSippyRunsController{
		logger: logger.WithValues("controller", SourceSippyRunsControllerName),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: SourceSippyRunsControllerName,
			},
		),
		reconcileInterval: sourceSippyRunsReconcileInterval,
		store:             deps.Store,
		sippyClient:       client,
		deps:              deps,
	}, nil
}

func (c *sourceSippyRunsController) Run(ctx context.Context, threadiness int) {
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

func (c *sourceSippyRunsController) RunOnce(ctx context.Context, key string) error {
	c.logger.Info("Reconciling one key.", "key", key)
	return c.processKey(ctx, key)
}

func (c *sourceSippyRunsController) SyncOnce(ctx context.Context) error {
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

func (c *sourceSippyRunsController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *sourceSippyRunsController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.processKey(ctx, key); err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, fmt.Errorf("failed processing key %q", key), "Error syncing; requeuing for later retry", "controller", SourceSippyRunsControllerName, "key", key)
	c.queue.AddRateLimited(key)
	return true
}

func (c *sourceSippyRunsController) queueMetadata(ctx context.Context) {
	keys, err := c.listKeys(ctx)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed listing keys for periodic enqueue", "controller", SourceSippyRunsControllerName)
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.queue.Add(key)
	}
}

func (c *sourceSippyRunsController) listKeys(_ context.Context) ([]string, error) {
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

func (c *sourceSippyRunsController) processKey(ctx context.Context, key string) error {
	normalized := strings.TrimSpace(key)
	if normalized == "" {
		return fmt.Errorf("empty key")
	}
	if strings.Contains(normalized, "|") {
		return c.syncSingleRunByKey(ctx, normalized)
	}
	return c.syncEnvironment(ctx, normalizeEnvironment(normalized))
}

func (c *sourceSippyRunsController) syncEnvironment(ctx context.Context, environment string) error {
	release, err := c.releaseForEnvironment(environment)
	if err != nil {
		return err
	}
	jobName, err := sippyJobNameForEnvironment(environment)
	if err != nil {
		return err
	}
	usePRRepoFilter := supportsPRLookupForEnvironment(environment)
	org := c.deps.Source.SippyOrg
	repo := c.deps.Source.SippyRepo
	if !usePRRepoFilter {
		org = ""
		repo = ""
	}

	checkpointTime, err := c.getCheckpointTime(ctx, environment)
	if err != nil {
		return err
	}
	since := c.resolveSince(checkpointTime)

	allRuns, err := c.sippyClient.ListJobRuns(ctx, sippysource.ListJobRunsOptions{
		Release:  release,
		Org:      org,
		Repo:     repo,
		JobName:  jobName,
		Since:    since,
		PageSize: sourceSippyRunsDefaultPageSize,
	})
	if err != nil {
		return fmt.Errorf("list sippy job runs for environment %q: %w", environment, err)
	}

	runs := filterRunsByJobName(allRuns, jobName)
	hourlyCounts := buildHourlyRunCounts(environment, runs)
	if len(hourlyCounts) > 0 {
		if err := c.store.UpsertRunCountsHourly(ctx, hourlyCounts); err != nil {
			return fmt.Errorf("upsert %d hourly run-count records for environment %q: %w", len(hourlyCounts), environment, err)
		}
	}

	failedRuns := make([]sippysource.JobRun, 0, len(runs))
	for _, run := range runs {
		if !run.Failed {
			continue
		}
		failedRuns = append(failedRuns, run)
	}

	now := time.Now().UTC()
	runRecords := make([]contracts.RunRecord, 0, len(failedRuns))
	for _, run := range failedRuns {
		existing, found, err := c.store.GetRun(ctx, environment, run.RunURL)
		if err != nil {
			return fmt.Errorf("get existing run record for environment=%q run_url=%q: %w", environment, run.RunURL, err)
		}
		record := mapToRunRecord(environment, run, existing, found)
		if record.RunURL == "" {
			continue
		}
		runRecords = append(runRecords, record)
	}
	runRecords = dedupeRunRecords(runRecords)

	if len(runRecords) > 0 {
		if err := c.store.UpsertRuns(ctx, runRecords); err != nil {
			return fmt.Errorf("upsert %d run records for environment %q: %w", len(runRecords), environment, err)
		}
	}

	nextCheckpoint := computeNextCheckpoint(checkpointTime, runs)
	checkpoint := contracts.CheckpointRecord{
		Name:      checkpointNameForEnvironment(environment),
		Value:     nextCheckpoint.Format(time.RFC3339Nano),
		UpdatedAt: now.Format(time.RFC3339Nano),
	}
	if err := c.store.UpsertCheckpoints(ctx, []contracts.CheckpointRecord{checkpoint}); err != nil {
		return fmt.Errorf("update checkpoint for environment %q: %w", environment, err)
	}

	c.logger.Info(
		"Synced Sippy runs for environment.",
		"environment", environment,
		"job_name", jobName,
		"fetched_total", len(allRuns),
		"fetched_job_matched", len(runs),
		"failed", len(failedRuns),
		"hourly_buckets", len(hourlyCounts),
		"upserted_runs", len(runRecords),
		"pr_lookup_enabled", false,
		"since", since.Format(time.RFC3339),
	)
	return nil
}

func (c *sourceSippyRunsController) syncSingleRunByKey(ctx context.Context, key string) error {
	parts := strings.SplitN(key, "|", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid run key %q: expected environment|run_url", key)
	}

	environment := normalizeEnvironment(parts[0])
	runURL := strings.TrimSpace(parts[1])
	if runURL == "" {
		return fmt.Errorf("invalid run key %q: empty run_url", key)
	}

	release, err := c.releaseForEnvironment(environment)
	if err != nil {
		return err
	}
	jobName, err := sippyJobNameForEnvironment(environment)
	if err != nil {
		return err
	}
	usePRRepoFilter := supportsPRLookupForEnvironment(environment)
	org := c.deps.Source.SippyOrg
	repo := c.deps.Source.SippyRepo
	if !usePRRepoFilter {
		org = ""
		repo = ""
	}

	since := c.resolveSince(time.Time{})
	allRuns, err := c.sippyClient.ListJobRuns(ctx, sippysource.ListJobRunsOptions{
		Release:  release,
		Org:      org,
		Repo:     repo,
		JobName:  jobName,
		Since:    since,
		PageSize: sourceSippyRunsDefaultPageSize,
	})
	if err != nil {
		return fmt.Errorf("list sippy job runs for key %q: %w", key, err)
	}

	runs := filterRunsByJobName(allRuns, jobName)
	targetRun, found := findRunByURL(runs, runURL)
	if !found {
		return fmt.Errorf("run not found in lookback window for key %q (job=%q)", key, jobName)
	}

	if !targetRun.StartedAt.IsZero() {
		if err := c.store.UpsertRunCountsHourly(ctx, buildHourlyRunCountsForHour(environment, runs, targetRun.StartedAt)); err != nil {
			return fmt.Errorf("upsert run-count hourly for key %q: %w", key, err)
		}
	}

	if !targetRun.Failed {
		c.logger.Info("Skipping run because it is not failed.", "key", key, "run_url", runURL)
		return nil
	}

	existing, foundExisting, err := c.store.GetRun(ctx, environment, runURL)
	if err != nil {
		return fmt.Errorf("get existing run metadata for key %q: %w", key, err)
	}

	record := mapToRunRecord(environment, targetRun, existing, foundExisting)
	if err := c.store.UpsertRuns(ctx, []contracts.RunRecord{record}); err != nil {
		return fmt.Errorf("upsert run %q: %w", runURL, err)
	}

	c.logger.Info("Synced one run.", "key", key)
	return nil
}

func (c *sourceSippyRunsController) releaseForEnvironment(environment string) (string, error) {
	normalized := normalizeEnvironment(environment)
	if normalized == "" {
		return "", fmt.Errorf("empty environment")
	}
	release := strings.TrimSpace(c.deps.Source.SippyReleaseByEnv[normalized])
	if release == "" {
		return "", fmt.Errorf("missing sippy release for environment %q", normalized)
	}
	return release, nil
}

func sippyJobNameForEnvironment(environment string) (string, error) {
	normalized := normalizeEnvironment(environment)
	if normalized == "" {
		return "", fmt.Errorf("empty environment")
	}
	jobName := strings.TrimSpace(sippyJobNameByEnvironment[normalized])
	if jobName == "" {
		return "", fmt.Errorf("missing sippy job mapping for environment %q", normalized)
	}
	return jobName, nil
}

func (c *sourceSippyRunsController) getCheckpointTime(ctx context.Context, environment string) (time.Time, error) {
	checkpoint, found, err := c.store.GetCheckpoint(ctx, checkpointNameForEnvironment(environment))
	if err != nil {
		return time.Time{}, fmt.Errorf("get checkpoint for environment %q: %w", environment, err)
	}
	if !found {
		return time.Time{}, nil
	}
	if parsed, ok := parseTimestamp(checkpoint.Value); ok {
		return parsed.UTC(), nil
	}
	c.logger.Info("Checkpoint timestamp is invalid; ignoring saved value.", "environment", environment, "value", checkpoint.Value)
	return time.Time{}, nil
}

func (c *sourceSippyRunsController) resolveSince(lastCheckpoint time.Time) time.Time {
	lookback := c.deps.Source.SippyLookback
	if lookback <= 0 {
		lookback = 7 * 24 * time.Hour
	}
	if !lastCheckpoint.IsZero() {
		return lastCheckpoint.UTC().Add(-sourceSippyRunsReplayWindow).Truncate(time.Hour)
	}
	return time.Now().UTC().Add(-lookback).Truncate(time.Hour)
}

func computeNextCheckpoint(previous time.Time, runs []sippysource.JobRun) time.Time {
	next := previous.UTC()
	for _, run := range runs {
		if run.StartedAt.IsZero() {
			continue
		}
		startedAt := run.StartedAt.UTC()
		if startedAt.After(next) {
			next = startedAt
		}
	}
	if next.IsZero() {
		next = time.Now().UTC()
	}
	return next
}

func filterRunsByJobName(runs []sippysource.JobRun, jobName string) []sippysource.JobRun {
	normalizedJobName := strings.TrimSpace(jobName)
	if normalizedJobName == "" {
		return []sippysource.JobRun{}
	}
	filtered := make([]sippysource.JobRun, 0, len(runs))
	for _, run := range runs {
		if strings.TrimSpace(run.JobName) != normalizedJobName {
			continue
		}
		filtered = append(filtered, run)
	}
	return filtered
}

func mapToRunRecord(environment string, run sippysource.JobRun, existing contracts.RunRecord, existingFound bool) contracts.RunRecord {
	normalizedPRSHA := strings.TrimSpace(run.PRSHA)
	periodicMergedCodeEnvironment := !supportsPRLookupForEnvironment(environment)
	record := contracts.RunRecord{
		Environment: normalizeEnvironment(environment),
		RunURL:      strings.TrimSpace(run.RunURL),
		JobName:     strings.TrimSpace(run.JobName),
		PRNumber:    run.PRNumber,
		PRSHA:       normalizedPRSHA,
	}
	if periodicMergedCodeEnvironment {
		record.PRState = ""
		record.FinalMergedSHA = ""
		record.MergedPR = true
		record.PostGoodCommit = true
	} else {
		if existingFound && existing.PRNumber == run.PRNumber {
			record.PRState = strings.TrimSpace(existing.PRState)
			record.FinalMergedSHA = strings.TrimSpace(existing.FinalMergedSHA)
			record.MergedPR = existing.MergedPR
			record.PostGoodCommit = existing.PostGoodCommit
		} else {
			record.PRState = ""
			record.FinalMergedSHA = ""
			record.MergedPR = false
			record.PostGoodCommit = false
		}
	}
	if !run.StartedAt.IsZero() {
		record.OccurredAt = run.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	return record
}

func supportsPRLookupForEnvironment(environment string) bool {
	return normalizeEnvironment(environment) == "dev"
}

func findRunByURL(runs []sippysource.JobRun, runURL string) (sippysource.JobRun, bool) {
	target := strings.TrimSpace(runURL)
	if target == "" {
		return sippysource.JobRun{}, false
	}
	for _, run := range runs {
		if strings.TrimSpace(run.RunURL) == target {
			return run, true
		}
	}
	return sippysource.JobRun{}, false
}

func dedupeRunRecords(rows []contracts.RunRecord) []contracts.RunRecord {
	if len(rows) == 0 {
		return []contracts.RunRecord{}
	}
	byKey := make(map[string]contracts.RunRecord, len(rows))
	for _, row := range rows {
		normalizedEnvironment := normalizeEnvironment(row.Environment)
		normalizedRunURL := strings.TrimSpace(row.RunURL)
		if normalizedEnvironment == "" || normalizedRunURL == "" {
			continue
		}
		key := normalizedEnvironment + "|" + normalizedRunURL
		byKey[key] = row
	}

	out := make([]contracts.RunRecord, 0, len(byKey))
	for _, row := range byKey {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Environment != out[j].Environment {
			return out[i].Environment < out[j].Environment
		}
		return out[i].RunURL < out[j].RunURL
	})
	return out
}

func checkpointNameForEnvironment(environment string) string {
	return SourceSippyRunsControllerName + "." + normalizeEnvironment(environment)
}

func buildHourlyRunCounts(environment string, runs []sippysource.JobRun) []contracts.RunCountHourlyRecord {
	return buildHourlyRunCountsFiltered(environment, runs, time.Time{})
}

func buildHourlyRunCountsForHour(environment string, runs []sippysource.JobRun, hour time.Time) []contracts.RunCountHourlyRecord {
	return buildHourlyRunCountsFiltered(environment, runs, hour.UTC().Truncate(time.Hour))
}

func buildHourlyRunCountsFiltered(environment string, runs []sippysource.JobRun, onlyHour time.Time) []contracts.RunCountHourlyRecord {
	normalizedEnv := normalizeEnvironment(environment)
	if normalizedEnv == "" {
		return []contracts.RunCountHourlyRecord{}
	}

	filterHour := onlyHour.UTC().Truncate(time.Hour)
	useFilter := !onlyHour.IsZero()

	byHour := map[string]contracts.RunCountHourlyRecord{}
	for _, run := range runs {
		if run.StartedAt.IsZero() {
			continue
		}
		hour := run.StartedAt.UTC().Truncate(time.Hour)
		if useFilter && !hour.Equal(filterHour) {
			continue
		}
		hourKey := hour.Format(time.RFC3339)

		count := byHour[hourKey]
		if count.Environment == "" {
			count = contracts.RunCountHourlyRecord{
				Environment: normalizedEnv,
				Hour:        hourKey,
			}
		}
		count.TotalRuns++
		if run.Failed {
			count.FailedRuns++
		} else {
			count.SuccessfulRuns++
		}
		byHour[hourKey] = count
	}

	out := make([]contracts.RunCountHourlyRecord, 0, len(byHour))
	for _, row := range byHour {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Hour < out[j].Hour
	})
	return out
}

func normalizeEnvironment(environment string) string {
	return strings.ToLower(strings.TrimSpace(environment))
}

func parseTimestamp(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, false
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts, true
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts, true
	}
	return time.Time{}, false
}
