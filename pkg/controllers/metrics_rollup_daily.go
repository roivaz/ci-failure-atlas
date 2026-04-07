package controllers

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	sourcelanes "ci-failure-atlas/pkg/source/lanes"
	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const metricsRollupDailyReconcileInterval = 5 * time.Minute

const (
	metricRunCount                = "run_count"
	metricFailureCount            = "failure_count"
	metricFailedCIInfraRunCount   = "failed_ci_infra_run_count"
	metricFailedProvisionRunCount = "failed_provision_run_count"
	metricFailedE2ERunCount       = "failed_e2e_run_count"

	metricPostGoodRunCount                = "post_good_run_count"
	metricPostGoodFailedE2EJobs           = "post_good_failed_e2e_jobs"
	metricPostGoodFailedCIInfraRunCount   = "post_good_failed_ci_infra_run_count"
	metricPostGoodFailedProvisionRunCount = "post_good_failed_provision_run_count"
)

const (
	laneFamilyCIInfra   = "ci_infra"
	laneFamilyProvision = "provision"
	laneFamilyE2E       = "e2e"
)

type metricsRollupDailyController struct {
	logger            logr.Logger
	reconcileInterval time.Duration
	queue             workqueue.TypedRateLimitingInterface[string]
	activeWindow      time.Duration

	store contracts.Store
	envs  []string
}

var _ Controller = (*metricsRollupDailyController)(nil)

func NewMetricsRollupDaily(logger logr.Logger, deps Dependencies) (Controller, error) {
	return newMetricsRollupDailyController(logger, deps)
}

func newMetricsRollupDailyController(logger logr.Logger, deps Dependencies) (*metricsRollupDailyController, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("metrics.rollup.daily: store dependency is required")
	}
	if deps.Source == nil {
		return nil, fmt.Errorf("metrics.rollup.daily: source options dependency is required")
	}
	if len(deps.Source.Environments) == 0 {
		return nil, fmt.Errorf("metrics.rollup.daily: at least one environment is required")
	}

	envs := make([]string, 0, len(deps.Source.Environments))
	for _, env := range deps.Source.Environments {
		normalized := normalizeEnvironment(env)
		if normalized == "" {
			continue
		}
		envs = append(envs, normalized)
	}
	if len(envs) == 0 {
		return nil, fmt.Errorf("metrics.rollup.daily: no valid environments configured")
	}

	return &metricsRollupDailyController{
		logger: logger.WithValues("controller", MetricsRollupDailyControllerName),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: MetricsRollupDailyControllerName,
			},
		),
		reconcileInterval: metricsRollupDailyReconcileInterval,
		activeWindow:      activeReconcileWindow(deps.Source),
		store:             deps.Store,
		envs:              envs,
	}, nil
}

func (c *metricsRollupDailyController) Run(ctx context.Context, threadiness int) {
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

func (c *metricsRollupDailyController) RunOnce(ctx context.Context, key string) error {
	c.logger.Info("Reconciling one key.", "key", key)
	return c.processKey(ctx, key)
}

func (c *metricsRollupDailyController) SyncOnce(ctx context.Context) error {
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

func (c *metricsRollupDailyController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *metricsRollupDailyController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.processKey(ctx, key); err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, fmt.Errorf("failed processing key %q", key), "Error syncing; requeuing for later retry", "controller", MetricsRollupDailyControllerName, "key", key)
	c.queue.AddRateLimited(key)
	return true
}

func (c *metricsRollupDailyController) queueMetadata(ctx context.Context) {
	keys, err := c.listKeys(ctx)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed listing keys for periodic enqueue", "controller", MetricsRollupDailyControllerName)
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.queue.Add(key)
	}
}

func (c *metricsRollupDailyController) listKeys(ctx context.Context) ([]string, error) {
	dates, err := c.store.ListRunDates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list run dates: %w", err)
	}
	now := time.Now().UTC()
	filtered := make([]string, 0, len(dates))
	for _, date := range dates {
		dateValue := strings.TrimSpace(date)
		if dateValue == "" {
			continue
		}
		if !isDateWithinWindow(dateValue, c.activeWindow, now) {
			continue
		}
		filtered = append(filtered, dateValue)
	}
	sort.Strings(filtered)
	return filtered, nil
}

func (c *metricsRollupDailyController) processKey(ctx context.Context, key string) error {
	date, err := normalizeRollupDate(key)
	if err != nil {
		return fmt.Errorf("invalid rollup date key %q: %w", key, err)
	}

	out := make([]contracts.MetricDailyRecord, 0, len(c.envs)*9)
	for _, env := range c.envs {
		runs, err := c.store.ListRunsByDate(ctx, env, date)
		if err != nil {
			return fmt.Errorf("list runs for env=%q date=%q: %w", env, date, err)
		}

		rawFailures, err := c.store.ListRawFailuresByDate(ctx, env, date)
		if err != nil {
			return fmt.Errorf("list raw failures for env=%q date=%q: %w", env, date, err)
		}

		totalRuns := len(runs)
		failedRunsWithoutURL := 0
		failedRunURLs := map[string]struct{}{}
		postGoodRunCount := 0
		postGoodFailedRunURLs := map[string]struct{}{}
		postGoodFailedRunsWithoutURL := 0
		for _, run := range runs {
			runURL := strings.TrimSpace(run.RunURL)
			if run.PostGoodCommit {
				postGoodRunCount++
			}
			if !run.Failed {
				continue
			}
			if runURL == "" {
				failedRunsWithoutURL++
				if run.PostGoodCommit {
					postGoodFailedRunsWithoutURL++
				}
				continue
			}
			failedRunURLs[runURL] = struct{}{}
			if run.PostGoodCommit {
				postGoodFailedRunURLs[runURL] = struct{}{}
			}
		}

		if totalRuns == 0 && len(rawFailures) == 0 {
			continue
		}

		failedRunLaneByRunURL := map[string]string{}

		runCache := map[string]contracts.RunRecord{}
		runFoundCache := map[string]bool{}
		for _, row := range rawFailures {
			laneFamily := classifyMetricLaneFamily(env, row)
			runURL := strings.TrimSpace(row.RunURL)
			if runURL != "" {
				// Any raw-failure row implies that the backing run failed, even if
				// stale run metadata has failed=false.
				failedRunURLs[runURL] = struct{}{}
			}
			if _, failedRun := failedRunURLs[runURL]; failedRun && runURL != "" {
				failedRunLaneByRunURL[runURL] = mergeFailedRunLaneFamily(failedRunLaneByRunURL[runURL], laneFamily)
			}
			if env == "dev" && runURL != "" {
				postGoodRun, err := isMetricPostGoodRun(ctx, c.store, env, runURL, runCache, runFoundCache)
				if err != nil {
					return fmt.Errorf("check post-good run signal for env=%q date=%q run_url=%q: %w", env, date, runURL, err)
				}
				if postGoodRun {
					postGoodFailedRunURLs[runURL] = struct{}{}
				}
			}
		}

		failedRuns := failedRunsWithoutURL + len(failedRunURLs)

		failedCIInfraRuns := 0
		failedProvisionRuns := 0
		failedE2ERuns := 0
		for _, laneFamily := range failedRunLaneByRunURL {
			switch laneFamily {
			case laneFamilyProvision:
				failedProvisionRuns++
			case laneFamilyE2E:
				failedE2ERuns++
			default:
				failedCIInfraRuns++
			}
		}
		// If some failed runs still have no lane assignment (for example a delayed
		// raw-failure materialization race), attribute them to CI/Infra so the
		// failed-run lane partition remains complete for visualization.
		unclassifiedFailedRuns := failedRuns - (failedCIInfraRuns + failedProvisionRuns + failedE2ERuns)
		if unclassifiedFailedRuns > 0 {
			failedCIInfraRuns += unclassifiedFailedRuns
		}

		expectedRows := make([]contracts.MetricDailyRecord, 0, 9)
		expectedRows = append(expectedRows,
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricRunCount, Value: float64(totalRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailureCount, Value: float64(failedRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailedCIInfraRunCount, Value: float64(failedCIInfraRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailedProvisionRunCount, Value: float64(failedProvisionRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailedE2ERunCount, Value: float64(failedE2ERuns)},
		)
		if env == "dev" {
			postGoodFailedE2ERuns := 0
			postGoodFailedCIInfraRuns := postGoodFailedRunsWithoutURL
			postGoodFailedProvisionRuns := 0
			for runURL := range postGoodFailedRunURLs {
				switch normalizeFailedRunLaneFamily(failedRunLaneByRunURL[runURL]) {
				case laneFamilyProvision:
					postGoodFailedProvisionRuns++
				case laneFamilyE2E:
					postGoodFailedE2ERuns++
				default:
					postGoodFailedCIInfraRuns++
				}
			}
			expectedRows = append(expectedRows,
				contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricPostGoodRunCount, Value: float64(postGoodRunCount)},
				contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricPostGoodFailedE2EJobs, Value: float64(postGoodFailedE2ERuns)},
				contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricPostGoodFailedCIInfraRunCount, Value: float64(postGoodFailedCIInfraRuns)},
				contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricPostGoodFailedProvisionRunCount, Value: float64(postGoodFailedProvisionRuns)},
			)
		}

		existingRows, err := c.store.ListMetricsDailyByDate(ctx, env, date)
		if err != nil {
			return fmt.Errorf("list metrics daily rows for env=%q date=%q: %w", env, date, err)
		}
		if metricsDailyRowsMatch(existingRows, expectedRows) {
			c.logger.V(1).Info("Skipping env/date; metrics already up to date.", "date", date, "environment", env, "metrics", len(expectedRows))
			continue
		}
		out = append(out, expectedRows...)
	}

	if len(out) == 0 {
		c.logger.V(1).Info("No rollup output rows for date.", "date", date)
		return nil
	}

	if err := c.store.UpsertMetricsDaily(ctx, out); err != nil {
		return fmt.Errorf("upsert %d metric daily rows for date=%q: %w", len(out), date, err)
	}

	c.logger.Info("Rolled up daily metrics.", "date", date, "rows", len(out), "environments", len(c.envs))
	return nil
}

func normalizeRollupDate(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("date is empty")
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return "", err
	}
	return parsed.UTC().Format("2006-01-02"), nil
}

func isDateWithinWindow(value string, window time.Duration, now time.Time) bool {
	if window <= 0 {
		return true
	}
	normalized, err := normalizeRollupDate(value)
	if err != nil {
		return false
	}
	parsed, err := time.Parse("2006-01-02", normalized)
	if err != nil {
		return false
	}
	dayEnd := parsed.UTC().Add(24 * time.Hour)
	return now.Sub(dayEnd) <= window
}

func isMetricPostGoodRun(
	ctx context.Context,
	store contracts.Store,
	environment string,
	runURL string,
	runCache map[string]contracts.RunRecord,
	runFoundCache map[string]bool,
) (bool, error) {
	normalizedRunURL := strings.TrimSpace(runURL)
	if normalizedRunURL == "" {
		return false, nil
	}
	if cachedFound, ok := runFoundCache[normalizedRunURL]; ok {
		if !cachedFound {
			return false, nil
		}
		return runCache[normalizedRunURL].PostGoodCommit, nil
	}
	run, found, err := store.GetRun(ctx, environment, normalizedRunURL)
	if err != nil {
		return false, err
	}
	runFoundCache[normalizedRunURL] = found
	if !found {
		return false, nil
	}
	runCache[normalizedRunURL] = run
	return run.PostGoodCommit, nil
}

func mergeFailedRunLaneFamily(current string, next string) string {
	currentRank := failedRunLaneFamilyRank(current)
	nextRank := failedRunLaneFamilyRank(next)
	if nextRank > currentRank {
		return normalizeFailedRunLaneFamily(next)
	}
	return normalizeFailedRunLaneFamily(current)
}

func failedRunLaneFamilyRank(laneFamily string) int {
	switch normalizeFailedRunLaneFamily(laneFamily) {
	case laneFamilyProvision:
		return 3
	case laneFamilyE2E:
		return 2
	default:
		return 1
	}
}

func normalizeFailedRunLaneFamily(laneFamily string) string {
	switch strings.TrimSpace(laneFamily) {
	case laneFamilyProvision:
		return laneFamilyProvision
	case laneFamilyE2E:
		return laneFamilyE2E
	default:
		return laneFamilyCIInfra
	}
}

func classifyMetricLaneFamily(environment string, row contracts.RawFailureRecord) string {
	if row.NonArtifactBacked {
		// Synthetic rows are infrastructure ingestion gaps and should always be
		// attributed to ci/infra until artifact reconciliation completes.
		return laneFamilyCIInfra
	}

	switch sourcelanes.ClassifyLane(environment, row.TestSuite, row.TestName) {
	case sourcelanes.LaneProvision:
		return laneFamilyProvision
	case sourcelanes.LaneE2E:
		return laneFamilyE2E
	default:
		return laneFamilyCIInfra
	}
}

func metricsDailyRowsMatch(existingRows []contracts.MetricDailyRecord, expectedRows []contracts.MetricDailyRecord) bool {
	if len(existingRows) != len(expectedRows) {
		return false
	}
	if len(expectedRows) == 0 {
		return true
	}
	existingByMetric := make(map[string]float64, len(existingRows))
	for _, row := range existingRows {
		metric := strings.TrimSpace(row.Metric)
		if metric == "" {
			return false
		}
		existingByMetric[metric] = row.Value
	}
	if len(existingByMetric) != len(expectedRows) {
		return false
	}
	for _, row := range expectedRows {
		metric := strings.TrimSpace(row.Metric)
		existingValue, found := existingByMetric[metric]
		if !found {
			return false
		}
		if math.Abs(existingValue-row.Value) > 1e-9 {
			return false
		}
	}
	return true
}
