package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const metricsRollupDailyReconcileInterval = 5 * time.Minute

const (
	metricRunCount                = "run_count"
	metricFailureCount            = "failure_count"
	metricFailureRowCount         = "failure_row_count"
	metricFailedCIInfraRunCount   = "failed_ci_infra_run_count"
	metricFailedProvisionRunCount = "failed_provision_run_count"
	metricFailedE2ERunCount       = "failed_e2e_run_count"
	metricCIInfraFailureCount     = "ci_infra_failure_count"
	metricProvisionFailureCount   = "provision_failure_count"
	metricE2EFailureCount         = "e2e_failure_count"

	metricPostGoodFailureCount          = "post_good_failure_count"
	metricPostGoodFailedE2EJobs         = "post_good_failed_e2e_jobs"
	metricPostGoodCIInfraFailureCount   = "post_good_ci_infra_failure_count"
	metricPostGoodProvisionFailureCount = "post_good_provision_failure_count"
	metricPostGoodE2EFailureCount       = "post_good_e2e_failure_count"
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
	hours, err := c.store.ListRunCountHourlyHours(ctx)
	if err != nil {
		return nil, fmt.Errorf("list run count hourly hours: %w", err)
	}

	dateSet := map[string]struct{}{}
	now := time.Now().UTC()
	for _, hour := range hours {
		hourValue := strings.TrimSpace(hour)
		if !isTimestampWithinWindow(hourValue, c.activeWindow, now) {
			continue
		}
		date, ok := dateFromMetricTimestamp(hourValue)
		if !ok {
			continue
		}
		dateSet[date] = struct{}{}
	}

	dates := make([]string, 0, len(dateSet))
	for date := range dateSet {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	return dates, nil
}

func (c *metricsRollupDailyController) processKey(ctx context.Context, key string) error {
	date, err := normalizeRollupDate(key)
	if err != nil {
		return fmt.Errorf("invalid rollup date key %q: %w", key, err)
	}

	out := make([]contracts.MetricDailyRecord, 0, len(c.envs)*10)
	for _, env := range c.envs {
		existingRows, err := c.store.ListMetricsDailyByDate(ctx, env, date)
		if err != nil {
			return fmt.Errorf("list existing metric daily rows for env=%q date=%q: %w", env, date, err)
		}
		if hasRequiredMetricSet(existingRows, env) {
			c.logger.V(1).Info("Skipping env/date; required daily metrics already materialized.", "date", date, "environment", env, "existing_rows", len(existingRows))
			continue
		}

		rows, err := c.store.ListRunCountsHourlyByDate(ctx, env, date)
		if err != nil {
			return fmt.Errorf("list run-count hourly rows for env=%q date=%q: %w", env, date, err)
		}

		rawFailures, err := c.store.ListRawFailuresByDate(ctx, env, date)
		if err != nil {
			return fmt.Errorf("list raw failures for env=%q date=%q: %w", env, date, err)
		}

		totalRuns := 0
		failedRuns := 0
		for _, row := range rows {
			totalRuns += row.TotalRuns
			failedRuns += row.FailedRuns
		}

		failureRowCount := len(rawFailures)
		if totalRuns == 0 && failureRowCount == 0 {
			continue
		}

		ciInfraRows := 0
		provisionRows := 0
		e2eRows := 0
		failedRunLaneByRunURL := map[string]string{}
		postGoodFailureRows := 0
		postGoodFailedE2EJobsSet := map[string]struct{}{}
		postGoodCIInfraRows := 0
		postGoodProvisionRows := 0
		postGoodE2ERows := 0

		runCache := map[string]contracts.RunRecord{}
		runFoundCache := map[string]bool{}
		for _, row := range rawFailures {
			laneFamily, err := classifyMetricLaneFamily(ctx, c.store, env, row, runCache, runFoundCache)
			if err != nil {
				return fmt.Errorf("classify lane family for env=%q date=%q run_url=%q row_id=%q: %w", env, date, row.RunURL, row.RowID, err)
			}
			switch laneFamily {
			case laneFamilyProvision:
				provisionRows++
			case laneFamilyE2E:
				e2eRows++
			default:
				ciInfraRows++
			}
			runURL := strings.TrimSpace(row.RunURL)
			if runURL != "" {
				failedRunLaneByRunURL[runURL] = mergeFailedRunLaneFamily(failedRunLaneByRunURL[runURL], laneFamily)
			}

			if env == "dev" && row.PostGoodCommitFailures > 0 {
				postGoodFailureRows++
				if runURL != "" {
					postGoodFailedE2EJobsSet[runURL] = struct{}{}
				}
				switch laneFamily {
				case laneFamilyProvision:
					postGoodProvisionRows++
				case laneFamilyE2E:
					postGoodE2ERows++
				default:
					postGoodCIInfraRows++
				}
			}
		}

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

		out = append(out,
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricRunCount, Value: float64(totalRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailureCount, Value: float64(failedRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailureRowCount, Value: float64(failureRowCount)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailedCIInfraRunCount, Value: float64(failedCIInfraRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailedProvisionRunCount, Value: float64(failedProvisionRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailedE2ERunCount, Value: float64(failedE2ERuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricCIInfraFailureCount, Value: float64(ciInfraRows)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricProvisionFailureCount, Value: float64(provisionRows)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricE2EFailureCount, Value: float64(e2eRows)},
		)
		if env == "dev" {
			out = append(out,
				contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricPostGoodFailureCount, Value: float64(postGoodFailureRows)},
				contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricPostGoodFailedE2EJobs, Value: float64(len(postGoodFailedE2EJobsSet))},
				contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricPostGoodCIInfraFailureCount, Value: float64(postGoodCIInfraRows)},
				contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricPostGoodProvisionFailureCount, Value: float64(postGoodProvisionRows)},
				contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricPostGoodE2EFailureCount, Value: float64(postGoodE2ERows)},
			)
		}
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

func dateFromMetricTimestamp(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", false
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts.UTC().Format("2006-01-02"), true
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts.UTC().Format("2006-01-02"), true
	}
	return "", false
}

func hasRequiredMetricSet(existingRows []contracts.MetricDailyRecord, environment string) bool {
	required := requiredMetricSet(environment)
	if len(required) == 0 {
		return false
	}
	if len(existingRows) == 0 {
		return false
	}
	seen := map[string]struct{}{}
	for _, row := range existingRows {
		metric := strings.TrimSpace(row.Metric)
		if metric == "" {
			continue
		}
		seen[metric] = struct{}{}
	}
	for _, metric := range required {
		if _, ok := seen[metric]; !ok {
			return false
		}
	}
	return true
}

func requiredMetricSet(environment string) []string {
	required := []string{
		metricRunCount,
		metricFailureCount,
		metricFailureRowCount,
		metricFailedCIInfraRunCount,
		metricFailedProvisionRunCount,
		metricFailedE2ERunCount,
		metricCIInfraFailureCount,
		metricE2EFailureCount,
		metricProvisionFailureCount,
	}
	if normalizeEnvironment(environment) != "dev" {
		return required
	}
	return append(required,
		metricPostGoodFailureCount,
		metricPostGoodFailedE2EJobs,
		metricPostGoodCIInfraFailureCount,
		metricPostGoodProvisionFailureCount,
		metricPostGoodE2EFailureCount,
	)
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

func classifyMetricLaneFamily(
	ctx context.Context,
	store contracts.Store,
	environment string,
	row contracts.RawFailureRecord,
	runCache map[string]contracts.RunRecord,
	runFoundCache map[string]bool,
) (string, error) {
	if row.NonArtifactBacked {
		// Synthetic rows are infrastructure ingestion gaps and should always be
		// attributed to ci/infra until artifact reconciliation completes.
		return laneFamilyCIInfra, nil
	}

	runURL := strings.TrimSpace(row.RunURL)
	jobName := ""
	if runURL != "" {
		if cachedFound, ok := runFoundCache[runURL]; ok {
			if cachedFound {
				jobName = strings.TrimSpace(runCache[runURL].JobName)
			}
		} else {
			run, found, err := store.GetRun(ctx, environment, runURL)
			if err != nil {
				return "", err
			}
			runFoundCache[runURL] = found
			if found {
				runCache[runURL] = run
				jobName = strings.TrimSpace(run.JobName)
			}
		}
	}

	lane := deriveLane(jobName, row.TestName, row.TestSuite)
	switch lane {
	case laneFamilyProvision:
		return laneFamilyProvision, nil
	case laneFamilyE2E:
		return laneFamilyE2E, nil
	default:
		return laneFamilyCIInfra, nil
	}
}

func deriveLane(jobName string, testName string, testSuite string) string {
	normalizedJob := strings.ToLower(strings.TrimSpace(jobName))
	normalizedName := strings.ToLower(strings.TrimSpace(testName))
	normalizedSuite := strings.ToLower(strings.TrimSpace(testSuite))

	switch {
	case strings.Contains(normalizedSuite, "step graph"):
		return laneFamilyProvision
	case strings.HasPrefix(normalizedName, "run pipeline step "):
		return laneFamilyProvision
	case strings.Contains(normalizedJob, "provision"):
		return laneFamilyProvision
	case strings.Contains(normalizedJob, "e2e"):
		return laneFamilyE2E
	default:
		return "unknown"
	}
}
