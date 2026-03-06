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
	metricTotalRuns      = "total_runs"
	metricFailedRuns     = "failed_runs"
	metricSuccessfulRuns = "successful_runs"
	metricRunFailureRate = "run_failure_rate"
	metricRawFailureRows = "raw_failure_rows"
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

	out := make([]contracts.MetricDailyRecord, 0, len(c.envs)*5)
	for _, env := range c.envs {
		existingRows, err := c.store.ListMetricsDailyByDate(ctx, env, date)
		if err != nil {
			return fmt.Errorf("list existing metric daily rows for env=%q date=%q: %w", env, date, err)
		}
		if len(existingRows) > 0 {
			c.logger.V(1).Info("Skipping env/date; daily metrics already materialized.", "date", date, "environment", env, "existing_rows", len(existingRows))
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
		successfulRuns := 0
		for _, row := range rows {
			totalRuns += row.TotalRuns
			failedRuns += row.FailedRuns
			successfulRuns += row.SuccessfulRuns
		}

		rawFailureRows := len(rawFailures)

		if totalRuns == 0 && rawFailureRows == 0 {
			continue
		}

		failureRate := 0.0
		if totalRuns > 0 {
			failureRate = float64(failedRuns) / float64(totalRuns)
		}

		out = append(out,
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricTotalRuns, Value: float64(totalRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricFailedRuns, Value: float64(failedRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricSuccessfulRuns, Value: float64(successfulRuns)},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricRunFailureRate, Value: failureRate},
			contracts.MetricDailyRecord{Environment: env, Date: date, Metric: metricRawFailureRows, Value: float64(rawFailureRows)},
		)
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
