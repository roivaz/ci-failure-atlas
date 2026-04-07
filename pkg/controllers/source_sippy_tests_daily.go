package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	sourcelanes "ci-failure-atlas/pkg/source/lanes"
	sippysource "ci-failure-atlas/pkg/source/sippy"
	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	sourceSippyTestsDailyReconcileInterval = 1 * time.Hour
	sourceSippyTestsDailyDefaultPeriod     = "default"
	sourceSippyTestsDailyTwoDayPeriod      = "twoDay"
)

var requiredSippyTestsDailyPeriods = []string{
	sourceSippyTestsDailyDefaultPeriod,
	sourceSippyTestsDailyTwoDayPeriod,
}

type compiledTestFilter struct {
	testSuite        string
	testNameRegex    *regexp.Regexp
	testNameContains string
}

type sippyFilterItem struct {
	ColumnField   string `json:"columnField"`
	OperatorValue string `json:"operatorValue"`
	Value         string `json:"value"`
}

type sippyFilterModel struct {
	Items        []sippyFilterItem `json:"items"`
	LinkOperator string            `json:"linkOperator"`
}

type sourceSippyTestsDailyController struct {
	logger            logr.Logger
	reconcileInterval time.Duration
	queue             workqueue.TypedRateLimitingInterface[string]

	store       contracts.Store
	sippyClient sippysource.Client
	deps        Dependencies
	envSet      map[string]struct{}
	testFilters map[string][]compiledTestFilter
}

var _ Controller = (*sourceSippyTestsDailyController)(nil)

func NewSourceSippyTestsDaily(logger logr.Logger, deps Dependencies) (Controller, error) {
	return newSourceSippyTestsDailyController(logger, deps, nil)
}

func newSourceSippyTestsDailyController(logger logr.Logger, deps Dependencies, client sippysource.Client) (*sourceSippyTestsDailyController, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("source.sippy.tests-daily: store dependency is required")
	}
	if deps.Source == nil {
		return nil, fmt.Errorf("source.sippy.tests-daily: source options dependency is required")
	}
	if len(deps.Source.Environments) == 0 {
		return nil, fmt.Errorf("source.sippy.tests-daily: at least one environment is required")
	}

	envSet := map[string]struct{}{}
	for _, env := range deps.Source.Environments {
		normalized := normalizeEnvironment(env)
		if normalized == "" {
			continue
		}
		if strings.TrimSpace(deps.Source.SippyReleaseByEnv[normalized]) == "" {
			return nil, fmt.Errorf("source.sippy.tests-daily: missing sippy release mapping for environment %q", normalized)
		}
		envSet[normalized] = struct{}{}
	}
	if len(envSet) == 0 {
		return nil, fmt.Errorf("source.sippy.tests-daily: no valid environments configured")
	}
	testFilters, err := buildEnvironmentTestFilters(envSet)
	if err != nil {
		return nil, err
	}

	if client == nil {
		client = sippysource.NewHTTPClient(deps.Source.SippyBaseURL)
	}

	return &sourceSippyTestsDailyController{
		logger: logger.WithValues("controller", SourceSippyTestsDailyControllerName),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: SourceSippyTestsDailyControllerName,
			},
		),
		reconcileInterval: sourceSippyTestsDailyReconcileInterval,
		store:             deps.Store,
		sippyClient:       client,
		deps:              deps,
		envSet:            envSet,
		testFilters:       testFilters,
	}, nil
}

func (c *sourceSippyTestsDailyController) Run(ctx context.Context, threadiness int) {
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

func (c *sourceSippyTestsDailyController) RunOnce(ctx context.Context, key string) error {
	c.logger.Info("Reconciling one key.", "key", key)
	return c.processKey(ctx, key)
}

func (c *sourceSippyTestsDailyController) SyncOnce(ctx context.Context) error {
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

func (c *sourceSippyTestsDailyController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *sourceSippyTestsDailyController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.processKey(ctx, key); err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, fmt.Errorf("failed processing key %q", key), "Error syncing; requeuing for later retry", "controller", SourceSippyTestsDailyControllerName, "key", key)
	c.queue.AddRateLimited(key)
	return true
}

func (c *sourceSippyTestsDailyController) queueMetadata(ctx context.Context) {
	keys, err := c.listKeys(ctx)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed listing keys for periodic enqueue", "controller", SourceSippyTestsDailyControllerName)
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.queue.Add(key)
	}
}

func (c *sourceSippyTestsDailyController) listKeys(_ context.Context) ([]string, error) {
	date := time.Now().UTC().Format("2006-01-02")
	keys := make([]string, 0, len(c.deps.Source.Environments))
	for _, env := range c.deps.Source.Environments {
		normalized := normalizeEnvironment(env)
		if normalized == "" {
			continue
		}
		if !c.isEnvironmentEnabled(normalized) {
			continue
		}
		keys = append(keys, sippyTestsDailyKey(normalized, date))
	}
	sort.Strings(keys)
	return keys, nil
}

func (c *sourceSippyTestsDailyController) processKey(ctx context.Context, key string) error {
	environment, date, err := parseSippyTestsDailyKey(key)
	if err != nil {
		return err
	}
	if !c.isEnvironmentEnabled(environment) {
		return fmt.Errorf("environment %q is not enabled", environment)
	}

	existingRows, err := c.store.ListTestMetadataDailyByDate(ctx, environment, date)
	if err != nil {
		return fmt.Errorf("list test metadata daily rows for env=%q date=%q: %w", environment, date, err)
	}
	missingPeriods := missingSippyTestsDailyPeriods(existingRows)
	if len(missingPeriods) == 0 {
		c.logger.V(1).Info("Skipping env/date; all required test metadata daily periods already exist.", "environment", environment, "date", date, "rows", len(existingRows))
		return nil
	}

	release := strings.TrimSpace(c.deps.Source.SippyReleaseByEnv[environment])
	if release == "" {
		return fmt.Errorf("missing sippy release mapping for environment %q", environment)
	}
	filters := c.testFilters[environment]
	if len(filters) == 0 {
		return fmt.Errorf("missing test filter configuration for environment %q", environment)
	}

	ingestedAt := time.Now().UTC().Format(time.RFC3339)
	rows := make([]contracts.TestMetadataDailyRecord, 0)
	for _, period := range missingPeriods {
		tests, err := c.listFilteredTests(ctx, environment, release, period, filters)
		if err != nil {
			return err
		}
		if len(tests) == 0 {
			c.logger.Info("No tests returned for env/date/period; leaving datapoint absent for retry.", "environment", environment, "date", date, "period", period, "release", release)
			continue
		}

		for _, row := range tests {
			testName := strings.TrimSpace(row.Name)
			if testName == "" {
				continue
			}
			rows = append(rows, contracts.TestMetadataDailyRecord{
				Environment:            environment,
				Date:                   date,
				Release:                release,
				Period:                 period,
				TestName:               testName,
				TestSuite:              strings.TrimSpace(row.SuiteName),
				CurrentPassPercentage:  row.CurrentPassPercentage,
				CurrentRuns:            row.CurrentRuns,
				PreviousPassPercentage: row.PreviousPassPercentage,
				PreviousRuns:           row.PreviousRuns,
				NetImprovement:         row.NetImprovement,
				IngestedAt:             ingestedAt,
			})
		}
	}
	if len(rows) == 0 {
		c.logger.Info("No valid test rows returned for missing periods after normalization; leaving datapoint absent for retry.", "environment", environment, "date", date, "release", release, "missing_periods", strings.Join(missingPeriods, ","))
		return nil
	}

	if err := c.store.UpsertTestMetadataDaily(ctx, rows); err != nil {
		return fmt.Errorf("upsert test metadata daily rows for env=%q date=%q: %w", environment, date, err)
	}
	c.logger.Info("Stored test metadata daily datapoint.", "environment", environment, "date", date, "release", release, "rows", len(rows))
	return nil
}

func (c *sourceSippyTestsDailyController) listFilteredTests(ctx context.Context, environment string, release string, period string, filters []compiledTestFilter) ([]sippysource.TestSummary, error) {
	mergedByKey := map[string]sippysource.TestSummary{}
	for _, testFilter := range filters {
		filterJSON, err := buildSippyTestsFilterJSON(testFilter)
		if err != nil {
			return nil, fmt.Errorf("build sippy tests filter for env=%q suite=%q: %w", environment, testFilter.testSuite, err)
		}
		rows, err := c.sippyClient.ListTests(ctx, sippysource.ListTestsOptions{
			Release:   release,
			Period:    period,
			SortField: "name",
			Sort:      "asc",
			Filter:    filterJSON,
		})
		if err != nil {
			return nil, fmt.Errorf("list tests for env=%q release=%q period=%q suite=%q: %w", environment, release, period, testFilter.testSuite, err)
		}

		for _, row := range rows {
			if !matchesTestFilter(row, testFilter) {
				continue
			}
			key := strings.TrimSpace(row.SuiteName) + "|" + strings.TrimSpace(row.Name)
			if strings.TrimSpace(key) == "|" {
				continue
			}
			mergedByKey[key] = row
		}
	}

	out := make([]sippysource.TestSummary, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if strings.TrimSpace(out[i].SuiteName) != strings.TrimSpace(out[j].SuiteName) {
			return strings.TrimSpace(out[i].SuiteName) < strings.TrimSpace(out[j].SuiteName)
		}
		return strings.TrimSpace(out[i].Name) < strings.TrimSpace(out[j].Name)
	})
	return out, nil
}

func (c *sourceSippyTestsDailyController) isEnvironmentEnabled(environment string) bool {
	_, ok := c.envSet[normalizeEnvironment(environment)]
	return ok
}

func parseSippyTestsDailyKey(key string) (string, string, error) {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return "", "", fmt.Errorf("empty key")
	}

	parts := strings.Split(trimmed, "|")
	switch len(parts) {
	case 1:
		environment := normalizeEnvironment(parts[0])
		if environment == "" {
			return "", "", fmt.Errorf("invalid key %q: missing environment", key)
		}
		return environment, time.Now().UTC().Format("2006-01-02"), nil
	case 2:
		environment := normalizeEnvironment(parts[0])
		if environment == "" {
			return "", "", fmt.Errorf("invalid key %q: missing environment", key)
		}
		date, err := normalizeControllerDate(parts[1])
		if err != nil {
			return "", "", fmt.Errorf("invalid key %q: %w", key, err)
		}
		return environment, date, nil
	default:
		return "", "", fmt.Errorf("invalid key %q: expected format <environment>|<YYYY-MM-DD>", key)
	}
}

func normalizeControllerDate(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("missing date")
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return "", fmt.Errorf("invalid date %q: %w", raw, err)
	}
	return parsed.UTC().Format("2006-01-02"), nil
}

func sippyTestsDailyKey(environment string, date string) string {
	return normalizeEnvironment(environment) + "|" + strings.TrimSpace(date)
}

func buildEnvironmentTestFilters(enabledEnvironments map[string]struct{}) (map[string][]compiledTestFilter, error) {
	out := map[string][]compiledTestFilter{}
	for environment := range enabledEnvironments {
		rawFilters, ok := sourcelanes.FiltersForEnvironment(environment)
		if !ok || len(rawFilters) == 0 {
			return nil, fmt.Errorf("source.sippy.tests-daily: missing test filter map for environment %q", environment)
		}

		compiledFilters := make([]compiledTestFilter, 0, len(rawFilters))
		for _, raw := range rawFilters {
			suite := strings.TrimSpace(raw.TestSuite)
			if suite == "" {
				return nil, fmt.Errorf("source.sippy.tests-daily: environment %q has empty test suite filter", environment)
			}
			compiled := compiledTestFilter{
				testSuite:        suite,
				testNameContains: extractRegexLiteralContains(raw.TestNameRegex),
			}
			if strings.TrimSpace(raw.TestNameRegex) != "" {
				re, err := regexp.Compile(strings.TrimSpace(raw.TestNameRegex))
				if err != nil {
					return nil, fmt.Errorf("source.sippy.tests-daily: compile test regex for environment %q suite %q: %w", environment, suite, err)
				}
				compiled.testNameRegex = re
			}
			compiledFilters = append(compiledFilters, compiled)
		}
		out[environment] = compiledFilters
	}
	return out, nil
}

func buildSippyTestsFilterJSON(testFilter compiledTestFilter) (string, error) {
	items := []sippyFilterItem{
		{
			ColumnField:   "suite_name",
			OperatorValue: "equals",
			Value:         testFilter.testSuite,
		},
	}
	if testFilter.testNameContains != "" {
		items = append(items, sippyFilterItem{
			ColumnField:   "name",
			OperatorValue: "contains",
			Value:         testFilter.testNameContains,
		})
	}
	payload := sippyFilterModel{
		Items:        items,
		LinkOperator: "and",
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func matchesTestFilter(row sippysource.TestSummary, testFilter compiledTestFilter) bool {
	if strings.TrimSpace(row.SuiteName) != testFilter.testSuite {
		return false
	}
	if testFilter.testNameRegex == nil {
		return true
	}
	return testFilter.testNameRegex.MatchString(strings.TrimSpace(row.Name))
}

func extractRegexLiteralContains(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	candidate := strings.TrimPrefix(trimmed, "^")
	candidate = strings.TrimSuffix(candidate, "$")
	candidate = strings.ReplaceAll(candidate, `\.`, ".")
	candidate = strings.ReplaceAll(candidate, `\\`, `\`)
	if strings.ContainsAny(candidate, "[](){}+*?|") {
		return ""
	}
	return strings.TrimSpace(candidate)
}

func missingSippyTestsDailyPeriods(rows []contracts.TestMetadataDailyRecord) []string {
	existing := map[string]struct{}{}
	for _, row := range rows {
		period := strings.TrimSpace(row.Period)
		if period == "" {
			continue
		}
		existing[period] = struct{}{}
	}
	missing := make([]string, 0, len(requiredSippyTestsDailyPeriods))
	for _, period := range requiredSippyTestsDailyPeriods {
		if _, ok := existing[period]; ok {
			continue
		}
		missing = append(missing, period)
	}
	return missing
}
