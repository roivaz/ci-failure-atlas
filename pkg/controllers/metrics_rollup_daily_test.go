package controllers

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/sourceoptions"
	"ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestMetricsRollupDailyRunOnceComputesMetrics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertRunCountsHourly(ctx, []contracts.RunCountHourlyRecord{
		{Environment: "dev", Hour: "2026-03-05T10:00:00Z", TotalRuns: 7, FailedRuns: 2, SuccessfulRuns: 5},
		{Environment: "dev", Hour: "2026-03-05T11:00:00Z", TotalRuns: 5, FailedRuns: 1, SuccessfulRuns: 4},
		{Environment: "int", Hour: "2026-03-05T10:00:00Z", TotalRuns: 4, FailedRuns: 1, SuccessfulRuns: 3},
	}); err != nil {
		t.Fatalf("upsert run counts hourly: %v", err)
	}

	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "dev-row-1",
			RunURL:         "https://run-dev-1",
			SignatureID:    "sig-a",
			OccurredAt:     "2026-03-05T10:00:00Z",
			RawText:        "raw-a",
			NormalizedText: "norm-a",
		},
		{
			Environment:    "dev",
			RowID:          "dev-row-2",
			RunURL:         "https://run-dev-2",
			SignatureID:    "sig-a",
			OccurredAt:     "2026-03-05T10:01:00Z",
			RawText:        "raw-b",
			NormalizedText: "norm-b",
		},
		{
			Environment:    "dev",
			RowID:          "dev-row-3",
			RunURL:         "https://run-dev-3",
			SignatureID:    "sig-b",
			OccurredAt:     "2026-03-05T11:00:00Z",
			RawText:        "raw-c",
			NormalizedText: "norm-c",
		},
	}); err != nil {
		t.Fatalf("upsert raw failures: %v", err)
	}

	controller, err := newMetricsRollupDailyController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptionsForMetrics(t, []string{"dev", "int"}),
	})
	if err != nil {
		t.Fatalf("create metrics controller: %v", err)
	}

	if err := controller.RunOnce(ctx, "2026-03-05"); err != nil {
		t.Fatalf("run once: %v", err)
	}

	rows := mustReadMetricDailyRows(t, filepath.Join(dataDir, "facts", "metrics_daily.ndjson"))
	if len(rows) != 10 {
		t.Fatalf("unexpected metric row count: got=%d want=10", len(rows))
	}

	devMetrics := toMetricMap(rows, "dev", "2026-03-05")
	if got := devMetrics[metricTotalRuns]; got != 12 {
		t.Fatalf("dev total_runs mismatch: got=%v want=12", got)
	}
	if got := devMetrics[metricFailedRuns]; got != 3 {
		t.Fatalf("dev failed_runs mismatch: got=%v want=3", got)
	}
	if got := devMetrics[metricSuccessfulRuns]; got != 9 {
		t.Fatalf("dev successful_runs mismatch: got=%v want=9", got)
	}
	if got := devMetrics[metricRunFailureRate]; got != 0.25 {
		t.Fatalf("dev run_failure_rate mismatch: got=%v want=0.25", got)
	}
	if got := devMetrics[metricRawFailureRows]; got != 3 {
		t.Fatalf("dev raw_failure_rows mismatch: got=%v want=3", got)
	}
	if _, exists := devMetrics["unique_failure_signatures"]; exists {
		t.Fatalf("expected deprecated unique_failure_signatures metric to be absent in dev rollup output, got=%v", devMetrics["unique_failure_signatures"])
	}

	intMetrics := toMetricMap(rows, "int", "2026-03-05")
	if got := intMetrics[metricTotalRuns]; got != 4 {
		t.Fatalf("int total_runs mismatch: got=%v want=4", got)
	}
	if got := intMetrics[metricFailedRuns]; got != 1 {
		t.Fatalf("int failed_runs mismatch: got=%v want=1", got)
	}
	if got := intMetrics[metricSuccessfulRuns]; got != 3 {
		t.Fatalf("int successful_runs mismatch: got=%v want=3", got)
	}
	if got := intMetrics[metricRunFailureRate]; got != 0.25 {
		t.Fatalf("int run_failure_rate mismatch: got=%v want=0.25", got)
	}
	if got := intMetrics[metricRawFailureRows]; got != 0 {
		t.Fatalf("int raw_failure_rows mismatch: got=%v want=0", got)
	}
}

func TestMetricsRollupDailyRunOnceSkipsAlreadyMaterializedDateEnvironment(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertRunCountsHourly(ctx, []contracts.RunCountHourlyRecord{
		{Environment: "dev", Hour: "2026-03-07T10:00:00Z", TotalRuns: 10, FailedRuns: 3, SuccessfulRuns: 7},
	}); err != nil {
		t.Fatalf("upsert run counts hourly: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "dev-row-100",
			RunURL:         "https://run-dev-100",
			SignatureID:    "sig-100",
			OccurredAt:     "2026-03-07T10:00:00Z",
			RawText:        "raw-100",
			NormalizedText: "norm-100",
		},
	}); err != nil {
		t.Fatalf("upsert raw failures: %v", err)
	}
	if err := store.UpsertMetricsDaily(ctx, []contracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-07", Metric: metricTotalRuns, Value: 999},
		{Environment: "dev", Date: "2026-03-07", Metric: metricFailedRuns, Value: 111},
		{Environment: "dev", Date: "2026-03-07", Metric: metricSuccessfulRuns, Value: 888},
		{Environment: "dev", Date: "2026-03-07", Metric: metricRunFailureRate, Value: 0.111},
		{Environment: "dev", Date: "2026-03-07", Metric: metricRawFailureRows, Value: 42},
	}); err != nil {
		t.Fatalf("upsert existing metrics: %v", err)
	}

	controller, err := newMetricsRollupDailyController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptionsForMetrics(t, []string{"dev"}),
	})
	if err != nil {
		t.Fatalf("create metrics controller: %v", err)
	}

	if err := controller.RunOnce(ctx, "2026-03-07"); err != nil {
		t.Fatalf("run once: %v", err)
	}

	rows := mustReadMetricDailyRows(t, filepath.Join(dataDir, "facts", "metrics_daily.ndjson"))
	devMetrics := toMetricMap(rows, "dev", "2026-03-07")
	if got := devMetrics[metricTotalRuns]; got != 999 {
		t.Fatalf("expected pre-existing total_runs=999 to be preserved by skip behavior, got=%v", got)
	}
	if got := devMetrics[metricRawFailureRows]; got != 42 {
		t.Fatalf("expected pre-existing raw_failure_rows=42 to be preserved by skip behavior, got=%v", got)
	}
}

func TestMetricsRollupDailySyncOnceUsesDatesFromRunCountHours(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	firstDay := time.Now().UTC().Add(-24 * time.Hour).Truncate(24 * time.Hour)
	secondDay := firstDay.Add(24 * time.Hour)
	if err := store.UpsertRunCountsHourly(ctx, []contracts.RunCountHourlyRecord{
		{Environment: "dev", Hour: firstDay.Add(10 * time.Hour).Format(time.RFC3339), TotalRuns: 3, FailedRuns: 1, SuccessfulRuns: 2},
		{Environment: "dev", Hour: secondDay.Add(10 * time.Hour).Format(time.RFC3339), TotalRuns: 6, FailedRuns: 2, SuccessfulRuns: 4},
	}); err != nil {
		t.Fatalf("upsert run counts hourly: %v", err)
	}

	controller, err := newMetricsRollupDailyController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptionsForMetrics(t, []string{"dev"}),
	})
	if err != nil {
		t.Fatalf("create metrics controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	dates, err := store.ListMetricDates(ctx)
	if err != nil {
		t.Fatalf("list metric dates: %v", err)
	}
	wantDates := []string{firstDay.Format("2006-01-02"), secondDay.Format("2006-01-02")}
	if len(dates) != len(wantDates) {
		t.Fatalf("unexpected metric date count: got=%d want=%d dates=%v", len(dates), len(wantDates), dates)
	}
	sort.Strings(dates)
	for i := range wantDates {
		if dates[i] != wantDates[i] {
			t.Fatalf("metric dates mismatch: got=%v want=%v", dates, wantDates)
		}
	}
}

func TestMetricsRollupDailySyncOnceSkipsDatesOutsideActiveWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	recentDay := time.Now().UTC().Add(-24 * time.Hour).Truncate(24 * time.Hour)
	oldDay := time.Now().UTC().Add(-20 * 24 * time.Hour).Truncate(24 * time.Hour)
	if err := store.UpsertRunCountsHourly(ctx, []contracts.RunCountHourlyRecord{
		{Environment: "dev", Hour: recentDay.Add(10 * time.Hour).Format(time.RFC3339), TotalRuns: 4, FailedRuns: 1, SuccessfulRuns: 3},
		{Environment: "dev", Hour: oldDay.Add(10 * time.Hour).Format(time.RFC3339), TotalRuns: 5, FailedRuns: 2, SuccessfulRuns: 3},
	}); err != nil {
		t.Fatalf("upsert run counts hourly: %v", err)
	}

	controller, err := newMetricsRollupDailyController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptionsForMetrics(t, []string{"dev"}),
	})
	if err != nil {
		t.Fatalf("create metrics controller: %v", err)
	}
	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	dates, err := store.ListMetricDates(ctx)
	if err != nil {
		t.Fatalf("list metric dates: %v", err)
	}
	wantDates := []string{recentDay.Format("2006-01-02")}
	if len(dates) != len(wantDates) || dates[0] != wantDates[0] {
		t.Fatalf("unexpected rolled-up dates after active-window filter: got=%v want=%v", dates, wantDates)
	}
}

func mustCompleteSourceOptionsForMetrics(t *testing.T, envs []string) *sourceoptions.Options {
	t.Helper()

	raw := sourceoptions.DefaultOptions()
	raw.Environments = append([]string(nil), envs...)
	raw.SippyReleaseInt = "INT"
	raw.SippyReleaseStg = "STG"
	raw.SippyReleaseProd = "PROD"

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

func mustReadMetricDailyRows(t *testing.T, path string) []contracts.MetricDailyRecord {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open metrics daily file %q: %v", path, err)
	}
	defer f.Close()

	rows := make([]contracts.MetricDailyRecord, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var row contracts.MetricDailyRecord
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatalf("decode metrics daily row: %v", err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan metrics daily file %q: %v", path, err)
	}
	return rows
}

func toMetricMap(rows []contracts.MetricDailyRecord, environment string, date string) map[string]float64 {
	out := map[string]float64{}
	for _, row := range rows {
		if row.Environment == environment && row.Date == date {
			out[row.Metric] = row.Value
		}
	}
	return out
}
