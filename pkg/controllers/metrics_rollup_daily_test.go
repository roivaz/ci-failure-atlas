package controllers

import (
	"bufio"
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/sourceoptions"
	"ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestMetricsRollupDailyRunOnceComputesAllEnvAndDevPostGoodMetrics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{Environment: "dev", RunURL: "https://run-dev-e2e", JobName: "pull-ci-Azure-ARO-HCP-main-e2e-parallel", Failed: true, PostGoodCommit: true, OccurredAt: "2026-03-05T10:00:00Z"},
		{Environment: "dev", RunURL: "https://run-dev-provision", JobName: "pull-ci-Azure-ARO-HCP-main-e2e-parallel", Failed: true, PostGoodCommit: true, OccurredAt: "2026-03-05T10:05:00Z"},
		{Environment: "dev", RunURL: "https://run-dev-ciinfra", JobName: "pull-ci-Azure-ARO-HCP-main-build", Failed: true, PostGoodCommit: false, OccurredAt: "2026-03-05T10:10:00Z"},
		{Environment: "dev", RunURL: "https://run-dev-success", JobName: "pull-ci-Azure-ARO-HCP-main-e2e-parallel", Failed: false, PostGoodCommit: true, OccurredAt: "2026-03-05T10:15:00Z"},
		{Environment: "int", RunURL: "https://run-int-e2e", JobName: "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel", Failed: true, OccurredAt: "2026-03-05T10:20:00Z"},
		{Environment: "int", RunURL: "https://run-int-success", JobName: "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel", Failed: false, OccurredAt: "2026-03-05T10:25:00Z"},
	}); err != nil {
		t.Fatalf("upsert runs: %v", err)
	}

	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "dev-row-e2e",
			RunURL:         "https://run-dev-e2e",
			SignatureID:    "sig-dev-e2e",
			OccurredAt:     "2026-03-05T10:00:00Z",
			RawText:        "e2e failure",
			NormalizedText: "e2e failure",
			TestName:       "should run user journey",
			TestSuite:      "rp-api-compat/parallel",
		},
		{
			Environment:    "dev",
			RowID:          "dev-row-provision",
			RunURL:         "https://run-dev-provision",
			SignatureID:    "sig-dev-provision",
			OccurredAt:     "2026-03-05T10:05:00Z",
			RawText:        "provision failure",
			NormalizedText: "provision failure",
			TestName:       "Run pipeline step gather",
			TestSuite:      "step graph",
		},
		{
			Environment:       "dev",
			RowID:             "dev-row-ciinfra",
			RunURL:            "https://run-dev-ciinfra",
			SignatureID:       "sig-dev-ciinfra",
			OccurredAt:        "2026-03-05T10:10:00Z",
			RawText:           "synthetic infra",
			NormalizedText:    "synthetic infra",
			TestName:          "unknown",
			TestSuite:         "unknown",
			NonArtifactBacked: true,
		},
		{
			Environment:    "int",
			RowID:          "int-row-e2e",
			RunURL:         "https://run-int-e2e",
			SignatureID:    "sig-int-e2e",
			OccurredAt:     "2026-03-05T10:20:00Z",
			RawText:        "int e2e failure",
			NormalizedText: "int e2e failure",
			TestName:       "int e2e",
			TestSuite:      "persistent/parallel",
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
	if len(rows) != 24 {
		t.Fatalf("unexpected metric row count: got=%d want=24", len(rows))
	}

	devMetrics := toMetricMap(rows, "dev", "2026-03-05")
	if len(devMetrics) != 15 {
		t.Fatalf("unexpected dev metric count: got=%d want=15 metrics=%v", len(devMetrics), devMetrics)
	}
	assertMetricValue(t, devMetrics, metricRunCount, 4)
	assertMetricValue(t, devMetrics, metricFailureCount, 3)
	assertMetricValue(t, devMetrics, metricFailureRowCount, 3)
	assertMetricValue(t, devMetrics, metricFailedCIInfraRunCount, 1)
	assertMetricValue(t, devMetrics, metricFailedProvisionRunCount, 1)
	assertMetricValue(t, devMetrics, metricFailedE2ERunCount, 1)
	assertMetricValue(t, devMetrics, metricCIInfraFailureCount, 1)
	assertMetricValue(t, devMetrics, metricProvisionFailureCount, 1)
	assertMetricValue(t, devMetrics, metricE2EFailureCount, 1)
	assertMetricValue(t, devMetrics, metricPostGoodRunCount, 3)
	assertMetricValue(t, devMetrics, metricPostGoodFailureCount, 2)
	assertMetricValue(t, devMetrics, metricPostGoodFailedE2EJobs, 2)
	assertMetricValue(t, devMetrics, metricPostGoodCIInfraFailureCount, 0)
	assertMetricValue(t, devMetrics, metricPostGoodProvisionFailureCount, 1)
	assertMetricValue(t, devMetrics, metricPostGoodE2EFailureCount, 1)

	intMetrics := toMetricMap(rows, "int", "2026-03-05")
	if len(intMetrics) != 9 {
		t.Fatalf("unexpected int metric count: got=%d want=9 metrics=%v", len(intMetrics), intMetrics)
	}
	assertMetricValue(t, intMetrics, metricRunCount, 2)
	assertMetricValue(t, intMetrics, metricFailureCount, 1)
	assertMetricValue(t, intMetrics, metricFailureRowCount, 1)
	assertMetricValue(t, intMetrics, metricFailedCIInfraRunCount, 0)
	assertMetricValue(t, intMetrics, metricFailedProvisionRunCount, 0)
	assertMetricValue(t, intMetrics, metricFailedE2ERunCount, 1)
	assertMetricValue(t, intMetrics, metricCIInfraFailureCount, 0)
	assertMetricValue(t, intMetrics, metricProvisionFailureCount, 0)
	assertMetricValue(t, intMetrics, metricE2EFailureCount, 1)
	if _, ok := intMetrics[metricPostGoodFailureCount]; ok {
		t.Fatalf("expected non-dev env to not emit post-good metric %q", metricPostGoodFailureCount)
	}
}

func TestMetricsRollupDailyRunOnceClassifiesFailedRunsWithDeterministicPrecedence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{Environment: "dev", RunURL: "https://run-dev-mixed", JobName: "pull-ci-Azure-ARO-HCP-main-e2e-parallel", Failed: true, OccurredAt: "2026-03-09T10:00:00Z"},
		{Environment: "dev", RunURL: "https://run-dev-e2e-only", JobName: "pull-ci-Azure-ARO-HCP-main-e2e-parallel", Failed: true, OccurredAt: "2026-03-09T10:05:00Z"},
	}); err != nil {
		t.Fatalf("upsert runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-mixed-e2e",
			RunURL:         "https://run-dev-mixed",
			SignatureID:    "sig-mixed-e2e",
			OccurredAt:     "2026-03-09T10:00:00Z",
			RawText:        "test failed",
			NormalizedText: "test failed",
			TestName:       "should fail",
			TestSuite:      "suite/parallel",
		},
		{
			Environment:    "dev",
			RowID:          "row-mixed-provision",
			RunURL:         "https://run-dev-mixed",
			SignatureID:    "sig-mixed-provision",
			OccurredAt:     "2026-03-09T10:00:00Z",
			RawText:        "provision failed",
			NormalizedText: "provision failed",
			TestName:       "Run pipeline step gather",
			TestSuite:      "cluster setup",
		},
		{
			Environment:    "dev",
			RowID:          "row-e2e-only",
			RunURL:         "https://run-dev-e2e-only",
			SignatureID:    "sig-e2e-only",
			OccurredAt:     "2026-03-09T10:05:00Z",
			RawText:        "test failed",
			NormalizedText: "test failed",
			TestName:       "another test",
			TestSuite:      "suite/parallel",
		},
	}); err != nil {
		t.Fatalf("upsert raw failures: %v", err)
	}

	controller, err := newMetricsRollupDailyController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptionsForMetrics(t, []string{"dev"}),
	})
	if err != nil {
		t.Fatalf("create metrics controller: %v", err)
	}
	if err := controller.RunOnce(ctx, "2026-03-09"); err != nil {
		t.Fatalf("run once: %v", err)
	}

	rows := mustReadMetricDailyRows(t, filepath.Join(dataDir, "facts", "metrics_daily.ndjson"))
	devMetrics := toMetricMap(rows, "dev", "2026-03-09")
	assertMetricValue(t, devMetrics, metricFailedProvisionRunCount, 1)
	assertMetricValue(t, devMetrics, metricFailedE2ERunCount, 1)
	assertMetricValue(t, devMetrics, metricFailedCIInfraRunCount, 0)
}

func TestMetricsRollupDailyRunOnceClassifiesSyntheticPeriodicE2EAsCIInfra(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment: "int",
			RunURL:      "https://run-int-periodic-e2e",
			JobName:     "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel",
			Failed:      true,
			OccurredAt:  "2026-03-10T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("upsert runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{
			Environment:       "int",
			RowID:             "row-int-synthetic",
			RunURL:            "https://run-int-periodic-e2e",
			SignatureID:       "sig-int-synthetic",
			OccurredAt:        "2026-03-10T10:00:00Z",
			RawText:           "non-artifact-backed failure (no junit artifacts)",
			NormalizedText:    "non-artifact-backed failure (no junit artifacts)",
			TestName:          "unknown",
			TestSuite:         "unknown",
			NonArtifactBacked: true,
		},
	}); err != nil {
		t.Fatalf("upsert raw failures: %v", err)
	}

	controller, err := newMetricsRollupDailyController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptionsForMetrics(t, []string{"int"}),
	})
	if err != nil {
		t.Fatalf("create metrics controller: %v", err)
	}
	if err := controller.RunOnce(ctx, "2026-03-10"); err != nil {
		t.Fatalf("run once: %v", err)
	}

	rows := mustReadMetricDailyRows(t, filepath.Join(dataDir, "facts", "metrics_daily.ndjson"))
	intMetrics := toMetricMap(rows, "int", "2026-03-10")
	assertMetricValue(t, intMetrics, metricFailedCIInfraRunCount, 1)
	assertMetricValue(t, intMetrics, metricFailedE2ERunCount, 0)
	assertMetricValue(t, intMetrics, metricCIInfraFailureCount, 1)
	assertMetricValue(t, intMetrics, metricE2EFailureCount, 0)
}

func TestMetricsRollupDailyRunOnceBackfillsWhenMetricSetIsPartial(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{Environment: "dev", RunURL: "https://run-dev-failed", JobName: "pull-ci-Azure-ARO-HCP-main-e2e-parallel", Failed: true, OccurredAt: "2026-03-07T10:00:00Z"},
		{Environment: "dev", RunURL: "https://run-dev-success", JobName: "pull-ci-Azure-ARO-HCP-main-e2e-parallel", Failed: false, OccurredAt: "2026-03-07T10:10:00Z"},
	}); err != nil {
		t.Fatalf("upsert runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "dev-row-100",
			RunURL:         "https://run-dev-failed",
			SignatureID:    "sig-100",
			OccurredAt:     "2026-03-07T10:00:00Z",
			RawText:        "raw-100",
			NormalizedText: "norm-100",
			TestName:       "test-100",
			TestSuite:      "suite-100",
		},
	}); err != nil {
		t.Fatalf("upsert raw failures: %v", err)
	}
	if err := store.UpsertMetricsDaily(ctx, []contracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-07", Metric: metricRunCount, Value: 999},
		{Environment: "dev", Date: "2026-03-07", Metric: "total_runs", Value: 10},
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
	if len(devMetrics) != 15 {
		t.Fatalf("unexpected dev metric count after backfill: got=%d want=15 metrics=%v", len(devMetrics), devMetrics)
	}
	assertMetricValue(t, devMetrics, metricRunCount, 2)
	assertMetricValue(t, devMetrics, metricFailureCount, 1)
	assertMetricValue(t, devMetrics, metricFailureRowCount, 1)
	assertMetricValue(t, devMetrics, metricPostGoodRunCount, 0)
	if _, exists := devMetrics["total_runs"]; exists {
		t.Fatalf("expected stale legacy metric to be replaced during backfill, got metrics=%v", devMetrics)
	}
}

func TestMetricsRollupDailyRunOnceSkipsWhenRequiredSetAlreadyExists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{Environment: "int", RunURL: "https://run-int-1", JobName: "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel", Failed: true, OccurredAt: "2026-03-08T10:00:00Z"},
	}); err != nil {
		t.Fatalf("upsert runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{
			Environment:    "int",
			RowID:          "int-row-1",
			RunURL:         "https://run-int-1",
			SignatureID:    "sig-int-1",
			OccurredAt:     "2026-03-08T10:00:00Z",
			RawText:        "int failure",
			NormalizedText: "int failure",
			TestName:       "test-int",
			TestSuite:      "suite-int",
		},
	}); err != nil {
		t.Fatalf("upsert raw failures: %v", err)
	}
	if err := store.UpsertMetricsDaily(ctx, []contracts.MetricDailyRecord{
		{Environment: "int", Date: "2026-03-08", Metric: metricRunCount, Value: 321},
		{Environment: "int", Date: "2026-03-08", Metric: metricFailureCount, Value: 123},
		{Environment: "int", Date: "2026-03-08", Metric: metricFailureRowCount, Value: 111},
		{Environment: "int", Date: "2026-03-08", Metric: metricFailedCIInfraRunCount, Value: 77},
		{Environment: "int", Date: "2026-03-08", Metric: metricFailedProvisionRunCount, Value: 8},
		{Environment: "int", Date: "2026-03-08", Metric: metricFailedE2ERunCount, Value: 38},
		{Environment: "int", Date: "2026-03-08", Metric: metricCIInfraFailureCount, Value: 11},
		{Environment: "int", Date: "2026-03-08", Metric: metricProvisionFailureCount, Value: 22},
		{Environment: "int", Date: "2026-03-08", Metric: metricE2EFailureCount, Value: 90},
	}); err != nil {
		t.Fatalf("upsert existing complete metric set: %v", err)
	}

	controller, err := newMetricsRollupDailyController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptionsForMetrics(t, []string{"int"}),
	})
	if err != nil {
		t.Fatalf("create metrics controller: %v", err)
	}
	if err := controller.RunOnce(ctx, "2026-03-08"); err != nil {
		t.Fatalf("run once: %v", err)
	}

	rows := mustReadMetricDailyRows(t, filepath.Join(dataDir, "facts", "metrics_daily.ndjson"))
	intMetrics := toMetricMap(rows, "int", "2026-03-08")
	if len(intMetrics) != 9 {
		t.Fatalf("unexpected int metric count: got=%d want=9 metrics=%v", len(intMetrics), intMetrics)
	}
	assertMetricValue(t, intMetrics, metricRunCount, 321)
	assertMetricValue(t, intMetrics, metricFailureCount, 123)
	assertMetricValue(t, intMetrics, metricFailureRowCount, 111)
	assertMetricValue(t, intMetrics, metricFailedCIInfraRunCount, 77)
	assertMetricValue(t, intMetrics, metricFailedProvisionRunCount, 8)
	assertMetricValue(t, intMetrics, metricFailedE2ERunCount, 38)
	assertMetricValue(t, intMetrics, metricCIInfraFailureCount, 11)
	assertMetricValue(t, intMetrics, metricProvisionFailureCount, 22)
	assertMetricValue(t, intMetrics, metricE2EFailureCount, 90)
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
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://run-dev-recent",
			JobName:     "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			Failed:      true,
			OccurredAt:  recentDay.Add(10 * time.Hour).Format(time.RFC3339),
		},
		{
			Environment: "dev",
			RunURL:      "https://run-dev-old",
			JobName:     "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			Failed:      true,
			OccurredAt:  oldDay.Add(10 * time.Hour).Format(time.RFC3339),
		},
	}); err != nil {
		t.Fatalf("upsert runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "dev-row-recent",
			RunURL:         "https://run-dev-recent",
			SignatureID:    "sig-recent",
			OccurredAt:     recentDay.Add(10 * time.Hour).Format(time.RFC3339),
			RawText:        "recent failure",
			NormalizedText: "recent failure",
			TestName:       "recent test",
			TestSuite:      "suite",
		},
		{
			Environment:    "dev",
			RowID:          "dev-row-old",
			RunURL:         "https://run-dev-old",
			SignatureID:    "sig-old",
			OccurredAt:     oldDay.Add(10 * time.Hour).Format(time.RFC3339),
			RawText:        "old failure",
			NormalizedText: "old failure",
			TestName:       "old test",
			TestSuite:      "suite",
		},
	}); err != nil {
		t.Fatalf("upsert raw failures: %v", err)
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

func assertMetricValue(t *testing.T, metrics map[string]float64, metric string, want float64) {
	t.Helper()
	got, ok := metrics[metric]
	if !ok {
		t.Fatalf("expected metric %q to exist; metrics=%v", metric, metrics)
	}
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("metric %q mismatch: got=%v want=%v", metric, got, want)
	}
}
