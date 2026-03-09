package weekly

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestValidateOptionsRequiresStartDate(t *testing.T) {
	t.Parallel()

	_, err := validateOptions(Options{
		OutputPath: "data/reports/weekly.html",
		StartDate:  "",
	})
	if err == nil || !strings.Contains(err.Error(), "missing --start-date") {
		t.Fatalf("expected missing start-date validation error, got=%v", err)
	}
}

func TestGenerateWritesWeeklyReportForAllEnvironments(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-01", Metric: metricRunCount, Value: 10},
		{Environment: "dev", Date: "2026-03-01", Metric: metricFailureCount, Value: 4},
		{Environment: "dev", Date: "2026-03-01", Metric: metricFailedCIInfraRunCount, Value: 1},
		{Environment: "dev", Date: "2026-03-01", Metric: metricFailedProvisionRunCount, Value: 1},
		{Environment: "dev", Date: "2026-03-01", Metric: metricFailedE2ERunCount, Value: 2},
		{Environment: "dev", Date: "2026-03-01", Metric: metricPostGoodRunCount, Value: 7},
		{Environment: "dev", Date: "2026-03-01", Metric: metricPostGoodFailedE2EJobs, Value: 0},
		{Environment: "dev", Date: "2026-03-01", Metric: metricPostGoodFailedCIInfra, Value: 0},
		{Environment: "dev", Date: "2026-03-01", Metric: metricPostGoodFailedProvision, Value: 1},
		{Environment: "int", Date: "2026-03-01", Metric: metricRunCount, Value: 8},
		{Environment: "int", Date: "2026-03-01", Metric: metricFailureCount, Value: 2},
		{Environment: "int", Date: "2026-03-01", Metric: metricFailedCIInfraRunCount, Value: 2},
		{Environment: "int", Date: "2026-03-01", Metric: metricFailedProvisionRunCount, Value: 0},
		{Environment: "int", Date: "2026-03-01", Metric: metricFailedE2ERunCount, Value: 0},
	}); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}

	outputPath := filepath.Join(dataDir, "reports", "weekly.html")
	if err := Generate(ctx, store, Options{
		OutputPath: outputPath,
		StartDate:  "2026-03-01",
	}); err != nil {
		t.Fatalf("generate weekly report: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read weekly report output: %v", err)
	}
	report := string(content)

	requiredSnippets := []string{
		"CI Weekly Metrics Report",
		"Window: <strong>2026-03-01</strong> to <strong>2026-03-07</strong>",
		"Environment: DEV",
		"Environment: INT",
		"Environment: STG",
		"Environment: PROD",
		"Success Rate",
		"E2E Jobs (good commits)",
		"Success Rate (good commits)",
		"Chart mode:",
		"Absolute counts",
		"100% stacked percentages",
		"Daily Run Outcomes (stacked by run-level lane)",
		"Daily Run Outcomes for Good PRs (stacked by run-level lane)",
		"Successful runs (good PR semantics)",
		"mode-count",
		"mode-percent",
		"segment-label",
		"60.0%",
		"20.0%",
		"85.71%",
		"S:6",
		"P:1",
		"E2E:2",
		"CI:1",
	}
	for _, snippet := range requiredSnippets {
		if !strings.Contains(report, snippet) {
			t.Fatalf("expected report to contain %q", snippet)
		}
	}
	forbiddenSnippets := []string{
		"<table>",
		"Success rate uses (E2E Jobs - Failed E2E Jobs)",
		"Failed E2E Jobs",
		"Failed Tests",
		"Failed E2E Jobs (good commits)",
		"Failed Tests (good commits)",
	}
	for _, snippet := range forbiddenSnippets {
		if strings.Contains(report, snippet) {
			t.Fatalf("expected report to not contain %q", snippet)
		}
	}
	if strings.Contains(report, "S:9") {
		t.Fatalf("expected post-good successful runs to not exceed baseline success count; report=%q", report)
	}
}
