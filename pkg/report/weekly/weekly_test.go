package weekly

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
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
	if err := store.UpsertTestMetadataDaily(ctx, []storecontracts.TestMetadataDailyRecord{
		{Environment: "dev", Date: "2026-03-05", Period: "default", TestName: "dev flaky scenario", TestSuite: "suite/dev", CurrentPassPercentage: 83.0, CurrentRuns: 25},
		{Environment: "dev", Date: "2026-03-06", Period: "default", TestName: "dev healthy scenario", TestSuite: "suite/dev", CurrentPassPercentage: 99.0, CurrentRuns: 25},
		{Environment: "int", Date: "2026-03-04", Period: "default", TestName: "int flaky scenario", TestSuite: "suite/int", CurrentPassPercentage: 92.0, CurrentRuns: 12},
		{Environment: "int", Date: "2026-03-04", Period: "twoDay", TestName: "int flaky scenario", TestSuite: "suite/int", CurrentPassPercentage: 70.0, CurrentRuns: 99},
		{Environment: "prod", Date: "2026-03-03", Period: "default", TestName: "tiny-sample ignored", TestSuite: "suite/prod", CurrentPassPercentage: 80.0, CurrentRuns: 2},
	}); err != nil {
		t.Fatalf("seed test metadata daily: %v", err)
	}
	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "dev-sig-1",
			SupportCount:            7,
			PostGoodCommitCount:     2,
			CanonicalEvidencePhrase: "timeout during CreateHCPClusterFromParam",
			References: []semanticcontracts.ReferenceRecord{
				{RunURL: "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-dev-latest/1", OccurredAt: "2026-03-06T12:00:00Z"},
				{RunURL: "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-dev-older/1", OccurredAt: "2026-03-05T12:00:00Z"},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase2ClusterID:         "int-sig-1",
			SupportCount:            5,
			PostGoodCommitCount:     0,
			CanonicalEvidencePhrase: "timeout during CreateHCPClusterFromParam",
			References: []semanticcontracts.ReferenceRecord{
				{RunURL: "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-int-latest/1", OccurredAt: "2026-03-04T08:00:00Z"},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase2ClusterID:         "int-sig-2",
			SupportCount:            4,
			PostGoodCommitCount:     0,
			CanonicalEvidencePhrase: "image pull backoff on bootstrap",
		},
	}); err != nil {
		t.Fatalf("seed global clusters: %v", err)
	}

	outputPath := filepath.Join(dataDir, "reports", "weekly.html")
	if err := Generate(ctx, store, Options{
		OutputPath: outputPath,
		StartDate:  "2026-03-01",
		TargetRate: 95,
	}); err != nil {
		t.Fatalf("generate weekly report: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read weekly report output: %v", err)
	}
	report := string(content)

	requiredSnippets := []string{
		"CI Weekly Report",
		"Window: <strong>2026-03-01</strong> to <strong>2026-03-07</strong>",
		"Goals:<br/>- e2e-integration, e2e-stage, e2e-prod job runs should each succeed 95% of the time<br/>- e2e-dev job runs should succeed 95% of the time after the last push of a PR that merges",
		"Environment: DEV",
		"Environment: INT",
		"Environment: STG",
		"Environment: PROD",
		"<span class=\"exec-heading-help\" title=\"INT/STG/PROD use all E2E job runs. DEV uses runs after the last push of a PR that merges.\">Goal basis</span>",
		"<span class=\"exec-heading-help\" title=\"Success rate on the goal basis: (runs - failed runs) / runs * 100.\">Success</span>",
		"Provision success (DEV)",
		"Provision change from last week (DEV)",
		"Change from last week",
		"Main reason for failed runs",
		"Most common failure pattern",
		"Lane outcomes",
		"Tests below 95%",
		"Top failure signatures",
		"Provision step success rate (CI/Infra excluded)",
		"88.89% (8/9)",
		"Source: Sippy test metadata (period: default). Top 5 tests below 95.00% success; minimum 10 runs. If the selected week has no metadata rows, this view falls back to the latest available metadata date.",
		"dev flaky scenario",
		"83.00%",
		"int flaky scenario",
		"92.00%",
		"Also seen in envs",
		"Latest job examples",
		"example-dev-latest",
		"timeout during CreateHCPClusterFromParam",
		"Success Rate",
		"E2E Jobs (after last push of merged PR)",
		"Success Rate (after last push of merged PR)",
		"Chart mode:",
		"Absolute counts",
		"100% stacked percentages",
		"Daily Run Outcomes (stacked by run-level lane)",
		"Daily Run Outcomes for DEV Goal Basis (after last push of merged PR)",
		"Successful runs (after last push of merged PR)",
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
		"Generated:",
		"Success rate uses (E2E Jobs - Failed E2E Jobs)",
		"Failed E2E Jobs",
		"Failed Tests",
		"Failed E2E Jobs (after last push of merged PR)",
		"Failed Tests (after last push of merged PR)",
		"Needs manual review",
		"WoW delta",
		"Primary failed lane",
		"Runs (goal basis)",
		"Success (goal basis)",
		"Top semantic signature (support delta)",
		"tiny-sample ignored",
		"70.00%",
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

func TestGenerateWithComparisonRendersExecutiveAndSemanticDeltas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	currentStore, err := ndjson.NewWithOptions(dataDir, ndjson.Options{SemanticSubdirectory: "2026-03-01"})
	if err != nil {
		t.Fatalf("create current store: %v", err)
	}
	t.Cleanup(func() { _ = currentStore.Close() })
	previousStore, err := ndjson.NewWithOptions(dataDir, ndjson.Options{SemanticSubdirectory: "2026-02-22"})
	if err != nil {
		t.Fatalf("create previous store: %v", err)
	}
	t.Cleanup(func() { _ = previousStore.Close() })

	if err := currentStore.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-01", Metric: metricRunCount, Value: 10},
		{Environment: "dev", Date: "2026-03-01", Metric: metricFailureCount, Value: 3},
		{Environment: "dev", Date: "2026-03-01", Metric: metricFailedProvisionRunCount, Value: 2},
		{Environment: "dev", Date: "2026-03-01", Metric: metricFailedE2ERunCount, Value: 1},
		{Environment: "dev", Date: "2026-02-22", Metric: metricRunCount, Value: 8},
		{Environment: "dev", Date: "2026-02-22", Metric: metricFailureCount, Value: 4},
	}); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}

	if err := currentStore.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "dev-current-1",
			SupportCount:            10,
			PostGoodCommitCount:     2,
			CanonicalEvidencePhrase: "timeout during CreateHCPClusterFromParam",
			References: []semanticcontracts.ReferenceRecord{
				{RunURL: "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-dev-comparison/1", OccurredAt: "2026-03-01T09:00:00Z"},
			},
		},
	}); err != nil {
		t.Fatalf("seed current global clusters: %v", err)
	}
	if err := previousStore.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "dev-previous-1",
			SupportCount:            6,
			PostGoodCommitCount:     1,
			CanonicalEvidencePhrase: "timeout during CreateHCPClusterFromParam",
		},
	}); err != nil {
		t.Fatalf("seed previous global clusters: %v", err)
	}

	if err := currentStore.UpsertReviewQueue(ctx, []semanticcontracts.ReviewItemRecord{
		{SchemaVersion: semanticcontracts.SchemaVersionV1, Environment: "dev", ReviewItemID: "curr-1", Phase: "phase1", Reason: "low_confidence_evidence"},
		{SchemaVersion: semanticcontracts.SchemaVersionV1, Environment: "dev", ReviewItemID: "curr-2", Phase: "phase1", Reason: "low_confidence_evidence"},
		{SchemaVersion: semanticcontracts.SchemaVersionV1, Environment: "dev", ReviewItemID: "curr-3", Phase: "phase2", Reason: "ambiguous_provider_merge"},
	}); err != nil {
		t.Fatalf("seed current review queue: %v", err)
	}
	if err := previousStore.UpsertReviewQueue(ctx, []semanticcontracts.ReviewItemRecord{
		{SchemaVersion: semanticcontracts.SchemaVersionV1, Environment: "dev", ReviewItemID: "prev-1", Phase: "phase1", Reason: "low_confidence_evidence"},
	}); err != nil {
		t.Fatalf("seed previous review queue: %v", err)
	}

	outputPath := filepath.Join(dataDir, "reports", "weekly-management.html")
	if err := GenerateWithComparison(ctx, currentStore, previousStore, Options{
		OutputPath: outputPath,
		StartDate:  "2026-03-01",
		TargetRate: 95,
	}); err != nil {
		t.Fatalf("generate weekly report with comparison: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read weekly report output: %v", err)
	}
	report := string(content)
	for _, snippet := range []string{
		"Executive Status (Week-over-Week)",
		"e2e-integration, e2e-stage, e2e-prod job runs should each succeed 95% of the time",
		"e2e-dev job runs should succeed 95% of the time after the last push of a PR that merges",
		"timeout during CreateHCPClusterFromParam",
		"+4",
		"Provision success (DEV)",
		"80.00% (8/10)",
		"-20.00pp",
		"Tests below 95%",
		"Top failure signatures",
		"example-dev-comparison",
	} {
		if !strings.Contains(report, snippet) {
			t.Fatalf("expected report to contain %q", snippet)
		}
	}
}

func TestGenerateFallsBackToLatestSippyMetadataWhenWindowHasNoRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "int", Date: "2026-03-01", Metric: metricRunCount, Value: 8},
		{Environment: "int", Date: "2026-03-09", Metric: metricRunCount, Value: 8},
	}); err != nil {
		t.Fatalf("seed metrics daily rows: %v", err)
	}
	if err := store.UpsertTestMetadataDaily(ctx, []storecontracts.TestMetadataDailyRecord{
		{
			Environment:           "int",
			Date:                  "2026-03-09",
			Period:                "default",
			TestName:              "fallback metadata test",
			TestSuite:             "integration/parallel",
			CurrentPassPercentage: 77.0,
			CurrentRuns:           22,
		},
	}); err != nil {
		t.Fatalf("seed test metadata daily rows: %v", err)
	}

	outputPath := filepath.Join(dataDir, "reports", "weekly-fallback.html")
	if err := Generate(ctx, store, Options{
		OutputPath: outputPath,
		StartDate:  "2026-03-01",
		TargetRate: 95,
	}); err != nil {
		t.Fatalf("generate weekly report: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read weekly report output: %v", err)
	}
	report := string(content)
	for _, snippet := range []string{
		"fallback metadata test",
		"2026-03-09",
		"77.00%",
	} {
		if !strings.Contains(report, snippet) {
			t.Fatalf("expected fallback weekly report to contain %q", snippet)
		}
	}
}
