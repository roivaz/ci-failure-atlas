package weekly

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ci-failure-atlas/pkg/report/triagehtml"
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
		{Environment: "dev", Date: "2026-03-08", Metric: metricRunCount, Value: 0},
		{Environment: "int", Date: "2026-03-01", Metric: metricRunCount, Value: 8},
		{Environment: "int", Date: "2026-03-01", Metric: metricFailureCount, Value: 2},
		{Environment: "int", Date: "2026-03-01", Metric: metricFailedCIInfraRunCount, Value: 2},
		{Environment: "int", Date: "2026-03-01", Metric: metricFailedProvisionRunCount, Value: 0},
		{Environment: "int", Date: "2026-03-01", Metric: metricFailedE2ERunCount, Value: 0},
		{Environment: "int", Date: "2026-03-08", Metric: metricRunCount, Value: 0},
	}); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}
	if err := store.UpsertTestMetadataDaily(ctx, []storecontracts.TestMetadataDailyRecord{
		{Environment: "dev", Date: "2026-03-05", Period: "default", TestName: "dev flaky scenario", TestSuite: "suite/dev", CurrentPassPercentage: 83.0, CurrentRuns: 25},
		{Environment: "dev", Date: "2026-03-06", Period: "default", TestName: "dev healthy scenario", TestSuite: "suite/dev", CurrentPassPercentage: 99.0, CurrentRuns: 25},
		{Environment: "int", Date: "2026-03-04", Period: "default", TestName: "int flaky scenario", TestSuite: "suite/int", CurrentPassPercentage: 92.0, CurrentRuns: 12},
		{Environment: "dev", Date: "2026-03-08", Period: "default", TestName: "dev flaky scenario", TestSuite: "suite/dev", CurrentPassPercentage: 83.0, CurrentRuns: 25},
		{Environment: "int", Date: "2026-03-08", Period: "default", TestName: "int flaky scenario", TestSuite: "suite/int", CurrentPassPercentage: 92.0, CurrentRuns: 12},
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
			MemberSignatureIDs:      []string{"sig-dev-timeout"},
			References: []semanticcontracts.ReferenceRecord{
				{RunURL: "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-dev-latest/1", OccurredAt: "2026-03-06T12:00:00Z", SignatureID: "sig-dev-timeout"},
				{RunURL: "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-dev-older/1", OccurredAt: "2026-03-05T12:00:00Z", SignatureID: "sig-dev-timeout"},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase2ClusterID:         "int-sig-1",
			SupportCount:            5,
			PostGoodCommitCount:     0,
			CanonicalEvidencePhrase: "timeout during CreateHCPClusterFromParam",
			MemberSignatureIDs:      []string{"sig-int-timeout"},
			References: []semanticcontracts.ReferenceRecord{
				{RunURL: "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-int-latest/1", OccurredAt: "2026-03-04T08:00:00Z", SignatureID: "sig-int-timeout"},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase2ClusterID:         "int-sig-2",
			SupportCount:            4,
			PostGoodCommitCount:     0,
			CanonicalEvidencePhrase: "image pull backoff on bootstrap",
			MemberSignatureIDs:      []string{"sig-int-image-pull"},
		},
	}); err != nil {
		t.Fatalf("seed global clusters: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment: "dev",
			RowID:       "raw-dev-1",
			RunURL:      "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-dev-latest/1",
			SignatureID: "sig-dev-timeout",
			OccurredAt:  "2026-03-06T12:00:00Z",
			RawText:     "panic: timed out waiting for cluster API\ncontext deadline exceeded",
		},
		{
			Environment: "int",
			RowID:       "raw-int-1",
			RunURL:      "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-int-latest/1",
			SignatureID: "sig-int-timeout",
			OccurredAt:  "2026-03-04T08:00:00Z",
			RawText:     "panic: timed out waiting for cluster API in int",
		},
		{
			Environment: "int",
			RowID:       "raw-int-2",
			RunURL:      "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/example-int-latest/2",
			SignatureID: "sig-int-image-pull",
			OccurredAt:  "2026-03-04T09:00:00Z",
			RawText:     "failed to pull image from registry",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	outputPath := filepath.Join(dataDir, "reports", "weekly.html")
	if err := Generate(ctx, store, Options{
		OutputPath: outputPath,
		StartDate:  "2026-03-01",
		TargetRate: 95,
		Chrome: triagehtml.ReportChromeOptions{
			CurrentWeek:  "2026-03-01",
			CurrentView:  triagehtml.ReportViewWeekly,
			PreviousWeek: "2026-02-22",
			PreviousHref: "../2026-02-22/weekly-metrics.html",
			NextWeek:     "2026-03-08",
			NextHref:     "../2026-03-08/weekly-metrics.html",
			WeeklyHref:   "weekly-metrics.html",
			GlobalHref:   "global-signature-triage.html",
			ArchiveHref:  "../archive/",
		},
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
		"id=\"theme-toggle\"",
		"class=\"report-chrome\"",
		"../2026-02-22/weekly-metrics.html",
		"../2026-03-08/weekly-metrics.html",
		"href=\"global-signature-triage.html\"",
		"href=\"../archive/\"",
		"Weekly Report",
		"Triage Report",
		"Window: <strong>2026-03-01</strong> to <strong>2026-03-07</strong>",
		"Goals:<br/>- e2e-integration, e2e-stage, e2e-prod job runs should each succeed 95% of the time<br/>- e2e-dev job runs should succeed 95% of the time after the last push of a PR that merges",
		"Environment: DEV",
		"Environment: INT",
		"Environment: STG",
		"Environment: PROD",
		"<span class=\"exec-heading-help\" title=\"INT/STG/PROD use all E2E job runs. DEV uses runs after the last push of a PR that merges.\">Goal basis</span>",
		"<span class=\"exec-heading-help\" title=\"Success rate on the goal basis: (runs - failed runs) / runs * 100.\">Success</span>",
		"Provision success",
		"Provision change WoW",
		"E2E success",
		"E2E success WoW",
		"Change WoW",
		"pp-negative",
		"Lane outcomes",
		"Tests below 95%",
		"Top failure signatures",
		"Jump to Global signature triage for this week",
		"global-signature-triage.html#env-dev",
		"Provision step success rate (Other excluded)",
		"E2E success (runs reaching E2E)",
		"Provision success (after last push of merged PR)",
		"E2E success (after last push of merged PR)",
		"cards cards-post-good cards-dev",
		"88.89% (8/9)",
		"Source: Sippy test metadata (period: default, rolling 7-day window). Top 5 tests below 95.00% success; minimum 10 runs. This view uses the first metadata datapoint available after the report window end date; if unavailable, it falls back to the latest datapoint before the end date.",
		"Up to 50 semantic signatures are loaded in this window (minimum 1.00% share), with 10 shown by default. Default sorting is flake score desc, jobs affected desc, share desc, count desc; click headers to re-sort.",
		"data-sort-key=\"count\"",
		"data-sort-key=\"after_last_push\"",
		"data-sort-key=\"jobs_affected\"",
		"data-sort-key=\"flake_score\"",
		"<th>Trend</th>",
		"2026-03-01..2026-03-07",
		"dev flaky scenario",
		"83.00%",
		"int flaky scenario",
		"92.00%",
		"<th>Seen in",
		"Full failure examples",
		"Full failure examples (1)",
		"Affected runs (",
		"Contributing tests (",
		"example-dev-latest",
		"panic: timed out waiting for cluster API",
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
		"Other:1",
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
		"Quality notes",
		"Quality score",
		"Provision success (DEV)",
		"Provision change from last week (DEV)",
		"Change from last week",
		"Main reason for failed runs",
		"Most common failure pattern",
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
		{Environment: "dev", Date: "2026-03-01", Metric: metricPostGoodRunCount, Value: 10},
		{Environment: "dev", Date: "2026-03-01", Metric: metricPostGoodFailedProvision, Value: 2},
		{Environment: "dev", Date: "2026-03-01", Metric: metricPostGoodFailedE2EJobs, Value: 1},
		{Environment: "dev", Date: "2026-03-01", Metric: metricPostGoodFailedCIInfra, Value: 0},
		{Environment: "dev", Date: "2026-02-22", Metric: metricRunCount, Value: 8},
		{Environment: "dev", Date: "2026-02-22", Metric: metricFailureCount, Value: 4},
		{Environment: "dev", Date: "2026-02-22", Metric: metricPostGoodRunCount, Value: 4},
		{Environment: "dev", Date: "2026-02-22", Metric: metricPostGoodFailedProvision, Value: 0},
		{Environment: "dev", Date: "2026-02-22", Metric: metricPostGoodFailedE2EJobs, Value: 0},
		{Environment: "dev", Date: "2026-02-22", Metric: metricPostGoodFailedCIInfra, Value: 0},
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
		"Provision success",
		"Provision change WoW",
		"E2E success",
		"E2E success WoW",
		"Change WoW",
		"pp-negative",
		"80.00% (8/10)",
		"-20.00pp",
		"Tests below 95%",
		"Top failure signatures",
		"<th>Trend</th>",
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

func TestGenerateUsesFirstMetadataDatapointAfterWindowEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "int", Date: "2026-03-05", Metric: metricRunCount, Value: 8},
		{Environment: "int", Date: "2026-03-08", Metric: metricRunCount, Value: 8},
	}); err != nil {
		t.Fatalf("seed metrics daily rows: %v", err)
	}
	if err := store.UpsertTestMetadataDaily(ctx, []storecontracts.TestMetadataDailyRecord{
		{
			Environment:           "int",
			Date:                  "2026-03-05",
			Period:                "default",
			TestName:              "inside-window-only-test",
			TestSuite:             "integration/parallel",
			CurrentPassPercentage: 70.0,
			CurrentRuns:           30,
		},
		{
			Environment:           "int",
			Date:                  "2026-03-08",
			Period:                "default",
			TestName:              "after-window-selected-test",
			TestSuite:             "integration/parallel",
			CurrentPassPercentage: 80.0,
			CurrentRuns:           30,
		},
	}); err != nil {
		t.Fatalf("seed test metadata daily rows: %v", err)
	}

	outputPath := filepath.Join(dataDir, "reports", "weekly-after-window-datapoint.html")
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
		"after-window-selected-test",
		"2026-03-08",
		"80.00%",
	} {
		if !strings.Contains(report, snippet) {
			t.Fatalf("expected weekly report to contain %q", snippet)
		}
	}
	if strings.Contains(report, "inside-window-only-test") {
		t.Fatalf("expected weekly report to ignore in-window metadata datapoint")
	}
}

func TestGenerateFallsBackToLatestPreEndDateMetadataDatapoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "int", Date: "2026-03-05", Metric: metricRunCount, Value: 8},
		{Environment: "int", Date: "2026-03-06", Metric: metricRunCount, Value: 8},
	}); err != nil {
		t.Fatalf("seed metrics daily rows: %v", err)
	}
	if err := store.UpsertTestMetadataDaily(ctx, []storecontracts.TestMetadataDailyRecord{
		{
			Environment:           "int",
			Date:                  "2026-03-05",
			Period:                "default",
			TestName:              "older-pre-end-test",
			TestSuite:             "integration/parallel",
			CurrentPassPercentage: 70.0,
			CurrentRuns:           30,
		},
		{
			Environment:           "int",
			Date:                  "2026-03-06",
			Period:                "default",
			TestName:              "latest-pre-end-test",
			TestSuite:             "integration/parallel",
			CurrentPassPercentage: 80.0,
			CurrentRuns:           30,
		},
	}); err != nil {
		t.Fatalf("seed test metadata daily rows: %v", err)
	}

	outputPath := filepath.Join(dataDir, "reports", "weekly-pre-end-fallback.html")
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
		"latest-pre-end-test",
		"2026-03-06",
		"80.00%",
	} {
		if !strings.Contains(report, snippet) {
			t.Fatalf("expected weekly report to contain %q", snippet)
		}
	}
	if strings.Contains(report, "older-pre-end-test") {
		t.Fatalf("expected weekly report to use latest pre-end-date datapoint only")
	}
}

func TestRankTopSignaturesByEnvironmentAppliesMinShareThreshold(t *testing.T) {
	t.Parallel()

	snapshot := semanticSnapshot{
		PhraseSupportByEnv: map[string]map[string]int{
			"dev": {
				"high-impact signature": 99,
				"low-signal signature":  1,
			},
		},
		PhrasePostGoodByEnv: map[string]map[string]int{
			"dev": {
				"high-impact signature": 5,
				"low-signal signature":  0,
			},
		},
		PhraseFullErrorsByEnv: map[string]map[string][]string{
			"dev": {
				"high-impact signature": {"high-impact full error"},
			},
		},
	}

	rowsByEnvironment := rankTopSignaturesByEnvironment(snapshot, 10, 2.0)
	rows := rowsByEnvironment["dev"]
	if len(rows) != 1 {
		t.Fatalf("expected exactly one signature above min share, got %d", len(rows))
	}
	if rows[0].Phrase != "high-impact signature" {
		t.Fatalf("expected high-impact signature to remain, got %q", rows[0].Phrase)
	}
	if len(rows[0].FullErrorSamples) != 1 || rows[0].FullErrorSamples[0] != "high-impact full error" {
		t.Fatalf("expected full error sample to be attached to ranked signature, got %#v", rows[0].FullErrorSamples)
	}
}

func TestRankTopSignaturesByEnvironmentPushesLikelyBadPRRowsToBottom(t *testing.T) {
	t.Parallel()

	snapshot := semanticSnapshot{
		PhraseSupportByEnv: map[string]map[string]int{
			"dev": {
				"likely bad pr signature": 100,
				"healthy signature":       10,
			},
		},
		PhrasePostGoodByEnv: map[string]map[string]int{
			"dev": {
				"likely bad pr signature": 0,
				"healthy signature":       3,
			},
		},
		PhraseReferencesByEnv: map[string]map[string][]triagehtml.RunReference{
			"dev": {
				"likely bad pr signature": {
					{
						RunURL:     "https://prow.example/run/dev-bad",
						PRNumber:   4313,
						OccurredAt: "2026-03-08T10:00:00Z",
					},
				},
				"healthy signature": {
					{
						RunURL:     "https://prow.example/run/dev-healthy",
						PRNumber:   4314,
						OccurredAt: "2026-03-08T09:00:00Z",
					},
				},
			},
		},
		PhraseFullErrorsByEnv: map[string]map[string][]string{
			"dev": {
				"likely bad pr signature": {"bad full error"},
				"healthy signature":       {"healthy full error"},
			},
		},
	}

	rowsByEnvironment := rankTopSignaturesByEnvironment(snapshot, 1, 0)
	rows := rowsByEnvironment["dev"]
	if len(rows) != 1 {
		t.Fatalf("expected one row after top limit, got %d", len(rows))
	}
	if rows[0].Phrase != "healthy signature" {
		t.Fatalf("expected healthy signature to win after bad-pr ordering, got %q", rows[0].Phrase)
	}
	if rows[0].BadPRScore != 0 {
		t.Fatalf("expected healthy signature bad-pr score to be 0, got %d", rows[0].BadPRScore)
	}
}

func TestSummarizeStepOutcomesForGoalBasisUsesPostGoodForDev(t *testing.T) {
	t.Parallel()

	devReport := envReport{
		Environment: "dev",
		Days: []dayReport{
			{
				Counts: counts{
					RunCount:                10,
					FailureCount:            4,
					FailedCIInfraRunCount:   1,
					FailedProvisionRunCount: 2,
					FailedE2ERunCount:       1,
				},
				PostGoodRunOutcomes: runOutcomes{
					TotalRuns:           4,
					SuccessfulRuns:      2,
					CIInfraFailedRuns:   0,
					ProvisionFailedRuns: 1,
					E2EFailedRuns:       1,
				},
			},
		},
	}

	provision := summarizeProvisionStepOutcomesForGoalBasis(devReport)
	if provision.TotalAttempted != 4 || provision.Successful != 3 || provision.Failed != 1 {
		t.Fatalf("expected DEV provision outcomes to use post-good basis, got %#v", provision)
	}

	e2e := summarizeE2EStepOutcomesForGoalBasis(devReport)
	if e2e.TotalAttempted != 3 || e2e.Successful != 2 || e2e.Failed != 1 {
		t.Fatalf("expected DEV e2e outcomes to use post-good basis, got %#v", e2e)
	}
}

func TestSummarizeStepOutcomesForGoalBasisUsesAllRunsForNonDev(t *testing.T) {
	t.Parallel()

	intReport := envReport{
		Environment: "int",
		Days: []dayReport{
			{
				Counts: counts{
					RunCount:                10,
					FailureCount:            4,
					FailedCIInfraRunCount:   1,
					FailedProvisionRunCount: 2,
					FailedE2ERunCount:       1,
				},
				PostGoodRunOutcomes: runOutcomes{
					TotalRuns:           4,
					SuccessfulRuns:      2,
					CIInfraFailedRuns:   0,
					ProvisionFailedRuns: 1,
					E2EFailedRuns:       1,
				},
			},
		},
	}

	provision := summarizeProvisionStepOutcomesForGoalBasis(intReport)
	if provision.TotalAttempted != 9 || provision.Successful != 7 || provision.Failed != 2 {
		t.Fatalf("expected non-DEV provision outcomes to use all runs, got %#v", provision)
	}

	e2e := summarizeE2EStepOutcomesForGoalBasis(intReport)
	if e2e.TotalAttempted != 7 || e2e.Successful != 6 || e2e.Failed != 1 {
		t.Fatalf("expected non-DEV e2e outcomes to use all runs, got %#v", e2e)
	}
}

func TestProvisionStepKPIIsDevOnly(t *testing.T) {
	t.Parallel()

	devReport := envReport{
		Environment: "dev",
		Days: []dayReport{
			{
				PostGoodRunOutcomes: runOutcomes{
					TotalRuns:           5,
					SuccessfulRuns:      3,
					ProvisionFailedRuns: 1,
					E2EFailedRuns:       1,
				},
			},
		},
	}
	devProvision, devAvailable := provisionStepKPI(devReport)
	if !devAvailable {
		t.Fatalf("expected DEV provision KPI to be available")
	}
	if devProvision.TotalAttempted != 5 || devProvision.Successful != 4 || devProvision.Failed != 1 {
		t.Fatalf("unexpected DEV provision KPI totals: %#v", devProvision)
	}

	intReport := envReport{
		Environment: "int",
		Days: []dayReport{
			{
				Counts: counts{
					RunCount:                10,
					FailureCount:            4,
					FailedCIInfraRunCount:   1,
					FailedProvisionRunCount: 2,
					FailedE2ERunCount:       1,
				},
			},
		},
	}
	_, intAvailable := provisionStepKPI(intReport)
	if intAvailable {
		t.Fatalf("expected non-DEV provision KPI to be unavailable")
	}
}

func TestFormatSignedPercentPointCellUsesDirectionalClasses(t *testing.T) {
	t.Parallel()

	positive := formatSignedPercentPointCell(1.5)
	if !strings.Contains(positive, "pp-positive") || !strings.Contains(positive, "+1.50pp") {
		t.Fatalf("expected positive pp cell style, got %q", positive)
	}

	negative := formatSignedPercentPointCell(-2.25)
	if !strings.Contains(negative, "pp-negative") || !strings.Contains(negative, "-2.25pp") {
		t.Fatalf("expected negative pp cell style, got %q", negative)
	}

	neutral := formatSignedPercentPointCell(0)
	if !strings.Contains(neutral, "pp-neutral") || !strings.Contains(neutral, "+0.00pp") {
		t.Fatalf("expected neutral pp cell style, got %q", neutral)
	}
}
