package readmodel

import (
	"context"
	"testing"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func TestBuildReviewWeekUsesServiceReadModel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	currentStore := fixture.openWeekStore(t, "2026-03-15")
	previousStore := fixture.openWeekStore(t, "2026-03-08")

	if err := currentStore.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed current materialized week: %v", err)
	}
	if err := previousStore.ReplaceMaterializedWeek(ctx, previousMaterializedWeek()); err != nil {
		t.Fatalf("seed previous materialized week: %v", err)
	}
	if err := currentStore.UpsertRawFailures(ctx, sampleRawFailuresFixture()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}
	if err := currentStore.UpsertMetricsDaily(ctx, reportMetricsDaily()); err != nil {
		t.Fatalf("seed metrics daily: %v", err)
	}
	if err := currentStore.UpsertPhase3Issues(ctx, []semanticcontracts.Phase3IssueRecord{{
		SchemaVersion: semanticcontracts.SchemaVersionV1,
		IssueID:       "QE-123",
		Title:         "OAuth flake",
		CreatedAt:     "2026-03-16T12:00:00Z",
		UpdatedAt:     "2026-03-16T12:00:00Z",
	}}); err != nil {
		t.Fatalf("seed phase3 issue: %v", err)
	}
	if err := currentStore.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{{
		SchemaVersion: semanticcontracts.SchemaVersionV1,
		IssueID:       "QE-123",
		Environment:   "dev",
		RunURL:        "https://prow.example.com/view/1",
		RowID:         "row-1",
		UpdatedAt:     "2026-03-16T12:00:00Z",
	}}); err != nil {
		t.Fatalf("seed phase3 link: %v", err)
	}

	snapshot, err := fixture.service.BuildReviewWeek(ctx, "2026-03-15")
	if err != nil {
		t.Fatalf("build review week: %v", err)
	}

	if got, want := snapshot.Week, "2026-03-15"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := snapshot.TotalClusters, 1; got != want {
		t.Fatalf("unexpected cluster count: got=%d want=%d", got, want)
	}
	if got, want := snapshot.OverallJobsByEnv["dev"], 7; got != want {
		t.Fatalf("unexpected overall jobs: got=%d want=%d", got, want)
	}
	if got, want := len(snapshot.Rows), 1; got != want {
		t.Fatalf("unexpected row count: got=%d want=%d", got, want)
	}
	row := snapshot.Rows[0]
	if got, want := row.ManualIssueID, "QE-123"; got != want {
		t.Fatalf("unexpected manual issue id: got=%q want=%q", got, want)
	}
	if len(row.FullErrorSamples) == 0 {
		t.Fatalf("expected review row to include full error samples")
	}
}

func TestBuildFailurePatternReportDataProjectsSamplesAndCounts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	store := fixture.openWeekStore(t, "2026-03-15")

	if err := store.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, sampleRawFailuresFixture()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}
	if err := store.UpsertMetricsDaily(ctx, reportMetricsDaily()); err != nil {
		t.Fatalf("seed metrics daily: %v", err)
	}

	data, err := BuildFailurePatternReportData(ctx, store, FailurePatternReportBuildOptions{
		Week:         "2026-03-15",
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("build failure-pattern report data: %v", err)
	}

	if got, want := len(data.FailurePatternClusters), 1; got != want {
		t.Fatalf("unexpected failure-pattern count: got=%d want=%d", got, want)
	}
	if got, want := data.TargetEnvironments[0], "dev"; got != want {
		t.Fatalf("unexpected target environment: got=%q want=%q", got, want)
	}
	cluster := data.FailurePatternClusters[0]
	if got, want := cluster.Environment, "dev"; got != want {
		t.Fatalf("unexpected cluster environment: got=%q want=%q", got, want)
	}
	if len(cluster.FullErrorSamples) == 0 {
		t.Fatalf("expected failure pattern to include full error samples")
	}
	if got, want := data.OverallJobsByEnvironment["dev"], 7; got != want {
		t.Fatalf("unexpected overall jobs: got=%d want=%d", got, want)
	}
}

func TestBuildWeeklyReportDataBuildsCurrentAndPreviousReadModels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	currentStore := fixture.openWeekStore(t, "2026-03-15")
	previousStore := fixture.openWeekStore(t, "2026-03-08")

	if err := currentStore.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed current materialized week: %v", err)
	}
	if err := previousStore.ReplaceMaterializedWeek(ctx, previousMaterializedWeek()); err != nil {
		t.Fatalf("seed previous materialized week: %v", err)
	}
	if err := currentStore.UpsertRawFailures(ctx, sampleRawFailuresFixture()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}
	if err := currentStore.UpsertMetricsDaily(ctx, reportMetricsDaily()); err != nil {
		t.Fatalf("seed metrics daily: %v", err)
	}
	if err := currentStore.UpsertTestMetadataDaily(ctx, reportTestMetadataDaily()); err != nil {
		t.Fatalf("seed test metadata daily: %v", err)
	}

	data, err := BuildWeeklyReportData(ctx, currentStore, previousStore, WeeklyReportBuildOptions{
		StartDate:  time.Date(2026, time.March, 15, 0, 0, 0, 0, time.UTC),
		TargetRate: 95.0,
		Week:       "2026-03-15",
	})
	if err != nil {
		t.Fatalf("build weekly report data: %v", err)
	}

	if got, want := data.StartDate.Format("2006-01-02"), "2026-03-15"; got != want {
		t.Fatalf("unexpected start date: got=%q want=%q", got, want)
	}
	if got, want := data.EndDate.Format("2006-01-02"), "2026-03-21"; got != want {
		t.Fatalf("unexpected end date: got=%q want=%q", got, want)
	}
	devReport := weeklyEnvReportByName(t, data.CurrentReports, "dev")
	if got, want := devReport.Days[0].Counts.RunCount, 7; got != want {
		t.Fatalf("unexpected current run count: got=%d want=%d", got, want)
	}
	if got, want := len(data.TopSignaturesByEnv["dev"]), 1; got != want {
		t.Fatalf("unexpected top signature count: got=%d want=%d", got, want)
	}
	if got, want := data.TopSignaturesByEnv["dev"][0].Phrase, "OAuth timeout"; got != want {
		t.Fatalf("unexpected top signature phrase: got=%q want=%q", got, want)
	}
	if got, want := len(data.TestsBelowTargetByEnv["dev"]), 1; got != want {
		t.Fatalf("unexpected below-target tests count: got=%d want=%d", got, want)
	}
	if got, want := data.TestsBelowTargetByEnv["dev"][0].TestName, "should oauth"; got != want {
		t.Fatalf("unexpected below-target test name: got=%q want=%q", got, want)
	}
	if got, want := data.PreviousSemantic.ByEnvironment["dev"].FailurePatternClusters, 1; got != want {
		t.Fatalf("unexpected previous semantic failure-pattern count: got=%d want=%d", got, want)
	}
}

func weeklyEnvReportByName(t *testing.T, reports []WeeklyEnvReport, environment string) WeeklyEnvReport {
	t.Helper()
	for _, report := range reports {
		if report.Environment == environment {
			return report
		}
	}
	t.Fatalf("missing environment report %q", environment)
	return WeeklyEnvReport{}
}

func reportMetricsDaily() []storecontracts.MetricDailyRecord {
	return []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-15", Metric: "run_count", Value: 7},
		{Environment: "dev", Date: "2026-03-15", Metric: "failure_count", Value: 2},
		{Environment: "dev", Date: "2026-03-15", Metric: "failed_e2e_run_count", Value: 2},
		{Environment: "dev", Date: "2026-03-15", Metric: "post_good_run_count", Value: 4},
		{Environment: "dev", Date: "2026-03-15", Metric: "post_good_failed_e2e_jobs", Value: 1},
		{Environment: "dev", Date: "2026-03-08", Metric: "run_count", Value: 5},
		{Environment: "dev", Date: "2026-03-08", Metric: "failure_count", Value: 1},
		{Environment: "dev", Date: "2026-03-08", Metric: "failed_e2e_run_count", Value: 1},
	}
}

func reportTestMetadataDaily() []storecontracts.TestMetadataDailyRecord {
	return []storecontracts.TestMetadataDailyRecord{
		{
			Environment:            "dev",
			Date:                   "2026-03-15",
			Period:                 "default",
			TestName:               "should oauth",
			TestSuite:              "suite-a",
			CurrentPassPercentage:  90.0,
			CurrentRuns:            12,
			PreviousPassPercentage: 95.0,
			PreviousRuns:           10,
			NetImprovement:         -5.0,
		},
	}
}
