package readmodel

import (
	"context"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func TestBuildFailurePatternsProjectsWeeklyRowsIntoWindow(t *testing.T) {
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
	if err := currentStore.UpsertRuns(ctx, sampleRunsFixture()); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := currentStore.UpsertRawFailures(ctx, sampleRawFailuresFixture()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}
	if err := currentStore.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-16", Metric: "run_count", Value: 4},
	}); err != nil {
		t.Fatalf("seed metrics daily: %v", err)
	}

	data, err := fixture.service.BuildFailurePatterns(ctx, FailurePatternsQuery{
		StartDate:    "2026-03-16",
		EndDate:      "2026-03-16",
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("build failure patterns: %v", err)
	}

	if got, want := data.Meta.AnchorWeek, "2026-03-15"; got != want {
		t.Fatalf("unexpected anchor week: got=%q want=%q", got, want)
	}
	if got, want := len(data.Environments), 1; got != want {
		t.Fatalf("unexpected environment count: got=%d want=%d", got, want)
	}

	environment := data.Environments[0]
	if got, want := environment.Environment, "dev"; got != want {
		t.Fatalf("unexpected environment: got=%q want=%q", got, want)
	}
	if got, want := environment.Summary.TotalRuns, 4; got != want {
		t.Fatalf("unexpected total runs: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.FailedRuns, 2; got != want {
		t.Fatalf("unexpected failed runs: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.RawFailureCount, 3; got != want {
		t.Fatalf("unexpected raw failure count: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.MatchedFailureCount, 2; got != want {
		t.Fatalf("unexpected matched failure count: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.JobsAffected, 1; got != want {
		t.Fatalf("unexpected jobs affected summary: got=%d want=%d", got, want)
	}
	if got, want := len(environment.Rows), 1; got != want {
		t.Fatalf("unexpected row count: got=%d want=%d", got, want)
	}

	row := environment.Rows[0]
	if got, want := row.ClusterID, "cluster-dev-a"; got != want {
		t.Fatalf("unexpected cluster id: got=%q want=%q", got, want)
	}
	if got, want := row.WindowFailureCount, 2; got != want {
		t.Fatalf("unexpected window failure count: got=%d want=%d", got, want)
	}
	if got, want := row.JobsAffected, 1; got != want {
		t.Fatalf("unexpected jobs affected: got=%d want=%d", got, want)
	}
	if got, want := row.FailedRuns, 1; got != want {
		t.Fatalf("unexpected failed runs: got=%d want=%d", got, want)
	}
	if got, want := row.WeeklySupportCount, 7; got != want {
		t.Fatalf("unexpected weekly support count: got=%d want=%d", got, want)
	}
	if got, want := row.WeeklyPostGoodCount, 2; got != want {
		t.Fatalf("unexpected weekly post-good count: got=%d want=%d", got, want)
	}
	if got, want := row.ImpactPercent, 25.0; got != want {
		t.Fatalf("unexpected impact percent: got=%f want=%f", got, want)
	}
	if got, want := row.PriorWeeksPresent, 1; got != want {
		t.Fatalf("unexpected prior weeks present: got=%d want=%d", got, want)
	}
	if got, want := len(row.References), 2; got != want {
		t.Fatalf("unexpected reference count: got=%d want=%d", got, want)
	}
	if len(row.FullErrorSamples) == 0 {
		t.Fatalf("expected full error samples for matched row")
	}
}

func TestBuildFailurePatternsComposesCrossWeekWindows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	currentStore := fixture.openWeekStore(t, "2026-03-15")
	nextStore := fixture.openWeekStore(t, "2026-03-22")
	if err := currentStore.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed current materialized week: %v", err)
	}
	nextWeek := currentMaterializedWeek()
	nextWeek.FailurePatterns[0].Phase2ClusterID = "cluster-dev-b"
	nextWeek.FailurePatterns[0].SupportCount = 5
	nextWeek.FailurePatterns[0].PostGoodCommitCount = 1
	nextWeek.FailurePatterns[0].References = []semanticcontracts.ReferenceRecord{
		{
			RowID:       "row-22",
			RunURL:      "https://prow.example.com/view/22",
			OccurredAt:  "2026-03-22T08:00:00Z",
			SignatureID: "sig-a",
		},
	}
	if err := nextStore.ReplaceMaterializedWeek(ctx, nextWeek); err != nil {
		t.Fatalf("seed next materialized week: %v", err)
	}
	if err := currentStore.UpsertRuns(ctx, append(sampleRunsFixture(), storecontracts.RunRecord{
		Environment: "dev",
		RunURL:      "https://prow.example.com/view/22",
		JobName:     "periodic-ci",
		Failed:      true,
		OccurredAt:  "2026-03-22T08:00:00Z",
	})); err != nil {
		t.Fatalf("seed cross-week runs: %v", err)
	}
	if err := currentStore.UpsertRawFailures(ctx, append(sampleRawFailuresFixture(), storecontracts.RawFailureRecord{
		Environment:    "dev",
		RowID:          "row-22",
		RunURL:         "https://prow.example.com/view/22",
		TestName:       "should oauth",
		TestSuite:      "suite-a",
		SignatureID:    "sig-a",
		OccurredAt:     "2026-03-22T08:00:00Z",
		RawText:        "OAuth timeout while waiting for cluster operator",
		NormalizedText: "oauth timeout while waiting for cluster operator",
	})); err != nil {
		t.Fatalf("seed cross-week raw failures: %v", err)
	}
	if err := currentStore.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-16", Metric: "run_count", Value: 4},
		{Environment: "dev", Date: "2026-03-22", Metric: "run_count", Value: 1},
	}); err != nil {
		t.Fatalf("seed cross-week metrics daily: %v", err)
	}

	data, err := fixture.service.BuildFailurePatterns(ctx, FailurePatternsQuery{
		StartDate:    "2026-03-16",
		EndDate:      "2026-03-22",
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("expected cross-week query to succeed: %v", err)
	}

	environment := data.Environments[0]
	if got, want := len(environment.Rows), 1; got != want {
		t.Fatalf("unexpected merged row count: got=%d want=%d", got, want)
	}
	if got, want := environment.Rows[0].WindowFailureCount, 3; got != want {
		t.Fatalf("unexpected merged failure count: got=%d want=%d", got, want)
	}
	if got, want := environment.Rows[0].JobsAffected, 2; got != want {
		t.Fatalf("unexpected merged jobs affected: got=%d want=%d", got, want)
	}
}
