package readmodel

import (
	"context"
	"strings"
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

func TestBuildFailurePatternsIgnoresDeprecatedPhase3Links(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	store := fixture.openWeekStore(t, "2026-03-15")

	if err := store.ReplaceMaterializedWeek(ctx, jobHistoryMaterializedWeekWithExtraCluster()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertRuns(ctx, sampleRunsFixture()); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	rawFailures := append(sampleRawFailuresFixture(), storecontracts.RawFailureRecord{
		Environment:    "dev",
		RowID:          "row-4",
		RunURL:         "https://prow.example.com/view/1",
		TestName:       "should throttle",
		TestSuite:      "suite-c",
		SignatureID:    "sig-c",
		OccurredAt:     "2026-03-16T08:07:00Z",
		RawText:        "API throttling while reconciling install state",
		NormalizedText: "api throttling while reconciling install state",
	})
	if err := store.UpsertRawFailures(ctx, rawFailures); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}
	if err := store.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-16", Metric: "run_count", Value: 4},
	}); err != nil {
		t.Fatalf("seed metrics daily: %v", err)
	}
	fixture.seedDeprecatedPhase3Links(t,
		semanticcontracts.Phase3LinkRecord{
			SchemaVersion: semanticcontracts.CurrentSchemaVersion,
			IssueID:       "QE-123",
			Environment:   "dev",
			RunURL:        "https://prow.example.com/view/1",
			RowID:         "row-1",
			UpdatedAt:     "2026-03-16T12:00:00Z",
		},
		semanticcontracts.Phase3LinkRecord{
			SchemaVersion: semanticcontracts.CurrentSchemaVersion,
			IssueID:       "QE-123",
			Environment:   "dev",
			RunURL:        "https://prow.example.com/view/1",
			RowID:         "row-4",
			UpdatedAt:     "2026-03-16T12:00:00Z",
		},
	)

	data, err := fixture.service.BuildFailurePatterns(ctx, FailurePatternsQuery{
		StartDate:    "2026-03-16",
		EndDate:      "2026-03-16",
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("build failure patterns: %v", err)
	}

	environment := data.Environments[0]
	if got, want := len(environment.Rows), 2; got != want {
		t.Fatalf("unexpected row count after deprecated phase3 seeding: got=%d want=%d", got, want)
	}
	if got, want := environment.Rows[0].ClusterID, "cluster-dev-a"; got != want {
		t.Fatalf("unexpected first cluster id: got=%q want=%q", got, want)
	}
	if got, want := environment.Rows[1].ClusterID, "cluster-dev-c"; got != want {
		t.Fatalf("unexpected second cluster id: got=%q want=%q", got, want)
	}
	for _, row := range environment.Rows {
		if len(row.LinkedChildren) != 0 {
			t.Fatalf("expected deprecated phase3 links to produce no linked children, got=%d for %s", len(row.LinkedChildren), row.ClusterID)
		}
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
	if got, want := environment.Rows[0].WeeklyPostGoodCount, 2; got != want {
		t.Fatalf("unexpected merged post-good count: got=%d want=%d", got, want)
	}
	if got, want := len(environment.Rows[0].ScoringReferences), 3; got != want {
		t.Fatalf("unexpected merged scoring reference count: got=%d want=%d", got, want)
	}
}

func TestBuildFailurePatternsRejectsMixedSchemaAcrossWeeks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	currentStore := fixture.openWeekStore(t, "2026-03-15")
	nextStore := fixture.openWeekStore(t, "2026-03-22")

	if err := currentStore.ReplaceMaterializedWeek(ctx, materializedWeekWithSchemaVersion(currentMaterializedWeek(), semanticcontracts.SchemaVersionV1)); err != nil {
		t.Fatalf("seed current materialized week: %v", err)
	}
	if err := nextStore.ReplaceMaterializedWeek(ctx, materializedWeekWithSchemaVersion(currentMaterializedWeek(), semanticcontracts.SchemaVersionV2)); err != nil {
		t.Fatalf("seed next materialized week: %v", err)
	}

	_, err := fixture.service.BuildFailurePatterns(ctx, FailurePatternsQuery{
		StartDate:    "2026-03-16",
		EndDate:      "2026-03-22",
		Environments: []string{"dev"},
	})
	if err == nil {
		t.Fatalf("expected mixed-schema window build to fail")
	}
	if !strings.Contains(err.Error(), "semantic week 2026-03-15 uses legacy semantic schema v1") {
		t.Fatalf("expected mixed-schema error, got=%v", err)
	}
}

func TestBuildFailurePatternsBadPRScoreUsesWindowReferenceSpread(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	currentStore := fixture.openWeekStore(t, "2026-03-15")
	nextStore := fixture.openWeekStore(t, "2026-03-22")

	currentWeek := currentMaterializedWeek()
	currentWeek.FailurePatterns[0].SupportCount = 1
	currentWeek.FailurePatterns[0].PostGoodCommitCount = 0
	currentWeek.FailurePatterns[0].References = []semanticcontracts.ReferenceRecord{
		{
			RowID:          "row-1",
			RunURL:         "https://prow.example.com/view/1",
			OccurredAt:     "2026-03-16T08:00:00Z",
			SignatureID:    "sig-a",
			PRNumber:       4101,
			PostGoodCommit: false,
		},
	}
	if err := currentStore.ReplaceMaterializedWeek(ctx, currentWeek); err != nil {
		t.Fatalf("seed current materialized week: %v", err)
	}

	nextWeek := currentMaterializedWeek()
	nextWeek.FailurePatterns[0].Phase2ClusterID = "cluster-dev-b"
	nextWeek.FailurePatterns[0].SupportCount = 1
	nextWeek.FailurePatterns[0].PostGoodCommitCount = 0
	nextWeek.FailurePatterns[0].References = []semanticcontracts.ReferenceRecord{
		{
			RowID:          "row-22",
			RunURL:         "https://prow.example.com/view/22",
			OccurredAt:     "2026-03-22T08:00:00Z",
			SignatureID:    "sig-a",
			PRNumber:       4102,
			PostGoodCommit: false,
		},
	}
	if err := nextStore.ReplaceMaterializedWeek(ctx, nextWeek); err != nil {
		t.Fatalf("seed next materialized week: %v", err)
	}

	if err := currentStore.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/1",
			JobName:     "periodic-ci",
			PRNumber:    4101,
			Failed:      true,
			OccurredAt:  "2026-03-16T08:00:00Z",
		},
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/22",
			JobName:     "periodic-ci",
			PRNumber:    4102,
			Failed:      true,
			OccurredAt:  "2026-03-22T08:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed cross-week runs: %v", err)
	}
	if err := currentStore.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-1",
			RunURL:         "https://prow.example.com/view/1",
			TestName:       "should oauth",
			TestSuite:      "suite-a",
			SignatureID:    "sig-a",
			OccurredAt:     "2026-03-16T08:00:00Z",
			RawText:        "OAuth timeout while waiting for cluster operator",
			NormalizedText: "oauth timeout while waiting for cluster operator",
		},
		{
			Environment:    "dev",
			RowID:          "row-22",
			RunURL:         "https://prow.example.com/view/22",
			TestName:       "should oauth",
			TestSuite:      "suite-a",
			SignatureID:    "sig-a",
			OccurredAt:     "2026-03-22T08:00:00Z",
			RawText:        "OAuth timeout while waiting for cluster operator",
			NormalizedText: "oauth timeout while waiting for cluster operator",
		},
	}); err != nil {
		t.Fatalf("seed cross-week raw failures: %v", err)
	}
	if err := currentStore.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-16", Metric: "run_count", Value: 1},
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
		t.Fatalf("build failure patterns: %v", err)
	}

	row := data.Environments[0].Rows[0]
	score, reasons := BadPRScoreAndReasons(FailurePatternRow{
		Environment:        row.Environment,
		AfterLastPushCount: row.WeeklyPostGoodCount,
		AlsoIn:             row.SeenIn,
		AffectedRuns:       toWindowedHTMLRunReferences(row.References),
		ScoringReferences:  toWindowedHTMLRunReferences(row.ScoringReferences),
	})
	if got, want := row.WeeklyPostGoodCount, 0; got != want {
		t.Fatalf("unexpected window post-good count: got=%d want=%d", got, want)
	}
	if got, want := score, 2; got != want {
		t.Fatalf("unexpected bad PR score: got=%d want=%d reasons=%v", got, want, reasons)
	}
	for _, reason := range reasons {
		if reason == "only seen in one PR" {
			t.Fatalf("did not expect single-PR reason for multi-PR windowed row: %v", reasons)
		}
	}
}

func TestBuildFailurePatternsUsesStoredReferencesWhenClustersShareSignature(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	store := fixture.openWeekStore(t, "2026-03-15")

	week := storecontracts.MaterializedWeek{
		FailurePatterns: []semanticcontracts.FailurePatternRecord{
			{
				SchemaVersion:           semanticcontracts.CurrentSchemaVersion,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-finalize",
				CanonicalEvidencePhrase: "finalize-mce-config timeout",
				SearchQueryPhrase:       "finalize-mce-config timeout",
				SupportCount:            1,
				ContributingTestsCount:  1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "provision",
						JobName:      "periodic-ci",
						TestName:     "finalize step",
						SupportCount: 1,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-shared"},
				MemberSignatureIDs:     []string{"sig-shared"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-finalize",
						RunURL:      "https://prow.example.com/view/finalize",
						OccurredAt:  "2026-03-16T08:00:00Z",
						SignatureID: "sig-shared",
					},
				},
			},
			{
				SchemaVersion:           semanticcontracts.CurrentSchemaVersion,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-propagator",
				CanonicalEvidencePhrase: "grc-policy-propagator timeout",
				SearchQueryPhrase:       "grc-policy-propagator timeout",
				SupportCount:            1,
				ContributingTestsCount:  1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "provision",
						JobName:      "periodic-ci",
						TestName:     "propagator step",
						SupportCount: 1,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-shared"},
				MemberSignatureIDs:     []string{"sig-shared"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-propagator",
						RunURL:      "https://prow.example.com/view/propagator",
						OccurredAt:  "2026-03-16T09:00:00Z",
						SignatureID: "sig-shared",
					},
				},
			},
		},
	}
	if err := store.ReplaceMaterializedWeek(ctx, week); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/finalize",
			JobName:     "periodic-ci",
			Failed:      true,
			OccurredAt:  "2026-03-16T08:00:00Z",
		},
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/propagator",
			JobName:     "periodic-ci",
			Failed:      true,
			OccurredAt:  "2026-03-16T09:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-finalize",
			RunURL:         "https://prow.example.com/view/finalize",
			TestName:       "finalize step",
			TestSuite:      "suite-a",
			SignatureID:    "sig-shared",
			OccurredAt:     "2026-03-16T08:00:00Z",
			RawText:        "failed post-install: resource not ready, name: finalize-mce-config",
			NormalizedText: "finalize-mce-config timeout",
		},
		{
			Environment:    "dev",
			RowID:          "row-propagator",
			RunURL:         "https://prow.example.com/view/propagator",
			TestName:       "propagator step",
			TestSuite:      "suite-a",
			SignatureID:    "sig-shared",
			OccurredAt:     "2026-03-16T09:00:00Z",
			RawText:        "resource not ready, name: grc-policy-propagator",
			NormalizedText: "grc-policy-propagator timeout",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}
	if err := store.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-16", Metric: "run_count", Value: 2},
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
	if got, want := len(data.Environments), 1; got != want {
		t.Fatalf("unexpected environment count: got=%d want=%d", got, want)
	}
	if got, want := len(data.Environments[0].Rows), 2; got != want {
		t.Fatalf("unexpected row count: got=%d want=%d", got, want)
	}

	rowsByID := map[string]FailurePatternsRow{}
	for _, row := range data.Environments[0].Rows {
		rowsByID[row.ClusterID] = row
	}

	finalizeRow, ok := rowsByID["cluster-finalize"]
	if !ok {
		t.Fatalf("expected finalize row in response: %#v", data.Environments[0].Rows)
	}
	if got, want := finalizeRow.WindowFailureCount, 1; got != want {
		t.Fatalf("unexpected finalize window failure count: got=%d want=%d", got, want)
	}
	if got, want := len(finalizeRow.References), 1; got != want {
		t.Fatalf("unexpected finalize reference count: got=%d want=%d", got, want)
	}
	if got, want := finalizeRow.References[0].RunURL, "https://prow.example.com/view/finalize"; got != want {
		t.Fatalf("unexpected finalize run url: got=%q want=%q", got, want)
	}
	if got, want := len(finalizeRow.FullErrorSamples), 1; got != want {
		t.Fatalf("unexpected finalize sample count: got=%d want=%d", got, want)
	}
	if got, want := finalizeRow.FullErrorSamples[0], "failed post-install: resource not ready, name: finalize-mce-config"; got != want {
		t.Fatalf("unexpected finalize sample: got=%q want=%q", got, want)
	}

	propagatorRow, ok := rowsByID["cluster-propagator"]
	if !ok {
		t.Fatalf("expected propagator row in response: %#v", data.Environments[0].Rows)
	}
	if got, want := propagatorRow.WindowFailureCount, 1; got != want {
		t.Fatalf("unexpected propagator window failure count: got=%d want=%d", got, want)
	}
	if got, want := len(propagatorRow.References), 1; got != want {
		t.Fatalf("unexpected propagator reference count: got=%d want=%d", got, want)
	}
	if got, want := propagatorRow.References[0].RunURL, "https://prow.example.com/view/propagator"; got != want {
		t.Fatalf("unexpected propagator run url: got=%q want=%q", got, want)
	}
	if got, want := len(propagatorRow.FullErrorSamples), 1; got != want {
		t.Fatalf("unexpected propagator sample count: got=%d want=%d", got, want)
	}
	if got, want := propagatorRow.FullErrorSamples[0], "resource not ready, name: grc-policy-propagator"; got != want {
		t.Fatalf("unexpected propagator sample: got=%q want=%q", got, want)
	}
}
