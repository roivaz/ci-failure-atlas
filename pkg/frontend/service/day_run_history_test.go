package service

import (
	"context"
	"strings"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func TestBuildJobHistoryDayBuildsMatchedAndUnmatchedRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	store := fixture.openWeekStore(t, "2026-03-15")

	if err := store.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "issue-dev-oauth",
			Environment:   "dev",
			RunURL:        "https://prow.example.com/view/1",
			RowID:         "row-1",
		},
	}); err != nil {
		t.Fatalf("seed phase3 links: %v", err)
	}
	if err := store.UpsertRuns(ctx, sampleRunsFixture()); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, sampleRawFailuresFixture()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	data, err := fixture.service.BuildJobHistoryDay(ctx, JobHistoryDayQuery{
		Date:         "2026-03-16",
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("build job history day: %v", err)
	}

	if got, want := data.Meta.AnchorWeek, "2026-03-15"; got != want {
		t.Fatalf("unexpected anchor week: got=%q want=%q", got, want)
	}
	environment := jobHistoryEnvironmentByName(t, data, "dev")
	if got, want := environment.Summary.TotalRuns, 2; got != want {
		t.Fatalf("unexpected total runs: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.FailedRuns, 2; got != want {
		t.Fatalf("unexpected failed runs: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.RunsWithRawFailures, 2; got != want {
		t.Fatalf("unexpected runs with raw failures: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.RunsWithSemanticAttachment, 1; got != want {
		t.Fatalf("unexpected runs with semantic attachment: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.RunsUnmatchedSignatures, 1; got != want {
		t.Fatalf("unexpected runs with unmatched signatures: got=%d want=%d", got, want)
	}

	matchedRun := jobHistoryRunByURL(t, environment, "https://prow.example.com/view/1")
	if got, want := matchedRun.SemanticRollups.AttachmentSummary, "single_clustered"; got != want {
		t.Fatalf("unexpected matched run summary: got=%q want=%q", got, want)
	}
	if got, want := matchedRun.SemanticRollups.SignatureCount, 1; got != want {
		t.Fatalf("unexpected matched run signature count: got=%d want=%d", got, want)
	}
	if got, want := matchedRun.FailedTestCount, 1; got != want {
		t.Fatalf("unexpected matched run failed test count: got=%d want=%d", got, want)
	}
	if got, want := matchedRun.SemanticRollups.ClusteredRows, 2; got != want {
		t.Fatalf("unexpected clustered row count: got=%d want=%d", got, want)
	}
	if got, want := len(matchedRun.Lanes), 1; got != want {
		t.Fatalf("unexpected matched run lane count: got=%d want=%d", got, want)
	}
	if got, want := matchedRun.Lanes[0], "upgrade"; got != want {
		t.Fatalf("unexpected matched run lane: got=%q want=%q", got, want)
	}
	if got, want := len(matchedRun.FailureRows), 2; got != want {
		t.Fatalf("unexpected matched run failure row count: got=%d want=%d", got, want)
	}
	if got, want := matchedRun.FailureRows[0].Lane, "upgrade"; got != want {
		t.Fatalf("unexpected matched failure row lane: got=%q want=%q", got, want)
	}
	if got, want := matchedRun.FailureRows[0].SemanticAttachment.CanonicalEvidencePhrase, "OAuth timeout"; got != want {
		t.Fatalf("unexpected matched phrase: got=%q want=%q", got, want)
	}
	if got, want := matchedRun.FailureRows[0].Phase3IssueID, "issue-dev-oauth"; got != want {
		t.Fatalf("unexpected phase3 issue id: got=%q want=%q", got, want)
	}

	unmatchedRun := jobHistoryRunByURL(t, environment, "https://prow.example.com/view/2")
	if got, want := unmatchedRun.SemanticRollups.AttachmentSummary, "unmatched_only"; got != want {
		t.Fatalf("unexpected unmatched run summary: got=%q want=%q", got, want)
	}
	if got, want := unmatchedRun.SemanticRollups.UnmatchedRows, 1; got != want {
		t.Fatalf("unexpected unmatched row count: got=%d want=%d", got, want)
	}
	if got, want := unmatchedRun.FailedTestCount, 1; got != want {
		t.Fatalf("unexpected unmatched run failed test count: got=%d want=%d", got, want)
	}
	if got, want := unmatchedRun.FailureRows[0].SemanticAttachment.Status, "unmatched"; got != want {
		t.Fatalf("unexpected unmatched row status: got=%q want=%q", got, want)
	}
}

func TestBuildJobHistoryDayHandlesMultipleSignaturesOnOneRun(t *testing.T) {
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

	data, err := fixture.service.BuildJobHistoryDay(ctx, JobHistoryDayQuery{
		Date:         "2026-03-16",
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("build job history day: %v", err)
	}

	run := jobHistoryRunByURL(t, jobHistoryEnvironmentByName(t, data, "dev"), "https://prow.example.com/view/1")
	if got, want := run.SemanticRollups.AttachmentSummary, "multiple_clustered"; got != want {
		t.Fatalf("unexpected attachment summary: got=%q want=%q", got, want)
	}
	if got, want := run.SemanticRollups.SignatureCount, 2; got != want {
		t.Fatalf("unexpected signature count: got=%d want=%d", got, want)
	}
	if got, want := run.FailedTestCount, 2; got != want {
		t.Fatalf("unexpected failed test count: got=%d want=%d", got, want)
	}
	if got, want := len(run.Lanes), 1; got != want {
		t.Fatalf("unexpected lane count: got=%d want=%d", got, want)
	}
	if got, want := run.Lanes[0], "upgrade"; got != want {
		t.Fatalf("unexpected lane value: got=%q want=%q", got, want)
	}
	if got, want := len(run.SemanticRollups.DistinctClusterIDs), 2; got != want {
		t.Fatalf("unexpected distinct cluster count: got=%d want=%d", got, want)
	}
	if got, want := run.SemanticRollups.ClusteredRows, 3; got != want {
		t.Fatalf("unexpected clustered row count: got=%d want=%d", got, want)
	}
}

func TestBuildJobHistoryDayFlagsFailedRunsWithoutRawRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	store := fixture.openWeekStore(t, "2026-03-15")

	if err := store.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	runs := append(sampleRunsFixture(), storecontracts.RunRecord{
		Environment: "dev",
		RunURL:      "https://prow.example.com/view/3",
		JobName:     "periodic-ci-missing-raw",
		Failed:      true,
		OccurredAt:  "2026-03-16T10:00:00Z",
	})
	if err := store.UpsertRuns(ctx, runs); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, sampleRawFailuresFixture()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	data, err := fixture.service.BuildJobHistoryDay(ctx, JobHistoryDayQuery{
		Date:         "2026-03-16",
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("build job history day: %v", err)
	}

	environment := jobHistoryEnvironmentByName(t, data, "dev")
	if got, want := environment.Summary.FailedRunsWithoutRawRows, 1; got != want {
		t.Fatalf("unexpected failed runs without raw rows: got=%d want=%d", got, want)
	}
	run := jobHistoryRunByURL(t, environment, "https://prow.example.com/view/3")
	if got, want := run.SemanticRollups.AttachmentSummary, "failed_without_raw_rows"; got != want {
		t.Fatalf("unexpected attachment summary: got=%q want=%q", got, want)
	}
	if len(run.FailureRows) != 0 {
		t.Fatalf("expected no failure rows for raw-gap run, got=%d", len(run.FailureRows))
	}
}

func TestBuildJobHistoryDayUsesWeeklySemanticBadPRScore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	store := fixture.openWeekStore(t, "2026-03-15")

	if err := store.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	runs := sampleRunsFixture()
	runs[0].PRNumber = 123
	runs[0].PRState = "open"
	if err := store.UpsertRuns(ctx, runs); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, sampleRawFailuresFixture()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	data, err := fixture.service.BuildJobHistoryDay(ctx, JobHistoryDayQuery{
		Date:         "2026-03-16",
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("build job history day: %v", err)
	}

	run := jobHistoryRunByURL(t, jobHistoryEnvironmentByName(t, data, "dev"), "https://prow.example.com/view/1")
	if got := run.BadPRScore; got != 0 {
		t.Fatalf("expected weekly bad PR score to suppress single-run false positive, got=%d", got)
	}
}

func jobHistoryMaterializedWeekWithExtraCluster() storecontracts.MaterializedWeek {
	week := currentMaterializedWeek()
	week.GlobalClusters = append(week.GlobalClusters, semanticcontracts.GlobalClusterRecord{
		SchemaVersion:                semanticcontracts.SchemaVersionV1,
		Environment:                  "dev",
		Phase2ClusterID:              "cluster-dev-c",
		CanonicalEvidencePhrase:      "API throttling",
		SearchQueryPhrase:            "API throttling",
		SearchQuerySourceRunURL:      "https://prow.example.com/view/1",
		SearchQuerySourceSignatureID: "sig-c",
		SupportCount:                 3,
		ContributingTestsCount:       1,
		ContributingTests: []semanticcontracts.ContributingTestRecord{
			{
				Lane:         "upgrade",
				JobName:      "periodic-ci",
				TestName:     "should throttle",
				SupportCount: 3,
			},
		},
		MemberPhase1ClusterIDs: []string{"phase1-sig-c"},
		MemberSignatureIDs:     []string{"sig-c"},
		References: []semanticcontracts.ReferenceRecord{
			{
				RowID:       "row-4",
				RunURL:      "https://prow.example.com/view/1",
				OccurredAt:  "2026-03-16T08:07:00Z",
				SignatureID: "sig-c",
			},
		},
	})
	return week
}

func jobHistoryEnvironmentByName(t *testing.T, data JobHistoryDayData, environment string) JobHistoryDayEnvironment {
	t.Helper()
	for _, row := range data.Environments {
		if strings.TrimSpace(row.Environment) == strings.TrimSpace(environment) {
			return row
		}
	}
	t.Fatalf("environment %q not found", environment)
	return JobHistoryDayEnvironment{}
}

func jobHistoryRunByURL(t *testing.T, environment JobHistoryDayEnvironment, runURL string) JobHistoryRunRow {
	t.Helper()
	for _, row := range environment.Runs {
		if strings.TrimSpace(row.Run.RunURL) == strings.TrimSpace(runURL) {
			return row
		}
	}
	t.Fatalf("run %q not found", runURL)
	return JobHistoryRunRow{}
}
