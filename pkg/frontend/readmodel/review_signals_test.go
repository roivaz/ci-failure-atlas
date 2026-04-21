package readmodel

import (
	"context"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func TestBuildReviewSignalsWeekMatchesCurrentFailurePatterns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	store := fixture.openWeekStore(t, "2026-03-16")
	if err := store.ReplaceMaterializedWeek(ctx, reviewSignalsMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}

	data, err := fixture.service.BuildReviewSignalsWeek(ctx, "2026-03-16")
	if err != nil {
		t.Fatalf("build review signals week: %v", err)
	}
	if got, want := data.Week, "2026-03-16"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := data.Timezone, "UTC"; got != want {
		t.Fatalf("unexpected timezone: got=%q want=%q", got, want)
	}
	if got, want := data.SignalsByReason["low_confidence_evidence"], 1; got != want {
		t.Fatalf("unexpected low-confidence signal count: got=%d want=%d", got, want)
	}
	if got, want := data.SignalsByReason["ambiguous_provider_merge"], 1; got != want {
		t.Fatalf("unexpected ambiguous-provider signal count: got=%d want=%d", got, want)
	}
	if got := data.SignalsByReason["new_pattern"]; got != 2 {
		t.Fatalf("expected 2 new_pattern signals for first-seen patterns, got=%d", got)
	}
	expectedTotal := 4
	if got := data.TotalSignals; got != expectedTotal {
		t.Fatalf("unexpected total signal count: got=%d want=%d", got, expectedTotal)
	}

	rowsByReason := map[string]ReviewSignalRow{}
	for _, row := range data.Rows {
		rowsByReason[row.Reason] = row
	}

	lowConfidence, ok := rowsByReason["low_confidence_evidence"]
	if !ok {
		t.Fatalf("missing low_confidence_evidence row: %+v", data.Rows)
	}
	if got, want := len(lowConfidence.MatchedFailurePatterns), 1; got != want {
		t.Fatalf("unexpected low-confidence matched failure-pattern count: got=%d want=%d", got, want)
	}
	if got, want := lowConfidence.MatchedFailurePatterns[0].FailurePatternID, "cluster-dev-linked"; got != want {
		t.Fatalf("unexpected low-confidence matched failure-pattern id: got=%q want=%q", got, want)
	}

	ambiguousProvider, ok := rowsByReason["ambiguous_provider_merge"]
	if !ok {
		t.Fatalf("missing ambiguous_provider_merge row: %+v", data.Rows)
	}
	if got, want := len(ambiguousProvider.MatchedFailurePatterns), 1; got != want {
		t.Fatalf("unexpected ambiguous-provider matched failure-pattern count: got=%d want=%d", got, want)
	}
	if got, want := ambiguousProvider.MatchedFailurePatterns[0].FailurePatternID, "cluster-dev-unlinked"; got != want {
		t.Fatalf("unexpected ambiguous-provider matched failure-pattern id: got=%q want=%q", got, want)
	}
}

func reviewSignalsMaterializedWeek() storecontracts.MaterializedWeek {
	return storecontracts.MaterializedWeek{
		FailurePatterns: []semanticcontracts.FailurePatternRecord{
			{
				SchemaVersion:                semanticcontracts.CurrentSchemaVersion,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-linked",
				CanonicalEvidencePhrase:      "OAuth timeout",
				SearchQueryPhrase:            "OAuth timeout",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/1",
				SearchQuerySourceSignatureID: "sig-linked",
				SupportCount:                 2,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "upgrade",
						JobName:      "periodic-ci",
						TestName:     "should oauth",
						SupportCount: 2,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-linked"},
				MemberSignatureIDs:     []string{"sig-linked"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-1",
						RunURL:      "https://prow.example.com/view/1",
						OccurredAt:  "2026-03-16T08:00:00Z",
						SignatureID: "sig-linked",
					},
				},
			},
			{
				SchemaVersion:                semanticcontracts.CurrentSchemaVersion,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-unlinked",
				CanonicalEvidencePhrase:      "CreateNodePool timeout 45 min",
				SearchQueryPhrase:            "CreateNodePool timeout 45 min",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/2",
				SearchQuerySourceSignatureID: "sig-unlinked",
				SupportCount:                 1,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "install",
						JobName:      "periodic-ci-nodepool",
						TestName:     "should create nodepool",
						SupportCount: 1,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-unlinked"},
				MemberSignatureIDs:     []string{"sig-unlinked"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-2",
						RunURL:      "https://prow.example.com/view/2",
						OccurredAt:  "2026-03-16T09:00:00Z",
						SignatureID: "sig-unlinked",
					},
				},
			},
		},
		ReviewQueue: []semanticcontracts.ReviewItemRecord{
			{
				SchemaVersion:                        semanticcontracts.CurrentSchemaVersion,
				Environment:                          "dev",
				ReviewItemID:                         "review-low-confidence",
				Phase:                                "phase1",
				Reason:                               "low_confidence_evidence",
				ProposedCanonicalEvidencePhrase:      "OAuth timeout",
				ProposedSearchQueryPhrase:            "OAuth timeout",
				ProposedSearchQuerySourceRunURL:      "https://prow.example.com/view/1",
				ProposedSearchQuerySourceSignatureID: "sig-linked",
				SourcePhase1ClusterIDs:               []string{"phase1-linked"},
				MemberSignatureIDs:                   []string{"sig-linked"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-1",
						RunURL:      "https://prow.example.com/view/1",
						OccurredAt:  "2026-03-16T08:00:00Z",
						SignatureID: "sig-linked",
					},
				},
			},
			{
				SchemaVersion:                        semanticcontracts.CurrentSchemaVersion,
				Environment:                          "dev",
				ReviewItemID:                         "review-ambiguous-provider",
				Phase:                                "phase2",
				Reason:                               "ambiguous_provider_merge",
				ProposedCanonicalEvidencePhrase:      "CreateNodePool timeout 45 min",
				ProposedSearchQueryPhrase:            "CreateNodePool timeout 45 min",
				ProposedSearchQuerySourceRunURL:      "https://prow.example.com/view/2",
				ProposedSearchQuerySourceSignatureID: "sig-unlinked",
				SourcePhase1ClusterIDs:               []string{"phase1-unlinked"},
				MemberSignatureIDs:                   []string{"sig-unlinked"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-2",
						RunURL:      "https://prow.example.com/view/2",
						OccurredAt:  "2026-03-16T09:00:00Z",
						SignatureID: "sig-unlinked",
					},
				},
			},
		},
	}
}
