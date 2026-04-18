package phase3

import (
	"strings"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

func TestMergeGroupsLinkedClustersAndPreservesUnlinked(t *testing.T) {
	t.Parallel()

	merged, err := Merge(
		[]semanticcontracts.FailurePatternRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "phase2-dev-a",
				CanonicalEvidencePhrase: "context deadline exceeded",
				SearchQueryPhrase:       "context deadline exceeded",
				SupportCount:            2,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{Lane: "e2e", JobName: "job-a", TestName: "test-a", SupportCount: 2},
				},
				ContributingTestsCount: 1,
				MemberPhase1ClusterIDs: []string{"phase1-a"},
				MemberSignatureIDs:     []string{"sig-a"},
				References: []semanticcontracts.ReferenceRecord{
					{RowID: "row-a", RunURL: "https://prow.example/dev/run-a", SignatureID: "sig-a", OccurredAt: "2026-03-15T10:00:00Z"},
				},
			},
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "phase2-dev-b",
				CanonicalEvidencePhrase: "context deadline exceeded",
				SearchQueryPhrase:       "context deadline exceeded",
				SupportCount:            3,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{Lane: "e2e", JobName: "job-b", TestName: "test-b", SupportCount: 3},
				},
				ContributingTestsCount: 1,
				MemberPhase1ClusterIDs: []string{"phase1-b"},
				MemberSignatureIDs:     []string{"sig-b"},
				References: []semanticcontracts.ReferenceRecord{
					{RowID: "row-b", RunURL: "https://prow.example/dev/run-b", SignatureID: "sig-b", OccurredAt: "2026-03-15T11:00:00Z"},
				},
			},
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "phase2-dev-unlinked",
				CanonicalEvidencePhrase: "image mirror failure",
				SearchQueryPhrase:       "image mirror failure",
				SupportCount:            5,
				ContributingTestsCount:  0,
				MemberPhase1ClusterIDs:  []string{"phase1-u"},
				MemberSignatureIDs:      []string{"sig-u"},
				References: []semanticcontracts.ReferenceRecord{
					{RowID: "row-u", RunURL: "https://prow.example/dev/run-u", SignatureID: "sig-u", OccurredAt: "2026-03-15T12:00:00Z"},
				},
			},
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "int",
				Phase2ClusterID:         "phase2-int-a",
				CanonicalEvidencePhrase: "context deadline exceeded",
				SearchQueryPhrase:       "context deadline exceeded",
				SupportCount:            4,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{Lane: "e2e", JobName: "job-int", TestName: "test-int", SupportCount: 4},
				},
				ContributingTestsCount: 1,
				MemberPhase1ClusterIDs: []string{"phase1-int"},
				MemberSignatureIDs:     []string{"sig-int"},
				References: []semanticcontracts.ReferenceRecord{
					{RowID: "row-int", RunURL: "https://prow.example/int/run-a", SignatureID: "sig-int", OccurredAt: "2026-03-15T13:00:00Z"},
				},
			},
		},
		[]semanticcontracts.Phase3LinkRecord{
			{IssueID: "p3c-shared", Environment: "dev", RunURL: "https://prow.example/dev/run-a", RowID: "row-a"},
			{IssueID: "p3c-shared", Environment: "dev", RunURL: "https://prow.example/dev/run-b", RowID: "row-b"},
			{IssueID: "p3c-shared", Environment: "int", RunURL: "https://prow.example/int/run-a", RowID: "row-int"},
		},
	)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	if len(merged) != 3 {
		t.Fatalf("expected 3 merged rows, got=%d rows=%+v", len(merged), merged)
	}

	byKey := map[string]semanticcontracts.FailurePatternRecord{}
	for _, row := range merged {
		byKey[row.Environment+"|"+row.Phase2ClusterID] = row
	}

	devLinked, ok := byKey["dev|p3c-shared"]
	if !ok {
		t.Fatalf("expected dev linked cluster p3c-shared, rows=%+v", merged)
	}
	if devLinked.SupportCount != 5 {
		t.Fatalf("expected merged dev support=5, got=%d", devLinked.SupportCount)
	}
	if len(devLinked.References) != 2 {
		t.Fatalf("expected merged dev references=2, got=%d refs=%+v", len(devLinked.References), devLinked.References)
	}
	if len(devLinked.MemberSignatureIDs) != 2 {
		t.Fatalf("expected merged dev signatures=2, got=%d signatures=%+v", len(devLinked.MemberSignatureIDs), devLinked.MemberSignatureIDs)
	}

	if _, ok := byKey["dev|phase2-dev-unlinked"]; !ok {
		t.Fatalf("expected unlinked dev cluster to remain standalone, rows=%+v", merged)
	}
	intLinked, ok := byKey["int|p3c-shared"]
	if !ok {
		t.Fatalf("expected int linked cluster with same phase3 id but separate environment, rows=%+v", merged)
	}
	if intLinked.SupportCount != 4 {
		t.Fatalf("expected int linked support=4, got=%d", intLinked.SupportCount)
	}
}

func TestMergeFailsWhenClusterResolvesToMultiplePhase3Clusters(t *testing.T) {
	t.Parallel()

	_, err := Merge(
		[]semanticcontracts.FailurePatternRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "phase2-dev-conflict",
				CanonicalEvidencePhrase: "conflict phrase",
				SearchQueryPhrase:       "conflict phrase",
				SupportCount:            2,
				References: []semanticcontracts.ReferenceRecord{
					{RowID: "row-1", RunURL: "https://prow.example/dev/conflict", SignatureID: "sig-1"},
					{RowID: "row-2", RunURL: "https://prow.example/dev/conflict", SignatureID: "sig-2"},
				},
			},
		},
		[]semanticcontracts.Phase3LinkRecord{
			{IssueID: "p3c-one", Environment: "dev", RunURL: "https://prow.example/dev/conflict", RowID: "row-1"},
			{IssueID: "p3c-two", Environment: "dev", RunURL: "https://prow.example/dev/conflict", RowID: "row-2"},
		},
	)
	if err == nil {
		t.Fatalf("expected conflict error when a semantic cluster maps to multiple phase3 clusters")
	}
	if !strings.Contains(err.Error(), "resolves to multiple phase3 cluster IDs") {
		t.Fatalf("expected conflict error message, got=%v", err)
	}
}
