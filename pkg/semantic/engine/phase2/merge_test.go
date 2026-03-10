package phase2

import (
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

func TestMergeBuildsDeterministicGlobalCluster(t *testing.T) {
	t.Parallel()

	testClusters := []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:                semanticcontracts.SchemaVersionV1,
			Phase1ClusterID:              "phase1-b",
			Lane:                         "e2e",
			JobName:                      "job-a",
			TestName:                     "test-b",
			CanonicalEvidencePhrase:      "failed waiting for cluster operators",
			SearchQueryPhrase:            "cluster operators not available",
			SearchQuerySourceRunURL:      "https://prow.example/run/2",
			SearchQuerySourceSignatureID: "sig-2",
			SupportCount:                 3,
			PostGoodCommitCount:          1,
			MemberSignatureIDs:           []string{"sig-3", "sig-2"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/2",
					OccurredAt:  "2026-03-05T11:00:00Z",
					SignatureID: "sig-2",
				},
			},
		},
		{
			SchemaVersion:                semanticcontracts.SchemaVersionV1,
			Phase1ClusterID:              "phase1-a",
			Lane:                         "e2e",
			JobName:                      "job-a",
			TestName:                     "test-a",
			CanonicalEvidencePhrase:      "failed waiting for cluster operators",
			SearchQueryPhrase:            "cluster operators not available",
			SearchQuerySourceRunURL:      "https://prow.example/run/1",
			SearchQuerySourceSignatureID: "sig-1",
			SupportCount:                 2,
			MemberSignatureIDs:           []string{"sig-1"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/1",
					OccurredAt:  "2026-03-05T10:00:00Z",
					SignatureID: "sig-1",
				},
			},
		},
	}

	globalClusters, reviewItems, err := Merge(testClusters, nil)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(globalClusters) != 1 {
		t.Fatalf("unexpected global cluster count: got=%d want=1", len(globalClusters))
	}
	if len(reviewItems) != 0 {
		t.Fatalf("unexpected review item count: got=%d want=0", len(reviewItems))
	}

	cluster := globalClusters[0]
	expectedPhase2ID := fingerprint("phase1-a,phase1-b")
	if cluster.Phase2ClusterID != expectedPhase2ID {
		t.Fatalf("unexpected phase2 cluster id: got=%q want=%q", cluster.Phase2ClusterID, expectedPhase2ID)
	}
	if cluster.SupportCount != 5 {
		t.Fatalf("unexpected support count: got=%d want=5", cluster.SupportCount)
	}
	if cluster.PostGoodCommitCount != 1 || !cluster.SeenPostGoodCommit {
		t.Fatalf("unexpected post-good counters: %+v", cluster)
	}
	if cluster.ContributingTestsCount != 2 || len(cluster.ContributingTests) != 2 {
		t.Fatalf("unexpected contributing tests content: %+v", cluster.ContributingTests)
	}
	if cluster.ContributingTests[0].TestName != "test-a" || cluster.ContributingTests[1].TestName != "test-b" {
		t.Fatalf("unexpected contributing tests ordering/content: %+v", cluster.ContributingTests)
	}
	if len(cluster.MemberSignatureIDs) != 3 || cluster.MemberSignatureIDs[0] != "sig-1" || cluster.MemberSignatureIDs[2] != "sig-3" {
		t.Fatalf("unexpected member signatures: %+v", cluster.MemberSignatureIDs)
	}
}

func TestMergeSplitsGenericCanonicalByProvider(t *testing.T) {
	t.Parallel()

	testClusters := []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Phase1ClusterID:         "phase1-a",
			CanonicalEvidencePhrase: "ERROR CODE: DeploymentFailed",
			SearchQueryPhrase:       "/providers/Microsoft.EventGrid/topics/test",
			SupportCount:            1,
			MemberSignatureIDs:      []string{"sig-a"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/1",
					OccurredAt:  "2026-03-05T10:00:00Z",
					SignatureID: "sig-a",
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Phase1ClusterID:         "phase1-b",
			CanonicalEvidencePhrase: "ERROR CODE: DeploymentFailed",
			SearchQueryPhrase:       "/providers/Microsoft.Monitor/actionGroups/test",
			SupportCount:            1,
			MemberSignatureIDs:      []string{"sig-b"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/2",
					OccurredAt:  "2026-03-05T11:00:00Z",
					SignatureID: "sig-b",
				},
			},
		},
	}

	globalClusters, _, err := Merge(testClusters, nil)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(globalClusters) != 2 {
		t.Fatalf("expected provider split into 2 global clusters, got=%d", len(globalClusters))
	}
}

func TestMergeFallsBackSearchPhraseWhenSourceInvalid(t *testing.T) {
	t.Parallel()

	testClusters := []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:                semanticcontracts.SchemaVersionV1,
			Phase1ClusterID:              "phase1-a",
			CanonicalEvidencePhrase:      "timeout while provisioning",
			SearchQueryPhrase:            "string that does not exist",
			SearchQuerySourceRunURL:      "https://prow.example/unknown",
			SearchQuerySourceSignatureID: "sig-unknown",
			SupportCount:                 1,
			MemberSignatureIDs:           []string{"sig-a"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/1",
					OccurredAt:  "2026-03-05T10:00:00Z",
					SignatureID: "sig-a",
				},
			},
		},
	}

	globalClusters, _, err := Merge(testClusters, nil)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(globalClusters) != 1 {
		t.Fatalf("unexpected global cluster count: got=%d want=1", len(globalClusters))
	}
	if globalClusters[0].SearchQueryPhrase != "timeout while provisioning" {
		t.Fatalf("unexpected fallback search phrase: got=%q", globalClusters[0].SearchQueryPhrase)
	}
	if globalClusters[0].SearchQuerySourceRunURL != "https://prow.example/run/1" || globalClusters[0].SearchQuerySourceSignatureID != "sig-a" {
		t.Fatalf("unexpected fallback search source: run=%q sig=%q", globalClusters[0].SearchQuerySourceRunURL, globalClusters[0].SearchQuerySourceSignatureID)
	}
}

func TestMergeAppendsPhase2ReviewItemsAndDeduplicates(t *testing.T) {
	t.Parallel()

	testClusters := []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Phase1ClusterID:         "phase1-a",
			CanonicalEvidencePhrase: "ERROR CODE: DeploymentFailed",
			SearchQueryPhrase:       "/providers/Microsoft.EventGrid/topics/test",
			SupportCount:            1,
			MemberSignatureIDs:      []string{"sig-a"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/1",
					OccurredAt:  "2026-03-05T10:00:00Z",
					SignatureID: "sig-a",
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Phase1ClusterID:         "phase1-b",
			CanonicalEvidencePhrase: "ERROR CODE: DeploymentFailed",
			SearchQueryPhrase:       "/providers/Microsoft.Monitor/actionGroups/test",
			SupportCount:            1,
			MemberSignatureIDs:      []string{"sig-b"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/2",
					OccurredAt:  "2026-03-05T11:00:00Z",
					SignatureID: "sig-b",
				},
			},
		},
	}
	inputReview := []semanticcontracts.ReviewItemRecord{
		{
			SchemaVersion:          semanticcontracts.SchemaVersionV1,
			ReviewItemID:           "will-be-recomputed",
			Phase:                  "phase1",
			Reason:                 "low_confidence_evidence",
			SourcePhase1ClusterIDs: []string{"phase1-a"},
			MemberSignatureIDs:     []string{"sig-a"},
		},
		{
			SchemaVersion:          semanticcontracts.SchemaVersionV1,
			ReviewItemID:           "duplicate-row-with-different-id",
			Phase:                  "phase1",
			Reason:                 "low_confidence_evidence",
			SourcePhase1ClusterIDs: []string{"phase1-a"},
			MemberSignatureIDs:     []string{"sig-a"},
		},
	}

	_, reviewItems, err := Merge(testClusters, inputReview)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(reviewItems) != 2 {
		t.Fatalf("unexpected review count after dedupe/append: got=%d want=2", len(reviewItems))
	}
	if reviewItems[0].Phase != "phase1" || reviewItems[1].Phase != "phase2" {
		t.Fatalf("unexpected review phase ordering/content: %+v", reviewItems)
	}
	if reviewItems[1].Reason != "ambiguous_provider_merge" {
		t.Fatalf("unexpected phase2 review reason: %+v", reviewItems[1])
	}
	if len(reviewItems[1].SourcePhase1ClusterIDs) != 2 {
		t.Fatalf("unexpected phase2 review source ids: %+v", reviewItems[1].SourcePhase1ClusterIDs)
	}
}
