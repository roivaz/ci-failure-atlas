package phase2

import (
	"context"
	"testing"

	"github.com/go-logr/logr"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

func TestRunWorkflowPhase2WritesGlobalClustersAndMergedReview(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.NDJSONOptions.DataDirectory = t.TempDir()

	validated, err := opts.Validate()
	if err != nil {
		t.Fatalf("validate options: %v", err)
	}
	completed, err := validated.Complete(context.Background())
	if err != nil {
		t.Fatalf("complete options: %v", err)
	}

	ctx := logr.NewContext(context.Background(), logr.Discard())

	if err := completed.Store.UpsertTestClusters(ctx, []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase1ClusterID:         "phase1-a",
			Lane:                    "e2e",
			JobName:                 "job-a",
			TestName:                "test-a",
			CanonicalEvidencePhrase: "ERROR CODE: DeploymentFailed",
			SearchQueryPhrase:       "ERROR CODE: DeploymentFailed",
			SupportCount:            1,
			MemberSignatureIDs:      []string{"sig-a"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:         "https://prow.example/run/1",
					OccurredAt:     "2026-03-05T10:00:00Z",
					SignatureID:    "sig-a",
					RawTextExcerpt: "/providers/Microsoft.EventGrid/topics/test",
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase1ClusterID:         "phase1-b",
			Lane:                    "e2e",
			JobName:                 "job-a",
			TestName:                "test-b",
			CanonicalEvidencePhrase: "ERROR CODE: DeploymentFailed",
			SearchQueryPhrase:       "ERROR CODE: DeploymentFailed",
			SupportCount:            1,
			MemberSignatureIDs:      []string{"sig-b"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:         "https://prow.example/run/2",
					OccurredAt:     "2026-03-05T11:00:00Z",
					SignatureID:    "sig-b",
					RawTextExcerpt: "/providers/Microsoft.Monitor/actionGroups/test",
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed test clusters: %v", err)
	}

	if err := completed.Store.UpsertReviewQueue(ctx, []semanticcontracts.ReviewItemRecord{
		{
			SchemaVersion:          semanticcontracts.SchemaVersionV1,
			Environment:            "dev",
			ReviewItemID:           "0513ee4b07d75306505bf62bb23f7d479e261caa0b83084ea26b793d76a85352",
			Phase:                  "phase1",
			Reason:                 "low_confidence_evidence",
			SourcePhase1ClusterIDs: []string{"phase1-a"},
			MemberSignatureIDs:     []string{"sig-a"},
		},
	}); err != nil {
		t.Fatalf("seed review queue: %v", err)
	}

	if err := completed.Run(ctx); err != nil {
		t.Fatalf("run workflow phase2: %v", err)
	}

	globalRows, err := completed.Store.ListGlobalClusters(ctx)
	if err != nil {
		t.Fatalf("list global clusters: %v", err)
	}
	if len(globalRows) != 2 {
		t.Fatalf("unexpected global cluster size: got=%d want=2", len(globalRows))
	}

	reviewRows, err := completed.Store.ListReviewQueue(ctx)
	if err != nil {
		t.Fatalf("list review queue: %v", err)
	}
	if len(reviewRows) != 2 {
		t.Fatalf("unexpected review queue size: got=%d want=2", len(reviewRows))
	}

	foundPhase2Ambiguous := false
	for _, row := range reviewRows {
		if row.Phase == "phase2" && row.Reason == "ambiguous_provider_merge" {
			foundPhase2Ambiguous = true
			break
		}
	}
	if !foundPhase2Ambiguous {
		t.Fatalf("expected phase2 ambiguous provider review item, got=%+v", reviewRows)
	}
}
