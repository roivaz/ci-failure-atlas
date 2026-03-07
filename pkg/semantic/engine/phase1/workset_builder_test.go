package phase1

import (
	"testing"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func TestBuildWorksetSkipsNonArtifactBackedRows(t *testing.T) {
	t.Parallel()

	workset := BuildWorkset(
		[]storecontracts.RawFailureRecord{
			{
				Environment:    "dev",
				RowID:          "row-artifact",
				RunURL:         "https://run-1",
				TestName:       "test-a",
				TestSuite:      "suite-a",
				SignatureID:    "sig-a",
				OccurredAt:     "2026-03-06T10:00:00Z",
				RawText:        "artifact-backed failure",
				NormalizedText: "artifact-backed failure",
			},
			{
				Environment:       "dev",
				RowID:             "row-synthetic",
				RunURL:            "https://run-1",
				TestName:          "unknown",
				TestSuite:         "unknown",
				SignatureID:       "sig-synthetic",
				OccurredAt:        "2026-03-06T10:10:00Z",
				RawText:           "synthetic failure",
				NormalizedText:    "synthetic failure",
				NonArtifactBacked: true,
			},
		},
		[]storecontracts.RunRecord{
			{
				Environment: "dev",
				RunURL:      "https://run-1",
				JobName:     "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
				OccurredAt:  "2026-03-06T10:00:00Z",
			},
		},
	)

	if len(workset) != 1 {
		t.Fatalf("unexpected workset size: got=%d want=1", len(workset))
	}
	if workset[0].RunURL != "https://run-1" || workset[0].SignatureID != "sig-a" {
		t.Fatalf("unexpected workset row: %+v", workset[0])
	}
}
