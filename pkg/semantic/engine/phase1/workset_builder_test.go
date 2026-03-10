package phase1

import (
	"testing"

	semanticinput "ci-failure-atlas/pkg/semantic/input"
)

func TestBuildWorksetSkipsRowsMissingRequiredIdentityFields(t *testing.T) {
	t.Parallel()

	workset := BuildWorkset(
		[]semanticinput.EnrichedFailure{
			{
				Environment:    "dev",
				RowID:          "",
				RunURL:         "https://run-1",
				JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
				Lane:           "e2e",
				PRNumber:       4366,
				PostGoodCommit: true,
				TestName:       "test-missing-row-id",
				TestSuite:      "rp-api-compat-all/parallel",
				SignatureID:    "sig-missing-row-id",
				OccurredAt:     "2026-03-06T10:00:00Z",
				RawText:        "failure",
				NormalizedText: "failure",
			},
			{
				Environment:    "dev",
				RowID:          "row-artifact",
				RunURL:         "https://run-1",
				JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
				Lane:           "e2e",
				PRNumber:       4366,
				PostGoodCommit: true,
				TestName:       "test-a",
				TestSuite:      "suite-a",
				SignatureID:    "sig-a",
				OccurredAt:     "2026-03-06T10:00:00Z",
				RawText:        "artifact-backed failure",
				NormalizedText: "artifact-backed failure",
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

func TestBuildWorksetUsesProvidedEnrichedFieldsWithoutInference(t *testing.T) {
	t.Parallel()

	workset := BuildWorkset(
		[]semanticinput.EnrichedFailure{
			{
				Environment:    "dev",
				RowID:          "row-1",
				RunURL:         "https://prow.example/run/1",
				JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
				Lane:           "e2e",
				PRNumber:       4366,
				PostGoodCommit: true,
				TestName:       "Customer should be able to create an HCP cluster",
				TestSuite:      "rp-api-compat-all/parallel",
				SignatureID:    "sig-1",
				OccurredAt:     "2026-03-09T11:52:59Z",
				RawText:        "failure happened",
				NormalizedText: "failure happened",
			},
		},
	)

	if len(workset) != 1 {
		t.Fatalf("unexpected workset size: got=%d want=1", len(workset))
	}
	row := workset[0]
	if row.JobName != "pull-ci-Azure-ARO-HCP-main-e2e-parallel" {
		t.Fatalf("expected job name to match enriched row, got=%q", row.JobName)
	}
	if row.PRNumber != 4366 {
		t.Fatalf("expected PR number to match enriched row, got=%d", row.PRNumber)
	}
	if row.OccurredAt != "2026-03-09T11:52:59Z" {
		t.Fatalf("expected occurred_at to match enriched row, got=%q", row.OccurredAt)
	}
	if row.Lane != "e2e" {
		t.Fatalf("expected lane to match enriched row, got=%q", row.Lane)
	}
}
