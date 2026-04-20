package controllers

import (
	"testing"

	"ci-failure-atlas/pkg/store/contracts"
)

func TestMergeRunRecordFromSippyPreservesExistingFieldsWhenIncomingMissing(t *testing.T) {
	t.Parallel()

	existing := contracts.RunRecord{
		Environment:    "dev",
		RunURL:         "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
		JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		PRNumber:       4313,
		PRState:        "open",
		PRSHA:          "prow-sha",
		FinalMergedSHA: "final-sha",
		MergedPR:       true,
		PostGoodCommit: true,
		Failed:         true,
		OccurredAt:     "2026-04-20T10:00:00Z",
	}
	candidate := contracts.RunRecord{
		Environment: "dev",
		RunURL:      existing.RunURL,
		Failed:      false,
	}

	got := mergeRunRecordFromSippy(existing, true, candidate)
	want := existing
	want.Failed = false

	if got != want {
		t.Fatalf("merged record mismatch:\n got=%+v\nwant=%+v", got, want)
	}
}

func TestMergeRunRecordFromSippyUsesValidIncomingFieldsAuthoritatively(t *testing.T) {
	t.Parallel()

	existing := contracts.RunRecord{
		Environment:    "dev",
		RunURL:         "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
		JobName:        "stale-job-name",
		PRNumber:       4313,
		PRState:        "open",
		PRSHA:          "old-sha",
		FinalMergedSHA: "merged-sha",
		MergedPR:       true,
		PostGoodCommit: false,
		Failed:         true,
		OccurredAt:     "2026-04-20T09:55:00Z",
	}
	candidate := contracts.RunRecord{
		Environment:    "dev",
		RunURL:         existing.RunURL,
		JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		PRNumber:       4313,
		PRState:        "",
		PRSHA:          "new-sha",
		FinalMergedSHA: "",
		MergedPR:       false,
		PostGoodCommit: false,
		Failed:         false,
		OccurredAt:     "2026-04-20T10:00:00Z",
	}

	got := mergeRunRecordFromSippy(existing, true, candidate)
	want := contracts.RunRecord{
		Environment:    "dev",
		RunURL:         existing.RunURL,
		JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		PRNumber:       4313,
		PRState:        "open",
		PRSHA:          "new-sha",
		FinalMergedSHA: "merged-sha",
		MergedPR:       true,
		PostGoodCommit: false,
		Failed:         false,
		OccurredAt:     "2026-04-20T10:00:00Z",
	}

	if got != want {
		t.Fatalf("merged record mismatch:\n got=%+v\nwant=%+v", got, want)
	}
}
