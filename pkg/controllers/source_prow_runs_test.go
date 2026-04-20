package controllers

import (
	"testing"
	"time"

	"ci-failure-atlas/pkg/source/prowjobs"
	"ci-failure-atlas/pkg/store/contracts"
)

func TestMapProwJobToRunRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		environment string
		job         prowjobs.Job
		want        contracts.RunRecord
	}{
		{
			name:        "dev presubmit failure",
			environment: "dev",
			job: prowjobs.Job{
				Spec: prowjobs.JobSpec{
					Job: "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
					Refs: &prowjobs.Refs{
						Pulls: []prowjobs.Pull{
							{Number: 4313, SHA: "abc123"},
						},
					},
				},
				Status: prowjobs.JobStatus{
					State: "failure",
					URL:   "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
				},
			},
			want: contracts.RunRecord{
				Environment:    "dev",
				RunURL:         "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
				JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
				PRNumber:       4313,
				PRSHA:          "abc123",
				MergedPR:       false,
				PostGoodCommit: false,
				Failed:         true,
			},
		},
		{
			name:        "periodic success",
			environment: "int",
			job: prowjobs.Job{
				Spec: prowjobs.JobSpec{
					Job: "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel",
				},
				Status: prowjobs.JobStatus{
					State: "success",
					URL:   "gs://test-platform-results/logs/periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel/2029578186907455499",
				},
			},
			want: contracts.RunRecord{
				Environment:    "int",
				RunURL:         "https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel/2029578186907455499",
				JobName:        "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel",
				MergedPR:       true,
				PostGoodCommit: true,
				Failed:         false,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			startedAt := "2026-04-20T10:00:00Z"
			completedAt := "2026-04-20T10:45:00Z"
			tt.job.Status.StartTime = mustParseRFC3339(t, startedAt)
			tt.job.Status.CompletionTime = mustParseRFC3339(t, completedAt)
			tt.want.OccurredAt = startedAt

			got, ok := mapProwJobToRunRecord("https://prow.ci.openshift.org", tt.environment, tt.job)
			if !ok {
				t.Fatalf("expected job to map successfully")
			}
			if got != tt.want {
				t.Fatalf("mapped record mismatch:\n got=%+v\nwant=%+v", got, tt.want)
			}
		})
	}
}

func mustParseRFC3339(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse RFC3339 %q: %v", value, err)
	}
	return parsed.UTC()
}
