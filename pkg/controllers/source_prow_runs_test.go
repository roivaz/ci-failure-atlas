package controllers

import (
	"context"
	"fmt"
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

func TestListCompletedJobsSincePagesUntilCutoff(t *testing.T) {
	t.Parallel()

	jobName := "pull-ci-Azure-ARO-HCP-main-e2e-parallel"
	since := mustParseRFC3339(t, "2026-04-20T09:00:00Z")
	client := &fakeProwJobHistoryClient{
		pages: map[string]prowjobs.JobHistoryPage{
			"pr-logs/directory/" + jobName: {
				Builds: []prowjobs.JobHistoryBuild{
					{
						ID:           "2029578186907455488",
						SpyglassLink: "/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
						Started:      mustParseRFC3339(t, "2026-04-20T12:00:00Z"),
						Duration:     15 * time.Minute,
						Result:       "FAILURE",
						Refs: &prowjobs.Refs{
							Pulls: []prowjobs.Pull{{Number: 4313, SHA: "abc123"}},
						},
					},
					{
						ID:           "2029578186907455487",
						SpyglassLink: "/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4312/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455487",
						Started:      mustParseRFC3339(t, "2026-04-20T10:00:00Z"),
						Duration:     20 * time.Minute,
						Result:       "SUCCESS",
						Refs: &prowjobs.Refs{
							Pulls: []prowjobs.Pull{{Number: 4312, SHA: "def456"}},
						},
					},
				},
				OlderLink: "page-2",
			},
			"page-2": {
				Builds: []prowjobs.JobHistoryBuild{
					{
						ID:           "2029578186907455486",
						SpyglassLink: "/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4311/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455486",
						Started:      mustParseRFC3339(t, "2026-04-20T09:03:00Z"),
						Duration:     4 * time.Minute,
						Result:       "ABORTED",
						Refs: &prowjobs.Refs{
							Pulls: []prowjobs.Pull{{Number: 4311, SHA: "ghi789"}},
						},
					},
					{
						ID:           "2029578186907455485",
						SpyglassLink: "/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4310/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455485",
						Started:      mustParseRFC3339(t, "2026-04-20T08:00:00Z"),
						Duration:     10 * time.Minute,
						Result:       "SUCCESS",
						Refs: &prowjobs.Refs{
							Pulls: []prowjobs.Pull{{Number: 4310, SHA: "jkl012"}},
						},
					},
				},
				OlderLink: "page-3",
			},
			"page-3": {
				Builds: []prowjobs.JobHistoryBuild{
					{
						ID:           "2029578186907455484",
						SpyglassLink: "/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4309/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455484",
						Started:      mustParseRFC3339(t, "2026-04-20T07:00:00Z"),
						Duration:     10 * time.Minute,
						Result:       "SUCCESS",
					},
				},
			},
		},
	}

	jobs, stats, err := listCompletedJobsSince(
		context.Background(),
		client,
		"https://prow.ci.openshift.org",
		jobName,
		"pr-logs/directory/"+jobName,
		since,
	)
	if err != nil {
		t.Fatalf("listCompletedJobsSince returned error: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("unexpected job count: got=%d want=3", len(jobs))
	}
	if stats.PagesFetched != 2 {
		t.Fatalf("unexpected page count: got=%d want=2", stats.PagesFetched)
	}
	if stats.FetchedBuilds != 4 {
		t.Fatalf("unexpected fetched build count: got=%d want=4", stats.FetchedBuilds)
	}
	if len(client.calls) != 2 || client.calls[0] != "pr-logs/directory/"+jobName || client.calls[1] != "page-2" {
		t.Fatalf("unexpected page calls: got=%v", client.calls)
	}
	if jobs[0].Status.URL != "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488" {
		t.Fatalf("unexpected resolved run URL: got=%q", jobs[0].Status.URL)
	}
	if jobs[2].Status.CompletionTime.Before(since) {
		t.Fatalf("expected all returned jobs to satisfy the cutoff: got completion=%s since=%s", jobs[2].Status.CompletionTime.Format(time.RFC3339), since.Format(time.RFC3339))
	}
}

func TestShouldStopPagingJobHistoryIgnoresPendingJobs(t *testing.T) {
	t.Parallel()

	since := mustParseRFC3339(t, "2026-04-20T09:00:00Z")
	jobs := []prowjobs.Job{
		{
			Spec: prowjobs.JobSpec{Job: "pull-ci-Azure-ARO-HCP-main-e2e-parallel"},
			Status: prowjobs.JobStatus{
				State:     "pending",
				StartTime: mustParseRFC3339(t, "2026-04-20T12:00:00Z"),
			},
		},
	}
	if shouldStopPagingJobHistory(jobs, since) {
		t.Fatalf("expected paging to continue when a page has only non-terminal jobs")
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

type fakeProwJobHistoryClient struct {
	pages map[string]prowjobs.JobHistoryPage
	calls []string
}

func (f *fakeProwJobHistoryClient) ListJobs(_ context.Context) ([]prowjobs.Job, error) {
	return nil, nil
}

func (f *fakeProwJobHistoryClient) GetJobHistoryPage(_ context.Context, historyPathOrURL string) (prowjobs.JobHistoryPage, error) {
	f.calls = append(f.calls, historyPathOrURL)
	page, ok := f.pages[historyPathOrURL]
	if !ok {
		return prowjobs.JobHistoryPage{}, fmt.Errorf("unexpected history page %q", historyPathOrURL)
	}
	return page, nil
}
