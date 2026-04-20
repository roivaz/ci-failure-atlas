package prowjobs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClientListJobsDecodesWrappedJSResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prowjobs.js" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/javascript")
		_, _ = w.Write([]byte(`var allBuilds = {
  "items": [
    {
      "spec": {
        "job": "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
        "refs": {
          "pulls": [
            {
              "number": 4313,
              "sha": "abc123"
            }
          ]
        }
      },
      "status": {
        "state": "failure",
        "url": "gs://test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
        "build_id": "2029578186907455488",
        "startTime": "2026-04-20T10:00:00Z",
        "completionTime": "2026-04-20T10:45:00Z"
      }
    }
  ]
};`))
	}))
	t.Cleanup(server.Close)

	client := NewHTTPClient(server.URL)
	jobs, err := client.ListJobs(context.Background())
	if err != nil {
		t.Fatalf("ListJobs returned error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("unexpected job count: got=%d want=1", len(jobs))
	}
	if jobs[0].Spec.Job != "pull-ci-Azure-ARO-HCP-main-e2e-parallel" {
		t.Fatalf("unexpected job name: %+v", jobs[0])
	}
	if jobs[0].Status.State != "failure" {
		t.Fatalf("unexpected job state: %+v", jobs[0].Status)
	}
	if jobs[0].Spec.Refs == nil || len(jobs[0].Spec.Refs.Pulls) != 1 || jobs[0].Spec.Refs.Pulls[0].Number != 4313 {
		t.Fatalf("unexpected pull refs: %+v", jobs[0].Spec.Refs)
	}
}

func TestStateHelpers(t *testing.T) {
	t.Parallel()

	if !IsTerminalState("success") || !IsTerminalState("failure") || !IsTerminalState("error") || !IsTerminalState("aborted") {
		t.Fatalf("expected terminal states to be recognized")
	}
	if IsTerminalState("pending") {
		t.Fatalf("expected pending to be non-terminal")
	}
	if FailedFromState("success") {
		t.Fatalf("expected success to map to failed=false")
	}
	if !FailedFromState("error") || !FailedFromState("failure") || !FailedFromState("aborted") {
		t.Fatalf("expected non-success terminal states to map to failed=true")
	}
}

func TestHTTPClientGetJobHistoryPageParsesBuildsAndOlderLink(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/job-history/gs/test-platform-results/logs/periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!DOCTYPE html>
<html>
  <body>
    <script>
      var allBuilds = [
        {
          "ID": "2029578186907455499",
          "SpyglassLink": "/view/gs/test-platform-results/logs/periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel/2029578186907455499",
          "Started": "2026-04-20T10:00:00Z",
          "Duration": 2700000000000,
          "Result": "SUCCESS"
        },
        {
          "ID": "2029578186907455500",
          "SpyglassLink": "/view/gs/test-platform-results/logs/periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel/2029578186907455500",
          "Started": "2026-04-20T11:00:00Z",
          "Duration": 300000000000,
          "Result": "FAILURE",
          "Refs": {
            "pulls": [
              {
                "number": 4313,
                "sha": "abc123"
              }
            ]
          }
        }
      ];
    </script>
    <a href="/job-history/gs/test-platform-results/logs/periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel?buildId=2029578186907455499">&lt;- Older Runs</a>
  </body>
</html>`))
	}))
	t.Cleanup(server.Close)

	client := NewHTTPClient(server.URL)
	page, err := client.GetJobHistoryPage(context.Background(), "logs/periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel")
	if err != nil {
		t.Fatalf("GetJobHistoryPage returned error: %v", err)
	}
	if len(page.Builds) != 2 {
		t.Fatalf("unexpected build count: got=%d want=2", len(page.Builds))
	}
	if page.OlderLink != server.URL+"/job-history/gs/test-platform-results/logs/periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel?buildId=2029578186907455499" {
		t.Fatalf("unexpected older link: got=%q", page.OlderLink)
	}

	jobs := page.AsJobs(server.URL, "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel")
	if len(jobs) != 2 {
		t.Fatalf("unexpected job count after conversion: got=%d want=2", len(jobs))
	}
	if jobs[0].Status.URL != server.URL+"/view/gs/test-platform-results/logs/periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel/2029578186907455499" {
		t.Fatalf("unexpected resolved run URL: got=%q", jobs[0].Status.URL)
	}
	if jobs[0].Status.State != "success" {
		t.Fatalf("unexpected state: got=%q want=%q", jobs[0].Status.State, "success")
	}
	if jobs[0].Status.CompletionTime != mustParseRFC3339(t, "2026-04-20T10:45:00Z") {
		t.Fatalf("unexpected completion time: got=%s", jobs[0].Status.CompletionTime.Format(time.RFC3339))
	}
	if jobs[1].Spec.Refs == nil || len(jobs[1].Spec.Refs.Pulls) != 1 || jobs[1].Spec.Refs.Pulls[0].Number != 4313 {
		t.Fatalf("unexpected pull refs: %+v", jobs[1].Spec.Refs)
	}
	if jobs[1].Status.State != "failure" {
		t.Fatalf("unexpected second job state: got=%q want=%q", jobs[1].Status.State, "failure")
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
