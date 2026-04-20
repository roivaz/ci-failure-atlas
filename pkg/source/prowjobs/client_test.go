package prowjobs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
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
