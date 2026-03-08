package prowartifacts

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestHTTPClientListFailuresUsesDeterministicPaths(t *testing.T) {
	t.Parallel()

	requestedPaths := make([]string, 0, 8)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		requestedPaths = append(requestedPaths, r.URL.Path)

		switch r.URL.Path {
		case "/gcs/test-bucket/job/12345/artifacts/e2e-parallel/aro-hcp-provision-environment/artifacts/junit_entrypoint.xml":
			_, _ = w.Write([]byte(`<testsuite name="entrypoint">
<testcase classname="entry.suite" name="entry-test">
	<failure message="entrypoint failed">infra step failed</failure>
</testcase>
</testsuite>`))
			return
		case "/gcs/test-bucket/job/12345/prowjob_junit.xml":
			_, _ = w.Write([]byte(`<testsuite name="prowjob">
<testcase name="prowjob-wrapper">
	<failure message="job failed">container exited</failure>
</testcase>
</testsuite>`))
			return
		case "/gcs/test-bucket/job/12345/artifacts/e2e-parallel/aro-hcp-test-local/artifacts/junit.xml":
			http.NotFound(w, r)
			return
		default:
			http.NotFound(w, r)
			return
		}
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := NewHTTPClient(server.URL + "/gcs")
	failures, err := client.ListFailures(context.Background(), "dev", "https://prow.ci.openshift.org/view/gs/test-bucket/job/12345")
	if err != nil {
		t.Fatalf("list failures: %v", err)
	}

	if len(failures) != 2 {
		t.Fatalf("unexpected failure count: got=%d want=2", len(failures))
	}
	gotNames := []string{failures[0].TestName, failures[1].TestName}
	wantNames := []string{"entry-test", "prowjob-wrapper"}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("test names mismatch: got=%v want=%v", gotNames, wantNames)
	}

	wantRequested := []string{
		"/gcs/test-bucket/job/12345/artifacts/e2e-parallel/aro-hcp-provision-environment/artifacts/junit_entrypoint.xml",
		"/gcs/test-bucket/job/12345/prowjob_junit.xml",
		"/gcs/test-bucket/job/12345/artifacts/e2e-parallel/aro-hcp-test-local/artifacts/junit.xml",
	}
	if !reflect.DeepEqual(requestedPaths, wantRequested) {
		t.Fatalf("requested paths mismatch: got=%v want=%v", requestedPaths, wantRequested)
	}
}

func TestHTTPClientListFailuresUsesEnvironmentMap(t *testing.T) {
	t.Parallel()

	requestedPaths := make([]string, 0, 4)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		requestedPaths = append(requestedPaths, r.URL.Path)
		switch r.URL.Path {
		case "/gcs/test-bucket/job/12345/artifacts/integration-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml":
			_, _ = w.Write([]byte(`<testsuite name="integration-e2e">
<testcase classname="persistent.tests" name="test-persistent-int">
	<failure message="persistent failure">db bootstrap failed</failure>
</testcase>
</testsuite>`))
			return
		case "/gcs/test-bucket/job/12345/prowjob_junit.xml":
			_, _ = w.Write([]byte(`<testsuite name="prowjob">
<testcase name="job-wrapper"><failure message="failed">boom</failure></testcase>
</testsuite>`))
			return
		default:
			http.NotFound(w, r)
			return
		}
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := NewHTTPClient(server.URL + "/gcs")
	failures, err := client.ListFailures(context.Background(), "int", "https://prow.ci.openshift.org/view/gs/test-bucket/job/12345")
	if err != nil {
		t.Fatalf("list failures: %v", err)
	}
	if len(failures) != 2 {
		t.Fatalf("unexpected failure count: got=%d want=2", len(failures))
	}

	wantRequested := []string{
		"/gcs/test-bucket/job/12345/artifacts/integration-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
		"/gcs/test-bucket/job/12345/prowjob_junit.xml",
	}
	if !reflect.DeepEqual(requestedPaths, wantRequested) {
		t.Fatalf("requested paths mismatch: got=%v want=%v", requestedPaths, wantRequested)
	}
}

func TestHTTPClientJunitPathsForEnvironment(t *testing.T) {
	t.Parallel()

	client := NewHTTPClient("https://example.com/gcs")

	tests := []struct {
		environment string
		wantFirst   string
	}{
		{
			environment: "prod",
			wantFirst:   "artifacts/prod-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
		},
		{
			environment: "stg",
			wantFirst:   "artifacts/stage-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
		},
		{
			environment: "int",
			wantFirst:   "artifacts/integration-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.environment, func(t *testing.T) {
			t.Parallel()
			paths := client.junitPathsForEnvironment(tt.environment)
			if len(paths) == 0 {
				t.Fatalf("expected deterministic paths for env=%q", tt.environment)
			}
			if paths[0] != tt.wantFirst {
				t.Fatalf("unexpected first path for env=%q: got=%q want=%q", tt.environment, paths[0], tt.wantFirst)
			}
		})
	}
}

func TestHTTPClientJunitPathsForPeriodicEnvironmentsExcludeProvisionArtifacts(t *testing.T) {
	t.Parallel()

	client := NewHTTPClient("https://example.com/gcs")
	for _, environment := range []string{"int", "stg", "prod"} {
		paths := client.junitPathsForEnvironment(environment)
		if len(paths) == 0 {
			t.Fatalf("expected deterministic paths for env=%q", environment)
		}
		for _, p := range paths {
			if strings.Contains(strings.ToLower(p), "provision") {
				t.Fatalf("unexpected provision artifact path for env=%q: %q", environment, p)
			}
		}
	}
}

func TestArtifactPrefixFromRunURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		runURL string
		want   string
	}{
		{
			name:   "prow URL",
			runURL: "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4062/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
			want:   "test-platform-results/pr-logs/pull/Azure_ARO-HCP/4062/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
		},
		{
			name:   "gcsweb URL",
			runURL: "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4062/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
			want:   "test-platform-results/pr-logs/pull/Azure_ARO-HCP/4062/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
		},
		{
			name:   "gs URL",
			runURL: "gs://test-platform-results/pr-logs/pull/Azure_ARO-HCP/4062/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
			want:   "test-platform-results/pr-logs/pull/Azure_ARO-HCP/4062/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := artifactPrefixFromRunURL(tt.runURL)
			if err != nil {
				t.Fatalf("artifactPrefixFromRunURL(%q): %v", tt.runURL, err)
			}
			if got != tt.want {
				t.Fatalf("artifactPrefixFromRunURL(%q) mismatch: got=%q want=%q", tt.runURL, got, tt.want)
			}
		})
	}
}
