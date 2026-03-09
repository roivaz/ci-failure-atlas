package sippy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestBuildJobRunsFilterWithPRAndJobFilters(t *testing.T) {
	t.Parallel()

	since := time.Unix(1700000000, 0).UTC()
	filterJSON, err := buildJobRunsFilter("Azure", "ARO-HCP", "pull-ci-Azure-ARO-HCP-main-e2e-parallel", since)
	if err != nil {
		t.Fatalf("buildJobRunsFilter returned error: %v", err)
	}

	var parsed filterModel
	if err := json.Unmarshal([]byte(filterJSON), &parsed); err != nil {
		t.Fatalf("decode filter JSON: %v", err)
	}

	if !hasFilterField(parsed.Items, "pull_request_org") {
		t.Fatalf("expected pull_request_org filter in %v", parsed.Items)
	}
	if !hasFilterField(parsed.Items, "pull_request_repo") {
		t.Fatalf("expected pull_request_repo filter in %v", parsed.Items)
	}
	if !hasFilterField(parsed.Items, "job") {
		t.Fatalf("expected job filter in %v", parsed.Items)
	}
	if !hasFilterField(parsed.Items, "timestamp") {
		t.Fatalf("expected timestamp filter in %v", parsed.Items)
	}
}

func TestBuildJobRunsFilterPeriodicWithoutPRFilters(t *testing.T) {
	t.Parallel()

	since := time.Unix(1700000000, 0).UTC()
	filterJSON, err := buildJobRunsFilter("", "", "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel", since)
	if err != nil {
		t.Fatalf("buildJobRunsFilter returned error: %v", err)
	}

	var parsed filterModel
	if err := json.Unmarshal([]byte(filterJSON), &parsed); err != nil {
		t.Fatalf("decode filter JSON: %v", err)
	}

	if hasFilterField(parsed.Items, "pull_request_org") || hasFilterField(parsed.Items, "pull_request_repo") {
		t.Fatalf("unexpected PR filters in periodic query: %v", parsed.Items)
	}
	if !hasFilterField(parsed.Items, "job") {
		t.Fatalf("expected job filter in %v", parsed.Items)
	}
	if !hasFilterField(parsed.Items, "timestamp") {
		t.Fatalf("expected timestamp filter in %v", parsed.Items)
	}
}

func TestBuildJobRunsFilterRejectsPartialPRFilter(t *testing.T) {
	t.Parallel()

	if _, err := buildJobRunsFilter("Azure", "", "some-job", time.Now().UTC()); err == nil {
		t.Fatalf("expected error when only org is set")
	}
	if _, err := buildJobRunsFilter("", "ARO-HCP", "some-job", time.Now().UTC()); err == nil {
		t.Fatalf("expected error when only repo is set")
	}
}

func hasFilterField(items []filterItem, field string) bool {
	for _, item := range items {
		if item.ColumnField == field {
			return true
		}
	}
	return false
}

func TestHTTPClientListTests(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tests" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("release"); got != "Presubmits" {
			t.Fatalf("expected release Presubmits, got %q", got)
		}
		if got := r.URL.Query().Get("period"); got != "default" {
			t.Fatalf("expected period default, got %q", got)
		}
		if got := r.URL.Query().Get("sortField"); got != "name" {
			t.Fatalf("expected sortField name, got %q", got)
		}
		if got := r.URL.Query().Get("sort"); got != "asc" {
			t.Fatalf("expected sort asc, got %q", got)
		}

		_ = json.NewEncoder(w).Encode([]map[string]any{
			{
				"name":                     "Run pipeline step Microsoft.Azure.ARO.HCP.Region/regional/infra",
				"suite_name":               "step graph",
				"current_pass_percentage":  83.47,
				"current_runs":             351,
				"previous_pass_percentage": 81.12,
				"previous_runs":            340,
				"net_improvement":          2.35,
			},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL)
	rows, err := client.ListTests(context.Background(), ListTestsOptions{
		Release: "Presubmits",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Name == "" || rows[0].CurrentRuns != 351 || rows[0].PreviousRuns != 340 {
		t.Fatalf("unexpected test summary row: %+v", rows[0])
	}
}

func TestHTTPClientListTestsIncludesOptionalParams(t *testing.T) {
	t.Parallel()

	var gotQuery url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL)
	_, err := client.ListTests(context.Background(), ListTestsOptions{
		Release:   "aro-integration",
		Period:    "twoDay",
		SortField: "current_pass_percentage",
		Sort:      "desc",
		Filter:    `{"items":[{"columnField":"name","operatorValue":"contains","value":"[sig-sippy]"}]}`,
		Limit:     25,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := gotQuery.Get("release"); got != "aro-integration" {
		t.Fatalf("unexpected release query param: %q", got)
	}
	if got := gotQuery.Get("period"); got != "twoDay" {
		t.Fatalf("unexpected period query param: %q", got)
	}
	if got := gotQuery.Get("sortField"); got != "current_pass_percentage" {
		t.Fatalf("unexpected sortField query param: %q", got)
	}
	if got := gotQuery.Get("sort"); got != "desc" {
		t.Fatalf("unexpected sort query param: %q", got)
	}
	if got := gotQuery.Get("filter"); got == "" {
		t.Fatalf("expected filter query param to be set")
	}
	if got := gotQuery.Get("limit"); got != "25" {
		t.Fatalf("unexpected limit query param: %q", got)
	}
}
