package sippy

import (
	"encoding/json"
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
