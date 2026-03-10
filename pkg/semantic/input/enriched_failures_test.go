package input

import (
	"context"
	"strings"
	"testing"
	"time"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestBuildEnrichedFailuresBuildsRowsWithWindowAndEnvironmentFilters(t *testing.T) {
	t.Parallel()

	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         "https://prow.example/run/dev-1",
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       42,
			PostGoodCommit: true,
			OccurredAt:     "2026-03-06T10:00:00Z",
		},
		{
			Environment:    "int",
			RunURL:         "https://prow.example/run/int-1",
			JobName:        "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel",
			PRNumber:       0,
			PostGoodCommit: false,
			OccurredAt:     "2026-03-06T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "inside-window",
			RunURL:         "https://prow.example/run/dev-1",
			TestName:       "Customer should be able to create an HCP cluster",
			TestSuite:      "rp-api-compat-all/parallel",
			SignatureID:    "sig-dev-inside",
			OccurredAt:     "2026-03-06T10:15:00Z",
			RawText:        "inside window failure",
			NormalizedText: "inside window failure",
		},
		{
			Environment:    "dev",
			RowID:          "outside-window",
			RunURL:         "https://prow.example/run/dev-1",
			TestName:       "Customer should be able to create an HCP cluster",
			TestSuite:      "rp-api-compat-all/parallel",
			SignatureID:    "sig-dev-outside",
			OccurredAt:     "2026-03-06T12:15:00Z",
			RawText:        "outside window failure",
			NormalizedText: "outside window failure",
		},
		{
			Environment:    "int",
			RowID:          "int-row",
			RunURL:         "https://prow.example/run/int-1",
			TestName:       "Customer should be able to create an HCP cluster",
			TestSuite:      "integration/parallel",
			SignatureID:    "sig-int",
			OccurredAt:     "2026-03-06T10:15:00Z",
			RawText:        "int failure",
			NormalizedText: "int failure",
		},
		{
			Environment:       "dev",
			RowID:             "non-artifact",
			RunURL:            "https://prow.example/run/dev-1",
			TestName:          "unknown",
			TestSuite:         "unknown",
			SignatureID:       "sig-non-artifact",
			OccurredAt:        "2026-03-06T10:20:00Z",
			RawText:           "non artifact failure",
			NormalizedText:    "non artifact failure",
			NonArtifactBacked: true,
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	start := time.Date(2026, 3, 6, 10, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 6, 11, 0, 0, 0, time.UTC)
	result, err := BuildEnrichedFailures(ctx, store, BuildOptions{
		EnvironmentSet: map[string]struct{}{"dev": {}},
		WindowStart:    &start,
		WindowEnd:      &end,
	})
	if err != nil {
		t.Fatalf("build enriched failures: %v", err)
	}

	if got, want := len(result.Rows), 1; got != want {
		t.Fatalf("unexpected row count: got=%d want=%d", got, want)
	}
	if result.Rows[0].RowID != "inside-window" {
		t.Fatalf("unexpected included row: %+v", result.Rows[0])
	}
	if got, want := result.Diagnostics.RowsSkippedOutsideWindow, 1; got != want {
		t.Fatalf("unexpected outside-window skip count: got=%d want=%d", got, want)
	}
	if got, want := result.Diagnostics.RowsSkippedNonArtifact, 1; got != want {
		t.Fatalf("unexpected non-artifact skip count: got=%d want=%d", got, want)
	}
}

func TestBuildEnrichedFailuresReturnsErrorForMissingRunMetadata(t *testing.T) {
	t.Parallel()

	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-without-run",
			RunURL:         "https://prow.example/run/dev-missing",
			TestName:       "test",
			TestSuite:      "suite",
			SignatureID:    "sig",
			OccurredAt:     "2026-03-06T10:15:00Z",
			RawText:        "raw",
			NormalizedText: "raw",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	result, buildErr := BuildEnrichedFailures(ctx, store, BuildOptions{})
	if buildErr == nil {
		t.Fatalf("expected missing run metadata error")
	}
	if !strings.Contains(buildErr.Error(), "missing_run=1") {
		t.Fatalf("expected missing run metadata summary, got=%v", buildErr)
	}
	if got, want := result.Diagnostics.MissingRunMetadata, 1; got != want {
		t.Fatalf("unexpected missing run metadata count: got=%d want=%d", got, want)
	}
}
