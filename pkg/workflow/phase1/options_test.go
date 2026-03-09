package phase1

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func TestRunWorkflowPhase1WritesSemanticArtifacts(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.NDJSONOptions.DataDirectory = t.TempDir()

	validated, err := opts.Validate()
	if err != nil {
		t.Fatalf("validate options: %v", err)
	}
	completed, err := validated.Complete(context.Background())
	if err != nil {
		t.Fatalf("complete options: %v", err)
	}

	ctx := logr.NewContext(context.Background(), logr.Discard())

	if err := completed.Store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         "https://prow.example/run/1",
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       42,
			MergedPR:       true,
			PostGoodCommit: true,
			OccurredAt:     "2026-03-06T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}

	if err := completed.Store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "raw-1",
			RunURL:         "https://prow.example/run/1",
			TestName:       "Engineering should be able to retrieve expected metrics from the /metrics endpoint",
			TestSuite:      "rp-api-compat-all/parallel",
			SignatureID:    "sig-1",
			OccurredAt:     "2026-03-06T10:00:00Z",
			RawText:        "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
			NormalizedText: "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
		},
		{
			Environment:    "dev",
			RowID:          "raw-2",
			RunURL:         "https://prow.example/run/1",
			TestName:       "Engineering should be able to retrieve expected metrics from the /metrics endpoint",
			TestSuite:      "rp-api-compat-all/parallel",
			SignatureID:    "sig-2",
			OccurredAt:     "2026-03-06T10:10:00Z",
			RawText:        "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
			NormalizedText: "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
		},
		{
			Environment:       "dev",
			RowID:             "raw-non-artifact",
			RunURL:            "https://prow.example/run/1",
			TestName:          "unknown",
			TestSuite:         "unknown",
			SignatureID:       "sig-non-artifact",
			OccurredAt:        "2026-03-06T10:20:00Z",
			RawText:           "non-artifact-backed failure",
			NormalizedText:    "non-artifact-backed failure",
			NonArtifactBacked: true,
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	if err := completed.Run(ctx); err != nil {
		t.Fatalf("run workflow phase1: %v", err)
	}

	worksetRows, err := completed.Store.ListPhase1Workset(ctx)
	if err != nil {
		t.Fatalf("list phase1 workset: %v", err)
	}
	if len(worksetRows) != 2 {
		t.Fatalf("unexpected phase1 workset size: got=%d want=2", len(worksetRows))
	}

	clusterRows, err := completed.Store.ListTestClusters(ctx)
	if err != nil {
		t.Fatalf("list test clusters: %v", err)
	}
	if len(clusterRows) != 1 {
		t.Fatalf("unexpected test cluster size: got=%d want=1", len(clusterRows))
	}
	if clusterRows[0].SupportCount != 2 {
		t.Fatalf("unexpected test cluster support count: got=%d want=2", clusterRows[0].SupportCount)
	}
}

func TestRunWorkflowPhase1FiltersByEnvironment(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.NDJSONOptions.DataDirectory = t.TempDir()
	opts.Environments = []string{"dev"}

	validated, err := opts.Validate()
	if err != nil {
		t.Fatalf("validate options: %v", err)
	}
	completed, err := validated.Complete(context.Background())
	if err != nil {
		t.Fatalf("complete options: %v", err)
	}

	ctx := logr.NewContext(context.Background(), logr.Discard())

	if err := completed.Store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example/run/dev-1",
			JobName:     "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			OccurredAt:  "2026-03-06T10:00:00Z",
		},
		{
			Environment: "int",
			RunURL:      "https://prow.example/run/int-1",
			JobName:     "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel",
			OccurredAt:  "2026-03-06T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}

	if err := completed.Store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "dev-row-1",
			RunURL:         "https://prow.example/run/dev-1",
			TestName:       "test-dev",
			TestSuite:      "suite-dev",
			SignatureID:    "sig-dev",
			OccurredAt:     "2026-03-06T10:00:00Z",
			RawText:        "dev failure",
			NormalizedText: "dev failure",
		},
		{
			Environment:    "int",
			RowID:          "int-row-1",
			RunURL:         "https://prow.example/run/int-1",
			TestName:       "test-int",
			TestSuite:      "suite-int",
			SignatureID:    "sig-int",
			OccurredAt:     "2026-03-06T10:00:00Z",
			RawText:        "int failure",
			NormalizedText: "int failure",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	if err := completed.Run(ctx); err != nil {
		t.Fatalf("run workflow phase1: %v", err)
	}

	worksetRows, err := completed.Store.ListPhase1Workset(ctx)
	if err != nil {
		t.Fatalf("list phase1 workset: %v", err)
	}
	if len(worksetRows) != 1 {
		t.Fatalf("unexpected phase1 workset size: got=%d want=1", len(worksetRows))
	}
	if worksetRows[0].Environment != "dev" {
		t.Fatalf("unexpected environment in workset: %+v", worksetRows[0])
	}
}

func TestRunWorkflowPhase1FiltersByTimeWindow(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.NDJSONOptions.DataDirectory = t.TempDir()
	opts.Environments = []string{"dev"}
	opts.WindowStart = "2026-03-06T10:00:00Z"
	opts.WindowEnd = "2026-03-06T11:00:00Z"

	validated, err := opts.Validate()
	if err != nil {
		t.Fatalf("validate options: %v", err)
	}
	completed, err := validated.Complete(context.Background())
	if err != nil {
		t.Fatalf("complete options: %v", err)
	}

	ctx := logr.NewContext(context.Background(), logr.Discard())

	if err := completed.Store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example/run/dev-1",
			JobName:     "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			OccurredAt:  "2026-03-06T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}

	if err := completed.Store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "inside-window",
			RunURL:         "https://prow.example/run/dev-1",
			TestName:       "test-dev-a",
			TestSuite:      "suite-dev",
			SignatureID:    "sig-dev-a",
			OccurredAt:     "2026-03-06T10:15:00Z",
			RawText:        "inside failure",
			NormalizedText: "inside failure",
		},
		{
			Environment:    "dev",
			RowID:          "outside-window",
			RunURL:         "https://prow.example/run/dev-1",
			TestName:       "test-dev-b",
			TestSuite:      "suite-dev",
			SignatureID:    "sig-dev-b",
			OccurredAt:     "2026-03-06T11:15:00Z",
			RawText:        "outside failure",
			NormalizedText: "outside failure",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	if err := completed.Run(ctx); err != nil {
		t.Fatalf("run workflow phase1: %v", err)
	}

	worksetRows, err := completed.Store.ListPhase1Workset(ctx)
	if err != nil {
		t.Fatalf("list phase1 workset: %v", err)
	}
	if len(worksetRows) != 1 {
		t.Fatalf("unexpected phase1 workset size after time filtering: got=%d want=1", len(worksetRows))
	}
	if !strings.Contains(worksetRows[0].RawText, "inside") {
		t.Fatalf("expected only inside-window row to be kept, got=%+v", worksetRows[0])
	}
}

func TestValidateWorkflowPhase1RequiresBothWindowBoundaries(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.NDJSONOptions.DataDirectory = t.TempDir()
	opts.WindowStart = "2026-03-06"

	if _, err := opts.Validate(); err == nil {
		t.Fatalf("expected validation error when only workflow.window.start is set")
	}
}

func TestValidateWorkflowPhase1ParsesDateWindowAsDayRange(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.NDJSONOptions.DataDirectory = t.TempDir()
	opts.Environments = []string{"dev", "int"}
	opts.WindowStart = "2026-03-06"
	opts.WindowEnd = "2026-03-07"

	validated, err := opts.Validate()
	if err != nil {
		t.Fatalf("validate options: %v", err)
	}

	if validated.WindowStart == nil || validated.WindowEnd == nil {
		t.Fatalf("expected non-nil parsed workflow window")
	}
	if got, want := validated.WindowStart.Format(time.RFC3339), "2026-03-06T00:00:00Z"; got != want {
		t.Fatalf("unexpected parsed workflow window start: got=%s want=%s", got, want)
	}
	if got, want := validated.WindowEnd.Format(time.RFC3339), "2026-03-08T00:00:00Z"; got != want {
		t.Fatalf("unexpected parsed workflow window end: got=%s want=%s", got, want)
	}
	if len(validated.Environments) != 2 || validated.Environments[0] != "dev" || validated.Environments[1] != "int" {
		t.Fatalf("unexpected normalized environments: %+v", validated.Environments)
	}
}
