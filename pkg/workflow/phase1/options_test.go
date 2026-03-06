package phase1

import (
	"context"
	"testing"

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
			Environment:            "dev",
			RowID:                  "raw-1",
			RunURL:                 "https://prow.example/run/1",
			TestName:               "Engineering should be able to retrieve expected metrics from the /metrics endpoint",
			TestSuite:              "rp-api-compat-all/parallel",
			SignatureID:            "sig-1",
			OccurredAt:             "2026-03-06T10:00:00Z",
			RawText:                "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
			NormalizedText:         "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
			MergedPR:               true,
			PostGoodCommitFailures: 1,
		},
		{
			Environment:            "dev",
			RowID:                  "raw-2",
			RunURL:                 "https://prow.example/run/1",
			TestName:               "Engineering should be able to retrieve expected metrics from the /metrics endpoint",
			TestSuite:              "rp-api-compat-all/parallel",
			SignatureID:            "sig-2",
			OccurredAt:             "2026-03-06T10:10:00Z",
			RawText:                "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
			NormalizedText:         "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
			MergedPR:               true,
			PostGoodCommitFailures: 1,
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
