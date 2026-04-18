package workflow

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	"ci-failure-atlas/pkg/store/postgres/initdb"
	"ci-failure-atlas/pkg/store/postgres/migrations"
	"ci-failure-atlas/pkg/testsupport/pgtest"
)

func newTestStore(t *testing.T) storecontracts.Store {
	t.Helper()

	server, err := pgtest.StartEmbedded(t.TempDir())
	if err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Stop()
	})

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		server.User,
		server.Password,
		server.Host,
		server.Port,
		server.Database,
	)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open postgres pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := initdb.Initialize(context.Background(), pool); err != nil {
		t.Fatalf("initialize postgres schema: %v", err)
	}
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("run postgres migrations: %v", err)
	}

	store, err := postgresstore.New(pool, postgresstore.Options{
		Week: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("create postgres store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func TestRunPhase1BuildsSemanticArtifacts(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := logr.NewContext(context.Background(), logr.Discard())

	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
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

	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
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

	result, err := RunPhase1(ctx, store, RunOptions{
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("run phase1: %v", err)
	}

	if len(result.Workset) != 2 {
		t.Fatalf("unexpected workset size: got=%d want=2", len(result.Workset))
	}
	if len(result.TestClusters) != 1 {
		t.Fatalf("unexpected test cluster size: got=%d want=1", len(result.TestClusters))
	}
	if result.TestClusters[0].SupportCount != 2 {
		t.Fatalf("unexpected test cluster support count: got=%d want=2", result.TestClusters[0].SupportCount)
	}
}

func TestRunPhase1FiltersByEnvironment(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := logr.NewContext(context.Background(), logr.Discard())

	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
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

	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
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

	result, err := RunPhase1(ctx, store, RunOptions{
		Environments: []string{"dev"},
	})
	if err != nil {
		t.Fatalf("run phase1: %v", err)
	}

	if len(result.Workset) != 1 {
		t.Fatalf("unexpected workset size: got=%d want=1", len(result.Workset))
	}
	if result.Workset[0].Environment != "dev" {
		t.Fatalf("unexpected workset environment: %+v", result.Workset[0])
	}
}

func TestRunPhase1FiltersByTimeWindow(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := logr.NewContext(context.Background(), logr.Discard())

	windowStart := ptrTime(time.Date(2026, time.March, 6, 10, 0, 0, 0, time.UTC))
	windowEnd := ptrTime(time.Date(2026, time.March, 6, 11, 0, 0, 0, time.UTC))

	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example/run/dev-1",
			JobName:     "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			OccurredAt:  "2026-03-06T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}

	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
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

	result, err := RunPhase1(ctx, store, RunOptions{
		Environments: []string{"dev"},
		WindowStart:  windowStart,
		WindowEnd:    windowEnd,
	})
	if err != nil {
		t.Fatalf("run phase1: %v", err)
	}

	if len(result.Workset) != 1 {
		t.Fatalf("unexpected workset size after time filtering: got=%d want=1", len(result.Workset))
	}
	if !strings.Contains(result.Workset[0].RawText, "inside") {
		t.Fatalf("expected only inside-window row to be kept, got=%+v", result.Workset[0])
	}
}

func TestRunPhase1FailsWhenRunMetadataMissing(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := logr.NewContext(context.Background(), logr.Discard())

	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "missing-run-row",
			RunURL:         "https://prow.example/run/missing",
			TestName:       "test-dev",
			TestSuite:      "suite-dev",
			SignatureID:    "sig-dev",
			OccurredAt:     "2026-03-06T10:00:00Z",
			RawText:        "dev failure",
			NormalizedText: "dev failure",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	_, err := RunPhase1(ctx, store, RunOptions{
		Environments: []string{"dev"},
	})
	if err == nil {
		t.Fatalf("expected run to fail when run metadata is missing")
	}
	if !strings.Contains(err.Error(), "incomplete semantic input rows") {
		t.Fatalf("expected missing-run semantic input error, got=%v", err)
	}
}

func TestRunPhase2BuildsFailurePatternsAndMergedReview(t *testing.T) {
	t.Parallel()

	phase2Result, err := RunPhase2(Phase1Result{
		TestClusters: []semanticcontracts.TestClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase1ClusterID:         "phase1-a",
				Lane:                    "e2e",
				JobName:                 "job-a",
				TestName:                "test-a",
				CanonicalEvidencePhrase: "ERROR CODE: DeploymentFailed",
				SearchQueryPhrase:       "/providers/Microsoft.EventGrid/topics/test",
				SupportCount:            1,
				MemberSignatureIDs:      []string{"sig-a"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RunURL:      "https://prow.example/run/1",
						OccurredAt:  "2026-03-05T10:00:00Z",
						SignatureID: "sig-a",
					},
				},
			},
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase1ClusterID:         "phase1-b",
				Lane:                    "e2e",
				JobName:                 "job-a",
				TestName:                "test-b",
				CanonicalEvidencePhrase: "ERROR CODE: DeploymentFailed",
				SearchQueryPhrase:       "/providers/Microsoft.Monitor/actionGroups/test",
				SupportCount:            1,
				MemberSignatureIDs:      []string{"sig-b"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RunURL:      "https://prow.example/run/2",
						OccurredAt:  "2026-03-05T11:00:00Z",
						SignatureID: "sig-b",
					},
				},
			},
		},
		ReviewQueue: []semanticcontracts.ReviewItemRecord{
			{
				SchemaVersion:          semanticcontracts.SchemaVersionV1,
				Environment:            "dev",
				ReviewItemID:           "0513ee4b07d75306505bf62bb23f7d479e261caa0b83084ea26b793d76a85352",
				Phase:                  "phase1",
				Reason:                 "low_confidence_evidence",
				SourcePhase1ClusterIDs: []string{"phase1-a"},
				MemberSignatureIDs:     []string{"sig-a"},
			},
		},
	})
	if err != nil {
		t.Fatalf("run phase2: %v", err)
	}

	if len(phase2Result.FailurePatterns) != 2 {
		t.Fatalf("unexpected failure-pattern count: got=%d want=2", len(phase2Result.FailurePatterns))
	}
	if len(phase2Result.ReviewQueue) != 2 {
		t.Fatalf("unexpected review queue size: got=%d want=2", len(phase2Result.ReviewQueue))
	}

	foundPhase2Ambiguous := false
	for _, row := range phase2Result.ReviewQueue {
		if row.Phase == "phase2" && row.Reason == "ambiguous_provider_merge" {
			foundPhase2Ambiguous = true
			break
		}
	}
	if !foundPhase2Ambiguous {
		t.Fatalf("expected phase2 ambiguous provider review item, got=%+v", phase2Result.ReviewQueue)
	}
}

func TestMaterializeWeekRejectsNonSunday(t *testing.T) {
	t.Parallel()

	if _, err := MaterializeWeek(context.Background(), newTestStore(t), time.Date(2026, time.March, 16, 10, 0, 0, 0, time.UTC)); err == nil {
		t.Fatalf("expected validation error for non-Sunday materialization week")
	}
}

func TestNormalizeEnvironmentsSortsAndDedupes(t *testing.T) {
	t.Parallel()

	environments, environmentSet, err := NormalizeEnvironments([]string{"int", "dev", "INT", "dev"})
	if err != nil {
		t.Fatalf("normalize environments: %v", err)
	}
	if len(environments) != 2 || environments[0] != "dev" || environments[1] != "int" {
		t.Fatalf("unexpected normalized environments: %+v", environments)
	}
	if _, ok := environmentSet["dev"]; !ok {
		t.Fatalf("expected environment set to include dev")
	}
	if _, ok := environmentSet["int"]; !ok {
		t.Fatalf("expected environment set to include int")
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
