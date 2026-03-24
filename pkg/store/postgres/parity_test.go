package postgres

import (
	"context"
	"net"
	"reflect"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
	"ci-failure-atlas/pkg/store/postgres/initdb"
	"ci-failure-atlas/pkg/store/postgres/migrations"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFactsStateParityWithNDJSON(t *testing.T) {
	ctx := context.Background()
	semanticSubdir := "2026-03-22"

	ndjsonStore, postgresStore := newParityStores(t, semanticSubdir)

	mustNoErr(t, ndjsonStore.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment:    "DEV",
			RunURL:         "https://prow.example/run/dev-1",
			JobName:        "job-a",
			PRNumber:       11,
			PRState:        "CLOSED",
			PRSHA:          "sha-a",
			FinalMergedSHA: "merge-a",
			MergedPR:       true,
			PostGoodCommit: true,
			Failed:         true,
			OccurredAt:     "2026-03-22T10:00:00Z",
		},
		{
			Environment: "int",
			RunURL:      "https://prow.example/run/int-1",
			JobName:     "job-b",
			PRNumber:    0,
			OccurredAt:  "2026-03-22T11:00:00Z",
		},
	}))
	mustNoErr(t, postgresStore.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment:    "DEV",
			RunURL:         "https://prow.example/run/dev-1",
			JobName:        "job-a",
			PRNumber:       11,
			PRState:        "CLOSED",
			PRSHA:          "sha-a",
			FinalMergedSHA: "merge-a",
			MergedPR:       true,
			PostGoodCommit: true,
			Failed:         true,
			OccurredAt:     "2026-03-22T10:00:00Z",
		},
		{
			Environment: "int",
			RunURL:      "https://prow.example/run/int-1",
			JobName:     "job-b",
			PRNumber:    0,
			OccurredAt:  "2026-03-22T11:00:00Z",
		},
	}))

	ndRuns, err := ndjsonStore.ListRuns(ctx)
	mustNoErr(t, err)
	pgRuns, err := postgresStore.ListRuns(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgRuns, ndRuns)

	ndRunKeys, err := ndjsonStore.ListRunKeys(ctx)
	mustNoErr(t, err)
	pgRunKeys, err := postgresStore.ListRunKeys(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgRunKeys, ndRunKeys)

	ndRunDates, err := ndjsonStore.ListRunDates(ctx)
	mustNoErr(t, err)
	pgRunDates, err := postgresStore.ListRunDates(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgRunDates, ndRunDates)

	ndRunsByDate, err := ndjsonStore.ListRunsByDate(ctx, "dev", "2026-03-22")
	mustNoErr(t, err)
	pgRunsByDate, err := postgresStore.ListRunsByDate(ctx, "dev", "2026-03-22")
	mustNoErr(t, err)
	assertDeepEqual(t, pgRunsByDate, ndRunsByDate)

	ndRun, ndFound, err := ndjsonStore.GetRun(ctx, "dev", "https://prow.example/run/dev-1")
	mustNoErr(t, err)
	pgRun, pgFound, err := postgresStore.GetRun(ctx, "dev", "https://prow.example/run/dev-1")
	mustNoErr(t, err)
	assertDeepEqual(t, pgFound, ndFound)
	assertDeepEqual(t, pgRun, ndRun)

	mustNoErr(t, ndjsonStore.UpsertPullRequests(ctx, []storecontracts.PullRequestRecord{
		{
			PRNumber:       11,
			State:          "open",
			Merged:         true,
			HeadSHA:        "head-a",
			MergeCommitSHA: "merge-a",
			MergedAt:       "2026-03-21T00:00:00Z",
			ClosedAt:       "2026-03-21T00:00:00Z",
		},
	}))
	mustNoErr(t, postgresStore.UpsertPullRequests(ctx, []storecontracts.PullRequestRecord{
		{
			PRNumber:       11,
			State:          "open",
			Merged:         true,
			HeadSHA:        "head-a",
			MergeCommitSHA: "merge-a",
			MergedAt:       "2026-03-21T00:00:00Z",
			ClosedAt:       "2026-03-21T00:00:00Z",
		},
	}))

	ndPRs, err := ndjsonStore.ListPullRequests(ctx)
	mustNoErr(t, err)
	pgPRs, err := postgresStore.ListPullRequests(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgPRs, ndPRs)

	ndPR, ndPRFound, err := ndjsonStore.GetPullRequest(ctx, 11)
	mustNoErr(t, err)
	pgPR, pgPRFound, err := postgresStore.GetPullRequest(ctx, 11)
	mustNoErr(t, err)
	assertDeepEqual(t, pgPRFound, ndPRFound)
	assertDeepEqual(t, pgPR, ndPR)

	mustNoErr(t, ndjsonStore.UpsertArtifactFailures(ctx, []storecontracts.ArtifactFailureRecord{
		{
			Environment:   "dev",
			ArtifactRowID: "a1",
			RunURL:        "https://prow.example/run/dev-1",
			TestName:      "test-a",
			TestSuite:     "suite-a",
			SignatureID:   "sig-a",
			FailureText:   "failure-a",
		},
	}))
	mustNoErr(t, postgresStore.UpsertArtifactFailures(ctx, []storecontracts.ArtifactFailureRecord{
		{
			Environment:   "dev",
			ArtifactRowID: "a1",
			RunURL:        "https://prow.example/run/dev-1",
			TestName:      "test-a",
			TestSuite:     "suite-a",
			SignatureID:   "sig-a",
			FailureText:   "failure-a",
		},
	}))

	ndArtifactRunKeys, err := ndjsonStore.ListArtifactRunKeys(ctx)
	mustNoErr(t, err)
	pgArtifactRunKeys, err := postgresStore.ListArtifactRunKeys(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgArtifactRunKeys, ndArtifactRunKeys)

	ndArtifacts, err := ndjsonStore.ListArtifactFailuresByRun(ctx, "dev", "https://prow.example/run/dev-1")
	mustNoErr(t, err)
	pgArtifacts, err := postgresStore.ListArtifactFailuresByRun(ctx, "dev", "https://prow.example/run/dev-1")
	mustNoErr(t, err)
	assertDeepEqual(t, pgArtifacts, ndArtifacts)

	rawBatchA := []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-a",
			RunURL:         "https://prow.example/run/dev-1",
			TestName:       "test-a",
			TestSuite:      "suite-a",
			SignatureID:    "sig-a",
			OccurredAt:     "2026-03-22T10:00:00Z",
			RawText:        "raw-a",
			NormalizedText: "norm-a",
		},
		{
			Environment:    "dev",
			RowID:          "row-b",
			RunURL:         "https://prow.example/run/dev-1",
			TestName:       "test-b",
			TestSuite:      "suite-a",
			SignatureID:    "sig-b",
			OccurredAt:     "2026-03-22T10:05:00Z",
			RawText:        "raw-b",
			NormalizedText: "norm-b",
		},
	}
	mustNoErr(t, ndjsonStore.UpsertRawFailures(ctx, rawBatchA))
	mustNoErr(t, postgresStore.UpsertRawFailures(ctx, rawBatchA))

	// Touched-run replacement behavior should keep only this row for dev-1.
	rawBatchB := []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-c",
			RunURL:         "https://prow.example/run/dev-1",
			TestName:       "test-c",
			TestSuite:      "suite-a",
			SignatureID:    "sig-c",
			OccurredAt:     "2026-03-22T10:10:00Z",
			RawText:        "raw-c",
			NormalizedText: "norm-c",
		},
	}
	mustNoErr(t, ndjsonStore.UpsertRawFailures(ctx, rawBatchB))
	mustNoErr(t, postgresStore.UpsertRawFailures(ctx, rawBatchB))

	ndRaw, err := ndjsonStore.ListRawFailures(ctx)
	mustNoErr(t, err)
	pgRaw, err := postgresStore.ListRawFailures(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgRaw, ndRaw)

	ndRawRunKeys, err := ndjsonStore.ListRawFailureRunKeys(ctx)
	mustNoErr(t, err)
	pgRawRunKeys, err := postgresStore.ListRawFailureRunKeys(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgRawRunKeys, ndRawRunKeys)

	ndRawByRun, err := ndjsonStore.ListRawFailuresByRun(ctx, "dev", "https://prow.example/run/dev-1")
	mustNoErr(t, err)
	pgRawByRun, err := postgresStore.ListRawFailuresByRun(ctx, "dev", "https://prow.example/run/dev-1")
	mustNoErr(t, err)
	assertDeepEqual(t, pgRawByRun, ndRawByRun)

	ndRawByDate, err := ndjsonStore.ListRawFailuresByDate(ctx, "dev", "2026-03-22")
	mustNoErr(t, err)
	pgRawByDate, err := postgresStore.ListRawFailuresByDate(ctx, "dev", "2026-03-22")
	mustNoErr(t, err)
	assertDeepEqual(t, pgRawByDate, ndRawByDate)

	metricsA := []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-22", Metric: "run_count", Value: 10},
		{Environment: "dev", Date: "2026-03-22", Metric: "fail_count", Value: 4},
	}
	mustNoErr(t, ndjsonStore.UpsertMetricsDaily(ctx, metricsA))
	mustNoErr(t, postgresStore.UpsertMetricsDaily(ctx, metricsA))
	metricsB := []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-22", Metric: "run_count", Value: 11},
	}
	mustNoErr(t, ndjsonStore.UpsertMetricsDaily(ctx, metricsB))
	mustNoErr(t, postgresStore.UpsertMetricsDaily(ctx, metricsB))

	ndMetrics, err := ndjsonStore.ListMetricsDaily(ctx)
	mustNoErr(t, err)
	pgMetrics, err := postgresStore.ListMetricsDaily(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgMetrics, ndMetrics)

	ndMetricsByDate, err := ndjsonStore.ListMetricsDailyByDate(ctx, "dev", "2026-03-22")
	mustNoErr(t, err)
	pgMetricsByDate, err := postgresStore.ListMetricsDailyByDate(ctx, "dev", "2026-03-22")
	mustNoErr(t, err)
	assertDeepEqual(t, pgMetricsByDate, ndMetricsByDate)

	ndMetricDates, err := ndjsonStore.ListMetricDates(ctx)
	mustNoErr(t, err)
	pgMetricDates, err := postgresStore.ListMetricDates(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgMetricDates, ndMetricDates)

	testMetaA := []storecontracts.TestMetadataDailyRecord{
		{
			Environment:           "dev",
			Date:                  "2026-03-22",
			Release:               "Presubmits",
			Period:                "default",
			TestName:              "test-a",
			TestSuite:             "suite-a",
			CurrentPassPercentage: 98.0,
			CurrentRuns:           25,
		},
		{
			Environment:           "dev",
			Date:                  "2026-03-22",
			Release:               "Presubmits",
			Period:                "default",
			TestName:              "test-b",
			TestSuite:             "suite-a",
			CurrentPassPercentage: 93.0,
			CurrentRuns:           12,
		},
	}
	mustNoErr(t, ndjsonStore.UpsertTestMetadataDaily(ctx, testMetaA))
	mustNoErr(t, postgresStore.UpsertTestMetadataDaily(ctx, testMetaA))
	testMetaB := []storecontracts.TestMetadataDailyRecord{
		{
			Environment:           "dev",
			Date:                  "2026-03-22",
			Release:               "Presubmits",
			Period:                "default",
			TestName:              "test-c",
			TestSuite:             "suite-a",
			CurrentPassPercentage: 88.0,
			CurrentRuns:           10,
		},
	}
	mustNoErr(t, ndjsonStore.UpsertTestMetadataDaily(ctx, testMetaB))
	mustNoErr(t, postgresStore.UpsertTestMetadataDaily(ctx, testMetaB))

	ndTestMetaByDate, err := ndjsonStore.ListTestMetadataDailyByDate(ctx, "dev", "2026-03-22")
	mustNoErr(t, err)
	pgTestMetaByDate, err := postgresStore.ListTestMetadataDailyByDate(ctx, "dev", "2026-03-22")
	mustNoErr(t, err)
	assertDeepEqual(t, pgTestMetaByDate, ndTestMetaByDate)

	mustNoErr(t, ndjsonStore.UpsertCheckpoints(ctx, []storecontracts.CheckpointRecord{
		{Name: "source.sippy.runs", Value: "cursor-1"},
	}))
	mustNoErr(t, postgresStore.UpsertCheckpoints(ctx, []storecontracts.CheckpointRecord{
		{Name: "source.sippy.runs", Value: "cursor-1"},
	}))

	ndCheckpoint, ndCheckpointFound, err := ndjsonStore.GetCheckpoint(ctx, "source.sippy.runs")
	mustNoErr(t, err)
	pgCheckpoint, pgCheckpointFound, err := postgresStore.GetCheckpoint(ctx, "source.sippy.runs")
	mustNoErr(t, err)
	assertDeepEqual(t, pgCheckpointFound, ndCheckpointFound)
	assertDeepEqual(t, pgCheckpoint.Name, ndCheckpoint.Name)
	assertDeepEqual(t, pgCheckpoint.Value, ndCheckpoint.Value)
	if pgCheckpoint.UpdatedAt == "" || ndCheckpoint.UpdatedAt == "" {
		t.Fatalf("expected checkpoint updated_at to be set")
	}

	mustNoErr(t, ndjsonStore.AppendDeadLetters(ctx, []storecontracts.DeadLetterRecord{
		{Controller: "facts.raw-failures", Key: "dev|run-1", Error: "boom"},
		{Controller: "facts.raw-failures", Key: "dev|run-2", Error: "bang"},
	}))
	mustNoErr(t, postgresStore.AppendDeadLetters(ctx, []storecontracts.DeadLetterRecord{
		{Controller: "facts.raw-failures", Key: "dev|run-1", Error: "boom"},
		{Controller: "facts.raw-failures", Key: "dev|run-2", Error: "bang"},
	}))

	ndDeadLetters, err := ndjsonStore.ListDeadLetters(ctx, 0)
	mustNoErr(t, err)
	pgDeadLetters, err := postgresStore.ListDeadLetters(ctx, 0)
	mustNoErr(t, err)
	assertDeepEqual(t, pgDeadLetters, ndDeadLetters)

	ndDeadLettersLimited, err := ndjsonStore.ListDeadLetters(ctx, 1)
	mustNoErr(t, err)
	pgDeadLettersLimited, err := postgresStore.ListDeadLetters(ctx, 1)
	mustNoErr(t, err)
	assertDeepEqual(t, pgDeadLettersLimited, ndDeadLettersLimited)
}

func TestSemanticPhase3ParityWithNDJSON(t *testing.T) {
	ctx := context.Background()
	semanticSubdir := "2026-03-22"

	ndjsonStore, postgresStore := newParityStores(t, semanticSubdir)

	worksetA := []semanticcontracts.Phase1WorksetRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			Environment:   "dev",
			RowID:         "r-1",
			GroupKey:      "dev|e2e|job-a|test-a",
			Lane:          "e2e",
			JobName:       "job-a",
			TestName:      "test-a",
			TestSuite:     "suite-a",
			SignatureID:   "sig-a",
			OccurredAt:    "2026-03-22T10:00:00Z",
			RunURL:        "https://prow.example/run/dev-1",
			RawText:       "raw-a",
		},
	}
	mustNoErr(t, ndjsonStore.UpsertPhase1Workset(ctx, worksetA))
	mustNoErr(t, postgresStore.UpsertPhase1Workset(ctx, worksetA))

	worksetB := []semanticcontracts.Phase1WorksetRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			Environment:   "dev",
			RowID:         "r-2",
			GroupKey:      "dev|e2e|job-a|test-b",
			Lane:          "e2e",
			JobName:       "job-a",
			TestName:      "test-b",
			TestSuite:     "suite-a",
			SignatureID:   "sig-b",
			OccurredAt:    "2026-03-22T10:05:00Z",
			RunURL:        "https://prow.example/run/dev-1",
			RawText:       "raw-b",
		},
	}
	// Environment replacement should replace dev rows with just r-2.
	mustNoErr(t, ndjsonStore.UpsertPhase1Workset(ctx, worksetB))
	mustNoErr(t, postgresStore.UpsertPhase1Workset(ctx, worksetB))

	ndWorkset, err := ndjsonStore.ListPhase1Workset(ctx)
	mustNoErr(t, err)
	pgWorkset, err := postgresStore.ListPhase1Workset(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgWorkset, ndWorkset)

	testClustersA := []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase1ClusterID:         "p1-a",
			Lane:                    "e2e",
			JobName:                 "job-a",
			TestName:                "test-a",
			CanonicalEvidencePhrase: "error-a",
			SupportCount:            2,
		},
	}
	mustNoErr(t, ndjsonStore.UpsertTestClusters(ctx, testClustersA))
	mustNoErr(t, postgresStore.UpsertTestClusters(ctx, testClustersA))
	ndTestClusters, err := ndjsonStore.ListTestClusters(ctx)
	mustNoErr(t, err)
	pgTestClusters, err := postgresStore.ListTestClusters(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgTestClusters, ndTestClusters)

	globalClustersA := []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "g-a",
			CanonicalEvidencePhrase: "ERROR CODE: timeout",
			SearchQueryPhrase:       "timeout",
			SupportCount:            3,
			ContributingTests: []semanticcontracts.ContributingTestRecord{
				{Lane: "e2e", JobName: "job-a", TestName: "test-a", SupportCount: 3},
			},
			References: []semanticcontracts.ReferenceRecord{
				{RowID: "r-2", RunURL: "https://prow.example/run/dev-1", OccurredAt: "2026-03-22T10:05:00Z", SignatureID: "sig-b"},
			},
		},
	}
	mustNoErr(t, ndjsonStore.UpsertGlobalClusters(ctx, globalClustersA))
	mustNoErr(t, postgresStore.UpsertGlobalClusters(ctx, globalClustersA))

	ndGlobal, err := ndjsonStore.ListGlobalClusters(ctx)
	mustNoErr(t, err)
	pgGlobal, err := postgresStore.ListGlobalClusters(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgGlobal, ndGlobal)

	reviewQueueA := []semanticcontracts.ReviewItemRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			Environment:   "dev",
			ReviewItemID:  "rv-1",
			Phase:         "phase1",
			Reason:        "ambiguous",
		},
	}
	mustNoErr(t, ndjsonStore.UpsertReviewQueue(ctx, reviewQueueA))
	mustNoErr(t, postgresStore.UpsertReviewQueue(ctx, reviewQueueA))

	ndReview, err := ndjsonStore.ListReviewQueue(ctx)
	mustNoErr(t, err)
	pgReview, err := postgresStore.ListReviewQueue(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgReview, ndReview)

	issues := []semanticcontracts.Phase3IssueRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-aaaa",
			CreatedAt:     "2026-03-22T11:00:00Z",
		},
	}
	mustNoErr(t, ndjsonStore.UpsertPhase3Issues(ctx, issues))
	mustNoErr(t, postgresStore.UpsertPhase3Issues(ctx, issues))
	ndIssues, err := ndjsonStore.ListPhase3Issues(ctx)
	mustNoErr(t, err)
	pgIssues, err := postgresStore.ListPhase3Issues(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgIssues, ndIssues)

	links := []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-aaaa",
			Environment:   "dev",
			RunURL:        "https://prow.example/run/dev-1",
			RowID:         "r-2",
			UpdatedAt:     "2026-03-22T11:10:00Z",
		},
	}
	mustNoErr(t, ndjsonStore.UpsertPhase3Links(ctx, links))
	mustNoErr(t, postgresStore.UpsertPhase3Links(ctx, links))
	ndLinks, err := ndjsonStore.ListPhase3Links(ctx)
	mustNoErr(t, err)
	pgLinks, err := postgresStore.ListPhase3Links(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgLinks, ndLinks)

	mustNoErr(t, ndjsonStore.DeletePhase3Links(ctx, links))
	mustNoErr(t, postgresStore.DeletePhase3Links(ctx, links))
	ndLinksAfterDelete, err := ndjsonStore.ListPhase3Links(ctx)
	mustNoErr(t, err)
	pgLinksAfterDelete, err := postgresStore.ListPhase3Links(ctx)
	mustNoErr(t, err)
	assertDeepEqual(t, pgLinksAfterDelete, ndLinksAfterDelete)

	events := []semanticcontracts.Phase3EventRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			EventID:       "evt-1",
			Action:        "link",
			IssueID:       "p3c-aaaa",
			Environment:   "dev",
			RunURL:        "https://prow.example/run/dev-1",
			RowID:         "r-2",
			At:            "2026-03-22T11:20:00Z",
		},
	}
	mustNoErr(t, ndjsonStore.AppendPhase3Events(ctx, events))
	mustNoErr(t, postgresStore.AppendPhase3Events(ctx, events))

	ndEvents, err := ndjsonStore.ListPhase3Events(ctx, 0)
	mustNoErr(t, err)
	pgEvents, err := postgresStore.ListPhase3Events(ctx, 0)
	mustNoErr(t, err)
	assertDeepEqual(t, pgEvents, ndEvents)

	ndEventsLimited, err := ndjsonStore.ListPhase3Events(ctx, 1)
	mustNoErr(t, err)
	pgEventsLimited, err := postgresStore.ListPhase3Events(ctx, 1)
	mustNoErr(t, err)
	assertDeepEqual(t, pgEventsLimited, ndEventsLimited)
}

func newParityStores(t *testing.T, semanticSubdir string) (*ndjson.Store, *Store) {
	t.Helper()
	dataDir := t.TempDir()

	ndjsonStore, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: semanticSubdir,
	})
	if err != nil {
		t.Fatalf("create ndjson store: %v", err)
	}
	t.Cleanup(func() {
		_ = ndjsonStore.Close()
	})

	port := freeTCPPort(t)
	embeddedConfig := embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V18).
		Port(port).
		DataPath(t.TempDir())
	embeddedDB := embeddedpostgres.NewDatabase(embeddedConfig)
	if err := embeddedDB.Start(); err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	t.Cleanup(func() {
		_ = embeddedDB.Stop()
	})

	poolConfig, err := pgxpool.ParseConfig(embeddedConfig.GetConnectionURL())
	if err != nil {
		t.Fatalf("parse embedded postgres URL: %v", err)
	}
	poolConfig.MaxConns = 4
	poolConfig.MinConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatalf("create postgres pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("ping embedded postgres: %v", err)
	}

	if err := initdb.Initialize(context.Background(), pool); err != nil {
		t.Fatalf("initialize embedded postgres schema: %v", err)
	}
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("run embedded postgres migrations: %v", err)
	}

	postgresStore, err := New(pool, Options{
		SemanticSubdirectory: semanticSubdir,
	})
	if err != nil {
		t.Fatalf("create postgres store: %v", err)
	}
	return ndjsonStore, postgresStore
}

func freeTCPPort(t *testing.T) uint32 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate free TCP port: %v", err)
	}
	defer listener.Close()
	return uint32(listener.Addr().(*net.TCPAddr).Port)
}

func mustNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertDeepEqual(t *testing.T, got any, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("values are different:\n got: %#v\nwant: %#v", got, want)
	}
}
