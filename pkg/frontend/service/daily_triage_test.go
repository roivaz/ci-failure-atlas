package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	"ci-failure-atlas/pkg/store/postgres/initdb"
	"ci-failure-atlas/pkg/store/postgres/migrations"
	"ci-failure-atlas/pkg/testsupport/pgtest"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestBuildDailyTriageProjectsFactsAgainstSemanticWeek(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	currentStore := fixture.openWeekStore(t, "2026-03-15")
	previousStore := fixture.openWeekStore(t, "2026-03-08")

	if err := currentStore.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed current materialized week: %v", err)
	}
	if err := previousStore.ReplaceMaterializedWeek(ctx, previousMaterializedWeek()); err != nil {
		t.Fatalf("seed previous materialized week: %v", err)
	}

	now := "2026-03-16T12:00:00Z"
	if err := currentStore.UpsertPhase3Issues(ctx, []semanticcontracts.Phase3IssueRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "QE-123",
			Title:         "OAuth flake",
			CreatedAt:     now,
			UpdatedAt:     now,
		},
	}); err != nil {
		t.Fatalf("seed current phase3 issue: %v", err)
	}
	if err := previousStore.UpsertPhase3Issues(ctx, []semanticcontracts.Phase3IssueRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "QE-123",
			Title:         "OAuth flake",
			CreatedAt:     "2026-03-08T10:00:00Z",
			UpdatedAt:     "2026-03-08T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed previous phase3 issue: %v", err)
	}
	if err := currentStore.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "QE-123",
			Environment:   "dev",
			RunURL:        "https://prow.example.com/view/1",
			RowID:         "row-1",
			UpdatedAt:     now,
		},
	}); err != nil {
		t.Fatalf("seed current phase3 link: %v", err)
	}
	if err := previousStore.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "QE-123",
			Environment:   "dev",
			RunURL:        "https://prow.example.com/view/prev-1",
			RowID:         "prev-row-1",
			UpdatedAt:     "2026-03-08T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed previous phase3 link: %v", err)
	}

	if err := currentStore.UpsertRuns(ctx, runsForDailyTriage()); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := currentStore.UpsertRawFailures(ctx, rawFailuresForDailyTriage()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	response, err := fixture.service.BuildDailyTriage(ctx, DailyTriageQuery{
		Date:         "2026-03-16",
		Environments: []string{"dev"},
		GeneratedAt:  time.Date(2026, time.March, 16, 14, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("build daily triage: %v", err)
	}

	if got, want := response.Meta.ResolvedWeek, "2026-03-15"; got != want {
		t.Fatalf("unexpected resolved week: got=%q want=%q", got, want)
	}
	if got, want := len(response.Environments), 1; got != want {
		t.Fatalf("unexpected environment count: got=%d want=%d", got, want)
	}

	environment := response.Environments[0]
	if got, want := environment.Environment, "dev"; got != want {
		t.Fatalf("unexpected environment: got=%q want=%q", got, want)
	}
	if got, want := environment.Summary.RawFailureCount, 3; got != want {
		t.Fatalf("unexpected raw failure count: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.DistinctRuns, 2; got != want {
		t.Fatalf("unexpected distinct run count: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.FailedRuns, 2; got != want {
		t.Fatalf("unexpected failed run count: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.PostGoodRawFailureCount, 2; got != want {
		t.Fatalf("unexpected post-good raw failure count: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.PostGoodRunCount, 1; got != want {
		t.Fatalf("unexpected post-good run count: got=%d want=%d", got, want)
	}

	if got, want := len(environment.Items), 2; got != want {
		t.Fatalf("unexpected item count: got=%d want=%d", got, want)
	}

	matched := environment.Items[0]
	if got, want := matched.SignatureID, "sig-a"; got != want {
		t.Fatalf("unexpected matched signature id: got=%q want=%q", got, want)
	}
	if got, want := matched.ClusterID, "QE-123"; got != want {
		t.Fatalf("unexpected matched cluster id: got=%q want=%q", got, want)
	}
	if got, want := matched.DailyCount, 2; got != want {
		t.Fatalf("unexpected matched daily count: got=%d want=%d", got, want)
	}
	if got, want := matched.WeeklySupportCount, 7; got != want {
		t.Fatalf("unexpected matched weekly support count: got=%d want=%d", got, want)
	}
	if got, want := matched.PriorWeeksPresent, 1; got != want {
		t.Fatalf("unexpected matched prior weeks: got=%d want=%d", got, want)
	}
	if got, want := matched.Lane, "upgrade"; got != want {
		t.Fatalf("unexpected matched lane: got=%q want=%q", got, want)
	}
	if got, want := len(matched.Phase3Issues), 1; got != want {
		t.Fatalf("unexpected matched phase3 issue count: got=%d want=%d", got, want)
	}
	if got, want := matched.Phase3Issues[0].IssueID, "QE-123"; got != want {
		t.Fatalf("unexpected matched phase3 issue id: got=%q want=%q", got, want)
	}
	if got, want := matched.Phase3Issues[0].Title, "OAuth flake"; got != want {
		t.Fatalf("unexpected matched phase3 issue title: got=%q want=%q", got, want)
	}
	if got, want := len(matched.DailyRunURLs), 1; got != want {
		t.Fatalf("unexpected matched daily run url count: got=%d want=%d", got, want)
	}
	if got, want := matched.DailyRunURLs[0], "https://prow.example.com/view/1"; got != want {
		t.Fatalf("unexpected matched daily run url: got=%q want=%q", got, want)
	}

	unmatched := environment.Items[1]
	if got, want := unmatched.SignatureID, "sig-b"; got != want {
		t.Fatalf("unexpected unmatched signature id: got=%q want=%q", got, want)
	}
	if unmatched.ClusterID != "" {
		t.Fatalf("expected unmatched item to have empty cluster id, got=%q", unmatched.ClusterID)
	}
	if got, want := unmatched.DailyCount, 1; got != want {
		t.Fatalf("unexpected unmatched daily count: got=%d want=%d", got, want)
	}
	if unmatched.SampleRawText == "" {
		t.Fatalf("expected unmatched item to include sample raw text")
	}
}

type integrationFixture struct {
	service *Service
	pool    *pgxpool.Pool
}

func newIntegrationFixture(t *testing.T, defaultWeek string) *integrationFixture {
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

	service, err := New(Options{
		DefaultWeek:  defaultWeek,
		PostgresPool: pool,
	})
	if err != nil {
		t.Fatalf("create frontend service: %v", err)
	}
	return &integrationFixture{
		service: service,
		pool:    pool,
	}
}

func (f *integrationFixture) openWeekStore(t *testing.T, week string) storeWithClose {
	t.Helper()

	store, err := postgresstore.New(f.pool, postgresstore.Options{Week: week})
	if err != nil {
		t.Fatalf("create postgres store for %s: %v", week, err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

type storeWithClose interface {
	ReplaceMaterializedWeek(context.Context, storecontracts.MaterializedWeek) error
	UpsertPhase3Issues(context.Context, []semanticcontracts.Phase3IssueRecord) error
	UpsertPhase3Links(context.Context, []semanticcontracts.Phase3LinkRecord) error
	UpsertRuns(context.Context, []storecontracts.RunRecord) error
	UpsertRawFailures(context.Context, []storecontracts.RawFailureRecord) error
	Close() error
}

func currentMaterializedWeek() storecontracts.MaterializedWeek {
	return storecontracts.MaterializedWeek{
		GlobalClusters: []semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:                semanticcontracts.SchemaVersionV1,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-a",
				CanonicalEvidencePhrase:      "OAuth timeout",
				SearchQueryPhrase:            "OAuth timeout",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/1",
				SearchQuerySourceSignatureID: "sig-a",
				SupportCount:                 7,
				SeenPostGoodCommit:           true,
				PostGoodCommitCount:          2,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "upgrade",
						JobName:      "periodic-ci",
						TestName:     "should oauth",
						SupportCount: 7,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-sig-a"},
				MemberSignatureIDs:     []string{"sig-a"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:          "row-1",
						RunURL:         "https://prow.example.com/view/1",
						OccurredAt:     "2026-03-16T08:00:00Z",
						SignatureID:    "sig-a",
						PostGoodCommit: true,
					},
					{
						RowID:          "row-2",
						RunURL:         "https://prow.example.com/view/1",
						OccurredAt:     "2026-03-16T08:05:00Z",
						SignatureID:    "sig-a",
						PostGoodCommit: true,
					},
				},
			},
		},
	}
}

func previousMaterializedWeek() storecontracts.MaterializedWeek {
	return storecontracts.MaterializedWeek{
		GlobalClusters: []semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:                semanticcontracts.SchemaVersionV1,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-old",
				CanonicalEvidencePhrase:      "OAuth timeout",
				SearchQueryPhrase:            "OAuth timeout",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/prev-1",
				SearchQuerySourceSignatureID: "sig-old",
				SupportCount:                 4,
				SeenPostGoodCommit:           false,
				PostGoodCommitCount:          0,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "upgrade",
						JobName:      "periodic-ci",
						TestName:     "should oauth",
						SupportCount: 4,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-sig-old"},
				MemberSignatureIDs:     []string{"sig-old"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "prev-row-1",
						RunURL:      "https://prow.example.com/view/prev-1",
						OccurredAt:  "2026-03-09T08:00:00Z",
						SignatureID: "sig-old",
					},
				},
			},
		},
	}
}

func runsForDailyTriage() []storecontracts.RunRecord {
	return []storecontracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         "https://prow.example.com/view/1",
			JobName:        "periodic-ci",
			PostGoodCommit: true,
			Failed:         true,
			OccurredAt:     "2026-03-16T08:00:00Z",
		},
		{
			Environment:    "dev",
			RunURL:         "https://prow.example.com/view/2",
			JobName:        "periodic-ci",
			PostGoodCommit: false,
			Failed:         true,
			OccurredAt:     "2026-03-16T09:00:00Z",
		},
	}
}

func rawFailuresForDailyTriage() []storecontracts.RawFailureRecord {
	return []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-1",
			RunURL:         "https://prow.example.com/view/1",
			TestName:       "should oauth",
			TestSuite:      "suite-a",
			SignatureID:    "sig-a",
			OccurredAt:     "2026-03-16T08:00:00Z",
			RawText:        "OAuth timeout while waiting for cluster operator",
			NormalizedText: "oauth timeout while waiting for cluster operator",
		},
		{
			Environment:    "dev",
			RowID:          "row-2",
			RunURL:         "https://prow.example.com/view/1",
			TestName:       "should oauth",
			TestSuite:      "suite-a",
			SignatureID:    "sig-a",
			OccurredAt:     "2026-03-16T08:05:00Z",
			RawText:        "OAuth timeout while waiting for cluster operator",
			NormalizedText: "oauth timeout while waiting for cluster operator",
		},
		{
			Environment:    "dev",
			RowID:          "row-3",
			RunURL:         "https://prow.example.com/view/2",
			TestName:       "should install",
			TestSuite:      "suite-b",
			SignatureID:    "sig-b",
			OccurredAt:     "2026-03-16T09:00:00Z",
			RawText:        "Installer failed to reach bootstrap machine",
			NormalizedText: "installer failed to reach bootstrap machine",
		},
	}
}
