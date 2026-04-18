package service

import (
	"context"
	"fmt"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	"ci-failure-atlas/pkg/store/postgres/initdb"
	"ci-failure-atlas/pkg/store/postgres/migrations"
	"ci-failure-atlas/pkg/testsupport/pgtest"

	"github.com/jackc/pgx/v5/pgxpool"
)

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

type storeWithClose = storecontracts.Store

func currentMaterializedWeek() storecontracts.MaterializedWeek {
	return storecontracts.MaterializedWeek{
		FailurePatterns: []semanticcontracts.FailurePatternRecord{
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
		FailurePatterns: []semanticcontracts.FailurePatternRecord{
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

func sampleRunsFixture() []storecontracts.RunRecord {
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

func sampleRawFailuresFixture() []storecontracts.RawFailureRecord {
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
