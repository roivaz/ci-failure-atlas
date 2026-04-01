package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/postgres/initdb"
	"ci-failure-atlas/pkg/store/postgres/migrations"
	"ci-failure-atlas/pkg/testsupport/pgtest"
)

func TestNewRequiresPool(t *testing.T) {
	t.Parallel()

	if _, err := New(nil, Options{}); err == nil {
		t.Fatalf("expected error when creating postgres store with nil pool")
	}
}

func TestMethodsRequireContext(t *testing.T) {
	t.Parallel()

	store := &Store{}
	if err := store.ReplaceMaterializedWeek(nil, storecontracts.MaterializedWeek{}); err == nil {
		t.Fatalf("expected context validation error")
	}
}

func TestNormalizeWeekAcceptsSundayWeek(t *testing.T) {
	t.Parallel()

	week, err := NormalizeWeek(" 2026-03-15 ")
	if err != nil {
		t.Fatalf("normalize week: %v", err)
	}
	if got, want := week, "2026-03-15"; got != want {
		t.Fatalf("unexpected normalized week: got=%q want=%q", got, want)
	}
}

func TestNormalizeWeekRejectsNonSunday(t *testing.T) {
	t.Parallel()

	if _, err := NormalizeWeek("2026-03-16"); err == nil {
		t.Fatalf("expected validation error for non-Sunday week")
	}
}

func TestReplaceMaterializedWeekClearsPriorSnapshot(t *testing.T) {
	store := newIntegrationStore(t, "2026-03-15")
	ctx := context.Background()

	if err := store.ReplaceMaterializedWeek(ctx, storecontracts.MaterializedWeek{
		GlobalClusters: []semanticcontracts.GlobalClusterRecord{
			{
				Environment:             "dev",
				Phase2ClusterID:         "cluster-a",
				CanonicalEvidencePhrase: "alpha",
				SearchQueryPhrase:       "alpha",
				SupportCount:            3,
				ContributingTestsCount:  1,
			},
		},
		ReviewQueue: []semanticcontracts.ReviewItemRecord{
			{
				Environment:  "dev",
				ReviewItemID: "review-a",
				Phase:        "phase2",
				Reason:       "ambiguous_provider_merge",
			},
		},
	}); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}

	if err := store.ReplaceMaterializedWeek(ctx, storecontracts.MaterializedWeek{
		GlobalClusters: []semanticcontracts.GlobalClusterRecord{
			{
				Environment:             "int",
				Phase2ClusterID:         "cluster-b",
				CanonicalEvidencePhrase: "beta",
				SearchQueryPhrase:       "beta",
				SupportCount:            1,
				ContributingTestsCount:  1,
			},
		},
	}); err != nil {
		t.Fatalf("replace materialized week: %v", err)
	}

	globalClusters, err := store.ListGlobalClusters(ctx)
	if err != nil {
		t.Fatalf("list global clusters: %v", err)
	}
	if len(globalClusters) != 1 || globalClusters[0].Environment != "int" || globalClusters[0].Phase2ClusterID != "cluster-b" {
		t.Fatalf("expected only replaced global cluster, got=%+v", globalClusters)
	}

	reviewQueue, err := store.ListReviewQueue(ctx)
	if err != nil {
		t.Fatalf("list review queue: %v", err)
	}
	if len(reviewQueue) != 0 {
		t.Fatalf("expected review queue to be cleared, got=%+v", reviewQueue)
	}
}

func newIntegrationStore(t *testing.T, week string) *Store {
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

	store, err := New(pool, Options{Week: week})
	if err != nil {
		t.Fatalf("create postgres store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}
