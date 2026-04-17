package weekly

import (
	"context"
	"fmt"
	"strings"
	"testing"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	"ci-failure-atlas/pkg/store/postgres/initdb"
	"ci-failure-atlas/pkg/store/postgres/migrations"
	"ci-failure-atlas/pkg/testsupport/pgtest"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGenerateHTMLWithComparisonLinksLaneOutcomeDatesToRunsPage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := newWeeklyTestPool(t)
	store, err := postgresstore.New(pool, postgresstore.Options{Week: "2026-03-15"})
	if err != nil {
		t.Fatalf("open week store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.ReplaceMaterializedWeek(ctx, storecontracts.MaterializedWeek{}); err != nil {
		t.Fatalf("seed empty materialized week: %v", err)
	}
	if err := store.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-16", Metric: "run_count", Value: 10},
		{Environment: "dev", Date: "2026-03-16", Metric: "run_success_count", Value: 7},
		{Environment: "dev", Date: "2026-03-16", Metric: "provision_failure_count", Value: 1},
		{Environment: "dev", Date: "2026-03-16", Metric: "e2e_failure_count", Value: 1},
		{Environment: "dev", Date: "2026-03-16", Metric: "ciinfra_failure_count", Value: 1},
	}); err != nil {
		t.Fatalf("seed metrics daily: %v", err)
	}

	rendered, err := GenerateHTMLWithComparison(ctx, store, nil, Options{
		StartDate:             "2026-03-15",
		TargetRate:            95.0,
		Week:                  "2026-03-15",
		DayRunHistoryBasePath: "/runs",
	})
	if err != nil {
		t.Fatalf("generate weekly HTML: %v", err)
	}

	if !strings.Contains(rendered, `/runs?date=2026-03-16&amp;env=dev&amp;week=2026-03-15`) {
		t.Fatalf("expected lane outcome day label to link to runs page, got %q", rendered)
	}
}

func newWeeklyTestPool(t *testing.T) *pgxpool.Pool {
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
	return pool
}
