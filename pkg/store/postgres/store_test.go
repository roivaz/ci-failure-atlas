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

func TestMigrationsDropDeprecatedPhase3Tables(t *testing.T) {
	t.Parallel()

	store := newIntegrationStore(t, "")
	ctx := context.Background()
	for _, table := range []string{
		"cfa_phase3_issues",
		"cfa_phase3_links",
		"cfa_phase3_events",
	} {
		var registered string
		err := store.pool.QueryRow(ctx, "SELECT COALESCE(to_regclass($1)::text, '')", "public."+table).Scan(&registered)
		if err != nil {
			t.Fatalf("check dropped table %q: %v", table, err)
		}
		if registered != "" {
			t.Fatalf("expected deprecated table %q to be absent after migrations, got %q", table, registered)
		}
	}
}

func TestReplaceMaterializedWeekClearsPriorSnapshot(t *testing.T) {
	store := newIntegrationStore(t, "2026-03-15")
	ctx := context.Background()

	if err := store.ReplaceMaterializedWeek(ctx, storecontracts.MaterializedWeek{
		FailurePatterns: []semanticcontracts.FailurePatternRecord{
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
		FailurePatterns: []semanticcontracts.FailurePatternRecord{
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

	globalClusters, err := store.ListFailurePatterns(ctx)
	if err != nil {
		t.Fatalf("list failure patterns: %v", err)
	}
	if len(globalClusters) != 1 || globalClusters[0].Environment != "int" || globalClusters[0].Phase2ClusterID != "cluster-b" {
		t.Fatalf("expected only replaced failure pattern, got=%+v", globalClusters)
	}

	reviewQueue, err := store.ListReviewQueue(ctx)
	if err != nil {
		t.Fatalf("list review queue: %v", err)
	}
	if len(reviewQueue) != 0 {
		t.Fatalf("expected review queue to be cleared, got=%+v", reviewQueue)
	}
}

func TestGetSemanticWeekSummaryAggregatesByEnvironment(t *testing.T) {
	store := newIntegrationStore(t, "2026-03-15")
	ctx := context.Background()

	if err := store.ReplaceMaterializedWeek(ctx, storecontracts.MaterializedWeek{
		FailurePatterns: []semanticcontracts.FailurePatternRecord{
			{
				Environment:             "dev",
				Phase2ClusterID:         "cluster-a",
				CanonicalEvidencePhrase: "alpha",
				SearchQueryPhrase:       "alpha",
				SupportCount:            3,
				ContributingTestsCount:  2,
				MemberPhase1ClusterIDs:  []string{"p1", "p2"},
			},
			{
				Environment:             "dev",
				Phase2ClusterID:         "cluster-b",
				CanonicalEvidencePhrase: "beta",
				SearchQueryPhrase:       "beta",
				SupportCount:            1,
				ContributingTestsCount:  1,
				MemberPhase1ClusterIDs:  []string{"p2", "p3", " "},
			},
			{
				Environment:             "int",
				Phase2ClusterID:         "cluster-c",
				CanonicalEvidencePhrase: "gamma",
				SearchQueryPhrase:       "gamma",
				SupportCount:            4,
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
			{
				Environment:  "prod",
				ReviewItemID: "review-b",
				Phase:        "phase2",
				Reason:       "missing_evidence",
			},
		},
	}); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}

	summary, err := store.GetSemanticWeekSummary(ctx)
	if err != nil {
		t.Fatalf("get semantic week summary: %v", err)
	}

	if got, want := summary.FailurePatternCountsByEnv["dev"], 2; got != want {
		t.Fatalf("unexpected dev failure-pattern count: got=%d want=%d", got, want)
	}
	if got, want := summary.FailurePatternCountsByEnv["int"], 1; got != want {
		t.Fatalf("unexpected int failure-pattern count: got=%d want=%d", got, want)
	}
	if got, want := summary.OccurrenceTotalsByEnv["dev"], 4; got != want {
		t.Fatalf("unexpected dev support total: got=%d want=%d", got, want)
	}
	if got, want := summary.OccurrenceTotalsByEnv["int"], 4; got != want {
		t.Fatalf("unexpected int support total: got=%d want=%d", got, want)
	}
	if got, want := summary.TestClusterCountsByEnv["dev"], 3; got != want {
		t.Fatalf("unexpected dev test cluster count: got=%d want=%d", got, want)
	}
	if got, want := summary.TestClusterCountsByEnv["int"], 0; got != want {
		t.Fatalf("unexpected int test cluster count: got=%d want=%d", got, want)
	}
	if got, want := summary.ReviewQueueCountsByEnv["dev"], 1; got != want {
		t.Fatalf("unexpected dev review count: got=%d want=%d", got, want)
	}
	if got, want := summary.ReviewQueueCountsByEnv["prod"], 1; got != want {
		t.Fatalf("unexpected prod review count: got=%d want=%d", got, want)
	}
	if got, want := summary.AvailableEnvironments, []string{"dev", "int", "prod"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("unexpected available environments: got=%v want=%v", got, want)
	}
}

func TestListRunsByDateUsesUTCDateProjection(t *testing.T) {
	t.Parallel()

	store := newIntegrationStore(t, "")
	ctx := context.Background()

	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/run/utc-prev",
			OccurredAt:  "2026-03-15T23:59:59Z",
		},
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/run/offset-next",
			OccurredAt:  "2026-03-15T23:30:00-05:00",
		},
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/run/invalid",
			OccurredAt:  "not-a-timestamp",
		},
	}); err != nil {
		t.Fatalf("upsert runs: %v", err)
	}

	dates, err := store.ListRunDates(ctx)
	if err != nil {
		t.Fatalf("list run dates: %v", err)
	}
	if got, want := dates, []string{"2026-03-15", "2026-03-16"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected run dates: got=%v want=%v", got, want)
	}

	rows, err := store.ListRunsByDate(ctx, "dev", "2026-03-16")
	if err != nil {
		t.Fatalf("list runs by date: %v", err)
	}
	if got, want := len(rows), 1; got != want {
		t.Fatalf("unexpected runs by date count: got=%d want=%d", got, want)
	}
	if got, want := rows[0].RunURL, "https://prow.example.com/run/offset-next"; got != want {
		t.Fatalf("unexpected run url: got=%q want=%q", got, want)
	}
}

func TestListRawFailuresByDateUsesUTCDateProjection(t *testing.T) {
	t.Parallel()

	store := newIntegrationStore(t, "")
	ctx := context.Background()

	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment: "dev",
			RowID:       "row-1",
			RunURL:      "https://prow.example.com/run/1",
			SignatureID: "sig-1",
			OccurredAt:  "2026-03-15T23:30:00-05:00",
			RawText:     "offset failure",
		},
		{
			Environment: "dev",
			RowID:       "row-2",
			RunURL:      "https://prow.example.com/run/2",
			SignatureID: "sig-2",
			OccurredAt:  "2026-03-15T12:00:00Z",
			RawText:     "same day failure",
		},
		{
			Environment: "dev",
			RowID:       "row-3",
			RunURL:      "https://prow.example.com/run/3",
			SignatureID: "sig-3",
			OccurredAt:  "bad-timestamp",
			RawText:     "ignored failure",
		},
	}); err != nil {
		t.Fatalf("upsert raw failures: %v", err)
	}

	rows, err := store.ListRawFailuresByDate(ctx, "dev", "2026-03-16")
	if err != nil {
		t.Fatalf("list raw failures by date: %v", err)
	}
	if got, want := len(rows), 1; got != want {
		t.Fatalf("unexpected raw failures by date count: got=%d want=%d", got, want)
	}
	if got, want := rows[0].RowID, "row-1"; got != want {
		t.Fatalf("unexpected row id: got=%q want=%q", got, want)
	}
}

func TestMetricsQueriesProvideDateScopedQueries(t *testing.T) {
	t.Parallel()

	store := newIntegrationStore(t, "")
	ctx := context.Background()

	if err := store.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-15", Metric: "run_count", Value: 5},
		{Environment: "dev", Date: "2026-03-15", Metric: "failure_count", Value: 2},
		{Environment: "dev", Date: "2026-03-16", Metric: "run_count", Value: 7},
		{Environment: "int", Date: "2026-03-16", Metric: "run_count", Value: 3},
		{Environment: "int", Date: "2026-03-17", Metric: "run_count", Value: 11},
	}); err != nil {
		t.Fatalf("upsert metrics daily: %v", err)
	}

	dates, err := store.ListMetricDates(ctx)
	if err != nil {
		t.Fatalf("list metric dates: %v", err)
	}
	if got, want := dates, []string{"2026-03-15", "2026-03-16", "2026-03-17"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("unexpected metric dates: got=%v want=%v", got, want)
	}

	rows, err := store.ListMetricsDailyForDates(ctx, []string{"int", "dev"}, []string{"2026-03-16", "2026-03-15"})
	if err != nil {
		t.Fatalf("list metrics daily for dates: %v", err)
	}
	if got, want := len(rows), 4; got != want {
		t.Fatalf("unexpected metrics row count: got=%d want=%d", got, want)
	}
	if got, want := rows[0].Environment, "dev"; got != want {
		t.Fatalf("unexpected first row environment: got=%q want=%q", got, want)
	}
	if got, want := rows[0].Date, "2026-03-15"; got != want {
		t.Fatalf("unexpected first row date: got=%q want=%q", got, want)
	}
	if got, want := rows[0].Metric, "failure_count"; got != want {
		t.Fatalf("unexpected first row metric: got=%q want=%q", got, want)
	}

	sums, err := store.SumMetricByEnvironmentForDates(ctx, "run_count", []string{"int", "dev"}, []string{"2026-03-15", "2026-03-16"})
	if err != nil {
		t.Fatalf("sum metric by environment for dates: %v", err)
	}
	if got, want := sums["dev"], 12.0; got != want {
		t.Fatalf("unexpected dev run_count sum: got=%v want=%v", got, want)
	}
	if got, want := sums["int"], 3.0; got != want {
		t.Fatalf("unexpected int run_count sum: got=%v want=%v", got, want)
	}
}

func TestTestMetadataQueriesProvideBelowTargetQueries(t *testing.T) {
	t.Parallel()

	store := newIntegrationStore(t, "")
	ctx := context.Background()

	if err := store.UpsertTestMetadataDaily(ctx, []storecontracts.TestMetadataDailyRecord{
		{Environment: "dev", Date: "2026-03-15", Period: "weekly", TestSuite: "suite-a", TestName: "test-a", CurrentPassPercentage: 91.0, CurrentRuns: 10},
		{Environment: "dev", Date: "2026-03-16", Period: "weekly", TestSuite: "suite-a", TestName: "test-a", CurrentPassPercentage: 89.0, CurrentRuns: 12},
		{Environment: "dev", Date: "2026-03-16", Period: "weekly", TestSuite: "suite-b", TestName: "test-b", CurrentPassPercentage: 70.0, CurrentRuns: 8},
		{Environment: "dev", Date: "2026-03-16", Period: "weekly", TestSuite: "suite-c", TestName: "test-c", CurrentPassPercentage: 99.0, CurrentRuns: 20},
		{Environment: "dev", Date: "2026-03-17", Period: "weekly", TestSuite: "suite-d", TestName: "test-d", CurrentPassPercentage: 60.0, CurrentRuns: 2},
		{Environment: "dev", Date: "2026-03-17", Period: "daily", TestSuite: "suite-e", TestName: "test-e", CurrentPassPercentage: 10.0, CurrentRuns: 50},
		{Environment: "int", Date: "2026-03-16", Period: "weekly", TestSuite: "suite-z", TestName: "test-z", CurrentPassPercentage: 80.0, CurrentRuns: 15},
	}); err != nil {
		t.Fatalf("upsert test metadata daily: %v", err)
	}

	dates, err := store.ListTestMetadataDatesByEnvironment(ctx, "dev", "weekly")
	if err != nil {
		t.Fatalf("list test metadata dates by environment: %v", err)
	}
	if got, want := dates, []string{"2026-03-15", "2026-03-16", "2026-03-17"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("unexpected test metadata dates: got=%v want=%v", got, want)
	}

	rows, err := store.ListBelowTargetTestMetadataByDate(ctx, "dev", "2026-03-16", "weekly", 95.0, 5, 2)
	if err != nil {
		t.Fatalf("list below-target test metadata by date: %v", err)
	}
	if got, want := len(rows), 2; got != want {
		t.Fatalf("unexpected below-target row count: got=%d want=%d", got, want)
	}
	if got, want := rows[0].TestName, "test-b"; got != want {
		t.Fatalf("unexpected first below-target test: got=%q want=%q", got, want)
	}
	if got, want := rows[1].TestName, "test-a"; got != want {
		t.Fatalf("unexpected second below-target test: got=%q want=%q", got, want)
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
