package report

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	frontui "ci-failure-atlas/pkg/frontend/ui"
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
	store, err := postgresstore.New(pool, postgresstore.Options{Week: "2026-03-16"})
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
		StartDate:         "2026-03-16",
		TargetRate:        95.0,
		Week:              "2026-03-16",
		RunLogDayBasePath: "/run-log",
	})
	if err != nil {
		t.Fatalf("generate weekly HTML: %v", err)
	}

	if !strings.Contains(rendered, `/run-log?date=2026-03-16&amp;env=dev`) {
		t.Fatalf("expected lane outcome day label to link to runs page, got %q", rendered)
	}
}

func TestBuildHTMLInlinesGoalDefinitionsInExecutiveTable(t *testing.T) {
	t.Parallel()

	startDate := time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2026, 3, 22, 0, 0, 0, 0, time.UTC)

	rendered := buildHTML(
		startDate,
		endDate,
		[]envReport{
			{
				Environment: "dev",
				Totals: counts{
					RunCount: 10,
				},
			},
			{
				Environment: "int",
				Totals: counts{
					RunCount:     12,
					FailureCount: 1,
				},
			},
		},
		nil,
		95.0,
		nil,
		nil,
		"/run-log",
		frontui.ReportChromeOptions{},
	)

	for _, snippet := range []string{
		">Goal</span>",
		"exec-heading-help",
		"Env</span><span class=\"inline-tooltip align-start",
		"Success</span><span class=\"inline-tooltip align-center",
		"E2E success vs prev</span><span class=\"inline-tooltip align-end",
		"95% - After last push of a PR that merges",
		"95% - All E2E job runs",
	} {
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected rendered report HTML to contain %q", snippet)
		}
	}
	if strings.Contains(rendered, "Goals:") {
		t.Fatalf("did not expect standalone goals section in rendered report HTML: %q", rendered)
	}
}

func TestBuildHTMLUsesUpdatedRunOutcomeCardsAndChartLayout(t *testing.T) {
	t.Parallel()

	startDate := time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2026, 3, 22, 0, 0, 0, 0, time.UTC)

	rendered := buildHTML(
		startDate,
		endDate,
		[]envReport{
			{
				Environment: "dev",
				Days: []dayReport{
					{
						Date: "2026-03-16",
						Counts: counts{
							RunCount:                10,
							FailureCount:            3,
							FailedCIInfraRunCount:   1,
							FailedProvisionRunCount: 1,
							FailedE2ERunCount:       1,
							PostGoodRunCount:        4,
							PostGoodFailedProvision: 1,
							PostGoodFailedE2EJobs:   1,
						},
						PostGoodRunOutcomes: runOutcomes{
							TotalRuns:           4,
							SuccessfulRuns:      2,
							ProvisionFailedRuns: 1,
							E2EFailedRuns:       1,
						},
					},
				},
				Totals: counts{
					RunCount:                10,
					FailureCount:            3,
					FailedCIInfraRunCount:   1,
					FailedProvisionRunCount: 1,
					FailedE2ERunCount:       1,
					PostGoodRunCount:        4,
					PostGoodFailedProvision: 1,
					PostGoodFailedE2EJobs:   1,
				},
			},
		},
		nil,
		95.0,
		nil,
		nil,
		"/run-log",
		frontui.ReportChromeOptions{},
	)

	required := []string{
		"Provision success",
		"E2E success",
		"Provisioning here means infrastructure setup before E2E tests start.",
		"These DEV-only metrics include only runs that happened after the final push to a PR that later merged.",
		"class=\"outcome-total\">10 runs</span>",
		"class=\"outcome-total\">4 runs</span>",
		"Successful runs: 7 of 10 runs (70.0%)",
		"role=\"tooltip\">Successful runs: 7 of 10 runs (70.0%)",
		"class=\"inline-tooltip align-center outcome-segment-wrap\" data-inline-tooltip style=\"left: 0.000000%; width: 70.000000%; background: #5f8a69;\"",
		"class=\"outcome-segment-label\">70%</span>",
		"role=\"button\" class=\"inline-tooltip-trigger outcome-segment seg-success\"",
		"E2E Jobs (after last push of merged PR)</div><span class=\"inline-tooltip align-start",
		"Success Rate (after last push of merged PR)</div><span class=\"inline-tooltip align-center",
		"E2E success (after last push of merged PR)</div><span class=\"inline-tooltip align-end",
		"background: #5f8a69;",
	}
	for _, snippet := range required {
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected rendered report HTML to contain %q", snippet)
		}
	}

	for _, stale := range []string{
		"Provision step success rate (Other excluded)",
		"E2E success (runs reaching E2E)",
		"Chart mode:",
		"mode-count",
		"mode-percent",
		"S:7",
		"<button type=\"button\" class=\"inline-tooltip-trigger outcome-segment",
		"title=\"Successful runs: 7 of 10 runs (70.0%)\"",
	} {
		if strings.Contains(rendered, stale) {
			t.Fatalf("did not expect rendered report HTML to contain %q", stale)
		}
	}
}

func TestRenderOutcomeChartScalesBarsByRelativeRunVolume(t *testing.T) {
	t.Parallel()

	rendered := renderOutcomeChart(
		"Daily Run Outcomes",
		[]outcomeChartDay{
			{
				Date:                "2026-03-16",
				TotalRuns:           10,
				SuccessfulRuns:      7,
				ProvisionFailedRuns: 1,
				E2EFailedRuns:       1,
				CIInfraFailedRuns:   1,
			},
			{
				Date:              "2026-03-17",
				TotalRuns:         1,
				CIInfraFailedRuns: 1,
			},
		},
		"dev",
		"/run-log",
		"Successful runs",
	)

	for _, snippet := range []string{
		"class=\"outcome-bar-shell\"",
		"class=\"outcome-bar\" style=\"width: 100.000000%;\"",
		"class=\"outcome-bar\" style=\"width: 10.000000%;\"",
		"Other failures: 1 of 1 runs (100.0%)",
		"style=\"left: 0.000000%; width: 100.000000%; background: #708092;\"",
	} {
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected scaled outcome chart HTML to contain %q", snippet)
		}
	}
	if strings.Contains(rendered, "class=\"outcome-segment-label\">100%</span>") {
		t.Fatalf("did not expect a 100%% label inside the narrow 10%%-width scaled bar")
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
