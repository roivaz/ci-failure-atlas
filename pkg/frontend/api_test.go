package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	frontservice "ci-failure-atlas/pkg/frontend/readmodel"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	"ci-failure-atlas/pkg/store/postgres/initdb"
	"ci-failure-atlas/pkg/store/postgres/migrations"
	"ci-failure-atlas/pkg/testsupport/pgtest"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHandleAPIDailyFailurePatternsRouteRemoved(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/failure-patterns/daily", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusNotFound; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
}

func TestHandleReviewRoutesRemoved(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/review", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if got, want := recorder.Code, http.StatusNotFound; got != want {
		t.Fatalf("unexpected /review status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/review/actions/links", strings.NewReader("week=2026-03-15"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if got, want := recorder.Code, http.StatusNotFound; got != want {
		t.Fatalf("unexpected /review/actions/links status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
}

func TestHandleHealthEndpoints(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected /healthz status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected /readyz status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
}

func TestHandleReadyzReturnsServiceUnavailableWhenPostgresClosed(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	fixture.pool.Close()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusServiceUnavailable; got != want {
		t.Fatalf("unexpected /readyz status code after pool close: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
}

func TestHandleAPIFailurePatternsReturnsJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, reviewAPIMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/1",
			JobName:     "periodic-ci",
			Failed:      true,
			OccurredAt:  "2026-03-16T08:00:00Z",
		},
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/2",
			JobName:     "periodic-ci-nodepool",
			Failed:      true,
			OccurredAt:  "2026-03-16T09:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, reviewAPIRawFailures()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}
	if err := store.UpsertMetricsDaily(ctx, []storecontracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-16", Metric: "run_count", Value: 5},
	}); err != nil {
		t.Fatalf("seed metrics daily: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/failure-patterns/window?start_date=2026-03-16&end_date=2026-03-16&env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("unexpected content type: %q", got)
	}

	var payload frontservice.FailurePatternsData
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if strings.Contains(recorder.Body.String(), "\"resolved_week\"") {
		t.Fatalf("did not expect resolved_week in failure-pattern payload: %s", recorder.Body.String())
	}
	if body := recorder.Body.String(); !strings.Contains(body, "\"failure_pattern_id\"") || !strings.Contains(body, "\"matched_occurrences\"") || !strings.Contains(body, "\"runs_affected\"") {
		t.Fatalf("expected renamed failure-pattern keys in body, got %q", body)
	} else if strings.Contains(body, "\"cluster_id\"") || strings.Contains(body, "\"matched_failure_count\"") || strings.Contains(body, "\"jobs_affected\"") {
		t.Fatalf("did not expect stale failure-pattern keys in body, got %q", body)
	}
	if got, want := payload.Meta.Timezone, "UTC"; got != want {
		t.Fatalf("unexpected failure-pattern payload timezone: got=%q want=%q", got, want)
	}
	if got, want := len(payload.Environments), 1; got != want {
		t.Fatalf("unexpected environment count: got=%d want=%d", got, want)
	}
	if got, want := payload.Environments[0].Summary.TotalRuns, 5; got != want {
		t.Fatalf("unexpected total runs: got=%d want=%d", got, want)
	}
	if got, want := len(payload.Environments[0].Rows), 2; got != want {
		t.Fatalf("unexpected row count: got=%d want=%d", got, want)
	}
	var linkedRow *frontservice.FailurePatternsRow
	for index := range payload.Environments[0].Rows {
		row := &payload.Environments[0].Rows[index]
		if row.ClusterID == "cluster-dev-linked" {
			linkedRow = row
			break
		}
	}
	if linkedRow == nil {
		t.Fatalf("expected linked failure-pattern row in payload")
	}
	if got, want := len(linkedRow.FullErrorSamples), 1; got != want {
		t.Fatalf("unexpected full error sample count: got=%d want=%d", got, want)
	}
	if got, want := linkedRow.FullErrorSamples[0], reviewAPILongRawFailureText(); got != want {
		t.Fatalf("expected full raw failure sample without truncation: got=%q want=%q", got, want)
	}
}

func TestHandleAPIFailurePatternsReturnsJSONError(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/failure-patterns/window?start_date=2026-03-16", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusBadRequest; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got := payload["error"]; !strings.Contains(got, "start_date and end_date must both be set") {
		t.Fatalf("unexpected error message: %q", got)
	}
}

func TestHandleFailurePatternsPageWindowRendersHTML(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	targetStore := fixture.openWeekStore(t, "2026-03-15")
	laterStore := fixture.openWeekStore(t, "2026-03-22")
	if err := targetStore.ReplaceMaterializedWeek(ctx, reviewAPIMaterializedWeek()); err != nil {
		t.Fatalf("seed target materialized week: %v", err)
	}
	if err := laterStore.ReplaceMaterializedWeek(ctx, reviewAPIMaterializedWeek()); err != nil {
		t.Fatalf("seed later materialized week: %v", err)
	}
	if err := targetStore.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/1",
			JobName:     "periodic-ci",
			Failed:      true,
			OccurredAt:  "2026-03-16T08:00:00Z",
		},
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/2",
			JobName:     "periodic-ci-nodepool",
			Failed:      true,
			OccurredAt:  "2026-03-16T09:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := targetStore.UpsertRawFailures(ctx, reviewAPIRawFailures()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/failure-patterns?start_date=2026-03-16&end_date=2026-03-16&env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Fatalf("unexpected content type: %q", got)
	}
	body := recorder.Body.String()
	if strings.Contains(body, "Resolved semantic week (UTC)") {
		t.Fatalf("did not expect resolved week note in body, got %q", body)
	}
	if !strings.Contains(body, "Runs affected, run impact, and seen-in are recomputed across the selected window") {
		t.Fatalf("expected failure-pattern guidance in body, got %q", body)
	}
	if !strings.Contains(body, "OAuth timeout") {
		t.Fatalf("expected failure-pattern row phrase in body, got %q", body)
	}
	if !strings.Contains(body, `name="start_date" value="2026-03-16"`) {
		t.Fatalf("expected start_date control in body, got %q", body)
	}
	if !strings.Contains(body, `name="end_date" value="2026-03-16"`) {
		t.Fatalf("expected end_date control in body, got %q", body)
	}
	if !strings.Contains(body, `name="env" value="dev"`) {
		t.Fatalf("expected env control in body, got %q", body)
	}
	if strings.Contains(body, `type="hidden" name="week"`) {
		t.Fatalf("did not expect hidden week input in body, got %q", body)
	}
	if !strings.Contains(body, "Reset to full week") {
		t.Fatalf("expected reset link in body, got %q", body)
	}
}

func TestHandleFailurePatternsPageDefaultsToFullWeekWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, reviewAPIMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/1",
			JobName:     "periodic-ci",
			Failed:      true,
			OccurredAt:  "2026-03-16T08:00:00Z",
		},
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/2",
			JobName:     "periodic-ci-nodepool",
			Failed:      true,
			OccurredAt:  "2026-03-16T09:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, reviewAPIRawFailures()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/failure-patterns?week=2026-03-15", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `name="start_date" value="2026-03-15"`) {
		t.Fatalf("expected default start_date in body, got %q", body)
	}
	if !strings.Contains(body, `name="end_date" value="2026-03-21"`) {
		t.Fatalf("expected default end_date in body, got %q", body)
	}
	if !strings.Contains(body, "Apply window") {
		t.Fatalf("expected apply button in body, got %q", body)
	}
}

func TestHandleAPIRunsDayReturnsJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, jobHistoryAPIMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertRuns(ctx, jobHistoryAPIRuns()); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, jobHistoryAPIRawFailures()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}
	fixture.seedDeprecatedPhase3Links(t,
		semanticcontracts.Phase3LinkRecord{
			SchemaVersion: semanticcontracts.CurrentSchemaVersion,
			IssueID:       "QE-999",
			Environment:   "dev",
			RunURL:        "https://prow.example.com/view/job-history-1",
			RowID:         "job-history-row-1",
			UpdatedAt:     "2026-03-16T12:00:00Z",
		},
		semanticcontracts.Phase3LinkRecord{
			SchemaVersion: semanticcontracts.CurrentSchemaVersion,
			IssueID:       "QE-999",
			Environment:   "dev",
			RunURL:        "https://prow.example.com/view/job-history-1",
			RowID:         "job-history-row-2",
			UpdatedAt:     "2026-03-16T12:00:00Z",
		},
	)

	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/run-log/day?date=2026-03-16&env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("unexpected content type: %q", got)
	}

	var payload frontservice.RunLogDayData
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if strings.Contains(recorder.Body.String(), "\"resolved_week\"") {
		t.Fatalf("did not expect resolved_week in runs payload: %s", recorder.Body.String())
	}
	if body := recorder.Body.String(); !strings.Contains(body, "\"failure_pattern_match\"") || !strings.Contains(body, "\"failure_pattern_summary\"") || !strings.Contains(body, "\"failed_at\"") {
		t.Fatalf("expected renamed run-log keys in body, got %q", body)
	} else if strings.Contains(body, "\"semantic_attachment\"") || strings.Contains(body, "\"semantic_rollups\"") || strings.Contains(body, "\"lanes\"") {
		t.Fatalf("did not expect stale run-log keys in body, got %q", body)
	}
	if got, want := payload.Meta.Timezone, "UTC"; got != want {
		t.Fatalf("unexpected runs payload timezone: got=%q want=%q", got, want)
	}
	if got, want := len(payload.Environments), 1; got != want {
		t.Fatalf("unexpected environment count: got=%d want=%d", got, want)
	}
	environment := payload.Environments[0]
	if got, want := environment.Summary.TotalRuns, 3; got != want {
		t.Fatalf("unexpected total runs: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.FailedRunsWithoutRawRows, 1; got != want {
		t.Fatalf("unexpected failed runs without raw rows: got=%d want=%d", got, want)
	}
	if got, want := environment.Summary.RunsUnmatchedSignatures, 1; got != want {
		t.Fatalf("unexpected unmatched signature runs: got=%d want=%d", got, want)
	}

	var multipleRun *frontservice.JobHistoryRunRow
	var unmatchedRun *frontservice.JobHistoryRunRow
	var noRawRun *frontservice.JobHistoryRunRow
	for index := range environment.Runs {
		row := &environment.Runs[index]
		switch row.Run.RunURL {
		case "https://prow.example.com/view/job-history-1":
			multipleRun = row
		case "https://prow.example.com/view/job-history-2":
			unmatchedRun = row
		case "https://prow.example.com/view/job-history-3":
			noRawRun = row
		}
	}
	if multipleRun == nil || unmatchedRun == nil || noRawRun == nil {
		t.Fatalf("expected matched, unmatched, and no-raw runs in payload")
	}
	if got, want := multipleRun.SemanticRollups.AttachmentSummary, "multiple_clustered"; got != want {
		t.Fatalf("unexpected multiple run summary: got=%q want=%q", got, want)
	}
	if got, want := multipleRun.FailedTestCount, 2; got != want {
		t.Fatalf("unexpected multiple run failed test count: got=%d want=%d", got, want)
	}
	if got, want := multipleRun.BadPRScore, 3; got != want {
		t.Fatalf("unexpected multiple run bad PR score: got=%d want=%d", got, want)
	}
	if got := len(multipleRun.BadPRReasons); got == 0 {
		t.Fatalf("expected multiple run bad PR reasons in payload")
	}
	if got, want := len(multipleRun.Lanes), 1; got != want {
		t.Fatalf("unexpected multiple run lane count: got=%d want=%d", got, want)
	}
	if got, want := multipleRun.Lanes[0], "upgrade"; got != want {
		t.Fatalf("unexpected multiple run lane: got=%q want=%q", got, want)
	}
	if got, want := unmatchedRun.SemanticRollups.AttachmentSummary, "unmatched_only"; got != want {
		t.Fatalf("unexpected unmatched run summary: got=%q want=%q", got, want)
	}
	if got, want := unmatchedRun.FailedTestCount, 1; got != want {
		t.Fatalf("unexpected unmatched run failed test count: got=%d want=%d", got, want)
	}
	if got, want := noRawRun.SemanticRollups.AttachmentSummary, "failed_without_raw_rows"; got != want {
		t.Fatalf("unexpected no-raw run summary: got=%q want=%q", got, want)
	}
}

func TestHandleAPIRunsDayReturnsValidationError(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/run-log/day?env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusBadRequest; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got := payload["error"]; !strings.Contains(got, "invalid date") {
		t.Fatalf("unexpected error message: %q", got)
	}
}

func TestHandleRunsPageRendersHTML(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, jobHistoryAPIMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertRuns(ctx, jobHistoryAPIRuns()); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, jobHistoryAPIRawFailures()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/run-log?date=2026-03-16&env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Fatalf("unexpected content type: %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "CI Runs") {
		t.Fatalf("expected runs title in body, got %q", body)
	}
	if !strings.Contains(body, "Open failure patterns for this day") {
		t.Fatalf("expected failure-pattern CTA in body, got %q", body)
	}
	if !strings.Contains(body, "View JSON API") {
		t.Fatalf("expected JSON API link in body, got %q", body)
	}
	if !strings.Contains(body, "Date (UTC)") {
		t.Fatalf("expected UTC date label in body, got %q", body)
	}
	if strings.Contains(body, "Generated (UTC)") {
		t.Fatalf("did not expect UTC generated label in body, got %q", body)
	}
	if !strings.Contains(body, "<th>Time (UTC)</th>") {
		t.Fatalf("expected UTC time header in body, got %q", body)
	}
	if !strings.Contains(body, "<th>Failed at</th>") {
		t.Fatalf("expected Failed at column in body, got %q", body)
	}
	if !strings.Contains(body, "<th>Failed tests</th>") {
		t.Fatalf("expected failed tests column in body, got %q", body)
	}
	if !strings.Contains(body, "<th>Details</th>") {
		t.Fatalf("expected details column in body, got %q", body)
	}
	if strings.Contains(body, "Semantic status") {
		t.Fatalf("did not expect semantic status column in body, got %q", body)
	}
	if strings.Contains(body, "Runs are listed once and enriched with semantic attachments") {
		t.Fatalf("did not expect internal implementation details text in body, got %q", body)
	}
	if strings.Contains(body, "Runs with semantic attachment") {
		t.Fatalf("did not expect semantic attachment card in body, got %q", body)
	}
	if !strings.Contains(body, "Multiple failures (2)") {
		t.Fatalf("expected simplified multiple failure summary in body, got %q", body)
	}
	if !strings.Contains(body, "Installer failed to reach bootstrap machine") {
		t.Fatalf("expected unmatched failure text in body, got %q", body)
	}
	if !strings.Contains(body, "Failure details unavailable") {
		t.Fatalf("expected failed-without-raw-rows summary in body, got %q", body)
	}
	if !strings.Contains(body, "Show raw failure") {
		t.Fatalf("expected raw failure toggle in body, got %q", body)
	}
	if !strings.Contains(body, ">upgrade<") {
		t.Fatalf("expected lane value in body, got %q", body)
	}
	if !strings.Contains(body, `class="job-link" href="https://prow.example.com/view/job-history-1"`) {
		t.Fatalf("expected job name to be the run link, got %q", body)
	}
	if !strings.Contains(body, `class="bad-pr-flag"`) {
		t.Fatalf("expected bad-pr flag in PR column, got %q", body)
	}
	if !strings.Contains(body, "#123 (open)") {
		t.Fatalf("expected open PR state label in body, got %q", body)
	}
	if !strings.Contains(body, "/failure-patterns?") || !strings.Contains(body, "start_date=2026-03-16") || !strings.Contains(body, "end_date=2026-03-16") {
		t.Fatalf("expected same-day failure-pattern link in body, got %q", body)
	}
}

func TestHandleAPIReviewSignalsWeekReturnsJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, reviewAPIMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/review/signals/week?week=2026-03-15", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("unexpected content type: %q", got)
	}

	var payload frontservice.ReviewSignalsWeekSnapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := payload.Week, "2026-03-15"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := payload.Timezone, "UTC"; got != want {
		t.Fatalf("unexpected review-signals timezone: got=%q want=%q", got, want)
	}
	if got, want := payload.TotalSignals, 2; got != want {
		t.Fatalf("unexpected total signal count: got=%d want=%d", got, want)
	}
	if got, want := payload.SignalsByReason["low_confidence_evidence"], 1; got != want {
		t.Fatalf("unexpected low-confidence signal count: got=%d want=%d", got, want)
	}
	if got, want := payload.SignalsByReason["ambiguous_provider_merge"], 1; got != want {
		t.Fatalf("unexpected ambiguous-provider signal count: got=%d want=%d", got, want)
	}

	rowsByReason := map[string]frontservice.ReviewSignalRow{}
	for _, row := range payload.Rows {
		rowsByReason[row.Reason] = row
	}

	lowConfidence, ok := rowsByReason["low_confidence_evidence"]
	if !ok {
		t.Fatalf("missing low_confidence_evidence row: %+v", payload.Rows)
	}
	if got, want := len(lowConfidence.MatchedFailurePatterns), 1; got != want {
		t.Fatalf("unexpected low-confidence matched failure-pattern count: got=%d want=%d", got, want)
	}
	if got, want := lowConfidence.MatchedFailurePatterns[0].FailurePatternID, "cluster-dev-linked"; got != want {
		t.Fatalf("unexpected low-confidence matched failure-pattern id: got=%q want=%q", got, want)
	}

	ambiguousProvider, ok := rowsByReason["ambiguous_provider_merge"]
	if !ok {
		t.Fatalf("missing ambiguous_provider_merge row: %+v", payload.Rows)
	}
	if got, want := len(ambiguousProvider.MatchedFailurePatterns), 1; got != want {
		t.Fatalf("unexpected ambiguous-provider matched failure-pattern count: got=%d want=%d", got, want)
	}
	if got, want := ambiguousProvider.MatchedFailurePatterns[0].FailurePatternID, "cluster-dev-unlinked"; got != want {
		t.Fatalf("unexpected ambiguous-provider matched failure-pattern id: got=%q want=%q", got, want)
	}
}

type handlerFixture struct {
	pool *pgxpool.Pool
}

func newHandlerFixture(t *testing.T) *handlerFixture {
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

	return &handlerFixture{pool: pool}
}

func (f *handlerFixture) seedDeprecatedPhase3Links(t *testing.T, rows ...semanticcontracts.Phase3LinkRecord) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	ctx := context.Background()
	_, err := f.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS cfa_phase3_links (
  environment TEXT NOT NULL,
  run_url TEXT NOT NULL,
  row_id TEXT NOT NULL,
  issue_id TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  payload JSONB NOT NULL,
  PRIMARY KEY (environment, run_url, row_id)
)`)
	if err != nil {
		t.Fatalf("ensure deprecated phase3 link table: %v", err)
	}
	for _, row := range rows {
		payload, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("marshal deprecated phase3 link payload: %v", err)
		}
		_, err = f.pool.Exec(ctx, `
INSERT INTO cfa_phase3_links (environment, run_url, row_id, issue_id, updated_at, payload)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (environment, run_url, row_id)
DO UPDATE SET issue_id = EXCLUDED.issue_id, updated_at = EXCLUDED.updated_at, payload = EXCLUDED.payload
`, row.Environment, row.RunURL, row.RowID, row.IssueID, row.UpdatedAt, payload)
		if err != nil {
			t.Fatalf("insert deprecated phase3 link: %v", err)
		}
	}
}

func (f *handlerFixture) openWeekStore(t *testing.T, week string) storeWithClose {
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
	UpsertRuns(context.Context, []storecontracts.RunRecord) error
	UpsertRawFailures(context.Context, []storecontracts.RawFailureRecord) error
	UpsertMetricsDaily(context.Context, []storecontracts.MetricDailyRecord) error
	Close() error
}

func reviewAPIMaterializedWeek() storecontracts.MaterializedWeek {
	return storecontracts.MaterializedWeek{
		FailurePatterns: []semanticcontracts.FailurePatternRecord{
			{
				SchemaVersion:                semanticcontracts.CurrentSchemaVersion,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-linked",
				CanonicalEvidencePhrase:      "OAuth timeout",
				SearchQueryPhrase:            "OAuth timeout",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/1",
				SearchQuerySourceSignatureID: "sig-linked",
				SupportCount:                 2,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "upgrade",
						JobName:      "periodic-ci",
						TestName:     "should oauth",
						SupportCount: 2,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-linked"},
				MemberSignatureIDs:     []string{"sig-linked"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-1",
						RunURL:      "https://prow.example.com/view/1",
						OccurredAt:  "2026-03-16T08:00:00Z",
						SignatureID: "sig-linked",
					},
				},
			},
			{
				SchemaVersion:                semanticcontracts.CurrentSchemaVersion,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-unlinked",
				CanonicalEvidencePhrase:      "CreateNodePool timeout 45 min",
				SearchQueryPhrase:            "CreateNodePool timeout 45 min",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/2",
				SearchQuerySourceSignatureID: "sig-unlinked",
				SupportCount:                 1,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "install",
						JobName:      "periodic-ci-nodepool",
						TestName:     "should create nodepool",
						SupportCount: 1,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-unlinked"},
				MemberSignatureIDs:     []string{"sig-unlinked"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-2",
						RunURL:      "https://prow.example.com/view/2",
						OccurredAt:  "2026-03-16T09:00:00Z",
						SignatureID: "sig-unlinked",
					},
				},
			},
		},
		ReviewQueue: []semanticcontracts.ReviewItemRecord{
			{
				SchemaVersion:                        semanticcontracts.CurrentSchemaVersion,
				Environment:                          "dev",
				ReviewItemID:                         "review-low-confidence",
				Phase:                                "phase1",
				Reason:                               "low_confidence_evidence",
				ProposedCanonicalEvidencePhrase:      "OAuth timeout",
				ProposedSearchQueryPhrase:            "OAuth timeout",
				ProposedSearchQuerySourceRunURL:      "https://prow.example.com/view/1",
				ProposedSearchQuerySourceSignatureID: "sig-linked",
				SourcePhase1ClusterIDs:               []string{"phase1-linked"},
				MemberSignatureIDs:                   []string{"sig-linked"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-1",
						RunURL:      "https://prow.example.com/view/1",
						OccurredAt:  "2026-03-16T08:00:00Z",
						SignatureID: "sig-linked",
					},
				},
			},
			{
				SchemaVersion:                        semanticcontracts.CurrentSchemaVersion,
				Environment:                          "dev",
				ReviewItemID:                         "review-ambiguous-provider",
				Phase:                                "phase2",
				Reason:                               "ambiguous_provider_merge",
				ProposedCanonicalEvidencePhrase:      "CreateNodePool timeout 45 min",
				ProposedSearchQueryPhrase:            "CreateNodePool timeout 45 min",
				ProposedSearchQuerySourceRunURL:      "https://prow.example.com/view/2",
				ProposedSearchQuerySourceSignatureID: "sig-unlinked",
				SourcePhase1ClusterIDs:               []string{"phase1-unlinked"},
				MemberSignatureIDs:                   []string{"sig-unlinked"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-2",
						RunURL:      "https://prow.example.com/view/2",
						OccurredAt:  "2026-03-16T09:00:00Z",
						SignatureID: "sig-unlinked",
					},
				},
			},
		},
	}
}

func reviewAPIRawFailures() []storecontracts.RawFailureRecord {
	return []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-1",
			RunURL:         "https://prow.example.com/view/1",
			TestName:       "should oauth",
			TestSuite:      "suite-a",
			SignatureID:    "sig-linked",
			OccurredAt:     "2026-03-16T08:00:00Z",
			RawText:        reviewAPILongRawFailureText(),
			NormalizedText: "oauth timeout while waiting for cluster operator",
		},
		{
			Environment:    "dev",
			RowID:          "row-2",
			RunURL:         "https://prow.example.com/view/2",
			TestName:       "should create nodepool",
			TestSuite:      "suite-b",
			SignatureID:    "sig-unlinked",
			OccurredAt:     "2026-03-16T09:00:00Z",
			RawText:        "CreateNodePool timeout after 45 minutes",
			NormalizedText: "createnodepool timeout after 45 minutes",
		},
	}
}

func reviewAPILongRawFailureText() string {
	return strings.Join([]string{
		`time=2026-03-16T08:00:00Z level=INFO msg="Running step." serviceGroup=Microsoft.Azure.ARO.HCP.ACM resourceGroup=management step=deploy-mce-config description="Step deploy-mce-config\n Kind: Helm\n"`,
		`time=2026-03-16T08:00:01Z level=ERROR msg="error running Helm release deployment Step, failed to deploy helm release: failed post-install: resource not ready, name: finalize-mce-config, kind: Job, status: InProgress"`,
		`time=2026-03-16T08:04:01Z level=ERROR msg="context deadline exceeded"`,
	}, "\n")
}

func jobHistoryAPIMaterializedWeek() storecontracts.MaterializedWeek {
	return storecontracts.MaterializedWeek{
		FailurePatterns: []semanticcontracts.FailurePatternRecord{
			{
				SchemaVersion:                semanticcontracts.CurrentSchemaVersion,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-oauth",
				CanonicalEvidencePhrase:      "OAuth timeout",
				SearchQueryPhrase:            "OAuth timeout",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/job-history-1",
				SearchQuerySourceSignatureID: "sig-oauth",
				SupportCount:                 4,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "upgrade",
						JobName:      "periodic-ci",
						TestName:     "should oauth",
						SupportCount: 4,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-oauth"},
				MemberSignatureIDs:     []string{"sig-oauth"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "job-history-row-1",
						RunURL:      "https://prow.example.com/view/job-history-1",
						OccurredAt:  "2026-03-16T08:00:00Z",
						SignatureID: "sig-oauth",
						PRNumber:    123,
					},
				},
			},
			{
				SchemaVersion:                semanticcontracts.CurrentSchemaVersion,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-api-throttle",
				CanonicalEvidencePhrase:      "API throttling",
				SearchQueryPhrase:            "API throttling",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/job-history-1",
				SearchQuerySourceSignatureID: "sig-throttle",
				SupportCount:                 2,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "upgrade",
						JobName:      "periodic-ci",
						TestName:     "should throttle",
						SupportCount: 2,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-throttle"},
				MemberSignatureIDs:     []string{"sig-throttle"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "job-history-row-2",
						RunURL:      "https://prow.example.com/view/job-history-1",
						OccurredAt:  "2026-03-16T08:05:00Z",
						SignatureID: "sig-throttle",
						PRNumber:    123,
					},
				},
			},
		},
	}
}

func jobHistoryAPIRuns() []storecontracts.RunRecord {
	return []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/job-history-1",
			JobName:     "periodic-ci",
			PRNumber:    123,
			PRState:     "open",
			PRSHA:       "1111111abcdef",
			Failed:      true,
			OccurredAt:  "2026-03-16T08:00:00Z",
		},
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/job-history-2",
			JobName:     "periodic-ci-install",
			Failed:      true,
			OccurredAt:  "2026-03-16T09:00:00Z",
		},
		{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/job-history-3",
			JobName:     "periodic-ci-missing-raw",
			Failed:      true,
			OccurredAt:  "2026-03-16T10:00:00Z",
		},
	}
}

func jobHistoryAPIRawFailures() []storecontracts.RawFailureRecord {
	return []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "job-history-row-1",
			RunURL:         "https://prow.example.com/view/job-history-1",
			TestName:       "should oauth",
			TestSuite:      "suite-a",
			SignatureID:    "sig-oauth",
			OccurredAt:     "2026-03-16T08:00:00Z",
			RawText:        "OAuth timeout while waiting for cluster operator",
			NormalizedText: "oauth timeout while waiting for cluster operator",
		},
		{
			Environment:    "dev",
			RowID:          "job-history-row-2",
			RunURL:         "https://prow.example.com/view/job-history-1",
			TestName:       "should throttle",
			TestSuite:      "suite-a",
			SignatureID:    "sig-throttle",
			OccurredAt:     "2026-03-16T08:05:00Z",
			RawText:        "API throttling while reconciling install state",
			NormalizedText: "api throttling while reconciling install state",
		},
		{
			Environment:    "dev",
			RowID:          "job-history-row-3",
			RunURL:         "https://prow.example.com/view/job-history-2",
			TestName:       "should install",
			TestSuite:      "suite-b",
			SignatureID:    "sig-unmatched",
			OccurredAt:     "2026-03-16T09:00:00Z",
			RawText:        "Installer failed to reach bootstrap machine",
			NormalizedText: "installer failed to reach bootstrap machine",
		},
	}
}
