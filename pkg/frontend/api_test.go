package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	frontservice "ci-failure-atlas/pkg/frontend/service"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	"ci-failure-atlas/pkg/store/postgres/initdb"
	"ci-failure-atlas/pkg/store/postgres/migrations"
	"ci-failure-atlas/pkg/testsupport/pgtest"

	"github.com/jackc/pgx/v5/pgxpool"
)

type reviewAPIWeekTestContributingTest struct {
	Lane         string `json:"lane"`
	JobName      string `json:"job_name"`
	TestName     string `json:"test_name"`
	SupportCount int    `json:"support_count"`
}

type reviewAPIWeekTestChildRow struct {
	ClusterID       string `json:"cluster_id"`
	SelectionID     string `json:"selection_id"`
	Phase3ClusterID string `json:"phase3_cluster_id"`
}

type reviewAPIWeekTestRow struct {
	Environment       string                              `json:"environment"`
	ClusterID         string                              `json:"cluster_id"`
	SelectionID       string                              `json:"selection_id"`
	Phrase            string                              `json:"phrase"`
	SearchQuery       string                              `json:"search_query"`
	SupportCount      int                                 `json:"support_count"`
	Phase3ClusterID   string                              `json:"phase3_cluster_id"`
	IsLinked          bool                                `json:"is_linked"`
	Lane              string                              `json:"lane"`
	JobName           string                              `json:"job_name"`
	TestName          string                              `json:"test_name"`
	ContributingTests []reviewAPIWeekTestContributingTest `json:"contributing_tests"`
	FullErrorSamples  []string                            `json:"full_error_samples"`
	LinkedChildren    []reviewAPIWeekTestChildRow         `json:"linked_children"`
}

type reviewAPIWeekTestPayload struct {
	Week          string                 `json:"week"`
	Timezone      string                 `json:"timezone"`
	TotalClusters int                    `json:"total_clusters"`
	Rows          []reviewAPIWeekTestRow `json:"rows"`
}

type reviewActionTestResponse struct {
	OK                   bool     `json:"ok"`
	Week                 string   `json:"week"`
	Action               string   `json:"action"`
	Notice               string   `json:"notice"`
	RedirectURL          string   `json:"redirect_url"`
	SelectedClusterIDs   []string `json:"selected_cluster_ids"`
	Phase3ClusterID      string   `json:"phase3_cluster_id"`
	Created              bool     `json:"created"`
	SelectedAnchorCount  int      `json:"selected_anchor_count"`
	CrossWeekAnchorCount int      `json:"cross_week_anchor_count"`
	TotalAnchorCount     int      `json:"total_anchor_count"`
}

func TestHandleAPIDailyTriageRouteRemoved(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/triage/daily", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusNotFound; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
}

func TestHandleAPIWindowedTriageReturnsJSON(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodGet, "/api/triage/window?start_date=2026-03-16&end_date=2026-03-16&env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("unexpected content type: %q", got)
	}

	var payload frontservice.WindowedTriageData
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := payload.Meta.ResolvedWeek, "2026-03-15"; got != want {
		t.Fatalf("unexpected resolved week: got=%q want=%q", got, want)
	}
	if got, want := payload.Meta.Timezone, "UTC"; got != want {
		t.Fatalf("unexpected windowed triage payload timezone: got=%q want=%q", got, want)
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
}

func TestHandleAPIWindowedTriageReturnsJSONError(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/triage/window?start_date=2026-03-16", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusBadRequest; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got := payload["error"]; !strings.Contains(got, "invalid end_date") {
		t.Fatalf("unexpected error message: %q", got)
	}
}

func TestHandleTriagePageWindowRendersHTML(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodGet, "/triage?start_date=2026-03-16&end_date=2026-03-16&env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Fatalf("unexpected content type: %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "Resolved semantic week (UTC)") {
		t.Fatalf("expected resolved week note in body, got %q", body)
	}
	if !strings.Contains(body, "Jobs affected, impact, and seen-in values reflect the selected window") {
		t.Fatalf("expected windowed triage guidance in body, got %q", body)
	}
	if !strings.Contains(body, "OAuth timeout") {
		t.Fatalf("expected triage row phrase in body, got %q", body)
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
	if !strings.Contains(body, `type="hidden" name="week" value="2026-03-15"`) {
		t.Fatalf("expected resolved week hidden input in body, got %q", body)
	}
	if !strings.Contains(body, "Reset to full week") {
		t.Fatalf("expected reset link in body, got %q", body)
	}
}

func TestHandleTriagePageDefaultsToFullWeekWindow(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodGet, "/triage?week=2026-03-15", nil)
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
	if err := store.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "QE-999",
			Environment:   "dev",
			RunURL:        "https://prow.example.com/view/job-history-1",
			RowID:         "job-history-row-1",
		},
	}); err != nil {
		t.Fatalf("seed phase3 link: %v", err)
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

	req := httptest.NewRequest(http.MethodGet, "/api/runs/day?date=2026-03-16&env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("unexpected content type: %q", got)
	}

	var payload frontservice.JobHistoryDayData
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := payload.Meta.ResolvedWeek, "2026-03-15"; got != want {
		t.Fatalf("unexpected resolved week: got=%q want=%q", got, want)
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

	req := httptest.NewRequest(http.MethodGet, "/api/runs/day?env=dev", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/runs?date=2026-03-16&env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Fatalf("unexpected content type: %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "CI Day Run History") {
		t.Fatalf("expected day run history title in body, got %q", body)
	}
	if !strings.Contains(body, "Open triage for this day") {
		t.Fatalf("expected triage CTA in body, got %q", body)
	}
	if !strings.Contains(body, "View JSON API") {
		t.Fatalf("expected JSON API link in body, got %q", body)
	}
	if !strings.Contains(body, "Date (UTC)") {
		t.Fatalf("expected UTC date label in body, got %q", body)
	}
	if !strings.Contains(body, "Generated (UTC)") {
		t.Fatalf("expected UTC generated label in body, got %q", body)
	}
	if !strings.Contains(body, "<th>Time (UTC)</th>") {
		t.Fatalf("expected UTC time header in body, got %q", body)
	}
	if !strings.Contains(body, "<th>Lane</th>") {
		t.Fatalf("expected lane column in body, got %q", body)
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
	if !strings.Contains(body, "/triage?") || !strings.Contains(body, "start_date=2026-03-16") || !strings.Contains(body, "end_date=2026-03-16") {
		t.Fatalf("expected same-day triage link in body, got %q", body)
	}
}

func TestHandleReviewAPIWeekReturnsJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, reviewAPIMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertPhase3Issues(ctx, []semanticcontracts.Phase3IssueRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "QE-123",
			Title:         "OAuth flake",
			CreatedAt:     "2026-03-16T12:00:00Z",
			UpdatedAt:     "2026-03-16T12:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed phase3 issue: %v", err)
	}
	if err := store.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "QE-123",
			Environment:   "dev",
			RunURL:        "https://prow.example.com/view/1",
			RowID:         "row-1",
			UpdatedAt:     "2026-03-16T12:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed phase3 link: %v", err)
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

	req := httptest.NewRequest(http.MethodGet, "/review/api/week?week=2026-03-15", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("unexpected content type: %q", got)
	}

	var payload reviewAPIWeekTestPayload
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := payload.Week, "2026-03-15"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := payload.Timezone, "UTC"; got != want {
		t.Fatalf("unexpected review API timezone: got=%q want=%q", got, want)
	}
	if got, want := payload.TotalClusters, 2; got != want {
		t.Fatalf("unexpected total clusters: got=%d want=%d", got, want)
	}
	if got, want := len(payload.Rows), 2; got != want {
		t.Fatalf("unexpected row count: got=%d want=%d", got, want)
	}

	var linkedRow *reviewAPIWeekTestRow
	var unlinkedRow *reviewAPIWeekTestRow
	for index := range payload.Rows {
		row := &payload.Rows[index]
		switch row.SelectionID {
		case "dev|QE-123":
			linkedRow = row
		case "dev|cluster-dev-unlinked":
			unlinkedRow = row
		}
	}
	if linkedRow == nil {
		t.Fatalf("expected linked row in payload")
	}
	if got, want := linkedRow.Phase3ClusterID, "QE-123"; got != want {
		t.Fatalf("unexpected linked phase3 cluster id: got=%q want=%q", got, want)
	}
	if !linkedRow.IsLinked {
		t.Fatalf("expected linked row to report is_linked=true")
	}
	if got, want := len(linkedRow.LinkedChildren), 1; got != want {
		t.Fatalf("unexpected linked child count: got=%d want=%d", got, want)
	}
	if unlinkedRow == nil {
		t.Fatalf("expected unlinked row in payload")
	}
	if unlinkedRow.IsLinked {
		t.Fatalf("expected unlinked row to report is_linked=false")
	}
	if got, want := unlinkedRow.Phrase, "CreateNodePool timeout 45 min"; got != want {
		t.Fatalf("unexpected unlinked phrase: got=%q want=%q", got, want)
	}
	if got, want := unlinkedRow.SearchQuery, "CreateNodePool timeout 45 min"; got != want {
		t.Fatalf("unexpected unlinked search query: got=%q want=%q", got, want)
	}
	if got, want := unlinkedRow.Lane, "install"; got != want {
		t.Fatalf("unexpected unlinked lane: got=%q want=%q", got, want)
	}
	if got, want := len(unlinkedRow.ContributingTests), 1; got != want {
		t.Fatalf("unexpected contributing tests count: got=%d want=%d", got, want)
	}
	if got, want := unlinkedRow.ContributingTests[0].JobName, "periodic-ci-nodepool"; got != want {
		t.Fatalf("unexpected contributing test job: got=%q want=%q", got, want)
	}
	if len(unlinkedRow.FullErrorSamples) == 0 {
		t.Fatalf("expected unlinked row to include full error samples")
	}
	if got := unlinkedRow.Phase3ClusterID; got != "" {
		t.Fatalf("unexpected unlinked phase3 cluster id: got=%q", got)
	}
}

func TestHandleReviewLinksActionReturnsJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, reviewActionMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, reviewActionRawFailures()); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	form := url.Values{}
	form.Set("week", "2026-03-15")
	form.Set("action", "link")
	form.Add("cluster_id", "dev|cluster-dev-a")
	form.Add("cluster_id", "dev|cluster-dev-b")

	req := httptest.NewRequest(http.MethodPost, "/review/actions/links", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("unexpected content type: %q", got)
	}

	var payload reviewActionTestResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.OK {
		t.Fatalf("expected ok response: %#v", payload)
	}
	if got, want := payload.Week, "2026-03-15"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := payload.Action, "link"; got != want {
		t.Fatalf("unexpected action: got=%q want=%q", got, want)
	}
	if !payload.Created {
		t.Fatalf("expected created=true when linking two unlinked clusters")
	}
	if got, want := payload.SelectedAnchorCount, 2; got != want {
		t.Fatalf("unexpected selected anchor count: got=%d want=%d", got, want)
	}
	if got, want := payload.CrossWeekAnchorCount, 0; got != want {
		t.Fatalf("unexpected cross-week anchor count: got=%d want=%d", got, want)
	}
	if got, want := payload.TotalAnchorCount, 2; got != want {
		t.Fatalf("unexpected total anchor count: got=%d want=%d", got, want)
	}
	if got, want := len(payload.SelectedClusterIDs), 2; got != want {
		t.Fatalf("unexpected selected cluster count: got=%d want=%d", got, want)
	}
	if payload.Phase3ClusterID == "" {
		t.Fatalf("expected phase3 cluster id in response")
	}
	if !strings.Contains(payload.Notice, "linked 2 anchors") {
		t.Fatalf("unexpected notice: %q", payload.Notice)
	}
	if !strings.Contains(payload.RedirectURL, "/review/?") {
		t.Fatalf("unexpected redirect url: %q", payload.RedirectURL)
	}

	links, err := store.ListPhase3Links(ctx)
	if err != nil {
		t.Fatalf("list phase3 links: %v", err)
	}
	if got, want := len(links), 2; got != want {
		t.Fatalf("unexpected phase3 link count: got=%d want=%d", got, want)
	}
	for _, link := range links {
		if got, want := link.IssueID, payload.Phase3ClusterID; got != want {
			t.Fatalf("unexpected issue id: got=%q want=%q", got, want)
		}
	}
}

func TestHandleReviewLinksActionRedirectsForHTMLForms(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, reviewActionMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	form := url.Values{}
	form.Set("week", "2026-03-15")
	form.Set("action", "link")
	form.Add("cluster_id", "dev|cluster-dev-a")
	form.Add("cluster_id", "dev|cluster-dev-b")

	req := httptest.NewRequest(http.MethodPost, "/review/actions/links", strings.NewReader(form.Encode()))
	req.Header.Set("Accept", "text/html")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusSeeOther; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	location := recorder.Header().Get("Location")
	if location == "" {
		t.Fatalf("expected redirect location")
	}
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse redirect location: %v", err)
	}
	if got, want := parsed.Path, "/review/"; got != want {
		t.Fatalf("unexpected redirect path: got=%q want=%q", got, want)
	}
	if got, want := parsed.Query().Get("week"), "2026-03-15"; got != want {
		t.Fatalf("unexpected redirect week: got=%q want=%q", got, want)
	}
	if got := parsed.Query().Get("notice"); !strings.Contains(got, "linked 2 anchors") {
		t.Fatalf("unexpected redirect notice: %q", got)
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
	UpsertPhase3Issues(context.Context, []semanticcontracts.Phase3IssueRecord) error
	UpsertPhase3Links(context.Context, []semanticcontracts.Phase3LinkRecord) error
	UpsertRuns(context.Context, []storecontracts.RunRecord) error
	UpsertRawFailures(context.Context, []storecontracts.RawFailureRecord) error
	UpsertMetricsDaily(context.Context, []storecontracts.MetricDailyRecord) error
	ListPhase3Links(context.Context) ([]semanticcontracts.Phase3LinkRecord, error)
	Close() error
}

func reviewAPIMaterializedWeek() storecontracts.MaterializedWeek {
	return storecontracts.MaterializedWeek{
		GlobalClusters: []semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:                semanticcontracts.SchemaVersionV1,
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
				SchemaVersion:                semanticcontracts.SchemaVersionV1,
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
			RawText:        "OAuth timeout while waiting for cluster operator",
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

func jobHistoryAPIMaterializedWeek() storecontracts.MaterializedWeek {
	return storecontracts.MaterializedWeek{
		GlobalClusters: []semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:                semanticcontracts.SchemaVersionV1,
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
				SchemaVersion:                semanticcontracts.SchemaVersionV1,
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

func reviewActionMaterializedWeek() storecontracts.MaterializedWeek {
	return storecontracts.MaterializedWeek{
		GlobalClusters: []semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:                semanticcontracts.SchemaVersionV1,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-a",
				CanonicalEvidencePhrase:      "OAuth timeout waiting for kube-apiserver",
				SearchQueryPhrase:            "OAuth timeout waiting for kube-apiserver",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/a",
				SearchQuerySourceSignatureID: "sig-a",
				SupportCount:                 1,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "upgrade",
						JobName:      "periodic-ci-upgrade",
						TestName:     "should oauth",
						SupportCount: 1,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-a"},
				MemberSignatureIDs:     []string{"sig-a"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-a",
						RunURL:      "https://prow.example.com/view/a",
						OccurredAt:  "2026-03-16T08:00:00Z",
						SignatureID: "sig-a",
					},
				},
			},
			{
				SchemaVersion:                semanticcontracts.SchemaVersionV1,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-b",
				CanonicalEvidencePhrase:      "OAuth timeout while cluster operators settle",
				SearchQueryPhrase:            "OAuth timeout while cluster operators settle",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/b",
				SearchQuerySourceSignatureID: "sig-b",
				SupportCount:                 1,
				ContributingTestsCount:       1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "upgrade",
						JobName:      "periodic-ci-upgrade",
						TestName:     "should oauth",
						SupportCount: 1,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-b"},
				MemberSignatureIDs:     []string{"sig-b"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-b",
						RunURL:      "https://prow.example.com/view/b",
						OccurredAt:  "2026-03-16T09:00:00Z",
						SignatureID: "sig-b",
					},
				},
			},
		},
	}
}

func reviewActionRawFailures() []storecontracts.RawFailureRecord {
	return []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-a",
			RunURL:         "https://prow.example.com/view/a",
			TestName:       "should oauth",
			TestSuite:      "suite-a",
			SignatureID:    "sig-a",
			OccurredAt:     "2026-03-16T08:00:00Z",
			RawText:        "OAuth timeout waiting for kube-apiserver",
			NormalizedText: "oauth timeout waiting for kube-apiserver",
		},
		{
			Environment:    "dev",
			RowID:          "row-b",
			RunURL:         "https://prow.example.com/view/b",
			TestName:       "should oauth",
			TestSuite:      "suite-a",
			SignatureID:    "sig-b",
			OccurredAt:     "2026-03-16T09:00:00Z",
			RawText:        "OAuth timeout while cluster operators settle",
			NormalizedText: "oauth timeout while cluster operators settle",
		},
	}
}
