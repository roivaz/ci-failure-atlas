package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

func TestHandleAPIDailyTriageReturnsJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, storecontracts.MaterializedWeek{
		GlobalClusters: []semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:                semanticcontracts.SchemaVersionV1,
				Environment:                  "dev",
				Phase2ClusterID:              "cluster-dev-a",
				CanonicalEvidencePhrase:      "OAuth timeout",
				SearchQueryPhrase:            "OAuth timeout",
				SearchQuerySourceRunURL:      "https://prow.example.com/view/1",
				SearchQuerySourceSignatureID: "sig-a",
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
				MemberPhase1ClusterIDs: []string{"phase1-sig-a"},
				MemberSignatureIDs:     []string{"sig-a"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-1",
						RunURL:      "https://prow.example.com/view/1",
						OccurredAt:  "2026-03-16T08:00:00Z",
						SignatureID: "sig-a",
					},
				},
			},
		},
	}); err != nil {
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
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
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
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		PostgresPool: fixture.pool,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/triage/daily?date=2026-03-16&env=dev", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("unexpected content type: %q", got)
	}

	var payload frontservice.DailyTriageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := payload.Meta.Date, "2026-03-16"; got != want {
		t.Fatalf("unexpected response date: got=%q want=%q", got, want)
	}
	if got, want := payload.Meta.ResolvedWeek, "2026-03-15"; got != want {
		t.Fatalf("unexpected resolved week: got=%q want=%q", got, want)
	}
	if got, want := len(payload.Environments), 1; got != want {
		t.Fatalf("unexpected environment count: got=%d want=%d", got, want)
	}
	if got, want := payload.Environments[0].Items[0].SignatureID, "sig-a"; got != want {
		t.Fatalf("unexpected signature id: got=%q want=%q", got, want)
	}
}

func TestHandleAPIDailyTriageReturnsJSONError(t *testing.T) {
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

	if got, want := recorder.Code, http.StatusBadRequest; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d body=%s", got, want, recorder.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got := payload["error"]; !strings.Contains(got, "date query parameter is required") {
		t.Fatalf("unexpected error message: %q", got)
	}
}

func TestHandleReviewAPIWeekReturnsJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newHandlerFixture(t)
	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, storecontracts.MaterializedWeek{
		GlobalClusters: []semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-dev-a",
				CanonicalEvidencePhrase: "OAuth timeout",
				SearchQueryPhrase:       "OAuth timeout",
				SupportCount:            2,
				ContributingTestsCount:  1,
				ContributingTests: []semanticcontracts.ContributingTestRecord{
					{
						Lane:         "upgrade",
						JobName:      "periodic-ci",
						TestName:     "should oauth",
						SupportCount: 2,
					},
				},
				MemberPhase1ClusterIDs: []string{"phase1-sig-a"},
				MemberSignatureIDs:     []string{"sig-a"},
				References: []semanticcontracts.ReferenceRecord{
					{
						RowID:       "row-1",
						RunURL:      "https://prow.example.com/view/1",
						OccurredAt:  "2026-03-16T08:00:00Z",
						SignatureID: "sig-a",
					},
				},
			},
		},
	}); err != nil {
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

	var payload struct {
		Week          string           `json:"week"`
		TotalClusters int              `json:"total_clusters"`
		Rows          []map[string]any `json:"rows"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := payload.Week, "2026-03-15"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := payload.TotalClusters, 1; got != want {
		t.Fatalf("unexpected total clusters: got=%d want=%d", got, want)
	}
	if got, want := len(payload.Rows), 1; got != want {
		t.Fatalf("unexpected row count: got=%d want=%d", got, want)
	}
	if got, want := payload.Rows[0]["phase3_cluster_id"], "QE-123"; got != want {
		t.Fatalf("unexpected phase3 cluster id: got=%v want=%q", got, want)
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
	Close() error
}
