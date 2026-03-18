package review

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestHandlerRendersWeekAndPhase3Issue(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	ctx := context.Background()
	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "phase2-dev-1",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            3,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-1",
					RunURL:      "https://prow.example/run/1",
					OccurredAt:  "2026-03-15T10:00:00Z",
					SignatureID: "sig-1",
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert global clusters: %v", err)
	}
	if err := store.UpsertPhase1Workset(ctx, []semanticcontracts.Phase1WorksetRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			Environment:   "dev",
			RowID:         "row-1",
			RunURL:        "https://prow.example/run/1",
			SignatureID:   "sig-1",
			OccurredAt:    "2026-03-15T10:00:00Z",
			RawText:       "example full error text",
		},
	}); err != nil {
		t.Fatalf("upsert phase1 workset: %v", err)
	}
	if err := store.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "ISSUE-42",
			Environment:   "dev",
			RunURL:        "https://prow.example/run/1",
			RowID:         "row-1",
			UpdatedAt:     "2026-03-16T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("upsert phase3 links: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		DataDirectory:        dataDir,
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/?week=2026-03-15", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, snippet := range []string{
		"Semantic Review (Phase3)",
		"ISSUE-42",
		"Linked signatures (1)",
		"name=\"cluster_id\"",
		"Refresh",
		"id=\"theme-toggle\"",
		"ci-failure-report-theme-mode",
		"Full failure examples (1)",
		"example full error text",
	} {
		if !strings.Contains(body, snippet) {
			t.Fatalf("expected response body to contain %q", snippet)
		}
	}
}

func TestHandlerRendersOneTablePerEnvironmentAndNoUnlinkedQueue(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	ctx := context.Background()
	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "phase2-dev-1",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            2,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-dev-1",
					RunURL:      "https://prow.example/run/dev-1",
					OccurredAt:  "2026-03-15T10:00:00Z",
					SignatureID: "sig-dev-1",
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase2ClusterID:         "phase2-int-1",
			CanonicalEvidencePhrase: "api server timeout",
			SearchQueryPhrase:       "api server timeout",
			SupportCount:            1,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-int-1",
					RunURL:      "https://prow.example/run/int-1",
					OccurredAt:  "2026-03-15T11:00:00Z",
					SignatureID: "sig-int-1",
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert global clusters: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		DataDirectory:        dataDir,
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/?week=2026-03-15", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "Unlinked queue") {
		t.Fatalf("did not expect unlinked queue panel in review page")
	}
	for _, snippet := range []string{
		`<section id="env-dev" class="phase3-environment-section">`,
		`<section id="env-int" class="phase3-environment-section">`,
		"Environment: DEV",
		"Environment: INT",
	} {
		if !strings.Contains(body, snippet) {
			t.Fatalf("expected response body to contain %q", snippet)
		}
	}
	if strings.Count(body, `class="triage-table"`) != 2 {
		t.Fatalf("expected one triage table per environment, got body=%s", body)
	}
}

func TestHandlerAPIWeekCollapsesLinkedRows(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	ctx := context.Background()
	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "phase2-dev-a",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            2,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-a",
					RunURL:      "https://prow.example/run/a",
					OccurredAt:  "2026-03-15T10:00:00Z",
					SignatureID: "sig-a",
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "phase2-dev-b",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            3,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-b",
					RunURL:      "https://prow.example/run/b",
					OccurredAt:  "2026-03-15T11:00:00Z",
					SignatureID: "sig-b",
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert global clusters: %v", err)
	}
	if err := store.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-shared",
			Environment:   "dev",
			RunURL:        "https://prow.example/run/a",
			RowID:         "row-a",
			UpdatedAt:     "2026-03-16T10:00:00Z",
		},
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-shared",
			Environment:   "dev",
			RunURL:        "https://prow.example/run/b",
			RowID:         "row-b",
			UpdatedAt:     "2026-03-16T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("upsert phase3 links: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		DataDirectory:        dataDir,
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/week?week=2026-03-15", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d body=%s", rec.Code, rec.Body.String())
	}

	var payload struct {
		TotalClusters int `json:"total_clusters"`
		Rows          []struct {
			Environment     string `json:"environment"`
			ClusterID       string `json:"cluster_id"`
			SelectionID     string `json:"selection_id"`
			SupportCount    int    `json:"support_count"`
			Phase3Cluster   string `json:"phase3_cluster_id"`
			QualityScore    int    `json:"quality_score"`
			CanonicalPhrase string `json:"phrase"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode api payload: %v", err)
	}
	if payload.TotalClusters != 1 {
		t.Fatalf("expected collapsed total cluster count to be 1, got=%d payload=%s", payload.TotalClusters, rec.Body.String())
	}
	if len(payload.Rows) != 1 {
		t.Fatalf("expected exactly one collapsed row, got=%d payload=%s", len(payload.Rows), rec.Body.String())
	}
	row := payload.Rows[0]
	if row.ClusterID != "p3c-shared" {
		t.Fatalf("expected collapsed row cluster id to be phase3 id, got=%q", row.ClusterID)
	}
	if row.Phase3Cluster != "p3c-shared" {
		t.Fatalf("expected phase3 cluster column to be populated, got=%q", row.Phase3Cluster)
	}
	if row.SupportCount != 5 {
		t.Fatalf("expected merged support count 5, got=%d", row.SupportCount)
	}
	if row.SelectionID != "dev|p3c-shared" {
		t.Fatalf("expected selection id to include environment + cluster id, got=%q", row.SelectionID)
	}
}

func TestHandlerAPIWeekSelectionIDsRemainUniqueAcrossEnvironments(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	ctx := context.Background()
	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "phase2-dev-1",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            1,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-dev-1",
					RunURL:      "https://prow.example/run/dev-1",
					OccurredAt:  "2026-03-15T10:00:00Z",
					SignatureID: "sig-dev-1",
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase2ClusterID:         "phase2-int-1",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            1,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-int-1",
					RunURL:      "https://prow.example/run/int-1",
					OccurredAt:  "2026-03-15T10:00:00Z",
					SignatureID: "sig-int-1",
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert global clusters: %v", err)
	}
	if err := store.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-shared",
			Environment:   "dev",
			RunURL:        "https://prow.example/run/dev-1",
			RowID:         "row-dev-1",
			UpdatedAt:     "2026-03-16T10:00:00Z",
		},
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-shared",
			Environment:   "int",
			RunURL:        "https://prow.example/run/int-1",
			RowID:         "row-int-1",
			UpdatedAt:     "2026-03-16T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("upsert phase3 links: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		DataDirectory:        dataDir,
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/week?week=2026-03-15", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d body=%s", rec.Code, rec.Body.String())
	}

	var payload struct {
		Rows []struct {
			Environment   string `json:"environment"`
			ClusterID     string `json:"cluster_id"`
			SelectionID   string `json:"selection_id"`
			Phase3Cluster string `json:"phase3_cluster_id"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode api payload: %v", err)
	}
	if len(payload.Rows) != 2 {
		t.Fatalf("expected one collapsed row per environment, got=%d payload=%s", len(payload.Rows), rec.Body.String())
	}
	selectionIDs := map[string]struct{}{}
	for _, row := range payload.Rows {
		if row.ClusterID != "p3c-shared" || row.Phase3Cluster != "p3c-shared" {
			t.Fatalf("expected rows to be collapsed to the same phase3 id, row=%+v", row)
		}
		selectionIDs[row.SelectionID] = struct{}{}
	}
	for _, expected := range []string{"dev|p3c-shared", "int|p3c-shared"} {
		if _, ok := selectionIDs[expected]; !ok {
			t.Fatalf("expected selection id %q in payload, got=%+v", expected, selectionIDs)
		}
	}
}

func TestHandlerAssignActionWritesPhase3State(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	ctx := context.Background()
	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "phase2-dev-1",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            2,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-1",
					RunURL:      "https://prow.example/run/1",
					OccurredAt:  "2026-03-15T10:00:00Z",
					SignatureID: "sig-1",
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert global clusters: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		DataDirectory:        dataDir,
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	formBody := strings.NewReader("week=2026-03-15&action=link&cluster_id=phase2-dev-1")
	req := httptest.NewRequest(http.MethodPost, "/actions/links", formBody)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status: got=%d body=%s", rec.Code, rec.Body.String())
	}

	verifyStore, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new verification store: %v", err)
	}
	t.Cleanup(func() {
		_ = verifyStore.Close()
	})
	links, err := verifyStore.ListPhase3Links(context.Background())
	if err != nil {
		t.Fatalf("list links: %v", err)
	}
	if len(links) != 1 || !strings.HasPrefix(links[0].IssueID, "p3c-") {
		t.Fatalf("unexpected links after assign: %+v", links)
	}

	apiReq := httptest.NewRequest(http.MethodGet, "/api/week?week=2026-03-15", nil)
	apiRec := httptest.NewRecorder()
	handler.ServeHTTP(apiRec, apiReq)
	if apiRec.Code != http.StatusOK {
		t.Fatalf("unexpected api status: got=%d body=%s", apiRec.Code, apiRec.Body.String())
	}
	bodyBytes, _ := io.ReadAll(apiRec.Body)
	if !strings.Contains(string(bodyBytes), "\"phase3_cluster_id\":\"p3c-") {
		t.Fatalf("expected api payload to contain issue assignment, body=%s", string(bodyBytes))
	}
}

func TestHandlerLinkActionFailsOnMixedPhase3Clusters(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	ctx := context.Background()
	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "phase2-dev-1",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            2,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-1",
					RunURL:      "https://prow.example/run/1",
					OccurredAt:  "2026-03-15T10:00:00Z",
					SignatureID: "sig-1",
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "phase2-dev-2",
			CanonicalEvidencePhrase: "etcd lease timeout",
			SearchQueryPhrase:       "etcd lease timeout",
			SupportCount:            2,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-2",
					RunURL:      "https://prow.example/run/2",
					OccurredAt:  "2026-03-15T11:00:00Z",
					SignatureID: "sig-2",
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert global clusters: %v", err)
	}
	if err := store.UpsertPhase3Links(ctx, []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-aaa111",
			Environment:   "dev",
			RunURL:        "https://prow.example/run/1",
			RowID:         "row-1",
			UpdatedAt:     "2026-03-16T10:00:00Z",
		},
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-bbb222",
			Environment:   "dev",
			RunURL:        "https://prow.example/run/2",
			RowID:         "row-2",
			UpdatedAt:     "2026-03-16T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("upsert phase3 links: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		DataDirectory:        dataDir,
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	formBody := strings.NewReader("week=2026-03-15&action=link&cluster_id=phase2-dev-1&cluster_id=phase2-dev-2")
	req := httptest.NewRequest(http.MethodPost, "/actions/links", formBody)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status: got=%d body=%s", rec.Code, rec.Body.String())
	}

	verifyStore, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-15",
	})
	if err != nil {
		t.Fatalf("new verification store: %v", err)
	}
	t.Cleanup(func() {
		_ = verifyStore.Close()
	})
	links, err := verifyStore.ListPhase3Links(context.Background())
	if err != nil {
		t.Fatalf("list links: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("unexpected links count after hard-failure path: %+v", links)
	}
	found := map[string]string{}
	for _, row := range links {
		found[row.RowID] = row.IssueID
	}
	if found["row-1"] != "p3c-aaa111" || found["row-2"] != "p3c-bbb222" {
		t.Fatalf("expected pre-existing links to remain unchanged, got=%+v", links)
	}
}

func TestHandlerLinkActionPropagatesAcrossReconcileWindow(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	phraseLoaded := "Context   Deadline   Exceeded"
	phraseOld := "context deadline exceeded"
	phraseNext := "CONTEXT DEADLINE EXCEEDED"
	phraseCurrent := "context    deadline   exceeded"

	seedGlobalClusterForWeek(t, dataDir, "2026-03-01", "dev", "phase2-dev-old", phraseOld, "row-old")
	seedGlobalClusterForWeek(t, dataDir, "2026-03-08", "dev", "phase2-dev-loaded", phraseLoaded, "row-loaded")
	seedGlobalClusterForWeek(t, dataDir, "2026-03-15", "dev", "phase2-dev-next", phraseNext, "row-next")
	seedGlobalClusterForWeek(t, dataDir, "2026-03-22", "dev", "phase2-dev-current", phraseCurrent, "row-current")

	handler, err := NewHandler(HandlerOptions{
		DataDirectory:        dataDir,
		SemanticSubdirectory: "2026-03-08",
		HistoryHorizonWeeks:  4,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	formBody := strings.NewReader("week=2026-03-08&action=link&cluster_id=phase2-dev-loaded")
	req := httptest.NewRequest(http.MethodPost, "/actions/links", formBody)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status: got=%d body=%s", rec.Code, rec.Body.String())
	}

	verifyStore, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-22",
	})
	if err != nil {
		t.Fatalf("new verification store: %v", err)
	}
	t.Cleanup(func() {
		_ = verifyStore.Close()
	})
	links, err := verifyStore.ListPhase3Links(context.Background())
	if err != nil {
		t.Fatalf("list links: %v", err)
	}
	if len(links) != 4 {
		t.Fatalf("expected links across 4-week reconcile window, got=%d links=%+v", len(links), links)
	}
	issueID := strings.TrimSpace(links[0].IssueID)
	if !strings.HasPrefix(issueID, "p3c-") {
		t.Fatalf("expected generated phase3 cluster id prefix p3c-, got=%q", issueID)
	}
	rowsByID := map[string]string{}
	for _, row := range links {
		rowsByID[strings.TrimSpace(row.RowID)] = strings.TrimSpace(row.IssueID)
	}
	for _, rowID := range []string{"row-old", "row-loaded", "row-next", "row-current"} {
		if rowsByID[rowID] != issueID {
			t.Fatalf("expected row %q to be linked to %q, got=%q", rowID, issueID, rowsByID[rowID])
		}
	}
}

func TestHandlerLinkActionPropagationRespectsReconcileWindowBounds(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	phrase := "resource group cleanup failed due to timeout"

	seedGlobalClusterForWeek(t, dataDir, "2026-02-22", "dev", "phase2-dev-outside", phrase, "row-outside")
	seedGlobalClusterForWeek(t, dataDir, "2026-03-01", "dev", "phase2-dev-old", phrase, "row-old")
	seedGlobalClusterForWeek(t, dataDir, "2026-03-08", "dev", "phase2-dev-loaded", phrase, "row-loaded")
	seedGlobalClusterForWeek(t, dataDir, "2026-03-15", "dev", "phase2-dev-next", phrase, "row-next")
	seedGlobalClusterForWeek(t, dataDir, "2026-03-22", "dev", "phase2-dev-current", phrase, "row-current")
	seedGlobalClusterForWeek(t, dataDir, "scratch", "dev", "phase2-dev-invalid", phrase, "row-invalid")

	handler, err := NewHandler(HandlerOptions{
		DataDirectory:        dataDir,
		SemanticSubdirectory: "2026-03-08",
		HistoryHorizonWeeks:  4,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	formBody := strings.NewReader("week=2026-03-08&action=link&cluster_id=phase2-dev-loaded")
	req := httptest.NewRequest(http.MethodPost, "/actions/links", formBody)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status: got=%d body=%s", rec.Code, rec.Body.String())
	}

	verifyStore, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-22",
	})
	if err != nil {
		t.Fatalf("new verification store: %v", err)
	}
	t.Cleanup(func() {
		_ = verifyStore.Close()
	})
	links, err := verifyStore.ListPhase3Links(context.Background())
	if err != nil {
		t.Fatalf("list links: %v", err)
	}
	if len(links) != 4 {
		t.Fatalf("expected only four in-window links, got=%d links=%+v", len(links), links)
	}
	rowIDs := make([]string, 0, len(links))
	seen := map[string]struct{}{}
	for _, row := range links {
		rowID := strings.TrimSpace(row.RowID)
		rowIDs = append(rowIDs, rowID)
		seen[rowID] = struct{}{}
	}
	sort.Strings(rowIDs)
	for _, expected := range []string{"row-old", "row-loaded", "row-next", "row-current"} {
		if _, ok := seen[expected]; !ok {
			t.Fatalf("expected row %q to be linked, got row ids=%v", expected, rowIDs)
		}
	}
	for _, rowID := range rowIDs {
		if rowID == "row-outside" {
			t.Fatalf("expected reconcile window to exclude out-of-window week row-outside; links=%+v", links)
		}
		if rowID == "row-invalid" {
			t.Fatalf("expected reconcile window to skip invalid week directory row-invalid; links=%+v", links)
		}
	}
}

func TestHandlerLinkActionPropagationFailsOnCrossWeekMixedPhase3Clusters(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	phrase := "api server throttling during read request"

	seedGlobalClusterForWeek(t, dataDir, "2026-03-08", "dev", "phase2-dev-loaded", phrase, "row-loaded")
	seedGlobalClusterForWeek(t, dataDir, "2026-03-15", "dev", "phase2-dev-linked-a", phrase, "row-linked-a")
	seedGlobalClusterForWeek(t, dataDir, "2026-03-22", "dev", "phase2-dev-linked-b", phrase, "row-linked-b")

	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: "2026-03-22",
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	if err := store.UpsertPhase3Links(context.Background(), []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-111aaa",
			Environment:   "dev",
			RunURL:        "https://prow.example/2026-03-15/row-linked-a",
			RowID:         "row-linked-a",
			UpdatedAt:     "2026-03-16T10:00:00Z",
		},
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-222bbb",
			Environment:   "dev",
			RunURL:        "https://prow.example/2026-03-22/row-linked-b",
			RowID:         "row-linked-b",
			UpdatedAt:     "2026-03-16T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("upsert phase3 links: %v", err)
	}

	handler, err := NewHandler(HandlerOptions{
		DataDirectory:        dataDir,
		SemanticSubdirectory: "2026-03-08",
		HistoryHorizonWeeks:  4,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	formBody := strings.NewReader("week=2026-03-08&action=link&cluster_id=phase2-dev-loaded")
	req := httptest.NewRequest(http.MethodPost, "/actions/links", formBody)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status: got=%d body=%s", rec.Code, rec.Body.String())
	}

	links, err := store.ListPhase3Links(context.Background())
	if err != nil {
		t.Fatalf("list links: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("expected no partial writes on cross-week conflict, got=%d links=%+v", len(links), links)
	}
	rowToIssue := map[string]string{}
	for _, row := range links {
		rowToIssue[strings.TrimSpace(row.RowID)] = strings.TrimSpace(row.IssueID)
	}
	if rowToIssue["row-linked-a"] != "p3c-111aaa" || rowToIssue["row-linked-b"] != "p3c-222bbb" {
		t.Fatalf("expected pre-existing cross-week links unchanged, got=%+v", links)
	}
	if _, exists := rowToIssue["row-loaded"]; exists {
		t.Fatalf("expected selected row to remain unlinked after hard-failure, got=%+v", links)
	}
}

func seedGlobalClusterForWeek(
	t *testing.T,
	dataDir string,
	week string,
	environment string,
	clusterID string,
	phrase string,
	rowID string,
) {
	t.Helper()
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: week,
	})
	if err != nil {
		t.Fatalf("new store for week %q: %v", week, err)
	}
	defer func() {
		_ = store.Close()
	}()
	referenceRunURL := fmt.Sprintf("https://prow.example/%s/%s", week, rowID)
	referenceSignatureID := fmt.Sprintf("sig-%s", rowID)
	referenceOccurredAt := "2026-03-15T10:00:00Z"
	if err := store.UpsertGlobalClusters(context.Background(), []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             environment,
			Phase2ClusterID:         clusterID,
			CanonicalEvidencePhrase: phrase,
			SearchQueryPhrase:       phrase,
			SupportCount:            1,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       rowID,
					RunURL:      referenceRunURL,
					OccurredAt:  referenceOccurredAt,
					SignatureID: referenceSignatureID,
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert global clusters for week %q: %v", week, err)
	}
}
