package controllers

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/source/prowartifacts"
	"ci-failure-atlas/pkg/sourceoptions"
	"ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestSourceProwFailuresSyncOnceUpsertsArtifactRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()

	store, err := ndjson.New(dataDir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/1234/job/123456789"
	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      runURL,
			JobName:     "job",
			OccurredAt:  "2026-03-06T10:00:00Z",
		},
	}); err != nil {
		t.Fatalf("upsert runs: %v", err)
	}

	fakeClient := &fakeProwArtifactsClient{
		failuresByKey: map[string][]prowartifacts.Failure{
			"dev|" + runURL: []prowartifacts.Failure{
				{
					ArtifactURL: "https://gcsweb-ci/apps/.../junit_a.xml",
					TestSuite:   "suite-a",
					TestName:    "test-a",
					FailureText: "Connection timeout after 30s",
				},
				{
					ArtifactURL: "https://gcsweb-ci/apps/.../junit_a.xml",
					TestSuite:   "suite-a",
					TestName:    "test-b",
					FailureText: "connection   timeout after 30s",
				},
				{
					ArtifactURL: "https://gcsweb-ci/apps/.../junit_b.xml",
					TestSuite:   "suite-b",
					TestName:    "test-c",
					FailureText: "cluster provisioning failed",
				},
			},
		},
	}

	controller, err := newSourceProwFailuresController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptionsForProw(t),
	}, fakeClient)
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	if len(fakeClient.calls) != 1 || fakeClient.calls[0] != (prowCall{environment: "dev", runURL: runURL}) {
		t.Fatalf("unexpected prow client calls: %+v", fakeClient.calls)
	}

	runKeys, err := store.ListArtifactRunKeys(ctx)
	if err != nil {
		t.Fatalf("list artifact run keys: %v", err)
	}
	wantKeys := []string{"dev|" + runURL}
	if !reflect.DeepEqual(runKeys, wantKeys) {
		t.Fatalf("artifact run keys mismatch: got=%v want=%v", runKeys, wantKeys)
	}

	rows := mustReadArtifactFailureRows(t, filepath.Join(dataDir, "facts", "artifact_failures.ndjson"))
	if len(rows) != 3 {
		t.Fatalf("unexpected artifact row count: got=%d want=3", len(rows))
	}

	rowIDs := map[string]struct{}{}
	signatureByTestName := map[string]string{}
	for _, row := range rows {
		if row.Environment != "dev" {
			t.Fatalf("unexpected row environment: %+v", row)
		}
		if row.RunURL != runURL {
			t.Fatalf("unexpected row run_url: %+v", row)
		}
		if row.ArtifactRowID == "" {
			t.Fatalf("expected non-empty artifact_row_id: %+v", row)
		}
		if _, exists := rowIDs[row.ArtifactRowID]; exists {
			t.Fatalf("duplicate artifact_row_id detected: %s", row.ArtifactRowID)
		}
		rowIDs[row.ArtifactRowID] = struct{}{}
		signatureByTestName[row.TestName] = row.SignatureID
	}

	if signatureByTestName["test-a"] == "" || signatureByTestName["test-a"] != signatureByTestName["test-b"] {
		t.Fatalf("expected test-a and test-b to share normalized signature; got=%v", signatureByTestName)
	}

	// idempotency: processing the same key should not create duplicates.
	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("second sync once: %v", err)
	}
	rowsAfterSecondSync := mustReadArtifactFailureRows(t, filepath.Join(dataDir, "facts", "artifact_failures.ndjson"))
	if len(rowsAfterSecondSync) != 3 {
		t.Fatalf("unexpected artifact row count after second sync: got=%d want=3", len(rowsAfterSecondSync))
	}
}

func TestSourceProwFailuresRunOnceRejectsInvalidKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	controller, err := newSourceProwFailuresController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptionsForProw(t),
	}, &fakeProwArtifactsClient{})
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	if err := controller.RunOnce(ctx, "invalid-key"); err == nil {
		t.Fatalf("expected invalid key to fail")
	}
}

type fakeProwArtifactsClient struct {
	failuresByKey map[string][]prowartifacts.Failure
	err           error
	calls         []prowCall
}

type prowCall struct {
	environment string
	runURL      string
}

func (f *fakeProwArtifactsClient) ListFailures(_ context.Context, environment string, runURL string) ([]prowartifacts.Failure, error) {
	f.calls = append(f.calls, prowCall{
		environment: environment,
		runURL:      runURL,
	})
	if f.err != nil {
		return nil, f.err
	}
	failures := append([]prowartifacts.Failure(nil), f.failuresByKey[environment+"|"+runURL]...)
	return failures, nil
}

func mustCompleteSourceOptionsForProw(t *testing.T) *sourceoptions.Options {
	t.Helper()

	raw := sourceoptions.DefaultOptions()
	raw.ProwArtifactsBaseURL = "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs"

	validated, err := raw.Validate()
	if err != nil {
		t.Fatalf("validate source options: %v", err)
	}
	completed, err := validated.Complete(context.Background())
	if err != nil {
		t.Fatalf("complete source options: %v", err)
	}
	return completed
}

func mustReadArtifactFailureRows(t *testing.T, path string) []contracts.ArtifactFailureRecord {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open artifact failures file %q: %v", path, err)
	}
	defer f.Close()

	rows := make([]contracts.ArtifactFailureRecord, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var row contracts.ArtifactFailureRecord
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatalf("decode artifact failure row: %v", err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan artifact failures file: %v", err)
	}
	return rows
}
