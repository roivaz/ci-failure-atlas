package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestWorkflowBuildPersistsMinimalArtifactsByDefault(t *testing.T) {
	t.Parallel()

	ctx := logr.NewContext(context.Background(), logr.Discard())
	dataDir := t.TempDir()
	semanticSubdir := "2026-03-22-default"
	if err := seedWorkflowBuildInput(ctx, dataDir, semanticSubdir); err != nil {
		t.Fatalf("seed workflow input: %v", err)
	}

	cmd, err := NewWorkflowCommand()
	if err != nil {
		t.Fatalf("new workflow command: %v", err)
	}
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{
		"build",
		"--storage.ndjson.data-dir", dataDir,
		"--storage.ndjson.semantic-subdir", semanticSubdir,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute workflow build: %v", err)
	}

	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{SemanticSubdirectory: semanticSubdir})
	if err != nil {
		t.Fatalf("open semantic store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	worksetRows, err := store.ListPhase1Workset(ctx)
	if err != nil {
		t.Fatalf("list phase1 workset: %v", err)
	}
	if got, want := len(worksetRows), 1; got != want {
		t.Fatalf("unexpected workset row count: got=%d want=%d", got, want)
	}
	testClusters, err := store.ListTestClusters(ctx)
	if err != nil {
		t.Fatalf("list test clusters: %v", err)
	}
	if got, want := len(testClusters), 1; got != want {
		t.Fatalf("unexpected test cluster count: got=%d want=%d", got, want)
	}
	globalClusters, err := store.ListGlobalClusters(ctx)
	if err != nil {
		t.Fatalf("list global clusters: %v", err)
	}
	if got, want := len(globalClusters), 1; got != want {
		t.Fatalf("unexpected global cluster count: got=%d want=%d", got, want)
	}

	normalizedRows, err := store.ListPhase1Normalized(ctx)
	if err != nil {
		t.Fatalf("list phase1 normalized: %v", err)
	}
	if got := len(normalizedRows); got != 0 {
		t.Fatalf("expected no persisted phase1 normalized rows by default, got=%d", got)
	}
	assignmentRows, err := store.ListPhase1Assignments(ctx)
	if err != nil {
		t.Fatalf("list phase1 assignments: %v", err)
	}
	if got := len(assignmentRows); got != 0 {
		t.Fatalf("expected no persisted phase1 assignments by default, got=%d", got)
	}
}

func TestWorkflowBuildDebugDumpAndProfileOutput(t *testing.T) {
	t.Parallel()

	ctx := logr.NewContext(context.Background(), logr.Discard())
	dataDir := t.TempDir()
	semanticSubdir := "2026-03-22-debug"
	if err := seedWorkflowBuildInput(ctx, dataDir, semanticSubdir); err != nil {
		t.Fatalf("seed workflow input: %v", err)
	}

	profilePath := filepath.Join(dataDir, "reports", "workflow-build-profile.json")

	cmd, err := NewWorkflowCommand()
	if err != nil {
		t.Fatalf("new workflow command: %v", err)
	}
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{
		"build",
		"--storage.ndjson.data-dir", dataDir,
		"--storage.ndjson.semantic-subdir", semanticSubdir,
		"--workflow.debug-dump-intermediate",
		"--workflow.profile-output", profilePath,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute workflow build: %v", err)
	}

	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{SemanticSubdirectory: semanticSubdir})
	if err != nil {
		t.Fatalf("open semantic store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	normalizedRows, err := store.ListPhase1Normalized(ctx)
	if err != nil {
		t.Fatalf("list phase1 normalized: %v", err)
	}
	if got := len(normalizedRows); got == 0 {
		t.Fatalf("expected persisted phase1 normalized rows with debug dump enabled")
	}
	assignmentRows, err := store.ListPhase1Assignments(ctx)
	if err != nil {
		t.Fatalf("list phase1 assignments: %v", err)
	}
	if got := len(assignmentRows); got == 0 {
		t.Fatalf("expected persisted phase1 assignments with debug dump enabled")
	}

	var profile workflowBuildProfile
	if err := readJSONFile(profilePath, &profile); err != nil {
		t.Fatalf("read workflow profile: %v", err)
	}
	if !profile.DebugDumpIntermediates {
		t.Fatalf("expected profile to record debug_dump_intermediates=true")
	}
	for _, key := range []string{"phase1_enrich_input", "phase2_merge", "persist_test_clusters"} {
		if _, ok := profile.StageDurationsMS[key]; !ok {
			t.Fatalf("expected stage duration %q in profile", key)
		}
	}
}

func seedWorkflowBuildInput(ctx context.Context, dataDir string, semanticSubdir string) error {
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{SemanticSubdirectory: semanticSubdir})
	if err != nil {
		return err
	}
	defer store.Close()

	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment:    "dev",
			RunURL:         "https://prow.example/run/dev-1",
			JobName:        "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			PRNumber:       1001,
			PostGoodCommit: true,
			OccurredAt:     "2026-03-22T10:00:00Z",
		},
	}); err != nil {
		return err
	}
	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    "dev",
			RowID:          "row-1",
			RunURL:         "https://prow.example/run/dev-1",
			TestName:       "test-dev",
			TestSuite:      "suite-dev",
			SignatureID:    "sig-dev",
			OccurredAt:     "2026-03-22T10:05:00Z",
			RawText:        "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
			NormalizedText: "failed to get service aro-hcp-exporter/aro-hcp-exporter: services \"aro-hcp-exporter\" not found",
		},
	}); err != nil {
		return err
	}
	return nil
}

func readJSONFile(path string, out *workflowBuildProfile) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, out)
}
