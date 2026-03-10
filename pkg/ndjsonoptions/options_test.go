package ndjsonoptions

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateAcceptsSemanticSubdirectory(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.DataDirectory = t.TempDir()
	opts.SemanticSubdirectory = "2026-03-01"

	validated, err := opts.Validate()
	if err != nil {
		t.Fatalf("validate options: %v", err)
	}
	if got, want := validated.SemanticSubdirectory, "2026-03-01"; got != want {
		t.Fatalf("unexpected semantic subdirectory: got=%q want=%q", got, want)
	}

	completed, err := validated.Complete(context.Background())
	if err != nil {
		t.Fatalf("complete options: %v", err)
	}
	if got, want := completed.SemanticSubdirectory, "2026-03-01"; got != want {
		t.Fatalf("unexpected completed semantic subdirectory: got=%q want=%q", got, want)
	}

	semanticWeekPath := filepath.Join(opts.DataDirectory, "semantic", "2026-03-01")
	info, statErr := os.Stat(semanticWeekPath)
	if statErr != nil {
		t.Fatalf("expected semantic subdirectory to exist: %v", statErr)
	}
	if !info.IsDir() {
		t.Fatalf("expected semantic subdirectory path to be a directory: %q", semanticWeekPath)
	}
}

func TestValidateRejectsSemanticSubdirectoryTraversal(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.DataDirectory = t.TempDir()
	opts.SemanticSubdirectory = "../outside"

	if _, err := opts.Validate(); err == nil {
		t.Fatalf("expected validation error for semantic subdirectory traversal")
	}
}
