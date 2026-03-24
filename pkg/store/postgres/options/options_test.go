package options

import (
	"strings"
	"testing"
)

func TestValidateDisabledBackendSucceeds(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.Enabled = false

	if _, err := opts.Validate(); err != nil {
		t.Fatalf("expected validate success for disabled backend: %v", err)
	}
}

func TestValidateRemoteBackendRequiresCoreConnectionFields(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.Enabled = true
	opts.Embedded = false
	opts.Hostname = ""
	opts.User = ""
	opts.Database = ""

	if _, err := opts.Validate(); err == nil {
		t.Fatalf("expected validate error for missing remote connection fields")
	}
}

func TestValidateEmbeddedBackendRequiresDataDir(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.Enabled = true
	opts.Embedded = true
	opts.EmbeddedDataDir = ""

	if _, err := opts.Validate(); err == nil {
		t.Fatalf("expected validate error when embedded data dir is missing")
	}
}

func TestValidateNormalizesSemanticSubdirectory(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.SemanticSubdirectory = " 2026-03-16 "

	validated, err := opts.Validate()
	if err != nil {
		t.Fatalf("validate returned unexpected error: %v", err)
	}
	if validated.SemanticSubdirectory != "2026-03-16" {
		t.Fatalf("expected normalized semantic subdirectory, got %q", validated.SemanticSubdirectory)
	}
}

func TestValidateRejectsDotDotInSemanticSubdirectory(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.SemanticSubdirectory = "../invalid"

	_, err := opts.Validate()
	if err == nil {
		t.Fatalf("expected validate error for invalid semantic subdirectory")
	}
	if !strings.Contains(err.Error(), "semantic-subdir") {
		t.Fatalf("expected semantic-subdir error, got %v", err)
	}
}
