package sourceoptions

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestValidateAndCompleteEnvironments(t *testing.T) {
	t.Parallel()

	raw := DefaultOptions()
	raw.Environments = []string{"DEV", "int", "dev", "stg"}
	raw.SippyReleaseInt = "Int"
	raw.SippyReleaseStg = "Stg"
	raw.SippyLookback = "48h"

	validated, err := raw.Validate()
	if err != nil {
		t.Fatalf("validate options: %v", err)
	}

	completed, err := validated.Complete(context.Background())
	if err != nil {
		t.Fatalf("complete options: %v", err)
	}

	want := []string{"dev", "int", "stg"}
	if !reflect.DeepEqual(completed.Environments, want) {
		t.Fatalf("environment list mismatch: got=%v want=%v", completed.Environments, want)
	}
	if completed.SippyLookback != 48*time.Hour {
		t.Fatalf("lookback mismatch: got=%s want=%s", completed.SippyLookback, 48*time.Hour)
	}
}

func TestValidateRejectsUnsupportedEnvironment(t *testing.T) {
	t.Parallel()

	raw := DefaultOptions()
	raw.Environments = []string{"dev", "qa"}

	if _, err := raw.Validate(); err == nil {
		t.Fatalf("expected validate to reject unsupported environment")
	}
}

func TestValidateRejectsEmptyEnvironmentList(t *testing.T) {
	t.Parallel()

	raw := DefaultOptions()
	raw.Environments = []string{"", " "}

	if _, err := raw.Validate(); err == nil {
		t.Fatalf("expected validate to reject empty environment list")
	}
}

func TestValidateRejectsMissingEnvironmentRelease(t *testing.T) {
	t.Parallel()

	raw := DefaultOptions()
	raw.Environments = []string{"int"}
	raw.SippyReleaseInt = ""

	if _, err := raw.Validate(); err == nil {
		t.Fatalf("expected validate to reject missing release for selected environment")
	}
}
