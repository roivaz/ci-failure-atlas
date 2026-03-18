package sourceoptions

import (
	"context"
	"reflect"
	"testing"
)

func TestDefaultOptionsSetsEnvironmentReleases(t *testing.T) {
	t.Parallel()

	raw := DefaultOptions()
	if raw.SippyReleaseDev != "Presubmits" {
		t.Fatalf("unexpected dev release default: got=%q want=%q", raw.SippyReleaseDev, "Presubmits")
	}
	if raw.SippyReleaseInt != "aro-integration" {
		t.Fatalf("unexpected int release default: got=%q want=%q", raw.SippyReleaseInt, "aro-integration")
	}
	if raw.SippyReleaseStg != "aro-stage" {
		t.Fatalf("unexpected stg release default: got=%q want=%q", raw.SippyReleaseStg, "aro-stage")
	}
	if raw.SippyReleaseProd != "aro-production" {
		t.Fatalf("unexpected prod release default: got=%q want=%q", raw.SippyReleaseProd, "aro-production")
	}
}

func TestValidateAndCompleteEnvironments(t *testing.T) {
	t.Parallel()

	raw := DefaultOptions()
	raw.Environments = []string{"DEV", "int", "dev", "stg"}
	raw.SippyReleaseInt = "Int"
	raw.SippyReleaseStg = "Stg"
	raw.HistoryHorizonWeeks = 6

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
	if completed.HistoryHorizonWeeks != 6 {
		t.Fatalf("history horizon mismatch: got=%d want=%d", completed.HistoryHorizonWeeks, 6)
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

func TestValidateRejectsInvalidHistoryHorizonWeeks(t *testing.T) {
	t.Parallel()

	raw := DefaultOptions()
	raw.HistoryHorizonWeeks = 0

	if _, err := raw.Validate(); err == nil {
		t.Fatalf("expected validate to reject invalid history horizon")
	}
}
