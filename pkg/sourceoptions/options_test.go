package sourceoptions

import (
	"context"
	"reflect"
	"testing"
)

func TestValidateAndCompleteEnvironments(t *testing.T) {
	t.Parallel()

	raw := DefaultOptions()
	raw.Environments = []string{"DEV", "int", "dev", "stg"}

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
