package cli

import (
	"testing"
	"time"
)

func TestNewSemanticCommandIncludesMaterializeSubcommand(t *testing.T) {
	t.Parallel()

	cmd, err := NewSemanticCommand()
	if err != nil {
		t.Fatalf("create semantic command: %v", err)
	}

	if got, want := cmd.Name(), "semantic"; got != want {
		t.Fatalf("unexpected command name: got=%q want=%q", got, want)
	}

	materializeCmd, _, err := cmd.Find([]string{"materialize"})
	if err != nil {
		t.Fatalf("find materialize subcommand: %v", err)
	}
	if materializeCmd == nil || materializeCmd.Name() != "materialize" {
		t.Fatalf("expected materialize subcommand, got=%v", materializeCmd)
	}
}

func TestResolveMaterializeScopeDefaultsToCurrentWeek(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)
	scope, err := resolveMaterializeScope("", now)
	if err != nil {
		t.Fatalf("resolve materialize scope: %v", err)
	}

	if got, want := scope.Week, "2026-03-29"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := scope.WeekStart.Format(time.RFC3339), "2026-03-29T00:00:00Z"; got != want {
		t.Fatalf("unexpected window start: got=%q want=%q", got, want)
	}
	if got, want := scope.WeekEnd.Format(time.RFC3339), "2026-04-05T00:00:00Z"; got != want {
		t.Fatalf("unexpected window end: got=%q want=%q", got, want)
	}
}

func TestResolveMaterializeScopeUsesExplicitWeek(t *testing.T) {
	t.Parallel()

	scope, err := resolveMaterializeScope("2026-03-08", time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("resolve materialize scope: %v", err)
	}

	if got, want := scope.Week, "2026-03-08"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := scope.WeekStart.Format(time.RFC3339), "2026-03-08T00:00:00Z"; got != want {
		t.Fatalf("unexpected window start: got=%q want=%q", got, want)
	}
	if got, want := scope.WeekEnd.Format(time.RFC3339), "2026-03-15T00:00:00Z"; got != want {
		t.Fatalf("unexpected window end: got=%q want=%q", got, want)
	}
}

func TestResolveMaterializeScopeRejectsNonSundayWeek(t *testing.T) {
	t.Parallel()

	if _, err := resolveMaterializeScope("2026-03-09", time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)); err == nil {
		t.Fatalf("expected validation error for non-Sunday week")
	}
}
