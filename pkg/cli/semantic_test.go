package cli

import (
	"errors"
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
	if flag := materializeCmd.Flags().Lookup("all"); flag == nil {
		t.Fatalf("expected materialize command to expose --all flag")
	}
}

func TestResolveMaterializeScopeDefaultsToCurrentWeek(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)
	scope, err := resolveMaterializeScope("", now)
	if err != nil {
		t.Fatalf("resolve materialize scope: %v", err)
	}

	if got, want := scope.Week, "2026-03-30"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := scope.WeekStart.Format(time.RFC3339), "2026-03-30T00:00:00Z"; got != want {
		t.Fatalf("unexpected window start: got=%q want=%q", got, want)
	}
	if got, want := scope.WeekEnd.Format(time.RFC3339), "2026-04-06T00:00:00Z"; got != want {
		t.Fatalf("unexpected window end: got=%q want=%q", got, want)
	}
}

func TestResolveMaterializeScopeUsesExplicitWeek(t *testing.T) {
	t.Parallel()

	scope, err := resolveMaterializeScope("2026-03-09", time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("resolve materialize scope: %v", err)
	}

	if got, want := scope.Week, "2026-03-09"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
	if got, want := scope.WeekStart.Format(time.RFC3339), "2026-03-09T00:00:00Z"; got != want {
		t.Fatalf("unexpected window start: got=%q want=%q", got, want)
	}
	if got, want := scope.WeekEnd.Format(time.RFC3339), "2026-03-16T00:00:00Z"; got != want {
		t.Fatalf("unexpected window end: got=%q want=%q", got, want)
	}
}

func TestResolveMaterializeScopeRejectsNonMondayWeek(t *testing.T) {
	t.Parallel()

	if _, err := resolveMaterializeScope("2026-03-08", time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)); err == nil {
		t.Fatalf("expected validation error for non-Monday week")
	}
}

func TestResolveMaterializeScopesDefaultsToCurrentWeek(t *testing.T) {
	t.Parallel()

	scopes, err := resolveMaterializeScopes("", false, time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC), nil)
	if err != nil {
		t.Fatalf("resolve materialize scopes: %v", err)
	}
	if got, want := len(scopes), 1; got != want {
		t.Fatalf("unexpected scope count: got=%d want=%d", got, want)
	}
	if got, want := scopes[0].Week, "2026-03-30"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
}

func TestResolveMaterializeScopesUsesExplicitWeek(t *testing.T) {
	t.Parallel()

	scopes, err := resolveMaterializeScopes("2026-03-09", false, time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC), nil)
	if err != nil {
		t.Fatalf("resolve materialize scopes: %v", err)
	}
	if got, want := len(scopes), 1; got != want {
		t.Fatalf("unexpected scope count: got=%d want=%d", got, want)
	}
	if got, want := scopes[0].Week, "2026-03-09"; got != want {
		t.Fatalf("unexpected week: got=%q want=%q", got, want)
	}
}

func TestResolveMaterializeScopesUsesStoredWeeksForAll(t *testing.T) {
	t.Parallel()

	scopes, err := resolveMaterializeScopes("", true, time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC), func() ([]string, error) {
		return []string{"2026-03-09", "2026-03-16", "2026-03-23"}, nil
	})
	if err != nil {
		t.Fatalf("resolve materialize scopes: %v", err)
	}

	if got, want := len(scopes), 3; got != want {
		t.Fatalf("unexpected scope count: got=%d want=%d", got, want)
	}
	for index, want := range []string{"2026-03-09", "2026-03-16", "2026-03-23"} {
		if got := scopes[index].Week; got != want {
			t.Fatalf("unexpected week at index %d: got=%q want=%q", index, got, want)
		}
	}
}

func TestResolveMaterializeScopesAllowsNoStoredWeeksForAll(t *testing.T) {
	t.Parallel()

	scopes, err := resolveMaterializeScopes("", true, time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC), func() ([]string, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("resolve materialize scopes: %v", err)
	}
	if len(scopes) != 0 {
		t.Fatalf("expected no scopes, got=%d", len(scopes))
	}
}

func TestResolveMaterializeScopesRejectsWeekAndAll(t *testing.T) {
	t.Parallel()

	if _, err := resolveMaterializeScopes("2026-03-09", true, time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC), nil); err == nil {
		t.Fatalf("expected mutual exclusivity error")
	}
}

func TestResolveMaterializeScopesPropagatesListWeeksErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	if _, err := resolveMaterializeScopes("", true, time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC), func() ([]string, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped list-weeks error, got=%v", err)
	}
}
