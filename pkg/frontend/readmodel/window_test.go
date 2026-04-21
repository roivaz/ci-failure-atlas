package readmodel

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestFilterAvailableWeeks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		requested []string
		available []string
		want      []string
	}{
		{
			name:      "all available",
			requested: []string{"2026-03-08", "2026-03-15"},
			available: []string{"2026-03-08", "2026-03-15"},
			want:      []string{"2026-03-08", "2026-03-15"},
		},
		{
			name:      "trailing missing",
			requested: []string{"2026-03-08", "2026-03-15"},
			available: []string{"2026-03-08"},
			want:      []string{"2026-03-08"},
		},
		{
			name:      "leading missing",
			requested: []string{"2026-03-08", "2026-03-15"},
			available: []string{"2026-03-15"},
			want:      []string{"2026-03-15"},
		},
		{
			name:      "none available",
			requested: []string{"2026-03-08", "2026-03-15"},
			available: []string{"2026-03-01"},
			want:      []string{},
		},
		{
			name:      "middle of three available",
			requested: []string{"2026-03-08", "2026-03-15", "2026-03-22"},
			available: []string{"2026-03-15"},
			want:      []string{"2026-03-15"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := filterAvailableWeeks(tc.requested, tc.available)
			if len(got) != len(tc.want) {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got[%d]=%q want=%q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestInteriorGap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		weeks []string
		want  string
	}{
		{name: "empty", weeks: nil, want: ""},
		{name: "single", weeks: []string{"2026-03-08"}, want: ""},
		{name: "contiguous two", weeks: []string{"2026-03-08", "2026-03-15"}, want: ""},
		{name: "contiguous three", weeks: []string{"2026-03-08", "2026-03-15", "2026-03-22"}, want: ""},
		{
			name:  "gap in middle of three",
			weeks: []string{"2026-03-08", "2026-03-22"},
			want:  "2026-03-15",
		},
		{
			name:  "gap in middle of four",
			weeks: []string{"2026-03-01", "2026-03-08", "2026-03-22"},
			want:  "2026-03-15",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := interiorGap(tc.weeks)
			if got != tc.want {
				t.Fatalf("got=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestResolveWindowTrailingEdgeWeekMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")

	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}

	scope, err := fixture.service.ResolveWindow(ctx, WindowRequest{
		StartDate: "2026-03-16",
		EndDate:   "2026-03-22",
	})
	if err != nil {
		t.Fatalf("expected trailing-edge tolerance: %v", err)
	}
	if got, want := len(scope.SemanticWeeks), 1; got != want {
		t.Fatalf("semantic weeks: got=%d want=%d", got, want)
	}
	if got, want := scope.SemanticWeeks[0], "2026-03-15"; got != want {
		t.Fatalf("semantic week: got=%q want=%q", got, want)
	}
	if got, want := scope.StartDate, "2026-03-16"; got != want {
		t.Fatalf("start date should be preserved: got=%q want=%q", got, want)
	}
	if got, want := scope.EndDate, "2026-03-22"; got != want {
		t.Fatalf("end date should be preserved: got=%q want=%q", got, want)
	}
}

func TestResolveWindowLeadingEdgeWeekMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")

	store := fixture.openWeekStore(t, "2026-03-22")
	if err := store.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}

	scope, err := fixture.service.ResolveWindow(ctx, WindowRequest{
		StartDate: "2026-03-16",
		EndDate:   "2026-03-28",
	})
	if err != nil {
		t.Fatalf("expected leading-edge tolerance: %v", err)
	}
	if got, want := len(scope.SemanticWeeks), 1; got != want {
		t.Fatalf("semantic weeks: got=%d want=%d", got, want)
	}
	if got, want := scope.SemanticWeeks[0], "2026-03-22"; got != want {
		t.Fatalf("semantic week: got=%q want=%q", got, want)
	}
	if got, want := scope.StartDate, "2026-03-16"; got != want {
		t.Fatalf("start date should be preserved: got=%q want=%q", got, want)
	}
	if got, want := scope.EndDate, "2026-03-28"; got != want {
		t.Fatalf("end date should be preserved: got=%q want=%q", got, want)
	}
}

func TestResolveWindowInteriorGapFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")

	firstStore := fixture.openWeekStore(t, "2026-03-08")
	thirdStore := fixture.openWeekStore(t, "2026-03-22")
	if err := firstStore.ReplaceMaterializedWeek(ctx, previousMaterializedWeek()); err != nil {
		t.Fatalf("seed first week: %v", err)
	}
	if err := thirdStore.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed third week: %v", err)
	}

	_, err := fixture.service.ResolveWindow(ctx, WindowRequest{
		StartDate: "2026-03-08",
		EndDate:   "2026-03-28",
	})
	if err == nil {
		t.Fatalf("expected interior gap to cause failure")
	}
	if !strings.Contains(err.Error(), "2026-03-15") {
		t.Fatalf("expected error to mention missing interior week 2026-03-15, got=%v", err)
	}
}

func TestResolveWindowAllWeeksMissingFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")

	_, err := fixture.service.ResolveWindow(ctx, WindowRequest{
		StartDate: "2026-03-16",
		EndDate:   "2026-03-22",
	})
	if err == nil {
		t.Fatalf("expected all-missing weeks to cause failure")
	}
}

func TestResolveWindowSprintWithTrailingFutureWeek(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")

	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}

	scope, err := fixture.service.ResolveWindow(ctx, WindowRequest{
		StartDate: "2026-03-13",
		EndDate:   "2026-03-26",
		Now:       time.Date(2026, 3, 18, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("expected sprint-like window to tolerate trailing missing: %v", err)
	}
	if got, want := len(scope.SemanticWeeks), 1; got != want {
		t.Fatalf("semantic weeks: got=%d want=%d", got, want)
	}
	if got, want := scope.EndDate, "2026-03-26"; got != want {
		t.Fatalf("end date should be preserved: got=%q want=%q", got, want)
	}
}

func TestResolveWindowBothEdgesMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")

	store := fixture.openWeekStore(t, "2026-03-15")
	if err := store.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
		t.Fatalf("seed materialized week: %v", err)
	}

	scope, err := fixture.service.ResolveWindow(ctx, WindowRequest{
		StartDate: "2026-03-10",
		EndDate:   "2026-03-26",
	})
	if err != nil {
		t.Fatalf("expected both-edge tolerance: %v", err)
	}
	if got, want := len(scope.SemanticWeeks), 1; got != want {
		t.Fatalf("semantic weeks: got=%d want=%d", got, want)
	}
	if got, want := scope.StartDate, "2026-03-10"; got != want {
		t.Fatalf("start date should be preserved: got=%q want=%q", got, want)
	}
	if got, want := scope.EndDate, "2026-03-26"; got != want {
		t.Fatalf("end date should be preserved: got=%q want=%q", got, want)
	}
}

func TestResolveWindowMultipleAvailableWeeksNoClamping(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")

	for _, week := range []string{"2026-03-08", "2026-03-15"} {
		store := fixture.openWeekStore(t, week)
		if err := store.ReplaceMaterializedWeek(ctx, currentMaterializedWeek()); err != nil {
			t.Fatalf("seed %s: %v", week, err)
		}
	}

	scope, err := fixture.service.ResolveWindow(ctx, WindowRequest{
		StartDate: "2026-03-10",
		EndDate:   "2026-03-20",
	})
	if err != nil {
		t.Fatalf("expected success: %v", err)
	}
	if got, want := len(scope.SemanticWeeks), 2; got != want {
		t.Fatalf("semantic weeks: got=%d want=%d (%v)", got, want, scope.SemanticWeeks)
	}
	if got, want := scope.StartDate, "2026-03-10"; got != want {
		t.Fatalf("start date should not be clamped: got=%q want=%q", got, want)
	}
	if got, want := scope.EndDate, "2026-03-20"; got != want {
		t.Fatalf("end date should not be clamped: got=%q want=%q", got, want)
	}
}
