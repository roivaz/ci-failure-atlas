package readmodel

import (
	"testing"
	"time"
)

func TestSprintWindowForDateAnchorAlignment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		now       string
		wantStart string
		wantEnd   string
	}{
		{
			name:      "anchor date itself",
			now:       "2026-01-05",
			wantStart: "2026-01-05",
			wantEnd:   "2026-01-18",
		},
		{
			name:      "mid-first-sprint",
			now:       "2026-01-10",
			wantStart: "2026-01-05",
			wantEnd:   "2026-01-18",
		},
		{
			name:      "last day of first sprint",
			now:       "2026-01-18",
			wantStart: "2026-01-05",
			wantEnd:   "2026-01-18",
		},
		{
			name:      "first day of second sprint",
			now:       "2026-01-19",
			wantStart: "2026-01-19",
			wantEnd:   "2026-02-01",
		},
		{
			name:      "far future",
			now:       "2026-04-15",
			wantStart: "2026-04-13",
			wantEnd:   "2026-04-26",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now, _ := time.Parse("2006-01-02", tc.now)
			start, end := SprintWindowForDate(now)
			gotStart := start.Format("2006-01-02")
			gotEnd := end.Format("2006-01-02")
			if gotStart != tc.wantStart || gotEnd != tc.wantEnd {
				t.Fatalf("SprintWindowForDate(%s) = (%s, %s), want (%s, %s)",
					tc.now, gotStart, gotEnd, tc.wantStart, tc.wantEnd)
			}
		})
	}
}

func TestShiftSprintWindow(t *testing.T) {
	t.Parallel()

	ref, _ := time.Parse("2006-01-02", "2026-01-10")

	start, end := ShiftSprintWindow(ref, 1)
	if got := start.Format("2006-01-02"); got != "2026-01-19" {
		t.Fatalf("next sprint start = %s, want 2026-01-19", got)
	}
	if got := end.Format("2006-01-02"); got != "2026-02-01" {
		t.Fatalf("next sprint end = %s, want 2026-02-01", got)
	}

	start, end = ShiftSprintWindow(ref, -1)
	if got := start.Format("2006-01-02"); got != "2025-12-22" {
		t.Fatalf("previous sprint start = %s, want 2025-12-22", got)
	}
	if got := end.Format("2006-01-02"); got != "2026-01-04" {
		t.Fatalf("previous sprint end = %s, want 2026-01-04", got)
	}
}

func TestSprintDurationDays(t *testing.T) {
	t.Parallel()
	if got := SprintDurationDays(); got != 14 {
		t.Fatalf("SprintDurationDays() = %d, want 14", got)
	}
}
