package weeknav

import (
	"testing"
	"time"
)

func TestResolveWindowDefaultsToLatestCompleteWeek(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)
	week, previous, next, _ := ResolveWindow(
		[]string{"2026-03-08", "2026-03-15", "2026-03-22", "2026-03-29"},
		"",
		"",
		now,
	)
	if week != "2026-03-22" {
		t.Fatalf("unexpected default week: got=%q want=%q", week, "2026-03-22")
	}
	if previous != "2026-03-15" {
		t.Fatalf("unexpected previous week: got=%q want=%q", previous, "2026-03-15")
	}
	if next != "2026-03-29" {
		t.Fatalf("expected next week to point to current partial week, got=%q", next)
	}
}

func TestResolveWindowHonorsExplicitCurrentWeekRequest(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)
	week, previous, next, _ := ResolveWindow(
		[]string{"2026-03-08", "2026-03-15", "2026-03-22", "2026-03-29"},
		"2026-03-29",
		"",
		now,
	)
	if week != "2026-03-29" {
		t.Fatalf("unexpected selected week: got=%q want=%q", week, "2026-03-29")
	}
	if previous != "2026-03-22" {
		t.Fatalf("unexpected previous week: got=%q want=%q", previous, "2026-03-22")
	}
	if next != "" {
		t.Fatalf("expected no newer week after current week, got=%q", next)
	}
}

func TestResolveWindowDoesNotDefaultToCurrentWeekWhenConfigured(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)
	week, _, _, _ := ResolveWindow(
		[]string{"2026-03-15", "2026-03-22", "2026-03-29"},
		"",
		"2026-03-29",
		now,
	)
	if week != "2026-03-22" {
		t.Fatalf("unexpected default week from configured current week: got=%q want=%q", week, "2026-03-22")
	}
}

func TestResolveWindowAtOldestWeekDisablesOlderOnly(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)
	week, previous, next, _ := ResolveWindow(
		[]string{"2026-03-08", "2026-03-15", "2026-03-22", "2026-03-29"},
		"2026-03-08",
		"",
		now,
	)
	if week != "2026-03-08" {
		t.Fatalf("unexpected selected oldest week: got=%q want=%q", week, "2026-03-08")
	}
	if previous != "" {
		t.Fatalf("expected no older week, got=%q", previous)
	}
	if next != "2026-03-15" {
		t.Fatalf("expected newer week to remain available, got=%q", next)
	}
}
