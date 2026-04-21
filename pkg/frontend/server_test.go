package frontend

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReportHrefIncludesWindowAndMode(t *testing.T) {
	t.Parallel()

	if got, want := reportHref("/report", "2026-03-09", "2026-03-15", reportPageModeReport), "/report?end_date=2026-03-15&start_date=2026-03-09"; got != want {
		t.Fatalf("unexpected report href: got=%q want=%q", got, want)
	}
	if got, want := reportHref("/report", "2026-03-09", "2026-03-15", reportPageModeRolling), "/report?end_date=2026-03-15&mode=rolling&start_date=2026-03-09"; got != want {
		t.Fatalf("unexpected rolling report href: got=%q want=%q", got, want)
	}
	if got, want := reportHref("/report", "2026-04-13", "2026-04-26", reportPageModeSprint), "/report?end_date=2026-04-26&mode=sprint&start_date=2026-04-13"; got != want {
		t.Fatalf("unexpected sprint report href: got=%q want=%q", got, want)
	}
}

func TestViewHrefIncludesWeekQuery(t *testing.T) {
	t.Parallel()

	if got, want := viewHref("/report", "2026-03-15"), "/report?week=2026-03-15"; got != want {
		t.Fatalf("unexpected report href: got=%q want=%q", got, want)
	}
	if got, want := viewHref("/failure-patterns", "2026-03-15"), "/failure-patterns?week=2026-03-15"; got != want {
		t.Fatalf("unexpected failure-pattern href: got=%q want=%q", got, want)
	}
}

func TestNormalizeReportPageModeDefaultsToReport(t *testing.T) {
	t.Parallel()

	if got, want := normalizeReportPageMode(""), reportPageModeReport; got != want {
		t.Fatalf("unexpected default report mode: got=%q want=%q", got, want)
	}
	if got, want := normalizeReportPageMode("rolling"), reportPageModeRolling; got != want {
		t.Fatalf("unexpected rolling mode: got=%q want=%q", got, want)
	}
	if got, want := normalizeReportPageMode("sprint"), reportPageModeSprint; got != want {
		t.Fatalf("unexpected sprint mode: got=%q want=%q", got, want)
	}
	if got, want := normalizeReportPageMode("SPRINT"), reportPageModeSprint; got != want {
		t.Fatalf("unexpected uppercase sprint mode: got=%q want=%q", got, want)
	}
}

func TestHandleLegacyGlobalRedirectsToFailurePatterns(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/global?week=2026-03-15", nil)
	recorder := httptest.NewRecorder()

	(&handler{}).handleLegacyGlobalRedirect(recorder, req)

	if got, want := recorder.Code, http.StatusMovedPermanently; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d", got, want)
	}
	if got, want := recorder.Header().Get("Location"), "/failure-patterns?week=2026-03-15"; got != want {
		t.Fatalf("unexpected redirect target: got=%q want=%q", got, want)
	}
}

func TestAnchorWeekDateRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		anchorWeek    string
		fallbackStart string
		fallbackEnd   string
		wantStart     string
		wantEnd       string
	}{
		{
			name:          "valid anchor week returns Sun-Sat range",
			anchorWeek:    "2026-03-15",
			fallbackStart: "2026-03-18",
			fallbackEnd:   "2026-03-18",
			wantStart:     "2026-03-15",
			wantEnd:       "2026-03-21",
		},
		{
			name:          "empty anchor week falls back",
			anchorWeek:    "",
			fallbackStart: "2026-03-18",
			fallbackEnd:   "2026-03-18",
			wantStart:     "2026-03-18",
			wantEnd:       "2026-03-18",
		},
		{
			name:          "invalid anchor week falls back",
			anchorWeek:    "not-a-date",
			fallbackStart: "2026-04-01",
			fallbackEnd:   "2026-04-07",
			wantStart:     "2026-04-01",
			wantEnd:       "2026-04-07",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotStart, gotEnd := anchorWeekDateRange(tc.anchorWeek, tc.fallbackStart, tc.fallbackEnd)
			if gotStart != tc.wantStart || gotEnd != tc.wantEnd {
				t.Fatalf("anchorWeekDateRange(%q, %q, %q) = (%q, %q), want (%q, %q)",
					tc.anchorWeek, tc.fallbackStart, tc.fallbackEnd,
					gotStart, gotEnd, tc.wantStart, tc.wantEnd)
			}
		})
	}
}
