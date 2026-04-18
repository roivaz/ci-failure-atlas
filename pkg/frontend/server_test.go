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
}

func TestHandleLegacyGlobalRedirectsToTriage(t *testing.T) {
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
