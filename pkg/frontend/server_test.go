package frontend

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleRootRedirectsToWeekly(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/?week=2026-03-15", nil)
	recorder := httptest.NewRecorder()

	(&handler{}).handleRoot(recorder, req)

	if got, want := recorder.Code, http.StatusFound; got != want {
		t.Fatalf("unexpected status code: got=%d want=%d", got, want)
	}
	if got, want := recorder.Header().Get("Location"), "/weekly?week=2026-03-15"; got != want {
		t.Fatalf("unexpected redirect target: got=%q want=%q", got, want)
	}
}

func TestViewHrefIncludesWeekQuery(t *testing.T) {
	t.Parallel()

	if got, want := viewHref("/weekly", "2026-03-15"), "/weekly?week=2026-03-15"; got != want {
		t.Fatalf("unexpected href: got=%q want=%q", got, want)
	}
	if got, want := viewHref("/triage", "2026-03-15"), "/triage?week=2026-03-15"; got != want {
		t.Fatalf("unexpected triage href: got=%q want=%q", got, want)
	}
}

func TestNavigationHrefIsEmptyWithoutWeek(t *testing.T) {
	t.Parallel()

	if got := navigationHref("/weekly", ""); got != "" {
		t.Fatalf("expected empty navigation href without week, got=%q", got)
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
	if got, want := recorder.Header().Get("Location"), "/triage?week=2026-03-15"; got != want {
		t.Fatalf("unexpected redirect target: got=%q want=%q", got, want)
	}
}
