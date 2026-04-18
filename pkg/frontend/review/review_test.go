package review

import "testing"

func TestRoutePathAppliesConfiguredPrefix(t *testing.T) {
	t.Parallel()

	h := &handler{
		routePrefix: "/review",
	}

	if got, want := h.routePath("/actions/links"), "/review/actions/links"; got != want {
		t.Fatalf("unexpected prefixed action path: got=%q want=%q", got, want)
	}
	if got, want := h.routePath("/"), "/review/"; got != want {
		t.Fatalf("unexpected prefixed root path: got=%q want=%q", got, want)
	}
}

func TestViewHrefAppendsWeekQuery(t *testing.T) {
	t.Parallel()

	h := &handler{
		routePrefix: "/review",
	}

	if got, want := h.viewHref(h.routePath("/"), "2026-03-15"), "/review/?week=2026-03-15"; got != want {
		t.Fatalf("unexpected prefixed href: got=%q want=%q", got, want)
	}
	if got, want := h.viewHref("/report", "2026-03-15"), "/report?week=2026-03-15"; got != want {
		t.Fatalf("unexpected report href: got=%q want=%q", got, want)
	}
}
