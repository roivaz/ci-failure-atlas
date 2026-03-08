package githubpullrequests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClientListPullRequestsPageParsesResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/Azure/ARO-HCP/pulls" {
			t.Fatalf("unexpected path: %q", r.URL.Path)
		}
		query := r.URL.Query()
		if query.Get("state") != "all" || query.Get("sort") != "updated" || query.Get("direction") != "desc" || query.Get("per_page") != "100" || query.Get("page") != "2" {
			t.Fatalf("unexpected query: %s", r.URL.RawQuery)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Limit", "60")
		w.Header().Set("X-RateLimit-Remaining", "42")
		w.Header().Set("X-RateLimit-Reset", "1773000000")
		w.Header().Set("Link", `<https://api.github.com/repositories/1/pulls?state=all&sort=updated&direction=desc&per_page=100&page=3>; rel="next"`)
		_, _ = w.Write([]byte(`[
			{
				"number": 101,
				"state": "open",
				"merge_commit_sha": null,
				"merged_at": null,
				"closed_at": null,
				"updated_at": "2026-03-07T10:00:00Z",
				"head": {"sha": "head-101"}
			},
			{
				"number": 202,
				"state": "closed",
				"merge_commit_sha": "merge-202",
				"merged_at": "2026-03-06T09:30:00Z",
				"closed_at": "2026-03-06T09:30:00Z",
				"updated_at": "2026-03-06T09:31:00Z",
				"head": {"sha": "head-202"}
			}
		]`))
	}))
	t.Cleanup(server.Close)

	client := NewHTTPClient(server.URL)
	rows, rate, hasNext, err := client.ListPullRequestsPage(context.Background(), ListPullRequestsPageOptions{
		Owner:   "Azure",
		Repo:    "ARO-HCP",
		State:   "all",
		Sort:    "updated",
		PerPage: 100,
		Page:    2,
	})
	if err != nil {
		t.Fatalf("list pull request page: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("unexpected row count: got=%d want=2", len(rows))
	}
	if rows[0].Number != 101 || rows[0].State != "open" || rows[0].Merged || rows[0].HeadSHA != "head-101" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	if rows[1].Number != 202 || rows[1].State != "closed" || !rows[1].Merged || rows[1].HeadSHA != "head-202" || rows[1].MergeCommitSHA != "merge-202" {
		t.Fatalf("unexpected second row: %+v", rows[1])
	}
	if rows[1].MergedAt.IsZero() || rows[1].ClosedAt.IsZero() || rows[1].UpdatedAt.IsZero() {
		t.Fatalf("expected merged/closed/updated timestamps on second row: %+v", rows[1])
	}

	if rate.Limit != 60 || rate.Remaining != 42 {
		t.Fatalf("unexpected rate values: %+v", rate)
	}
	if rate.ResetAt.IsZero() {
		t.Fatalf("expected reset timestamp in rate values")
	}
	if !hasNext {
		t.Fatalf("expected hasNext=true from link header")
	}
}

func TestHTTPClientListPullRequestsPageValidatesInputs(t *testing.T) {
	t.Parallel()

	client := NewHTTPClient("https://api.github.com")
	_, _, _, err := client.ListPullRequestsPage(context.Background(), ListPullRequestsPageOptions{
		Owner: "",
		Repo:  "ARO-HCP",
	})
	if err == nil {
		t.Fatalf("expected validation error for missing owner")
	}

	_, _, _, err = client.ListPullRequestsPage(context.Background(), ListPullRequestsPageOptions{
		Owner: "Azure",
		Repo:  "ARO-HCP",
		State: "weird",
	})
	if err == nil {
		t.Fatalf("expected validation error for bad state")
	}
}

func TestHasLinkRelation(t *testing.T) {
	t.Parallel()

	link := `<https://api.github.com/repositories/1/pulls?page=2>; rel="next", <https://api.github.com/repositories/1/pulls?page=20>; rel="last"`
	if !hasLinkRelation(link, "next") {
		t.Fatalf("expected next relation to be found")
	}
	if hasLinkRelation(link, "prev") {
		t.Fatalf("did not expect prev relation to be found")
	}
}

func TestParseRateLimit(t *testing.T) {
	t.Parallel()

	h := http.Header{}
	h.Set("X-RateLimit-Limit", "60")
	h.Set("X-RateLimit-Remaining", "59")
	h.Set("X-RateLimit-Reset", "1773000000")
	parsed := parseRateLimit(h)
	if parsed.Limit != 60 || parsed.Remaining != 59 {
		t.Fatalf("unexpected rate fields: %+v", parsed)
	}
	if parsed.ResetAt.Equal(time.Time{}) {
		t.Fatalf("expected reset time to be parsed")
	}
}
