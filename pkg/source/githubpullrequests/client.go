package githubpullrequests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultGitHubAPIBaseURL = "https://api.github.com"
	defaultHTTPTimeout      = 60 * time.Second
	defaultPerPage          = 100
)

type PullRequest struct {
	Number         int
	State          string
	Merged         bool
	HeadSHA        string
	MergeCommitSHA string
	MergedAt       time.Time
	ClosedAt       time.Time
	UpdatedAt      time.Time
}

type RateLimit struct {
	Limit     int
	Remaining int
	ResetAt   time.Time
}

type ListPullRequestsPageOptions struct {
	Owner     string
	Repo      string
	State     string
	Sort      string
	Direction string
	PerPage   int
	Page      int
}

type Client interface {
	ListPullRequestsPage(ctx context.Context, opts ListPullRequestsPageOptions) ([]PullRequest, RateLimit, bool, error)
}

type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient {
	endpoint := strings.TrimSpace(baseURL)
	if endpoint == "" {
		endpoint = defaultGitHubAPIBaseURL
	}
	return &HTTPClient{
		baseURL: strings.TrimRight(endpoint, "/"),
		httpClient: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
	}
}

func (c *HTTPClient) ListPullRequestsPage(ctx context.Context, opts ListPullRequestsPageOptions) ([]PullRequest, RateLimit, bool, error) {
	owner := strings.TrimSpace(opts.Owner)
	repo := strings.TrimSpace(opts.Repo)
	if owner == "" {
		return nil, RateLimit{}, false, fmt.Errorf("owner is required")
	}
	if repo == "" {
		return nil, RateLimit{}, false, fmt.Errorf("repo is required")
	}

	state := strings.ToLower(strings.TrimSpace(opts.State))
	if state == "" {
		state = "all"
	}
	switch state {
	case "open", "closed", "all":
	default:
		return nil, RateLimit{}, false, fmt.Errorf("unsupported state %q", state)
	}

	sortBy := strings.ToLower(strings.TrimSpace(opts.Sort))
	if sortBy == "" {
		sortBy = "updated"
	}
	switch sortBy {
	case "created", "updated", "popularity", "long-running":
	default:
		return nil, RateLimit{}, false, fmt.Errorf("unsupported sort %q", sortBy)
	}

	direction := strings.ToLower(strings.TrimSpace(opts.Direction))
	if direction == "" {
		direction = "desc"
	}
	switch direction {
	case "asc", "desc":
	default:
		return nil, RateLimit{}, false, fmt.Errorf("unsupported direction %q", direction)
	}

	perPage := opts.PerPage
	if perPage <= 0 {
		perPage = defaultPerPage
	}
	if perPage > 100 {
		perPage = 100
	}
	page := opts.Page
	if page <= 0 {
		page = 1
	}

	params := url.Values{}
	params.Set("state", state)
	params.Set("sort", sortBy)
	params.Set("direction", direction)
	params.Set("per_page", strconv.Itoa(perPage))
	params.Set("page", strconv.Itoa(page))

	requestURL := fmt.Sprintf("%s/repos/%s/%s/pulls?%s", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), params.Encode())

	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, RateLimit{}, false, ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
		if err != nil {
			return nil, RateLimit{}, false, fmt.Errorf("build github request: %w", err)
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("User-Agent", "ci-failure-atlas")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if attempt < maxAttempts {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return nil, RateLimit{}, false, fmt.Errorf("github request failed: %w", err)
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, RateLimit{}, false, fmt.Errorf("read github response body: %w", readErr)
		}

		rate := parseRateLimit(resp.Header)
		if resp.StatusCode != http.StatusOK {
			if attempt < maxAttempts && resp.StatusCode >= 500 {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return nil, rate, false, fmt.Errorf("github request returned status %d for %s: %s", resp.StatusCode, requestURL, strings.TrimSpace(string(limitBytes(body, 2048))))
		}

		var payload []pullRequestResponse
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, rate, false, fmt.Errorf("decode github pull request response: %w", err)
		}

		out := make([]PullRequest, 0, len(payload))
		for _, row := range payload {
			if row.Number <= 0 {
				continue
			}

			state := strings.ToLower(strings.TrimSpace(row.State))
			switch state {
			case "open", "closed":
			default:
				state = ""
			}
			merged := row.MergedAt != nil
			if merged {
				state = "closed"
			}

			pr := PullRequest{
				Number:         row.Number,
				State:          state,
				Merged:         merged,
				HeadSHA:        strings.TrimSpace(row.Head.SHA),
				MergeCommitSHA: strings.TrimSpace(row.MergeCommitSHA),
			}
			if row.MergedAt != nil {
				pr.MergedAt = row.MergedAt.UTC()
			}
			if row.ClosedAt != nil {
				pr.ClosedAt = row.ClosedAt.UTC()
			}
			if row.UpdatedAt != nil {
				pr.UpdatedAt = row.UpdatedAt.UTC()
			}

			out = append(out, pr)
		}

		hasNextPage := hasLinkRelation(resp.Header.Get("Link"), "next")
		return out, rate, hasNextPage, nil
	}

	return nil, RateLimit{}, false, fmt.Errorf("github request failed without explicit error")
}

func parseRateLimit(headers http.Header) RateLimit {
	out := RateLimit{}
	if value := strings.TrimSpace(headers.Get("X-RateLimit-Limit")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			out.Limit = parsed
		}
	}
	if value := strings.TrimSpace(headers.Get("X-RateLimit-Remaining")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			out.Remaining = parsed
		}
	}
	if value := strings.TrimSpace(headers.Get("X-RateLimit-Reset")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil && parsed > 0 {
			out.ResetAt = time.Unix(parsed, 0).UTC()
		}
	}
	return out
}

func hasLinkRelation(linkHeader string, relation string) bool {
	targetRelation := strings.ToLower(strings.TrimSpace(relation))
	if targetRelation == "" {
		return false
	}
	parts := strings.Split(linkHeader, ",")
	for _, part := range parts {
		pieces := strings.Split(strings.TrimSpace(part), ";")
		for _, piece := range pieces[1:] {
			value := strings.TrimSpace(piece)
			if !strings.HasPrefix(strings.ToLower(value), "rel=") {
				continue
			}
			quoted := strings.Trim(strings.TrimSpace(strings.TrimPrefix(value, "rel=")), "\"")
			if strings.EqualFold(quoted, targetRelation) {
				return true
			}
		}
	}
	return false
}

type pullRequestResponse struct {
	Number         int        `json:"number"`
	State          string     `json:"state"`
	MergeCommitSHA string     `json:"merge_commit_sha"`
	MergedAt       *time.Time `json:"merged_at"`
	ClosedAt       *time.Time `json:"closed_at"`
	UpdatedAt      *time.Time `json:"updated_at"`
	Head           struct {
		SHA string `json:"sha"`
	} `json:"head"`
}

func limitBytes(in []byte, limit int) []byte {
	if len(in) <= limit {
		return in
	}
	return in[:limit]
}
