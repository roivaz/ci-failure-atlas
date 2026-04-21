package prowjobs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
)

const (
	defaultHTTPTimeout         = 90 * time.Second
	defaultJobHistoryBucket    = "test-platform-results"
	defaultJobHistoryPageLimit = 20
)

type Pull struct {
	Number int    `json:"number"`
	SHA    string `json:"sha"`
}

type Refs struct {
	Pulls []Pull `json:"pulls,omitempty"`
}

type JobSpec struct {
	Type string `json:"type,omitempty"`
	Job  string `json:"job,omitempty"`
	Refs *Refs  `json:"refs,omitempty"`
}

type JobStatus struct {
	State          string    `json:"state,omitempty"`
	StartTime      time.Time `json:"startTime,omitempty"`
	CompletionTime time.Time `json:"completionTime,omitempty"`
	URL            string    `json:"url,omitempty"`
	BuildID        string    `json:"build_id,omitempty"`
}

type Job struct {
	Spec   JobSpec   `json:"spec,omitempty"`
	Status JobStatus `json:"status,omitempty"`
}

type JobHistoryBuild struct {
	ID           string        `json:"ID"`
	SpyglassLink string        `json:"SpyglassLink"`
	Started      time.Time     `json:"Started"`
	Duration     time.Duration `json:"Duration"`
	Result       string        `json:"Result"`
	Refs         *Refs         `json:"Refs,omitempty"`
}

type JobHistoryPage struct {
	Builds    []JobHistoryBuild
	OlderLink string
}

type JobList struct {
	Items []Job `json:"items"`
}

type Client interface {
	ListJobs(ctx context.Context) ([]Job, error)
	GetJobHistoryPage(ctx context.Context, historyPathOrURL string) (JobHistoryPage, error)
}

type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient {
	endpoint := strings.TrimSpace(baseURL)
	if endpoint == "" {
		endpoint = "https://prow.ci.openshift.org"
	}
	return &HTTPClient{
		baseURL: strings.TrimRight(endpoint, "/"),
		httpClient: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
	}
}

func (b JobHistoryBuild) CompletionTime() time.Time {
	if b.Started.IsZero() || b.Duration <= 0 {
		return time.Time{}
	}
	return b.Started.Add(b.Duration).UTC()
}

func (b JobHistoryBuild) AsJob(prowBaseURL string, jobName string) (Job, bool) {
	normalizedJobName := strings.TrimSpace(jobName)
	if normalizedJobName == "" {
		return Job{}, false
	}
	runURL, err := resolveURL(prowBaseURL, b.SpyglassLink)
	if err != nil {
		return Job{}, false
	}
	return Job{
		Spec: JobSpec{
			Job:  normalizedJobName,
			Refs: b.Refs,
		},
		Status: JobStatus{
			State:          stateFromJobHistoryResult(b.Result),
			StartTime:      b.Started.UTC(),
			CompletionTime: b.CompletionTime(),
			URL:            runURL,
			BuildID:        strings.TrimSpace(b.ID),
		},
	}, true
}

func (p JobHistoryPage) AsJobs(prowBaseURL string, jobName string) []Job {
	jobs := make([]Job, 0, len(p.Builds))
	for _, build := range p.Builds {
		job, ok := build.AsJob(prowBaseURL, jobName)
		if !ok {
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs
}

func (c *HTTPClient) ListJobs(ctx context.Context) ([]Job, error) {
	endpoint, err := c.prowjobsEndpoint()
	if err != nil {
		return nil, err
	}

	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build prow jobs request: %w", err)
		}
		req.Header.Set("Accept", "application/json, application/javascript, text/javascript")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("prow jobs request failed: %w", err)
			if attempt < maxAttempts {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return nil, lastErr
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read prow jobs response body: %w", readErr)
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("prow jobs request returned status %d for %s: %s", resp.StatusCode, endpoint, strings.TrimSpace(string(limitBytes(body, 2048))))
			if attempt < maxAttempts && resp.StatusCode >= 500 {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return nil, lastErr
		}

		rows, err := decodeJobList(body)
		if err != nil {
			return nil, fmt.Errorf("decode prow jobs response: %w", err)
		}
		return rows, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("prow jobs request failed without explicit error")
}

func (c *HTTPClient) GetJobHistoryPage(ctx context.Context, historyPathOrURL string) (JobHistoryPage, error) {
	endpoint, err := c.jobHistoryEndpoint(historyPathOrURL)
	if err != nil {
		return JobHistoryPage{}, err
	}

	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return JobHistoryPage{}, ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return JobHistoryPage{}, fmt.Errorf("build prow job history request: %w", err)
		}
		req.Header.Set("Accept", "text/html,application/xhtml+xml")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("prow job history request failed: %w", err)
			if attempt < maxAttempts {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return JobHistoryPage{}, lastErr
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return JobHistoryPage{}, fmt.Errorf("read prow job history response body: %w", readErr)
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("prow job history request returned status %d for %s: %s", resp.StatusCode, endpoint, strings.TrimSpace(string(limitBytes(body, 2048))))
			if attempt < maxAttempts && resp.StatusCode >= 500 {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return JobHistoryPage{}, lastErr
		}

		page, err := decodeJobHistoryPage(body)
		if err != nil {
			return JobHistoryPage{}, fmt.Errorf("decode prow job history page: %w", err)
		}
		if page.OlderLink != "" {
			base := endpoint
			if resp.Request != nil && resp.Request.URL != nil {
				base = resp.Request.URL.String()
			}
			resolvedOlderLink, err := resolveURL(base, page.OlderLink)
			if err != nil {
				return JobHistoryPage{}, fmt.Errorf("resolve older-runs link %q: %w", page.OlderLink, err)
			}
			page.OlderLink = resolvedOlderLink
		}
		return page, nil
	}

	if lastErr != nil {
		return JobHistoryPage{}, lastErr
	}
	return JobHistoryPage{}, fmt.Errorf("prow job history request failed without explicit error")
}

func (c *HTTPClient) prowjobsEndpoint() (string, error) {
	if strings.TrimSpace(c.baseURL) == "" {
		return "", fmt.Errorf("prow base URL is required")
	}
	parsed, err := url.Parse(c.baseURL)
	if err != nil {
		return "", fmt.Errorf("parse prow base URL: %w", err)
	}
	if strings.HasSuffix(strings.ToLower(parsed.Path), ".js") {
		return parsed.String(), nil
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/prowjobs.js"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func (c *HTTPClient) jobHistoryEndpoint(historyPathOrURL string) (string, error) {
	if strings.TrimSpace(c.baseURL) == "" {
		return "", fmt.Errorf("prow base URL is required")
	}
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return "", fmt.Errorf("parse prow base URL: %w", err)
	}
	ref, err := url.Parse(strings.TrimSpace(historyPathOrURL))
	if err != nil {
		return "", fmt.Errorf("parse prow job history path: %w", err)
	}
	if ref.IsAbs() {
		return ref.String(), nil
	}
	if strings.TrimSpace(ref.Path) == "" {
		return "", fmt.Errorf("prow job history path is required")
	}
	switch {
	case strings.HasPrefix(ref.Path, "/"):
	case strings.HasPrefix(ref.Path, "job-history/"):
		ref.Path = "/" + ref.Path
	default:
		ref.Path = path.Join("/job-history/gs", defaultJobHistoryBucket, ref.Path)
	}
	return base.ResolveReference(ref).String(), nil
}

func decodeJobList(payload []byte) ([]Job, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty response body")
	}

	jsonPayload := trimmed
	if trimmed[0] != '{' && trimmed[0] != '[' {
		start := bytes.IndexAny(trimmed, "{[")
		end := bytes.LastIndexAny(trimmed, "}]")
		if start < 0 || end < start {
			return nil, fmt.Errorf("response body does not contain a JSON object")
		}
		jsonPayload = trimmed[start : end+1]
	}

	var list JobList
	if err := json.Unmarshal(jsonPayload, &list); err != nil {
		return nil, err
	}
	if list.Items == nil {
		return []Job{}, nil
	}
	return list.Items, nil
}

var (
	anchorTagPattern = regexp.MustCompile(`(?is)<a[^>]*href=(?:"([^"]*)"|'([^']*)')[^>]*>(.*?)</a>`)
	htmlTagPattern   = regexp.MustCompile(`(?is)<[^>]+>`)
)

func decodeJobHistoryPage(payload []byte) (JobHistoryPage, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return JobHistoryPage{}, fmt.Errorf("empty response body")
	}

	buildsPayload, err := extractAssignedJSON(trimmed, "allBuilds")
	if err != nil {
		return JobHistoryPage{}, err
	}

	var builds []JobHistoryBuild
	if err := json.Unmarshal(buildsPayload, &builds); err != nil {
		return JobHistoryPage{}, err
	}
	if builds == nil {
		builds = []JobHistoryBuild{}
	}

	return JobHistoryPage{
		Builds:    builds,
		OlderLink: extractJobHistoryLink(trimmed, "Older Runs"),
	}, nil
}

func extractAssignedJSON(payload []byte, variable string) ([]byte, error) {
	index := bytes.Index(payload, []byte(variable))
	if index < 0 {
		return nil, fmt.Errorf("missing %s assignment", variable)
	}
	assignmentIndex := bytes.IndexByte(payload[index:], '=')
	if assignmentIndex < 0 {
		return nil, fmt.Errorf("missing %s assignment operator", variable)
	}
	start := index + assignmentIndex + 1
	for start < len(payload) && isSpaceByte(payload[start]) {
		start++
	}
	if start >= len(payload) {
		return nil, fmt.Errorf("missing %s JSON payload", variable)
	}
	jsonPayload, err := extractBalancedJSON(payload[start:])
	if err != nil {
		return nil, fmt.Errorf("extract %s JSON payload: %w", variable, err)
	}
	return jsonPayload, nil
}

func extractBalancedJSON(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty JSON payload")
	}
	start := payload[0]
	if start != '{' && start != '[' {
		return nil, fmt.Errorf("expected JSON object or array, got %q", string(start))
	}

	var stack []byte
	inString := false
	escaped := false

	for index, current := range payload {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch current {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}

		switch current {
		case '"':
			inString = true
		case '{':
			stack = append(stack, '}')
		case '[':
			stack = append(stack, ']')
		case '}', ']':
			if len(stack) == 0 || current != stack[len(stack)-1] {
				return nil, fmt.Errorf("unexpected closing delimiter %q", string(current))
			}
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				return payload[:index+1], nil
			}
		}
	}

	return nil, fmt.Errorf("unterminated JSON payload")
}

func extractJobHistoryLink(payload []byte, label string) string {
	matches := anchorTagPattern.FindAllSubmatch(payload, -1)
	for _, match := range matches {
		href := strings.TrimSpace(string(match[1]))
		if href == "" {
			href = strings.TrimSpace(string(match[2]))
		}
		if href == "" {
			continue
		}
		text := html.UnescapeString(string(htmlTagPattern.ReplaceAll(match[3], nil)))
		text = strings.Join(strings.Fields(text), " ")
		if strings.Contains(text, label) {
			return href
		}
	}
	return ""
}

func resolveURL(baseURL string, raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("URL is required")
	}
	ref, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse URL %q: %w", raw, err)
	}
	if ref.IsAbs() {
		return ref.String(), nil
	}
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("parse base URL %q: %w", baseURL, err)
	}
	return base.ResolveReference(ref).String(), nil
}

func stateFromJobHistoryResult(result string) string {
	switch strings.ToLower(strings.TrimSpace(result)) {
	case "success":
		return "success"
	case "failure":
		return "failure"
	case "error":
		return "error"
	case "aborted":
		return "aborted"
	case "pending":
		return "pending"
	default:
		return strings.ToLower(strings.TrimSpace(result))
	}
}

func isSpaceByte(value byte) bool {
	switch value {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func IsTerminalState(state string) bool {
	switch normalizeState(state) {
	case "success", "failure", "error":
		return true
	default:
		return false
	}
}

func FailedFromState(state string) bool {
	return normalizeState(state) != "success"
}

func normalizeState(state string) string {
	return strings.ToLower(strings.TrimSpace(state))
}

func limitBytes(in []byte, limit int) []byte {
	if len(in) <= limit {
		return in
	}
	return in[:limit]
}
