package prowjobs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultHTTPTimeout = 90 * time.Second

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

type JobList struct {
	Items []Job `json:"items"`
}

type Client interface {
	ListJobs(ctx context.Context) ([]Job, error)
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

func IsTerminalState(state string) bool {
	switch normalizeState(state) {
	case "success", "failure", "error", "aborted":
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
