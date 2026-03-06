package sippy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type JobRun struct {
	RunURL    string
	JobName   string
	StartedAt time.Time
	Failed    bool
}

type ListJobRunsOptions struct {
	Release  string
	Org      string
	Repo     string
	Since    time.Time
	PageSize int
}

type Client interface {
	ListJobRuns(ctx context.Context, opts ListJobRunsOptions) ([]JobRun, error)
}

type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient {
	endpoint := strings.TrimSpace(baseURL)
	if endpoint == "" {
		endpoint = "https://sippy.dptools.openshift.org"
	}
	return &HTTPClient{
		baseURL: strings.TrimRight(endpoint, "/"),
		httpClient: &http.Client{
			Timeout: 90 * time.Second,
		},
	}
}

func (c *HTTPClient) ListJobRuns(ctx context.Context, opts ListJobRunsOptions) ([]JobRun, error) {
	if strings.TrimSpace(opts.Release) == "" {
		return nil, fmt.Errorf("release is required")
	}
	if strings.TrimSpace(opts.Org) == "" {
		return nil, fmt.Errorf("org is required")
	}
	if strings.TrimSpace(opts.Repo) == "" {
		return nil, fmt.Errorf("repo is required")
	}

	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = 1000
	}
	if pageSize > 1000 {
		pageSize = 1000
	}

	filterJSON, err := buildJobRunsFilter(opts.Org, opts.Repo, opts.Since)
	if err != nil {
		return nil, fmt.Errorf("build job runs filter: %w", err)
	}

	results := make([]JobRun, 0, pageSize)
	for page := 0; ; page++ {
		params := url.Values{}
		params.Set("release", opts.Release)
		params.Set("filter", filterJSON)
		params.Set("sortField", "timestamp")
		params.Set("sort", "desc")
		params.Set("perPage", strconv.Itoa(pageSize))
		params.Set("page", strconv.Itoa(page))

		var payload jobRunsResponse
		if err := c.getJSON(ctx, "/api/jobs/runs", params, &payload); err != nil {
			return nil, err
		}
		if len(payload.Rows) == 0 {
			break
		}

		for _, row := range payload.Rows {
			runURL := strings.TrimSpace(row.URL)
			if runURL == "" {
				continue
			}

			startedAt := time.Time{}
			if row.Timestamp > 0 {
				startedAt = time.UnixMilli(row.Timestamp).UTC()
			}

			failed := row.Failed || row.InfrastructureFailure
			if !failed && !row.Succeeded && !strings.EqualFold(strings.TrimSpace(row.OverallResult), "S") {
				failed = true
			}

			results = append(results, JobRun{
				RunURL:    runURL,
				JobName:   strings.TrimSpace(row.Job),
				StartedAt: startedAt,
				Failed:    failed,
			})
		}

		if len(payload.Rows) < pageSize {
			break
		}
	}

	return results, nil
}

func (c *HTTPClient) getJSON(ctx context.Context, endpoint string, params url.Values, target any) error {
	u := c.baseURL + endpoint
	if encoded := params.Encode(); encoded != "" {
		u += "?" + encoded
	}

	const maxAttempts = 3
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return fmt.Errorf("build sippy request: %w", err)
		}
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("sippy request failed: %w", err)
			if attempt < maxAttempts && retryableError(err) {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return lastErr
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read sippy response body: %w", readErr)
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("sippy request returned status %d for %s: %s", resp.StatusCode, u, strings.TrimSpace(string(body)))
			if attempt < maxAttempts && resp.StatusCode >= 500 {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return lastErr
		}

		if err := json.Unmarshal(body, target); err != nil {
			return fmt.Errorf("decode sippy response: %w", err)
		}
		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("sippy request failed without error for %s", u)
}

func retryableError(err error) bool {
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "temporary") {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

type filterItem struct {
	ColumnField   string `json:"columnField"`
	OperatorValue string `json:"operatorValue"`
	Value         string `json:"value"`
}

type filterModel struct {
	Items        []filterItem `json:"items"`
	LinkOperator string       `json:"linkOperator"`
}

func buildJobRunsFilter(org, repo string, since time.Time) (string, error) {
	items := []filterItem{
		{ColumnField: "pull_request_org", OperatorValue: "equals", Value: org},
		{ColumnField: "pull_request_repo", OperatorValue: "equals", Value: repo},
	}
	if !since.IsZero() {
		items = append(items, filterItem{
			ColumnField:   "timestamp",
			OperatorValue: ">",
			Value:         strconv.FormatInt(since.UTC().UnixMilli(), 10),
		})
	}

	b, err := json.Marshal(filterModel{
		Items:        items,
		LinkOperator: "and",
	})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type jobRunsResponse struct {
	Rows      []jobRunResponse `json:"rows"`
	PageSize  int              `json:"page_size"`
	Page      int              `json:"page"`
	TotalRows int              `json:"total_rows"`
}

type jobRunResponse struct {
	URL                   string `json:"url"`
	Job                   string `json:"job"`
	Timestamp             int64  `json:"timestamp"`
	Failed                bool   `json:"failed"`
	InfrastructureFailure bool   `json:"infrastructure_failure"`
	Succeeded             bool   `json:"succeeded"`
	OverallResult         string `json:"overall_result"`
}
