package prowartifacts

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

const (
	defaultArtifactsBaseURL = "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs"
	defaultHTTPTimeout      = 90 * time.Second
)

var deterministicJUnitPathsByEnvironment = map[string][]string{
	"dev": {
		"artifacts/e2e-parallel/aro-hcp-provision-environment/artifacts/junit_entrypoint.xml",
		"prowjob_junit.xml",
		"artifacts/e2e-parallel/aro-hcp-test-local/artifacts/junit.xml",
	},
	"int": {
		"artifacts/integration-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
		"prowjob_junit.xml",
	},
	"stg": {
		"artifacts/stage-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
		"prowjob_junit.xml",
	},
	"prod": {
		"artifacts/prod-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
		"prowjob_junit.xml",
	},
}

var defaultDeterministicJUnitPaths = []string{
	"prowjob_junit.xml",
}

type Failure struct {
	ArtifactURL string
	TestName    string
	TestSuite   string
	FailureText string
}

type Client interface {
	ListFailures(ctx context.Context, environment string, runURL string) ([]Failure, error)
}

type HTTPClient struct {
	artifactsBaseURL       string
	httpClient             *http.Client
	junitPathsByEnvMapping map[string][]string
}

func NewHTTPClient(artifactsBaseURL string) *HTTPClient {
	baseURL := strings.TrimSpace(artifactsBaseURL)
	if baseURL == "" {
		baseURL = defaultArtifactsBaseURL
	}
	return &HTTPClient{
		artifactsBaseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
		junitPathsByEnvMapping: copyJUnitPathMap(deterministicJUnitPathsByEnvironment),
	}
}

func (c *HTTPClient) ListFailures(ctx context.Context, environment string, runURL string) ([]Failure, error) {
	prefix, err := artifactPrefixFromRunURL(runURL)
	if err != nil {
		return nil, err
	}

	junitPaths := c.junitPathsForEnvironment(environment)
	failures := make([]Failure, 0, 8)
	seen := map[string]struct{}{}
	fetchErrors := make([]error, 0, len(junitPaths))

	for _, junitPath := range junitPaths {
		artifactURL := c.artifactURL(prefix, junitPath)
		contents, found, err := c.fetchArtifact(ctx, artifactURL)
		if err != nil {
			fetchErrors = append(fetchErrors, fmt.Errorf("fetch junit %q: %w", junitPath, err))
			continue
		}
		if !found {
			continue
		}

		rows, err := parseJUnitFailures(contents, artifactURL)
		if err != nil {
			fetchErrors = append(fetchErrors, fmt.Errorf("parse junit %q: %w", junitPath, err))
			continue
		}

		for _, row := range rows {
			testName := strings.TrimSpace(row.TestName)
			failureText := strings.TrimSpace(row.FailureText)
			if testName == "" || failureText == "" {
				continue
			}
			key := strings.TrimSpace(row.ArtifactURL) + "\x00" + strings.TrimSpace(row.TestSuite) + "\x00" + testName + "\x00" + failureText
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			failures = append(failures, row)
		}
	}

	if len(failures) > 0 {
		sort.Slice(failures, func(i, j int) bool {
			if failures[i].ArtifactURL != failures[j].ArtifactURL {
				return failures[i].ArtifactURL < failures[j].ArtifactURL
			}
			if failures[i].TestSuite != failures[j].TestSuite {
				return failures[i].TestSuite < failures[j].TestSuite
			}
			if failures[i].TestName != failures[j].TestName {
				return failures[i].TestName < failures[j].TestName
			}
			return failures[i].FailureText < failures[j].FailureText
		})
		return failures, nil
	}
	if len(fetchErrors) > 0 {
		return nil, errors.Join(fetchErrors...)
	}
	return []Failure{}, nil
}

func (c *HTTPClient) junitPathsForEnvironment(environment string) []string {
	normalizedEnv := strings.ToLower(strings.TrimSpace(environment))
	if normalizedEnv != "" {
		if paths := normalizeDeterministicPaths(c.junitPathsByEnvMapping[normalizedEnv]); len(paths) > 0 {
			return paths
		}
	}
	return normalizeDeterministicPaths(defaultDeterministicJUnitPaths)
}

func (c *HTTPClient) artifactURL(prefix string, relPath string) string {
	joined := path.Join(strings.Trim(prefix, "/"), strings.Trim(relPath, "/"))
	return c.artifactsBaseURL + "/" + joined
}

func (c *HTTPClient) fetchArtifact(ctx context.Context, artifactURL string) ([]byte, bool, error) {
	const maxAttempts = 3
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, artifactURL, nil)
		if err != nil {
			return nil, false, fmt.Errorf("build artifact request: %w", err)
		}
		req.Header.Set("Accept", "application/xml,text/xml,text/plain")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("fetch artifact %q: %w", artifactURL, err)
			if attempt < maxAttempts {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return nil, false, lastErr
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, false, fmt.Errorf("read artifact response %q: %w", artifactURL, readErr)
		}

		if resp.StatusCode == http.StatusNotFound {
			return nil, false, nil
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("fetch artifact %q returned status %d: %s", artifactURL, resp.StatusCode, strings.TrimSpace(string(limitBytes(body, 2048))))
			if attempt < maxAttempts && resp.StatusCode >= 500 {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return nil, false, lastErr
		}

		contentType := strings.ToLower(resp.Header.Get("Content-Type"))
		if strings.Contains(contentType, "text/html") || looksLikeHTML(body) {
			return nil, false, nil
		}

		return body, true, nil
	}

	if lastErr != nil {
		return nil, false, lastErr
	}
	return nil, false, fmt.Errorf("fetch artifact %q failed without explicit error", artifactURL)
}

func artifactPrefixFromRunURL(runURL string) (string, error) {
	trimmed := strings.TrimSpace(runURL)
	if trimmed == "" {
		return "", fmt.Errorf("run URL is required")
	}
	if strings.HasPrefix(trimmed, "gs://") {
		return strings.Trim(strings.TrimPrefix(trimmed, "gs://"), "/"), nil
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse run URL: %w", err)
	}

	if strings.EqualFold(parsed.Scheme, "gs") {
		prefix := strings.Trim(parsed.Host+parsed.Path, "/")
		if prefix == "" {
			return "", fmt.Errorf("invalid gs run URL %q", runURL)
		}
		return prefix, nil
	}

	if prefix, ok := prefixFromHTTPPath(parsed.Path); ok {
		return prefix, nil
	}

	if strings.EqualFold(parsed.Host, "storage.googleapis.com") {
		prefix := strings.Trim(parsed.Path, "/")
		if prefix == "" {
			return "", fmt.Errorf("storage run URL missing object path %q", runURL)
		}
		return prefix, nil
	}

	return "", fmt.Errorf("unsupported run URL format %q", runURL)
}

func prefixFromHTTPPath(rawPath string) (string, bool) {
	p := strings.TrimSpace(rawPath)
	if strings.HasPrefix(p, "/view/gs/") {
		return strings.TrimPrefix(strings.Trim(p, "/"), "view/gs/"), true
	}
	if strings.HasPrefix(p, "/gcs/") {
		return strings.TrimPrefix(strings.Trim(p, "/"), "gcs/"), true
	}
	return "", false
}

func parseJUnitFailures(contents []byte, artifactURL string) ([]Failure, error) {
	rootName, err := detectRootElement(contents)
	if err != nil {
		return nil, err
	}

	failures := make([]Failure, 0, 8)

	switch strings.ToLower(rootName) {
	case "testsuite":
		var suite junitTestSuite
		if err := xml.Unmarshal(contents, &suite); err != nil {
			return nil, err
		}
		collectFailuresFromSuite(artifactURL, suite, "", &failures)
	case "testsuites":
		var root junitTestSuites
		if err := xml.Unmarshal(contents, &root); err != nil {
			return nil, err
		}
		for _, testcase := range root.TestCases {
			collectFailureFromCase(artifactURL, "", testcase, &failures)
		}
		for _, suite := range root.TestSuites {
			collectFailuresFromSuite(artifactURL, suite, "", &failures)
		}
	default:
		return nil, fmt.Errorf("unsupported junit root element %q", rootName)
	}

	return failures, nil
}

func detectRootElement(payload []byte) (string, error) {
	decoder := xml.NewDecoder(bytes.NewReader(payload))
	for {
		token, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				return "", fmt.Errorf("empty XML payload")
			}
			return "", err
		}
		if start, ok := token.(xml.StartElement); ok {
			return start.Name.Local, nil
		}
	}
}

func collectFailuresFromSuite(artifactURL string, suite junitTestSuite, parentSuiteName string, out *[]Failure) {
	suiteName := strings.TrimSpace(suite.Name)
	if suiteName == "" {
		suiteName = parentSuiteName
	}

	for _, testcase := range suite.TestCases {
		collectFailureFromCase(artifactURL, suiteName, testcase, out)
	}
	for _, child := range suite.TestSuites {
		collectFailuresFromSuite(artifactURL, child, suiteName, out)
	}
}

func collectFailureFromCase(artifactURL, fallbackSuite string, testcase junitTestCase, out *[]Failure) {
	failureText, ok := firstFailureText(testcase)
	if !ok {
		return
	}

	testName := strings.TrimSpace(testcase.Name)
	if testName == "" {
		return
	}

	testSuite := strings.TrimSpace(testcase.ClassName)
	if testSuite == "" {
		testSuite = fallbackSuite
	}

	*out = append(*out, Failure{
		ArtifactURL: strings.TrimSpace(artifactURL),
		TestName:    testName,
		TestSuite:   strings.TrimSpace(testSuite),
		FailureText: failureText,
	})
}

func firstFailureText(testcase junitTestCase) (string, bool) {
	if text, ok := firstFailureTextFromNodes(testcase.Failures); ok {
		return text, true
	}
	if text, ok := firstFailureTextFromNodes(testcase.Errors); ok {
		return text, true
	}
	return "", false
}

func firstFailureTextFromNodes(nodes []junitFailureNode) (string, bool) {
	for _, node := range nodes {
		message := strings.TrimSpace(node.Message)
		content := strings.TrimSpace(node.Content)
		switch {
		case message != "" && content != "":
			return message + "\n" + content, true
		case content != "":
			return content, true
		case message != "":
			return message, true
		}
	}
	return "", false
}

type junitTestSuites struct {
	TestSuites []junitTestSuite `xml:"testsuite"`
	TestCases  []junitTestCase  `xml:"testcase"`
}

type junitTestSuite struct {
	Name       string           `xml:"name,attr"`
	TestSuites []junitTestSuite `xml:"testsuite"`
	TestCases  []junitTestCase  `xml:"testcase"`
}

type junitTestCase struct {
	Name      string             `xml:"name,attr"`
	ClassName string             `xml:"classname,attr"`
	Failures  []junitFailureNode `xml:"failure"`
	Errors    []junitFailureNode `xml:"error"`
}

type junitFailureNode struct {
	Message string `xml:"message,attr"`
	Content string `xml:",chardata"`
}

func looksLikeHTML(body []byte) bool {
	const sniffBytes = 512
	if len(body) > sniffBytes {
		body = body[:sniffBytes]
	}
	trimmed := strings.ToLower(strings.TrimSpace(string(body)))
	if trimmed == "" {
		return false
	}
	return strings.HasPrefix(trimmed, "<!doctype html") || strings.HasPrefix(trimmed, "<html")
}

func normalizeDeterministicPaths(paths []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(paths))
	for _, value := range paths {
		normalized := strings.Trim(strings.TrimSpace(value), "/")
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func copyJUnitPathMap(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for k, values := range in {
		out[k] = append([]string(nil), values...)
	}
	return out
}

func limitBytes(in []byte, limit int) []byte {
	if len(in) <= limit {
		return in
	}
	return in[:limit]
}
