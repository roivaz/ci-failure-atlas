package ndjson

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	"ci-failure-atlas/pkg/store/contracts"
)

const (
	factsDirectory    = "facts"
	semanticDirectory = "semantic"
	stateDirectory    = "state"

	runsFilename                 = "runs.ndjson"
	pullRequestsFilename         = "pull_requests.ndjson"
	artifactFailuresFilename     = "artifact_failures.ndjson"
	rawFailuresFilename          = "raw_failures.ndjson"
	metricsDailyFilename         = "metrics_daily.ndjson"
	testMetadataDailyFile        = "test_metadata_daily.ndjson"
	phase1WorksetFilename        = "phase1_workset.ndjson"
	phase1NormalizedFilename     = "phase1_normalized.ndjson"
	phase1AssignmentsFile        = "phase1_assignments.ndjson"
	testClustersFilename         = "test_clusters.ndjson"
	globalClustersFilename       = "global_clusters.ndjson"
	phase3GlobalClustersFilename = "global_clusters_phase3.ndjson"
	reviewQueueFilename          = "review_queue.ndjson"
	checkpointsFilename          = "checkpoints.ndjson"
	deadLettersFilename          = "dead_letters.ndjson"
	phase3Directory              = "phase3"
	phase3IssuesFilename         = "issues.ndjson"
	phase3LinksFilename          = "links.ndjson"
	phase3EventsFilename         = "events.ndjson"
)

type Store struct {
	dataDirectory        string
	semanticSubdirectory string
	mu                   sync.RWMutex
}

var _ contracts.Store = (*Store)(nil)

type Options struct {
	SemanticSubdirectory string
}

func New(dataDirectory string) (*Store, error) {
	return NewWithOptions(dataDirectory, Options{})
}

func NewWithOptions(dataDirectory string, opts Options) (*Store, error) {
	if dataDirectory == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	cleanDataDirectory := filepath.Clean(dataDirectory)
	if err := os.MkdirAll(cleanDataDirectory, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	semanticSubdirectory, err := normalizeSemanticSubdirectory(opts.SemanticSubdirectory)
	if err != nil {
		return nil, fmt.Errorf("invalid semantic subdirectory: %w", err)
	}
	if semanticSubdirectory != "" {
		if err := os.MkdirAll(filepath.Join(cleanDataDirectory, semanticDirectory, semanticSubdirectory), 0o755); err != nil {
			return nil, fmt.Errorf("create semantic subdirectory: %w", err)
		}
	}
	return &Store{
		dataDirectory:        cleanDataDirectory,
		semanticSubdirectory: semanticSubdirectory,
	}, nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) UpsertRuns(ctx context.Context, runs []contracts.RunRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(runs) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.runsPath()
	existing, err := readNDJSON[contracts.RunRecord](path)
	if err != nil {
		return err
	}

	mergedByRunKey := map[string]contracts.RunRecord{}
	for _, row := range existing {
		normalized := normalizeRunRecord(row)
		key := runRecordKey(normalized)
		if key == "" {
			continue
		}
		mergedByRunKey[key] = normalized
	}
	for _, row := range runs {
		normalized := normalizeRunRecord(row)
		key := runRecordKey(normalized)
		if key == "" {
			return fmt.Errorf("run record missing environment and/or run_url")
		}
		mergedByRunKey[key] = normalized
	}

	merged := make([]contracts.RunRecord, 0, len(mergedByRunKey))
	for _, row := range mergedByRunKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		if merged[i].RunURL != merged[j].RunURL {
			return merged[i].RunURL < merged[j].RunURL
		}
		if merged[i].OccurredAt != merged[j].OccurredAt {
			return merged[i].OccurredAt < merged[j].OccurredAt
		}
		if merged[i].PRNumber != merged[j].PRNumber {
			return merged[i].PRNumber < merged[j].PRNumber
		}
		if merged[i].PRSHA != merged[j].PRSHA {
			return merged[i].PRSHA < merged[j].PRSHA
		}
		if merged[i].FinalMergedSHA != merged[j].FinalMergedSHA {
			return merged[i].FinalMergedSHA < merged[j].FinalMergedSHA
		}
		if merged[i].MergedPR != merged[j].MergedPR {
			return !merged[i].MergedPR && merged[j].MergedPR
		}
		if merged[i].PostGoodCommit != merged[j].PostGoodCommit {
			return !merged[i].PostGoodCommit && merged[j].PostGoodCommit
		}
		return merged[i].JobName < merged[j].JobName
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListRunKeys(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.RunRecord](s.runsPath())
	if err != nil {
		return nil, err
	}

	keysSet := map[string]struct{}{}
	for _, row := range rows {
		key := runRecordKey(normalizeRunRecord(row))
		if key == "" {
			continue
		}
		keysSet[key] = struct{}{}
	}
	keys := make([]string, 0, len(keysSet))
	for key := range keysSet {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *Store) ListRunDates(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.RunRecord](s.runsPath())
	if err != nil {
		return nil, err
	}

	dateSet := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizeRunRecord(row)
		if normalized.Environment == "" || normalized.RunURL == "" {
			continue
		}
		date, ok := dateFromTimestamp(normalized.OccurredAt)
		if !ok {
			continue
		}
		dateSet[date] = struct{}{}
	}
	out := make([]string, 0, len(dateSet))
	for date := range dateSet {
		out = append(out, date)
	}
	sort.Strings(out)
	return out, nil
}

func (s *Store) ListRunsByDate(ctx context.Context, environment string, date string) ([]contracts.RunRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	lookupEnv := normalizeEnvironment(environment)
	if lookupEnv == "" {
		return nil, fmt.Errorf("run lookup requires environment")
	}
	lookupDate, err := normalizeDate(date)
	if err != nil {
		return nil, fmt.Errorf("run lookup requires valid date (YYYY-MM-DD): %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.RunRecord](s.runsPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]contracts.RunRecord, 0)
	for _, row := range rows {
		normalized := normalizeRunRecord(row)
		if normalized.Environment != lookupEnv {
			continue
		}
		runDate, ok := dateFromTimestamp(normalized.OccurredAt)
		if !ok || runDate != lookupDate {
			continue
		}
		if normalized.RunURL == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].OccurredAt != filtered[j].OccurredAt {
			return filtered[i].OccurredAt < filtered[j].OccurredAt
		}
		return filtered[i].RunURL < filtered[j].RunURL
	})
	return filtered, nil
}

func (s *Store) GetRun(ctx context.Context, environment string, runURL string) (contracts.RunRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return contracts.RunRecord{}, false, err
	}

	lookup := normalizeRunRecord(contracts.RunRecord{
		Environment: environment,
		RunURL:      runURL,
	})
	if lookup.Environment == "" || lookup.RunURL == "" {
		return contracts.RunRecord{}, false, fmt.Errorf("run lookup requires environment and run_url")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.RunRecord](s.runsPath())
	if err != nil {
		return contracts.RunRecord{}, false, err
	}

	for _, row := range rows {
		normalized := normalizeRunRecord(row)
		if normalized.Environment == lookup.Environment && normalized.RunURL == lookup.RunURL {
			return normalized, true, nil
		}
	}
	return contracts.RunRecord{}, false, nil
}

func (s *Store) UpsertPullRequests(ctx context.Context, rows []contracts.PullRequestRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.pullRequestsPath()
	existing, err := readNDJSON[contracts.PullRequestRecord](path)
	if err != nil {
		return err
	}

	mergedByNumber := map[int]contracts.PullRequestRecord{}
	for _, row := range existing {
		normalized := normalizePullRequestRecord(row)
		if normalized.PRNumber <= 0 {
			continue
		}
		mergedByNumber[normalized.PRNumber] = normalized
	}
	for _, row := range rows {
		normalized := normalizePullRequestRecord(row)
		if normalized.PRNumber <= 0 {
			return fmt.Errorf("pull request record missing pr_number")
		}
		mergedByNumber[normalized.PRNumber] = normalized
	}

	merged := make([]contracts.PullRequestRecord, 0, len(mergedByNumber))
	for _, row := range mergedByNumber {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].PRNumber < merged[j].PRNumber
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListPullRequests(ctx context.Context) ([]contracts.PullRequestRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.PullRequestRecord](s.pullRequestsPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]contracts.PullRequestRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePullRequestRecord(row)
		if normalized.PRNumber <= 0 {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].PRNumber < filtered[j].PRNumber
	})
	return filtered, nil
}

func (s *Store) GetPullRequest(ctx context.Context, prNumber int) (contracts.PullRequestRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return contracts.PullRequestRecord{}, false, err
	}
	if prNumber <= 0 {
		return contracts.PullRequestRecord{}, false, fmt.Errorf("pull request lookup requires positive pr_number")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.PullRequestRecord](s.pullRequestsPath())
	if err != nil {
		return contracts.PullRequestRecord{}, false, err
	}

	for _, row := range rows {
		normalized := normalizePullRequestRecord(row)
		if normalized.PRNumber == prNumber {
			return normalized, true, nil
		}
	}
	return contracts.PullRequestRecord{}, false, nil
}

func (s *Store) UpsertArtifactFailures(ctx context.Context, rows []contracts.ArtifactFailureRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.artifactFailuresPath()
	existing, err := readNDJSON[contracts.ArtifactFailureRecord](path)
	if err != nil {
		return err
	}

	mergedByKey := map[string]contracts.ArtifactFailureRecord{}
	for _, row := range existing {
		normalized := normalizeArtifactFailureRecord(row)
		key := artifactFailureKey(normalized)
		if key == "" || normalized.RunURL == "" || normalized.SignatureID == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizeArtifactFailureRecord(row)
		if normalized.RunURL == "" {
			return fmt.Errorf("artifact failure record missing run_url")
		}
		if normalized.SignatureID == "" {
			return fmt.Errorf("artifact failure record missing signature_id")
		}
		key := artifactFailureKey(normalized)
		if key == "" {
			return fmt.Errorf("artifact failure record missing environment and/or artifact_row_id")
		}
		mergedByKey[key] = normalized
	}

	merged := make([]contracts.ArtifactFailureRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		if merged[i].RunURL != merged[j].RunURL {
			return merged[i].RunURL < merged[j].RunURL
		}
		if merged[i].TestSuite != merged[j].TestSuite {
			return merged[i].TestSuite < merged[j].TestSuite
		}
		if merged[i].TestName != merged[j].TestName {
			return merged[i].TestName < merged[j].TestName
		}
		if merged[i].ArtifactRowID != merged[j].ArtifactRowID {
			return merged[i].ArtifactRowID < merged[j].ArtifactRowID
		}
		if merged[i].SignatureID != merged[j].SignatureID {
			return merged[i].SignatureID < merged[j].SignatureID
		}
		return merged[i].FailureText < merged[j].FailureText
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListArtifactRunKeys(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.ArtifactFailureRecord](s.artifactFailuresPath())
	if err != nil {
		return nil, err
	}

	keysSet := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizeArtifactFailureRecord(row)
		key := runRecordKey(contracts.RunRecord{
			Environment: normalized.Environment,
			RunURL:      normalized.RunURL,
		})
		if key == "" {
			continue
		}
		keysSet[key] = struct{}{}
	}
	keys := make([]string, 0, len(keysSet))
	for key := range keysSet {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *Store) ListArtifactFailuresByRun(ctx context.Context, environment string, runURL string) ([]contracts.ArtifactFailureRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	lookup := normalizeRunRecord(contracts.RunRecord{
		Environment: environment,
		RunURL:      runURL,
	})
	if lookup.Environment == "" || lookup.RunURL == "" {
		return nil, fmt.Errorf("artifact failure lookup requires environment and run_url")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.ArtifactFailureRecord](s.artifactFailuresPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]contracts.ArtifactFailureRecord, 0)
	for _, row := range rows {
		normalized := normalizeArtifactFailureRecord(row)
		if normalized.Environment != lookup.Environment || normalized.RunURL != lookup.RunURL {
			continue
		}
		if normalized.ArtifactRowID == "" || normalized.SignatureID == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].TestSuite != filtered[j].TestSuite {
			return filtered[i].TestSuite < filtered[j].TestSuite
		}
		if filtered[i].TestName != filtered[j].TestName {
			return filtered[i].TestName < filtered[j].TestName
		}
		if filtered[i].ArtifactRowID != filtered[j].ArtifactRowID {
			return filtered[i].ArtifactRowID < filtered[j].ArtifactRowID
		}
		if filtered[i].SignatureID != filtered[j].SignatureID {
			return filtered[i].SignatureID < filtered[j].SignatureID
		}
		return filtered[i].FailureText < filtered[j].FailureText
	})

	return filtered, nil
}

func (s *Store) UpsertRawFailures(ctx context.Context, rows []contracts.RawFailureRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.rawFailuresPath()
	existing, err := readNDJSON[contracts.RawFailureRecord](path)
	if err != nil {
		return err
	}

	normalizedInput := make([]contracts.RawFailureRecord, 0, len(rows))
	touchedRunKeys := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizeRawFailureRecord(row)
		key := rawFailureKey(normalized)
		if key == "" {
			return fmt.Errorf("raw failure record missing environment and/or row_id")
		}
		runKey := runRecordKey(contracts.RunRecord{
			Environment: normalized.Environment,
			RunURL:      normalized.RunURL,
		})
		if runKey == "" {
			return fmt.Errorf("raw failure record missing run_url")
		}
		normalizedInput = append(normalizedInput, normalized)
		touchedRunKeys[runKey] = struct{}{}
	}

	mergedByRowID := map[string]contracts.RawFailureRecord{}
	for _, row := range existing {
		normalized := normalizeRawFailureRecord(row)
		key := rawFailureKey(normalized)
		if key == "" {
			continue
		}
		runKey := runRecordKey(contracts.RunRecord{
			Environment: normalized.Environment,
			RunURL:      normalized.RunURL,
		})
		if _, touched := touchedRunKeys[runKey]; touched {
			continue
		}
		mergedByRowID[key] = normalized
	}
	for _, normalized := range normalizedInput {
		key := rawFailureKey(normalized)
		mergedByRowID[key] = normalized
	}

	merged := make([]contracts.RawFailureRecord, 0, len(mergedByRowID))
	for _, row := range mergedByRowID {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		if merged[i].RowID != merged[j].RowID {
			return merged[i].RowID < merged[j].RowID
		}
		if merged[i].RunURL != merged[j].RunURL {
			return merged[i].RunURL < merged[j].RunURL
		}
		return merged[i].SignatureID < merged[j].SignatureID
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListRawFailureRunKeys(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.RawFailureRecord](s.rawFailuresPath())
	if err != nil {
		return nil, err
	}

	keysSet := map[string]struct{}{}
	for _, row := range rows {
		key := runRecordKey(contracts.RunRecord{
			Environment: normalizeEnvironment(row.Environment),
			RunURL:      strings.TrimSpace(row.RunURL),
		})
		if key == "" {
			continue
		}
		keysSet[key] = struct{}{}
	}
	keys := make([]string, 0, len(keysSet))
	for key := range keysSet {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *Store) ListRawFailuresByRun(ctx context.Context, environment string, runURL string) ([]contracts.RawFailureRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	lookup := normalizeRunRecord(contracts.RunRecord{
		Environment: environment,
		RunURL:      runURL,
	})
	if lookup.Environment == "" || lookup.RunURL == "" {
		return nil, fmt.Errorf("raw failure lookup requires environment and run_url")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.RawFailureRecord](s.rawFailuresPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]contracts.RawFailureRecord, 0)
	for _, row := range rows {
		normalized := normalizeRawFailureRecord(row)
		if normalized.Environment != lookup.Environment || normalized.RunURL != lookup.RunURL {
			continue
		}
		if normalized.RowID == "" || normalized.SignatureID == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].OccurredAt != filtered[j].OccurredAt {
			return filtered[i].OccurredAt < filtered[j].OccurredAt
		}
		if filtered[i].RowID != filtered[j].RowID {
			return filtered[i].RowID < filtered[j].RowID
		}
		return filtered[i].SignatureID < filtered[j].SignatureID
	})

	return filtered, nil
}

func (s *Store) ListRawFailuresByDate(ctx context.Context, environment string, date string) ([]contracts.RawFailureRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	lookupEnv := normalizeEnvironment(environment)
	if lookupEnv == "" {
		return nil, fmt.Errorf("raw failure lookup requires environment")
	}
	lookupDate, err := normalizeDate(date)
	if err != nil {
		return nil, fmt.Errorf("raw failure lookup requires valid date (YYYY-MM-DD): %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.RawFailureRecord](s.rawFailuresPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]contracts.RawFailureRecord, 0)
	for _, row := range rows {
		normalized := normalizeRawFailureRecord(row)
		if normalized.Environment != lookupEnv {
			continue
		}
		if rowDate, ok := dateFromTimestamp(normalized.OccurredAt); !ok || rowDate != lookupDate {
			continue
		}
		if normalized.RowID == "" || normalized.RunURL == "" || normalized.SignatureID == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].OccurredAt != filtered[j].OccurredAt {
			return filtered[i].OccurredAt < filtered[j].OccurredAt
		}
		if filtered[i].RunURL != filtered[j].RunURL {
			return filtered[i].RunURL < filtered[j].RunURL
		}
		if filtered[i].RowID != filtered[j].RowID {
			return filtered[i].RowID < filtered[j].RowID
		}
		return filtered[i].SignatureID < filtered[j].SignatureID
	})

	return filtered, nil
}

func (s *Store) UpsertMetricsDaily(ctx context.Context, rows []contracts.MetricDailyRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.metricsDailyPath()
	existing, err := readNDJSON[contracts.MetricDailyRecord](path)
	if err != nil {
		return err
	}

	normalizedInput := make([]contracts.MetricDailyRecord, 0, len(rows))
	touchedDateEnvironments := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizeMetricDailyRecord(row)
		key := metricDailyKey(normalized)
		if key == "" {
			return fmt.Errorf("metric daily record missing environment, date, and/or metric")
		}
		normalizedInput = append(normalizedInput, normalized)
		touchedDateEnvironments[normalized.Environment+"|"+normalized.Date] = struct{}{}
	}

	mergedByKey := map[string]contracts.MetricDailyRecord{}
	for _, row := range existing {
		normalized := normalizeMetricDailyRecord(row)
		key := metricDailyKey(normalized)
		if key == "" {
			continue
		}
		if _, touched := touchedDateEnvironments[normalized.Environment+"|"+normalized.Date]; touched {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, normalized := range normalizedInput {
		key := metricDailyKey(normalized)
		mergedByKey[key] = normalized
	}

	merged := make([]contracts.MetricDailyRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		if merged[i].Date != merged[j].Date {
			return merged[i].Date < merged[j].Date
		}
		return merged[i].Metric < merged[j].Metric
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListMetricsDailyByDate(ctx context.Context, environment string, date string) ([]contracts.MetricDailyRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	lookupEnv := normalizeEnvironment(environment)
	if lookupEnv == "" {
		return nil, fmt.Errorf("metric daily lookup requires environment")
	}
	lookupDate, err := normalizeDate(date)
	if err != nil {
		return nil, fmt.Errorf("metric daily lookup requires valid date (YYYY-MM-DD): %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.MetricDailyRecord](s.metricsDailyPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]contracts.MetricDailyRecord, 0)
	for _, row := range rows {
		normalized := normalizeMetricDailyRecord(row)
		if normalized.Environment != lookupEnv || normalized.Date != lookupDate {
			continue
		}
		if normalized.Metric == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Metric < filtered[j].Metric
	})

	return filtered, nil
}

func (s *Store) ListMetricDates(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.MetricDailyRecord](s.metricsDailyPath())
	if err != nil {
		return nil, err
	}

	datesSet := map[string]struct{}{}
	for _, row := range rows {
		date := strings.TrimSpace(row.Date)
		if date == "" {
			continue
		}
		datesSet[date] = struct{}{}
	}
	dates := make([]string, 0, len(datesSet))
	for date := range datesSet {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	return dates, nil
}

func (s *Store) UpsertTestMetadataDaily(ctx context.Context, rows []contracts.TestMetadataDailyRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.testMetadataDailyPath()
	existing, err := readNDJSON[contracts.TestMetadataDailyRecord](path)
	if err != nil {
		return err
	}

	normalizedInput := make([]contracts.TestMetadataDailyRecord, 0, len(rows))
	touchedDateEnvironments := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizeTestMetadataDailyRecord(row)
		key := testMetadataDailyKey(normalized)
		if key == "" {
			return fmt.Errorf("test metadata daily record missing environment, date, period, and/or test_name")
		}
		normalizedInput = append(normalizedInput, normalized)
		touchedDateEnvironments[normalized.Environment+"|"+normalized.Date+"|"+normalized.Period] = struct{}{}
	}

	mergedByKey := map[string]contracts.TestMetadataDailyRecord{}
	for _, row := range existing {
		normalized := normalizeTestMetadataDailyRecord(row)
		key := testMetadataDailyKey(normalized)
		if key == "" {
			continue
		}
		if _, touched := touchedDateEnvironments[normalized.Environment+"|"+normalized.Date+"|"+normalized.Period]; touched {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, normalized := range normalizedInput {
		key := testMetadataDailyKey(normalized)
		mergedByKey[key] = normalized
	}

	merged := make([]contracts.TestMetadataDailyRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		if merged[i].Date != merged[j].Date {
			return merged[i].Date < merged[j].Date
		}
		if merged[i].Period != merged[j].Period {
			return merged[i].Period < merged[j].Period
		}
		if merged[i].TestSuite != merged[j].TestSuite {
			return merged[i].TestSuite < merged[j].TestSuite
		}
		return merged[i].TestName < merged[j].TestName
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListTestMetadataDailyByDate(ctx context.Context, environment string, date string) ([]contracts.TestMetadataDailyRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	lookupEnv := normalizeEnvironment(environment)
	if lookupEnv == "" {
		return nil, fmt.Errorf("test metadata daily lookup requires environment")
	}
	lookupDate, err := normalizeDate(date)
	if err != nil {
		return nil, fmt.Errorf("test metadata daily lookup requires valid date (YYYY-MM-DD): %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.TestMetadataDailyRecord](s.testMetadataDailyPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]contracts.TestMetadataDailyRecord, 0)
	for _, row := range rows {
		normalized := normalizeTestMetadataDailyRecord(row)
		if normalized.Environment != lookupEnv || normalized.Date != lookupDate {
			continue
		}
		if normalized.TestName == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Period != filtered[j].Period {
			return filtered[i].Period < filtered[j].Period
		}
		if filtered[i].TestSuite != filtered[j].TestSuite {
			return filtered[i].TestSuite < filtered[j].TestSuite
		}
		return filtered[i].TestName < filtered[j].TestName
	})

	return filtered, nil
}

func (s *Store) UpsertCheckpoints(ctx context.Context, rows []contracts.CheckpointRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.checkpointsPath()
	existing, err := readNDJSON[contracts.CheckpointRecord](path)
	if err != nil {
		return err
	}

	mergedByName := map[string]contracts.CheckpointRecord{}
	for _, row := range existing {
		normalized := normalizeCheckpointRecord(row)
		if normalized.Name == "" {
			continue
		}
		mergedByName[normalized.Name] = normalized
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for _, row := range rows {
		normalized := normalizeCheckpointRecord(row)
		if normalized.Name == "" {
			return fmt.Errorf("checkpoint record missing name")
		}
		if normalized.UpdatedAt == "" {
			normalized.UpdatedAt = now
		}
		mergedByName[normalized.Name] = normalized
	}

	merged := make([]contracts.CheckpointRecord, 0, len(mergedByName))
	for _, row := range mergedByName {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Name < merged[j].Name
	})

	return writeNDJSON(path, merged)
}

func (s *Store) GetCheckpoint(ctx context.Context, name string) (contracts.CheckpointRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return contracts.CheckpointRecord{}, false, err
	}
	targetName := strings.TrimSpace(name)
	if targetName == "" {
		return contracts.CheckpointRecord{}, false, fmt.Errorf("checkpoint name is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.CheckpointRecord](s.checkpointsPath())
	if err != nil {
		return contracts.CheckpointRecord{}, false, err
	}
	for _, row := range rows {
		normalized := normalizeCheckpointRecord(row)
		if normalized.Name == targetName {
			return normalized, true, nil
		}
	}
	return contracts.CheckpointRecord{}, false, nil
}

func (s *Store) AppendDeadLetters(ctx context.Context, rows []contracts.DeadLetterRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.deadLettersPath()
	existing, err := readNDJSON[contracts.DeadLetterRecord](path)
	if err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for _, row := range rows {
		normalized := normalizeDeadLetterRecord(row)
		if normalized.Controller == "" {
			return fmt.Errorf("dead letter record missing controller")
		}
		if normalized.Key == "" {
			return fmt.Errorf("dead letter record missing key")
		}
		if normalized.Error == "" {
			return fmt.Errorf("dead letter record missing error")
		}
		if normalized.FailedAt == "" {
			normalized.FailedAt = now
		}
		existing = append(existing, normalized)
	}

	// Keep file output deterministic while preserving append semantics.
	sort.Slice(existing, func(i, j int) bool {
		if existing[i].FailedAt != existing[j].FailedAt {
			return existing[i].FailedAt < existing[j].FailedAt
		}
		if existing[i].Controller != existing[j].Controller {
			return existing[i].Controller < existing[j].Controller
		}
		if existing[i].Key != existing[j].Key {
			return existing[i].Key < existing[j].Key
		}
		return existing[i].Error < existing[j].Error
	})

	return writeNDJSON(path, existing)
}

func (s *Store) ListDeadLetters(ctx context.Context, limit int) ([]contracts.DeadLetterRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.DeadLetterRecord](s.deadLettersPath())
	if err != nil {
		return nil, err
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].FailedAt != rows[j].FailedAt {
			return rows[i].FailedAt > rows[j].FailedAt
		}
		if rows[i].Controller != rows[j].Controller {
			return rows[i].Controller < rows[j].Controller
		}
		if rows[i].Key != rows[j].Key {
			return rows[i].Key < rows[j].Key
		}
		return rows[i].Error < rows[j].Error
	})

	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (s *Store) UpsertPhase1Workset(ctx context.Context, rows []semanticcontracts.Phase1WorksetRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.phase1WorksetPath()
	existing, err := readNDJSON[semanticcontracts.Phase1WorksetRecord](path)
	if err != nil {
		return err
	}

	normalizedIncoming := make([]semanticcontracts.Phase1WorksetRecord, 0, len(rows))
	targetEnvironments := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizePhase1WorksetRecord(row)
		key := phase1WorksetKey(normalized)
		if key == "" {
			return fmt.Errorf("phase1 workset record missing row_id")
		}
		targetEnvironments[strings.TrimSpace(normalized.Environment)] = struct{}{}
		normalizedIncoming = append(normalizedIncoming, normalized)
	}

	mergedByKey := map[string]semanticcontracts.Phase1WorksetRecord{}
	for _, row := range existing {
		normalized := normalizePhase1WorksetRecord(row)
		if _, replaceEnvironment := targetEnvironments[strings.TrimSpace(normalized.Environment)]; replaceEnvironment {
			continue
		}
		key := phase1WorksetKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, normalized := range normalizedIncoming {
		key := phase1WorksetKey(normalized)
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.Phase1WorksetRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		return phase1WorksetLess(merged[i], merged[j])
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListPhase1Workset(ctx context.Context) ([]semanticcontracts.Phase1WorksetRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[semanticcontracts.Phase1WorksetRecord](s.phase1WorksetPath())
	if err != nil {
		return nil, err
	}
	filtered := make([]semanticcontracts.Phase1WorksetRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase1WorksetRecord(row)
		if phase1WorksetKey(normalized) == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return phase1WorksetLess(filtered[i], filtered[j])
	})
	return filtered, nil
}

func (s *Store) UpsertPhase1Normalized(ctx context.Context, rows []semanticcontracts.Phase1NormalizedRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.phase1NormalizedPath()
	existing, err := readNDJSON[semanticcontracts.Phase1NormalizedRecord](path)
	if err != nil {
		return err
	}

	normalizedIncoming := make([]semanticcontracts.Phase1NormalizedRecord, 0, len(rows))
	targetEnvironments := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizePhase1NormalizedRecord(row)
		key := phase1WorksetKey(semanticcontracts.Phase1WorksetRecord{Environment: normalized.Environment, RowID: normalized.RowID})
		if key == "" {
			return fmt.Errorf("phase1 normalized record missing row_id")
		}
		targetEnvironments[strings.TrimSpace(normalized.Environment)] = struct{}{}
		normalizedIncoming = append(normalizedIncoming, normalized)
	}

	mergedByKey := map[string]semanticcontracts.Phase1NormalizedRecord{}
	for _, row := range existing {
		normalized := normalizePhase1NormalizedRecord(row)
		if _, replaceEnvironment := targetEnvironments[strings.TrimSpace(normalized.Environment)]; replaceEnvironment {
			continue
		}
		key := phase1WorksetKey(semanticcontracts.Phase1WorksetRecord{Environment: normalized.Environment, RowID: normalized.RowID})
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, normalized := range normalizedIncoming {
		key := phase1WorksetKey(semanticcontracts.Phase1WorksetRecord{Environment: normalized.Environment, RowID: normalized.RowID})
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.Phase1NormalizedRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		if merged[i].Lane != merged[j].Lane {
			return merged[i].Lane < merged[j].Lane
		}
		if merged[i].JobName != merged[j].JobName {
			return merged[i].JobName < merged[j].JobName
		}
		if merged[i].TestName != merged[j].TestName {
			return merged[i].TestName < merged[j].TestName
		}
		if merged[i].OccurredAt != merged[j].OccurredAt {
			return merged[i].OccurredAt < merged[j].OccurredAt
		}
		if merged[i].RunURL != merged[j].RunURL {
			return merged[i].RunURL < merged[j].RunURL
		}
		if merged[i].SignatureID != merged[j].SignatureID {
			return merged[i].SignatureID < merged[j].SignatureID
		}
		return merged[i].RowID < merged[j].RowID
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListPhase1Normalized(ctx context.Context) ([]semanticcontracts.Phase1NormalizedRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[semanticcontracts.Phase1NormalizedRecord](s.phase1NormalizedPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]semanticcontracts.Phase1NormalizedRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase1NormalizedRecord(row)
		if phase1WorksetKey(semanticcontracts.Phase1WorksetRecord{Environment: normalized.Environment, RowID: normalized.RowID}) == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Environment != filtered[j].Environment {
			return filtered[i].Environment < filtered[j].Environment
		}
		if filtered[i].GroupKey != filtered[j].GroupKey {
			return filtered[i].GroupKey < filtered[j].GroupKey
		}
		if filtered[i].Phase1Key != filtered[j].Phase1Key {
			return filtered[i].Phase1Key < filtered[j].Phase1Key
		}
		return filtered[i].RowID < filtered[j].RowID
	})
	return filtered, nil
}

func (s *Store) UpsertPhase1Assignments(ctx context.Context, rows []semanticcontracts.Phase1AssignmentRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.phase1AssignmentsPath()
	existing, err := readNDJSON[semanticcontracts.Phase1AssignmentRecord](path)
	if err != nil {
		return err
	}

	normalizedIncoming := make([]semanticcontracts.Phase1AssignmentRecord, 0, len(rows))
	targetEnvironments := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizePhase1AssignmentRecord(row)
		key := phase1AssignmentKey(normalized)
		if key == "" {
			return fmt.Errorf("phase1 assignment record missing environment and/or row_id")
		}
		targetEnvironments[strings.TrimSpace(normalized.Environment)] = struct{}{}
		normalizedIncoming = append(normalizedIncoming, normalized)
	}

	mergedByKey := map[string]semanticcontracts.Phase1AssignmentRecord{}
	for _, row := range existing {
		normalized := normalizePhase1AssignmentRecord(row)
		if _, replaceEnvironment := targetEnvironments[strings.TrimSpace(normalized.Environment)]; replaceEnvironment {
			continue
		}
		key := phase1AssignmentKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, normalized := range normalizedIncoming {
		key := phase1AssignmentKey(normalized)
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.Phase1AssignmentRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		if merged[i].GroupKey != merged[j].GroupKey {
			return merged[i].GroupKey < merged[j].GroupKey
		}
		if merged[i].Phase1LocalClusterKey != merged[j].Phase1LocalClusterKey {
			return merged[i].Phase1LocalClusterKey < merged[j].Phase1LocalClusterKey
		}
		return merged[i].RowID < merged[j].RowID
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListPhase1Assignments(ctx context.Context) ([]semanticcontracts.Phase1AssignmentRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[semanticcontracts.Phase1AssignmentRecord](s.phase1AssignmentsPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]semanticcontracts.Phase1AssignmentRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase1AssignmentRecord(row)
		if phase1AssignmentKey(normalized) == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Environment != filtered[j].Environment {
			return filtered[i].Environment < filtered[j].Environment
		}
		if filtered[i].GroupKey != filtered[j].GroupKey {
			return filtered[i].GroupKey < filtered[j].GroupKey
		}
		if filtered[i].Phase1LocalClusterKey != filtered[j].Phase1LocalClusterKey {
			return filtered[i].Phase1LocalClusterKey < filtered[j].Phase1LocalClusterKey
		}
		return filtered[i].RowID < filtered[j].RowID
	})
	return filtered, nil
}

func (s *Store) UpsertTestClusters(ctx context.Context, rows []semanticcontracts.TestClusterRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.testClustersPath()
	existing, err := readNDJSON[semanticcontracts.TestClusterRecord](path)
	if err != nil {
		return err
	}

	normalizedIncoming := make([]semanticcontracts.TestClusterRecord, 0, len(rows))
	targetEnvironments := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizeTestClusterRecord(row)
		key := phase1ClusterKey(normalized)
		if key == "" {
			return fmt.Errorf("test cluster record missing environment and/or phase1_cluster_id")
		}
		targetEnvironments[strings.TrimSpace(normalized.Environment)] = struct{}{}
		normalizedIncoming = append(normalizedIncoming, normalized)
	}

	mergedByKey := map[string]semanticcontracts.TestClusterRecord{}
	for _, row := range existing {
		normalized := normalizeTestClusterRecord(row)
		if _, replaceEnvironment := targetEnvironments[strings.TrimSpace(normalized.Environment)]; replaceEnvironment {
			continue
		}
		key := phase1ClusterKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, normalized := range normalizedIncoming {
		key := phase1ClusterKey(normalized)
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.TestClusterRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		return testClusterLess(merged[i], merged[j])
	})
	return writeNDJSON(path, merged)
}

func (s *Store) ListTestClusters(ctx context.Context) ([]semanticcontracts.TestClusterRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[semanticcontracts.TestClusterRecord](s.testClustersPath())
	if err != nil {
		return nil, err
	}
	filtered := make([]semanticcontracts.TestClusterRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizeTestClusterRecord(row)
		if phase1ClusterKey(normalized) == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return testClusterLess(filtered[i], filtered[j])
	})
	return filtered, nil
}

func (s *Store) UpsertGlobalClusters(ctx context.Context, rows []semanticcontracts.GlobalClusterRecord) error {
	return s.upsertGlobalClustersPath(ctx, s.globalClustersPath(), rows)
}

func (s *Store) UpsertPhase3GlobalClusters(ctx context.Context, rows []semanticcontracts.GlobalClusterRecord) error {
	return s.upsertGlobalClustersPath(ctx, s.phase3GlobalClustersPath(), rows)
}

func (s *Store) upsertGlobalClustersPath(
	ctx context.Context,
	path string,
	rows []semanticcontracts.GlobalClusterRecord,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, err := readNDJSON[semanticcontracts.GlobalClusterRecord](path)
	if err != nil {
		return err
	}

	normalizedIncoming := make([]semanticcontracts.GlobalClusterRecord, 0, len(rows))
	targetEnvironments := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizeGlobalClusterRecord(row)
		key := globalClusterKey(normalized)
		if key == "" {
			return fmt.Errorf("global cluster record missing environment and/or phase2_cluster_id")
		}
		targetEnvironments[strings.TrimSpace(normalized.Environment)] = struct{}{}
		normalizedIncoming = append(normalizedIncoming, normalized)
	}

	mergedByKey := map[string]semanticcontracts.GlobalClusterRecord{}
	for _, row := range existing {
		normalized := normalizeGlobalClusterRecord(row)
		if _, replaceEnvironment := targetEnvironments[strings.TrimSpace(normalized.Environment)]; replaceEnvironment {
			continue
		}
		key := globalClusterKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, normalized := range normalizedIncoming {
		key := globalClusterKey(normalized)
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.GlobalClusterRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].SupportCount != merged[j].SupportCount {
			return merged[i].SupportCount > merged[j].SupportCount
		}
		if merged[i].ContributingTestsCount != merged[j].ContributingTestsCount {
			return merged[i].ContributingTestsCount > merged[j].ContributingTestsCount
		}
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		return merged[i].Phase2ClusterID < merged[j].Phase2ClusterID
	})
	return writeNDJSON(path, merged)
}

func (s *Store) ListGlobalClusters(ctx context.Context) ([]semanticcontracts.GlobalClusterRecord, error) {
	return s.listGlobalClustersPath(ctx, s.globalClustersPath())
}

func (s *Store) ListPhase3GlobalClusters(ctx context.Context) ([]semanticcontracts.GlobalClusterRecord, error) {
	return s.listGlobalClustersPath(ctx, s.phase3GlobalClustersPath())
}

func (s *Store) listGlobalClustersPath(
	ctx context.Context,
	path string,
) ([]semanticcontracts.GlobalClusterRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[semanticcontracts.GlobalClusterRecord](path)
	if err != nil {
		return nil, err
	}
	filtered := make([]semanticcontracts.GlobalClusterRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizeGlobalClusterRecord(row)
		if globalClusterKey(normalized) == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].SupportCount != filtered[j].SupportCount {
			return filtered[i].SupportCount > filtered[j].SupportCount
		}
		if filtered[i].ContributingTestsCount != filtered[j].ContributingTestsCount {
			return filtered[i].ContributingTestsCount > filtered[j].ContributingTestsCount
		}
		if filtered[i].Environment != filtered[j].Environment {
			return filtered[i].Environment < filtered[j].Environment
		}
		return filtered[i].Phase2ClusterID < filtered[j].Phase2ClusterID
	})
	return filtered, nil
}

func (s *Store) UpsertReviewQueue(ctx context.Context, rows []semanticcontracts.ReviewItemRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.reviewQueuePath()
	existing, err := readNDJSON[semanticcontracts.ReviewItemRecord](path)
	if err != nil {
		return err
	}

	normalizedIncoming := make([]semanticcontracts.ReviewItemRecord, 0, len(rows))
	targetEnvironments := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizeReviewItemRecord(row)
		key := reviewItemKey(normalized)
		if key == "" {
			return fmt.Errorf("review item record missing environment and/or review_item_id")
		}
		targetEnvironments[strings.TrimSpace(normalized.Environment)] = struct{}{}
		normalizedIncoming = append(normalizedIncoming, normalized)
	}

	mergedByKey := map[string]semanticcontracts.ReviewItemRecord{}
	for _, row := range existing {
		normalized := normalizeReviewItemRecord(row)
		if _, replaceEnvironment := targetEnvironments[strings.TrimSpace(normalized.Environment)]; replaceEnvironment {
			continue
		}
		key := reviewItemKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, normalized := range normalizedIncoming {
		key := reviewItemKey(normalized)
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.ReviewItemRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		if merged[i].Phase != merged[j].Phase {
			return merged[i].Phase < merged[j].Phase
		}
		if merged[i].Reason != merged[j].Reason {
			return merged[i].Reason < merged[j].Reason
		}
		return merged[i].ReviewItemID < merged[j].ReviewItemID
	})
	return writeNDJSON(path, merged)
}

func (s *Store) ListReviewQueue(ctx context.Context) ([]semanticcontracts.ReviewItemRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[semanticcontracts.ReviewItemRecord](s.reviewQueuePath())
	if err != nil {
		return nil, err
	}
	filtered := make([]semanticcontracts.ReviewItemRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizeReviewItemRecord(row)
		if reviewItemKey(normalized) == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Environment != filtered[j].Environment {
			return filtered[i].Environment < filtered[j].Environment
		}
		if filtered[i].Phase != filtered[j].Phase {
			return filtered[i].Phase < filtered[j].Phase
		}
		if filtered[i].Reason != filtered[j].Reason {
			return filtered[i].Reason < filtered[j].Reason
		}
		return filtered[i].ReviewItemID < filtered[j].ReviewItemID
	})
	return filtered, nil
}

func (s *Store) UpsertPhase3Issues(ctx context.Context, rows []semanticcontracts.Phase3IssueRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.phase3IssuesPath()
	existing, err := readNDJSON[semanticcontracts.Phase3IssueRecord](path)
	if err != nil {
		return err
	}

	mergedByKey := map[string]semanticcontracts.Phase3IssueRecord{}
	for _, row := range existing {
		normalized := normalizePhase3IssueRecord(row)
		key := phase3IssueKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizePhase3IssueRecord(row)
		key := phase3IssueKey(normalized)
		if key == "" {
			return fmt.Errorf("phase3 issue record missing issue_id")
		}
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.Phase3IssueRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].IssueID < merged[j].IssueID
	})
	return writeNDJSON(path, merged)
}

func (s *Store) ListPhase3Issues(ctx context.Context) ([]semanticcontracts.Phase3IssueRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[semanticcontracts.Phase3IssueRecord](s.phase3IssuesPath())
	if err != nil {
		return nil, err
	}
	filtered := make([]semanticcontracts.Phase3IssueRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase3IssueRecord(row)
		if phase3IssueKey(normalized) == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].IssueID < filtered[j].IssueID
	})
	return filtered, nil
}

func (s *Store) UpsertPhase3Links(ctx context.Context, rows []semanticcontracts.Phase3LinkRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.phase3LinksPath()
	existing, err := readNDJSON[semanticcontracts.Phase3LinkRecord](path)
	if err != nil {
		return err
	}

	mergedByKey := map[string]semanticcontracts.Phase3LinkRecord{}
	for _, row := range existing {
		normalized := normalizePhase3LinkRecord(row)
		key := phase3LinkKey(normalized)
		if key == "" || strings.TrimSpace(normalized.IssueID) == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizePhase3LinkRecord(row)
		key := phase3LinkKey(normalized)
		if key == "" {
			return fmt.Errorf("phase3 link record missing environment and/or run_url and/or row_id")
		}
		if strings.TrimSpace(normalized.IssueID) == "" {
			return fmt.Errorf("phase3 link record missing issue_id")
		}
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.Phase3LinkRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		if merged[i].RunURL != merged[j].RunURL {
			return merged[i].RunURL < merged[j].RunURL
		}
		if merged[i].RowID != merged[j].RowID {
			return merged[i].RowID < merged[j].RowID
		}
		return merged[i].IssueID < merged[j].IssueID
	})
	return writeNDJSON(path, merged)
}

func (s *Store) DeletePhase3Links(ctx context.Context, rows []semanticcontracts.Phase3LinkRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.phase3LinksPath()
	existing, err := readNDJSON[semanticcontracts.Phase3LinkRecord](path)
	if err != nil {
		return err
	}
	keysToRemove := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizePhase3LinkRecord(row)
		key := phase3LinkKey(normalized)
		if key == "" {
			return fmt.Errorf("phase3 link deletion record missing environment and/or run_url and/or row_id")
		}
		keysToRemove[key] = struct{}{}
	}

	remaining := make([]semanticcontracts.Phase3LinkRecord, 0, len(existing))
	for _, row := range existing {
		normalized := normalizePhase3LinkRecord(row)
		key := phase3LinkKey(normalized)
		if key == "" || strings.TrimSpace(normalized.IssueID) == "" {
			continue
		}
		if _, remove := keysToRemove[key]; remove {
			continue
		}
		remaining = append(remaining, normalized)
	}
	sort.Slice(remaining, func(i, j int) bool {
		if remaining[i].Environment != remaining[j].Environment {
			return remaining[i].Environment < remaining[j].Environment
		}
		if remaining[i].RunURL != remaining[j].RunURL {
			return remaining[i].RunURL < remaining[j].RunURL
		}
		if remaining[i].RowID != remaining[j].RowID {
			return remaining[i].RowID < remaining[j].RowID
		}
		return remaining[i].IssueID < remaining[j].IssueID
	})
	return writeNDJSON(path, remaining)
}

func (s *Store) ListPhase3Links(ctx context.Context) ([]semanticcontracts.Phase3LinkRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[semanticcontracts.Phase3LinkRecord](s.phase3LinksPath())
	if err != nil {
		return nil, err
	}
	filtered := make([]semanticcontracts.Phase3LinkRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase3LinkRecord(row)
		if phase3LinkKey(normalized) == "" || strings.TrimSpace(normalized.IssueID) == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Environment != filtered[j].Environment {
			return filtered[i].Environment < filtered[j].Environment
		}
		if filtered[i].RunURL != filtered[j].RunURL {
			return filtered[i].RunURL < filtered[j].RunURL
		}
		if filtered[i].RowID != filtered[j].RowID {
			return filtered[i].RowID < filtered[j].RowID
		}
		return filtered[i].IssueID < filtered[j].IssueID
	})
	return filtered, nil
}

func (s *Store) AppendPhase3Events(ctx context.Context, rows []semanticcontracts.Phase3EventRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.phase3EventsPath()
	existing, err := readNDJSON[semanticcontracts.Phase3EventRecord](path)
	if err != nil {
		return err
	}
	mergedByKey := map[string]semanticcontracts.Phase3EventRecord{}
	for _, row := range existing {
		normalized := normalizePhase3EventRecord(row)
		key := phase3EventKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizePhase3EventRecord(row)
		key := phase3EventKey(normalized)
		if key == "" {
			return fmt.Errorf("phase3 event record missing event_id")
		}
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.Phase3EventRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].At != merged[j].At {
			return merged[i].At < merged[j].At
		}
		return merged[i].EventID < merged[j].EventID
	})
	return writeNDJSON(path, merged)
}

func (s *Store) ListPhase3Events(ctx context.Context, limit int) ([]semanticcontracts.Phase3EventRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[semanticcontracts.Phase3EventRecord](s.phase3EventsPath())
	if err != nil {
		return nil, err
	}
	filtered := make([]semanticcontracts.Phase3EventRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase3EventRecord(row)
		if phase3EventKey(normalized) == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].At != filtered[j].At {
			return filtered[i].At > filtered[j].At
		}
		return filtered[i].EventID > filtered[j].EventID
	})
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered, nil
}

func (s *Store) runsPath() string {
	return filepath.Join(s.dataDirectory, factsDirectory, runsFilename)
}

func (s *Store) pullRequestsPath() string {
	return filepath.Join(s.dataDirectory, factsDirectory, pullRequestsFilename)
}

func (s *Store) artifactFailuresPath() string {
	return filepath.Join(s.dataDirectory, factsDirectory, artifactFailuresFilename)
}

func (s *Store) rawFailuresPath() string {
	return filepath.Join(s.dataDirectory, factsDirectory, rawFailuresFilename)
}

func (s *Store) metricsDailyPath() string {
	return filepath.Join(s.dataDirectory, factsDirectory, metricsDailyFilename)
}

func (s *Store) testMetadataDailyPath() string {
	return filepath.Join(s.dataDirectory, factsDirectory, testMetadataDailyFile)
}

func (s *Store) phase1WorksetPath() string {
	return filepath.Join(s.semanticBasePath(), phase1WorksetFilename)
}

func (s *Store) phase1NormalizedPath() string {
	return filepath.Join(s.semanticBasePath(), phase1NormalizedFilename)
}

func (s *Store) phase1AssignmentsPath() string {
	return filepath.Join(s.semanticBasePath(), phase1AssignmentsFile)
}

func (s *Store) testClustersPath() string {
	return filepath.Join(s.semanticBasePath(), testClustersFilename)
}

func (s *Store) globalClustersPath() string {
	return filepath.Join(s.semanticBasePath(), globalClustersFilename)
}

func (s *Store) phase3GlobalClustersPath() string {
	return filepath.Join(s.semanticBasePath(), phase3GlobalClustersFilename)
}

func (s *Store) reviewQueuePath() string {
	return filepath.Join(s.semanticBasePath(), reviewQueueFilename)
}

func (s *Store) checkpointsPath() string {
	return filepath.Join(s.dataDirectory, stateDirectory, checkpointsFilename)
}

func (s *Store) deadLettersPath() string {
	return filepath.Join(s.dataDirectory, stateDirectory, deadLettersFilename)
}

func (s *Store) phase3IssuesPath() string {
	return filepath.Join(s.dataDirectory, stateDirectory, phase3Directory, phase3IssuesFilename)
}

func (s *Store) phase3LinksPath() string {
	return filepath.Join(s.dataDirectory, stateDirectory, phase3Directory, phase3LinksFilename)
}

func (s *Store) phase3EventsPath() string {
	return filepath.Join(s.dataDirectory, stateDirectory, phase3Directory, phase3EventsFilename)
}

func normalizeRunRecord(row contracts.RunRecord) contracts.RunRecord {
	prNumber := row.PRNumber
	if prNumber < 0 {
		prNumber = 0
	}
	prState := strings.ToLower(strings.TrimSpace(row.PRState))
	switch prState {
	case "open", "closed":
	default:
		prState = ""
	}
	return contracts.RunRecord{
		Environment:    normalizeEnvironment(row.Environment),
		RunURL:         strings.TrimSpace(row.RunURL),
		JobName:        strings.TrimSpace(row.JobName),
		PRNumber:       prNumber,
		PRState:        prState,
		PRSHA:          strings.TrimSpace(row.PRSHA),
		FinalMergedSHA: strings.TrimSpace(row.FinalMergedSHA),
		MergedPR:       row.MergedPR,
		PostGoodCommit: row.PostGoodCommit,
		Failed:         row.Failed,
		OccurredAt:     strings.TrimSpace(row.OccurredAt),
	}
}

func normalizePullRequestRecord(row contracts.PullRequestRecord) contracts.PullRequestRecord {
	prNumber := row.PRNumber
	if prNumber < 0 {
		prNumber = 0
	}
	state := strings.ToLower(strings.TrimSpace(row.State))
	switch state {
	case "open", "closed":
	default:
		state = ""
	}
	merged := row.Merged
	if merged {
		state = "closed"
	}
	return contracts.PullRequestRecord{
		PRNumber:       prNumber,
		State:          state,
		Merged:         merged,
		HeadSHA:        strings.TrimSpace(row.HeadSHA),
		MergeCommitSHA: strings.TrimSpace(row.MergeCommitSHA),
		MergedAt:       strings.TrimSpace(row.MergedAt),
		ClosedAt:       strings.TrimSpace(row.ClosedAt),
		UpdatedAt:      strings.TrimSpace(row.UpdatedAt),
		LastCheckedAt:  strings.TrimSpace(row.LastCheckedAt),
	}
}

func normalizeArtifactFailureRecord(row contracts.ArtifactFailureRecord) contracts.ArtifactFailureRecord {
	return contracts.ArtifactFailureRecord{
		Environment:   normalizeEnvironment(row.Environment),
		ArtifactRowID: strings.TrimSpace(row.ArtifactRowID),
		RunURL:        strings.TrimSpace(row.RunURL),
		TestName:      strings.TrimSpace(row.TestName),
		TestSuite:     strings.TrimSpace(row.TestSuite),
		SignatureID:   strings.TrimSpace(row.SignatureID),
		FailureText:   strings.TrimSpace(row.FailureText),
	}
}

func normalizeRawFailureRecord(row contracts.RawFailureRecord) contracts.RawFailureRecord {
	return contracts.RawFailureRecord{
		Environment:       normalizeEnvironment(row.Environment),
		RowID:             strings.TrimSpace(row.RowID),
		RunURL:            strings.TrimSpace(row.RunURL),
		NonArtifactBacked: row.NonArtifactBacked,
		TestName:          strings.TrimSpace(row.TestName),
		TestSuite:         strings.TrimSpace(row.TestSuite),
		SignatureID:       strings.TrimSpace(row.SignatureID),
		OccurredAt:        strings.TrimSpace(row.OccurredAt),
		RawText:           strings.TrimSpace(row.RawText),
		NormalizedText:    strings.TrimSpace(row.NormalizedText),
	}
}

func normalizeMetricDailyRecord(row contracts.MetricDailyRecord) contracts.MetricDailyRecord {
	return contracts.MetricDailyRecord{
		Environment: normalizeEnvironment(row.Environment),
		Date:        strings.TrimSpace(row.Date),
		Metric:      strings.TrimSpace(row.Metric),
		Value:       row.Value,
	}
}

func normalizeTestMetadataDailyRecord(row contracts.TestMetadataDailyRecord) contracts.TestMetadataDailyRecord {
	period := strings.TrimSpace(row.Period)
	if period == "" {
		period = "default"
	}
	return contracts.TestMetadataDailyRecord{
		Environment:            normalizeEnvironment(row.Environment),
		Date:                   strings.TrimSpace(row.Date),
		Release:                strings.TrimSpace(row.Release),
		Period:                 period,
		TestName:               strings.TrimSpace(row.TestName),
		TestSuite:              strings.TrimSpace(row.TestSuite),
		CurrentPassPercentage:  row.CurrentPassPercentage,
		CurrentRuns:            row.CurrentRuns,
		PreviousPassPercentage: row.PreviousPassPercentage,
		PreviousRuns:           row.PreviousRuns,
		NetImprovement:         row.NetImprovement,
		IngestedAt:             strings.TrimSpace(row.IngestedAt),
	}
}

func normalizeCheckpointRecord(row contracts.CheckpointRecord) contracts.CheckpointRecord {
	return contracts.CheckpointRecord{
		Name:      strings.TrimSpace(row.Name),
		Value:     strings.TrimSpace(row.Value),
		UpdatedAt: strings.TrimSpace(row.UpdatedAt),
	}
}

func normalizeDeadLetterRecord(row contracts.DeadLetterRecord) contracts.DeadLetterRecord {
	return contracts.DeadLetterRecord{
		Controller: strings.TrimSpace(row.Controller),
		Key:        strings.TrimSpace(row.Key),
		Error:      strings.TrimSpace(row.Error),
		FailedAt:   strings.TrimSpace(row.FailedAt),
	}
}

func normalizePhase1WorksetRecord(row semanticcontracts.Phase1WorksetRecord) semanticcontracts.Phase1WorksetRecord {
	prNumber := row.PRNumber
	if prNumber < 0 {
		prNumber = 0
	}
	return semanticcontracts.Phase1WorksetRecord{
		SchemaVersion:  strings.TrimSpace(row.SchemaVersion),
		Environment:    normalizeSemanticEnvironment(row.Environment),
		RowID:          strings.TrimSpace(row.RowID),
		GroupKey:       strings.TrimSpace(row.GroupKey),
		Lane:           strings.TrimSpace(row.Lane),
		JobName:        strings.TrimSpace(row.JobName),
		TestName:       strings.TrimSpace(row.TestName),
		TestSuite:      strings.TrimSpace(row.TestSuite),
		SignatureID:    strings.TrimSpace(row.SignatureID),
		OccurredAt:     strings.TrimSpace(row.OccurredAt),
		RunURL:         strings.TrimSpace(row.RunURL),
		PRNumber:       prNumber,
		PostGoodCommit: row.PostGoodCommit,
		RawText:        strings.TrimSpace(row.RawText),
		NormalizedText: strings.TrimSpace(row.NormalizedText),
	}
}

func normalizePhase1NormalizedRecord(row semanticcontracts.Phase1NormalizedRecord) semanticcontracts.Phase1NormalizedRecord {
	prNumber := row.PRNumber
	if prNumber < 0 {
		prNumber = 0
	}
	return semanticcontracts.Phase1NormalizedRecord{
		SchemaVersion:           strings.TrimSpace(row.SchemaVersion),
		Environment:             normalizeSemanticEnvironment(row.Environment),
		RowID:                   strings.TrimSpace(row.RowID),
		GroupKey:                strings.TrimSpace(row.GroupKey),
		Lane:                    strings.TrimSpace(row.Lane),
		JobName:                 strings.TrimSpace(row.JobName),
		TestName:                strings.TrimSpace(row.TestName),
		TestSuite:               strings.TrimSpace(row.TestSuite),
		SignatureID:             strings.TrimSpace(row.SignatureID),
		OccurredAt:              strings.TrimSpace(row.OccurredAt),
		RunURL:                  strings.TrimSpace(row.RunURL),
		PRNumber:                prNumber,
		PostGoodCommit:          row.PostGoodCommit,
		RawText:                 strings.TrimSpace(row.RawText),
		NormalizedText:          strings.TrimSpace(row.NormalizedText),
		CanonicalEvidencePhrase: strings.TrimSpace(row.CanonicalEvidencePhrase),
		SearchQueryPhrase:       strings.TrimSpace(row.SearchQueryPhrase),
		ProviderAnchor:          strings.TrimSpace(row.ProviderAnchor),
		GenericPhrase:           row.GenericPhrase,
		Phase1Key:               strings.TrimSpace(row.Phase1Key),
	}
}

func normalizePhase1AssignmentRecord(row semanticcontracts.Phase1AssignmentRecord) semanticcontracts.Phase1AssignmentRecord {
	return semanticcontracts.Phase1AssignmentRecord{
		SchemaVersion:                    strings.TrimSpace(row.SchemaVersion),
		Environment:                      normalizeSemanticEnvironment(row.Environment),
		RowID:                            strings.TrimSpace(row.RowID),
		GroupKey:                         strings.TrimSpace(row.GroupKey),
		Phase1LocalClusterKey:            strings.TrimSpace(row.Phase1LocalClusterKey),
		CanonicalEvidencePhraseCandidate: strings.TrimSpace(row.CanonicalEvidencePhraseCandidate),
		SearchQueryPhraseCandidate:       strings.TrimSpace(row.SearchQueryPhraseCandidate),
		Confidence:                       strings.TrimSpace(row.Confidence),
		Reasons:                          normalizeStringSlice(row.Reasons),
	}
}

func normalizeReferenceRecord(row semanticcontracts.ReferenceRecord) semanticcontracts.ReferenceRecord {
	prNumber := row.PRNumber
	if prNumber < 0 {
		prNumber = 0
	}
	return semanticcontracts.ReferenceRecord{
		RowID:          strings.TrimSpace(row.RowID),
		RunURL:         strings.TrimSpace(row.RunURL),
		OccurredAt:     strings.TrimSpace(row.OccurredAt),
		SignatureID:    strings.TrimSpace(row.SignatureID),
		PRNumber:       prNumber,
		PostGoodCommit: row.PostGoodCommit,
	}
}

func normalizeReferenceSlice(rows []semanticcontracts.ReferenceRecord) []semanticcontracts.ReferenceRecord {
	if len(rows) == 0 {
		return nil
	}
	out := make([]semanticcontracts.ReferenceRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizeReferenceRecord(row)
		if normalized.RowID == "" && normalized.RunURL == "" && normalized.SignatureID == "" && normalized.OccurredAt == "" {
			continue
		}
		out = append(out, normalized)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OccurredAt != out[j].OccurredAt {
			return out[i].OccurredAt < out[j].OccurredAt
		}
		if out[i].RunURL != out[j].RunURL {
			return out[i].RunURL < out[j].RunURL
		}
		if out[i].RowID != out[j].RowID {
			return out[i].RowID < out[j].RowID
		}
		if out[i].SignatureID != out[j].SignatureID {
			return out[i].SignatureID < out[j].SignatureID
		}
		if out[i].PRNumber != out[j].PRNumber {
			return out[i].PRNumber < out[j].PRNumber
		}
		if out[i].PostGoodCommit != out[j].PostGoodCommit {
			return !out[i].PostGoodCommit && out[j].PostGoodCommit
		}
		return false
	})
	return out
}

func normalizeTestClusterRecord(row semanticcontracts.TestClusterRecord) semanticcontracts.TestClusterRecord {
	postGoodCommitCount := row.PostGoodCommitCount
	if postGoodCommitCount < 0 {
		postGoodCommitCount = 0
	}
	supportCount := row.SupportCount
	if supportCount < 0 {
		supportCount = 0
	}
	return semanticcontracts.TestClusterRecord{
		SchemaVersion:                strings.TrimSpace(row.SchemaVersion),
		Environment:                  normalizeSemanticEnvironment(row.Environment),
		Phase1ClusterID:              strings.TrimSpace(row.Phase1ClusterID),
		Lane:                         strings.TrimSpace(row.Lane),
		JobName:                      strings.TrimSpace(row.JobName),
		TestName:                     strings.TrimSpace(row.TestName),
		TestSuite:                    strings.TrimSpace(row.TestSuite),
		CanonicalEvidencePhrase:      strings.TrimSpace(row.CanonicalEvidencePhrase),
		SearchQueryPhrase:            strings.TrimSpace(row.SearchQueryPhrase),
		SearchQuerySourceRunURL:      strings.TrimSpace(row.SearchQuerySourceRunURL),
		SearchQuerySourceSignatureID: strings.TrimSpace(row.SearchQuerySourceSignatureID),
		SupportCount:                 supportCount,
		SeenPostGoodCommit:           row.SeenPostGoodCommit,
		PostGoodCommitCount:          postGoodCommitCount,
		MemberSignatureIDs:           normalizeStringSlice(row.MemberSignatureIDs),
		References:                   normalizeReferenceSlice(row.References),
	}
}

func normalizeContributingTests(rows []semanticcontracts.ContributingTestRecord) []semanticcontracts.ContributingTestRecord {
	if len(rows) == 0 {
		return nil
	}
	merged := map[string]semanticcontracts.ContributingTestRecord{}
	for _, row := range rows {
		lane := strings.TrimSpace(row.Lane)
		jobName := strings.TrimSpace(row.JobName)
		testName := strings.TrimSpace(row.TestName)
		if lane == "" && jobName == "" && testName == "" {
			continue
		}
		supportCount := row.SupportCount
		if supportCount < 0 {
			supportCount = 0
		}
		key := lane + "|" + jobName + "|" + testName
		existing := merged[key]
		if existing.Lane == "" {
			existing.Lane = lane
		}
		if existing.JobName == "" {
			existing.JobName = jobName
		}
		if existing.TestName == "" {
			existing.TestName = testName
		}
		existing.SupportCount += supportCount
		merged[key] = existing
	}

	out := make([]semanticcontracts.ContributingTestRecord, 0, len(merged))
	for _, row := range merged {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Lane != out[j].Lane {
			return out[i].Lane < out[j].Lane
		}
		if out[i].JobName != out[j].JobName {
			return out[i].JobName < out[j].JobName
		}
		return out[i].TestName < out[j].TestName
	})
	return out
}

func normalizeGlobalClusterRecord(row semanticcontracts.GlobalClusterRecord) semanticcontracts.GlobalClusterRecord {
	supportCount := row.SupportCount
	if supportCount < 0 {
		supportCount = 0
	}
	postGoodCommitCount := row.PostGoodCommitCount
	if postGoodCommitCount < 0 {
		postGoodCommitCount = 0
	}
	contributingTests := normalizeContributingTests(row.ContributingTests)
	return semanticcontracts.GlobalClusterRecord{
		SchemaVersion:                strings.TrimSpace(row.SchemaVersion),
		Environment:                  normalizeSemanticEnvironment(row.Environment),
		Phase2ClusterID:              strings.TrimSpace(row.Phase2ClusterID),
		CanonicalEvidencePhrase:      strings.TrimSpace(row.CanonicalEvidencePhrase),
		SearchQueryPhrase:            strings.TrimSpace(row.SearchQueryPhrase),
		SearchQuerySourceRunURL:      strings.TrimSpace(row.SearchQuerySourceRunURL),
		SearchQuerySourceSignatureID: strings.TrimSpace(row.SearchQuerySourceSignatureID),
		SupportCount:                 supportCount,
		SeenPostGoodCommit:           row.SeenPostGoodCommit || postGoodCommitCount > 0,
		PostGoodCommitCount:          postGoodCommitCount,
		ContributingTestsCount:       len(contributingTests),
		ContributingTests:            contributingTests,
		MemberPhase1ClusterIDs:       normalizeStringSlice(row.MemberPhase1ClusterIDs),
		MemberSignatureIDs:           normalizeStringSlice(row.MemberSignatureIDs),
		References:                   normalizeReferenceSlice(row.References),
	}
}

func normalizeReviewItemRecord(row semanticcontracts.ReviewItemRecord) semanticcontracts.ReviewItemRecord {
	return semanticcontracts.ReviewItemRecord{
		SchemaVersion:                        strings.TrimSpace(row.SchemaVersion),
		Environment:                          normalizeSemanticEnvironment(row.Environment),
		ReviewItemID:                         strings.TrimSpace(row.ReviewItemID),
		Phase:                                strings.TrimSpace(row.Phase),
		Reason:                               strings.TrimSpace(row.Reason),
		ProposedCanonicalEvidencePhrase:      strings.TrimSpace(row.ProposedCanonicalEvidencePhrase),
		ProposedSearchQueryPhrase:            strings.TrimSpace(row.ProposedSearchQueryPhrase),
		ProposedSearchQuerySourceRunURL:      strings.TrimSpace(row.ProposedSearchQuerySourceRunURL),
		ProposedSearchQuerySourceSignatureID: strings.TrimSpace(row.ProposedSearchQuerySourceSignatureID),
		SourcePhase1ClusterIDs:               normalizeStringSlice(row.SourcePhase1ClusterIDs),
		MemberSignatureIDs:                   normalizeStringSlice(row.MemberSignatureIDs),
		References:                           normalizeReferenceSlice(row.References),
	}
}

func normalizePhase3IssueRecord(row semanticcontracts.Phase3IssueRecord) semanticcontracts.Phase3IssueRecord {
	return semanticcontracts.Phase3IssueRecord{
		SchemaVersion: strings.TrimSpace(row.SchemaVersion),
		IssueID:       strings.TrimSpace(row.IssueID),
		Title:         strings.TrimSpace(row.Title),
		CreatedAt:     strings.TrimSpace(row.CreatedAt),
		UpdatedAt:     strings.TrimSpace(row.UpdatedAt),
	}
}

func normalizePhase3LinkRecord(row semanticcontracts.Phase3LinkRecord) semanticcontracts.Phase3LinkRecord {
	return semanticcontracts.Phase3LinkRecord{
		SchemaVersion: strings.TrimSpace(row.SchemaVersion),
		IssueID:       strings.TrimSpace(row.IssueID),
		Environment:   normalizeSemanticEnvironment(row.Environment),
		RunURL:        strings.TrimSpace(row.RunURL),
		RowID:         strings.TrimSpace(row.RowID),
		UpdatedAt:     strings.TrimSpace(row.UpdatedAt),
	}
}

func normalizePhase3EventRecord(row semanticcontracts.Phase3EventRecord) semanticcontracts.Phase3EventRecord {
	return semanticcontracts.Phase3EventRecord{
		SchemaVersion: strings.TrimSpace(row.SchemaVersion),
		EventID:       strings.TrimSpace(row.EventID),
		Action:        strings.TrimSpace(row.Action),
		IssueID:       strings.TrimSpace(row.IssueID),
		Environment:   normalizeSemanticEnvironment(row.Environment),
		RunURL:        strings.TrimSpace(row.RunURL),
		RowID:         strings.TrimSpace(row.RowID),
		At:            strings.TrimSpace(row.At),
	}
}

func normalizeStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		set[trimmed] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func artifactFailureKey(row contracts.ArtifactFailureRecord) string {
	if row.Environment == "" || row.ArtifactRowID == "" {
		return ""
	}
	return row.Environment + "|" + row.ArtifactRowID
}

func metricDailyKey(row contracts.MetricDailyRecord) string {
	if row.Environment == "" || row.Date == "" || row.Metric == "" {
		return ""
	}
	return row.Environment + "|" + row.Date + "|" + row.Metric
}

func globalClusterKey(row semanticcontracts.GlobalClusterRecord) string {
	environment := normalizeSemanticEnvironment(row.Environment)
	clusterID := strings.TrimSpace(row.Phase2ClusterID)
	if environment == "" || clusterID == "" {
		return ""
	}
	return environment + "|" + clusterID
}

func testMetadataDailyKey(row contracts.TestMetadataDailyRecord) string {
	if row.Environment == "" || row.Date == "" || row.Period == "" || row.TestName == "" {
		return ""
	}
	return row.Environment + "|" + row.Date + "|" + row.Period + "|" + row.TestSuite + "|" + row.TestName
}

func runRecordKey(row contracts.RunRecord) string {
	if row.Environment == "" || row.RunURL == "" {
		return ""
	}
	return row.Environment + "|" + row.RunURL
}

func rawFailureKey(row contracts.RawFailureRecord) string {
	if row.Environment == "" || row.RowID == "" {
		return ""
	}
	return row.Environment + "|" + row.RowID
}

func phase1WorksetKey(row semanticcontracts.Phase1WorksetRecord) string {
	environment := normalizeSemanticEnvironment(row.Environment)
	rowID := strings.TrimSpace(row.RowID)
	if environment == "" || rowID == "" {
		return ""
	}
	return environment + "|" + rowID
}

func phase1AssignmentKey(row semanticcontracts.Phase1AssignmentRecord) string {
	environment := normalizeSemanticEnvironment(row.Environment)
	rowID := strings.TrimSpace(row.RowID)
	if environment == "" || rowID == "" {
		return ""
	}
	return environment + "|" + rowID
}

func phase1ClusterKey(row semanticcontracts.TestClusterRecord) string {
	environment := normalizeSemanticEnvironment(row.Environment)
	clusterID := strings.TrimSpace(row.Phase1ClusterID)
	if environment == "" || clusterID == "" {
		return ""
	}
	return environment + "|" + clusterID
}

func reviewItemKey(row semanticcontracts.ReviewItemRecord) string {
	environment := normalizeSemanticEnvironment(row.Environment)
	reviewID := strings.TrimSpace(row.ReviewItemID)
	if environment == "" || reviewID == "" {
		return ""
	}
	return environment + "|" + reviewID
}

func phase3IssueKey(row semanticcontracts.Phase3IssueRecord) string {
	issueID := strings.TrimSpace(row.IssueID)
	if issueID == "" {
		return ""
	}
	return issueID
}

func phase3LinkKey(row semanticcontracts.Phase3LinkRecord) string {
	environment := normalizeSemanticEnvironment(row.Environment)
	runURL := strings.TrimSpace(row.RunURL)
	rowID := strings.TrimSpace(row.RowID)
	if environment == "" || runURL == "" || rowID == "" {
		return ""
	}
	return environment + "|" + runURL + "|" + rowID
}

func phase3EventKey(row semanticcontracts.Phase3EventRecord) string {
	eventID := strings.TrimSpace(row.EventID)
	if eventID == "" {
		return ""
	}
	return eventID
}

func phase1WorksetLess(a semanticcontracts.Phase1WorksetRecord, b semanticcontracts.Phase1WorksetRecord) bool {
	if a.Environment != b.Environment {
		return a.Environment < b.Environment
	}
	if a.Lane != b.Lane {
		return a.Lane < b.Lane
	}
	if a.JobName != b.JobName {
		return a.JobName < b.JobName
	}
	if a.TestName != b.TestName {
		return a.TestName < b.TestName
	}
	if a.OccurredAt != b.OccurredAt {
		return a.OccurredAt < b.OccurredAt
	}
	if a.RunURL != b.RunURL {
		return a.RunURL < b.RunURL
	}
	if a.SignatureID != b.SignatureID {
		return a.SignatureID < b.SignatureID
	}
	return a.RowID < b.RowID
}

func testClusterLess(a semanticcontracts.TestClusterRecord, b semanticcontracts.TestClusterRecord) bool {
	if a.Environment != b.Environment {
		return a.Environment < b.Environment
	}
	if a.Lane != b.Lane {
		return a.Lane < b.Lane
	}
	if a.JobName != b.JobName {
		return a.JobName < b.JobName
	}
	if a.TestName != b.TestName {
		return a.TestName < b.TestName
	}
	if a.SupportCount != b.SupportCount {
		return a.SupportCount > b.SupportCount
	}
	return a.Phase1ClusterID < b.Phase1ClusterID
}

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeSemanticEnvironment(value string) string {
	normalized := normalizeEnvironment(value)
	if normalized == "" {
		return "unknown"
	}
	return normalized
}

func normalizeDate(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("date is empty")
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return "", err
	}
	return parsed.UTC().Format("2006-01-02"), nil
}

func normalizeSemanticSubdirectory(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", nil
	}
	cleaned := filepath.Clean(trimmed)
	if cleaned == "." {
		return "", nil
	}
	if filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("must be a relative path")
	}
	for _, part := range strings.Split(cleaned, string(filepath.Separator)) {
		switch part {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("must not contain '..'")
		}
	}
	return cleaned, nil
}

func (s *Store) semanticBasePath() string {
	base := filepath.Join(s.dataDirectory, semanticDirectory)
	if s.semanticSubdirectory == "" {
		return base
	}
	return filepath.Join(base, s.semanticSubdirectory)
}

func dateFromTimestamp(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", false
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts.UTC().Format("2006-01-02"), true
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts.UTC().Format("2006-01-02"), true
	}
	return "", false
}

func readNDJSON[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []T{}, nil
		}
		return nil, fmt.Errorf("open NDJSON file %q: %w", path, err)
	}
	defer f.Close()

	out := make([]T, 0)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 20*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var item T
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			return nil, fmt.Errorf("decode NDJSON record in %q: %w", path, err)
		}
		out = append(out, item)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan NDJSON file %q: %w", path, err)
	}
	return out, nil
}

func writeNDJSON[T any](path string, rows []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create NDJSON parent directory for %q: %w", path, err)
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file for %q: %w", path, err)
	}

	tmpPath := tmpFile.Name()
	committed := false
	defer func() {
		_ = tmpFile.Close()
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	writer := bufio.NewWriter(tmpFile)
	for _, row := range rows {
		b, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("marshal NDJSON record for %q: %w", path, err)
		}
		if _, err := writer.Write(b); err != nil {
			return fmt.Errorf("write NDJSON record for %q: %w", path, err)
		}
		if _, err := io.WriteString(writer, "\n"); err != nil {
			return fmt.Errorf("write NDJSON newline for %q: %w", path, err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush temp NDJSON file for %q: %w", path, err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp NDJSON file for %q: %w", path, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp NDJSON file for %q: %w", path, err)
	}
	committed = true
	return nil
}
