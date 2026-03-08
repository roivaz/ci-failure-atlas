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

	runsFilename             = "runs.ndjson"
	pullRequestsFilename     = "pull_requests.ndjson"
	artifactFailuresFilename = "artifact_failures.ndjson"
	rawFailuresFilename      = "raw_failures.ndjson"
	metricsDailyFilename     = "metrics_daily.ndjson"
	runCountsHourlyFilename  = "run_counts_hourly.ndjson"
	phase1WorksetFilename    = "phase1_workset.ndjson"
	phase1NormalizedFilename = "phase1_normalized.ndjson"
	phase1AssignmentsFile    = "phase1_assignments.ndjson"
	testClustersFilename     = "test_clusters.ndjson"
	reviewQueueFilename      = "review_queue.ndjson"
	checkpointsFilename      = "checkpoints.ndjson"
	deadLettersFilename      = "dead_letters.ndjson"
)

type Store struct {
	dataDirectory string
	mu            sync.RWMutex
}

var _ contracts.Store = (*Store)(nil)

func New(dataDirectory string) (*Store, error) {
	if dataDirectory == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if err := os.MkdirAll(filepath.Clean(dataDirectory), 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	return &Store{dataDirectory: dataDirectory}, nil
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
		if normalized.ArtifactRowID == "" || normalized.SignatureID == "" || normalized.FailureText == "" {
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

func (s *Store) UpsertRunCountsHourly(ctx context.Context, rows []contracts.RunCountHourlyRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.runCountsHourlyPath()
	existing, err := readNDJSON[contracts.RunCountHourlyRecord](path)
	if err != nil {
		return err
	}

	mergedByKey := map[string]contracts.RunCountHourlyRecord{}
	for _, row := range existing {
		normalized := normalizeRunCountHourlyRecord(row)
		key := runCountHourlyKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizeRunCountHourlyRecord(row)
		key := runCountHourlyKey(normalized)
		if key == "" {
			return fmt.Errorf("run count hourly record missing environment and/or hour")
		}
		if normalized.TotalRuns < 0 || normalized.FailedRuns < 0 || normalized.SuccessfulRuns < 0 {
			return fmt.Errorf("run count hourly record has negative counters for key %q", key)
		}
		if normalized.TotalRuns != normalized.FailedRuns+normalized.SuccessfulRuns {
			return fmt.Errorf("run count hourly record has inconsistent counters for key %q", key)
		}
		mergedByKey[key] = normalized
	}

	merged := make([]contracts.RunCountHourlyRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Environment != merged[j].Environment {
			return merged[i].Environment < merged[j].Environment
		}
		return merged[i].Hour < merged[j].Hour
	})

	return writeNDJSON(path, merged)
}

func (s *Store) ListRunCountHourlyHours(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.RunCountHourlyRecord](s.runCountsHourlyPath())
	if err != nil {
		return nil, err
	}

	hoursSet := map[string]struct{}{}
	for _, row := range rows {
		hour := normalizeRunCountHourlyRecord(row).Hour
		if hour == "" {
			continue
		}
		hoursSet[hour] = struct{}{}
	}
	hours := make([]string, 0, len(hoursSet))
	for hour := range hoursSet {
		hours = append(hours, hour)
	}
	sort.Strings(hours)
	return hours, nil
}

func (s *Store) ListRunCountsHourlyByDate(ctx context.Context, environment string, date string) ([]contracts.RunCountHourlyRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	lookupEnv := normalizeEnvironment(environment)
	if lookupEnv == "" {
		return nil, fmt.Errorf("run count hourly lookup requires environment")
	}
	lookupDate, err := normalizeDate(date)
	if err != nil {
		return nil, fmt.Errorf("run count hourly lookup requires valid date (YYYY-MM-DD): %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := readNDJSON[contracts.RunCountHourlyRecord](s.runCountsHourlyPath())
	if err != nil {
		return nil, err
	}

	filtered := make([]contracts.RunCountHourlyRecord, 0)
	for _, row := range rows {
		normalized := normalizeRunCountHourlyRecord(row)
		if normalized.Environment != lookupEnv {
			continue
		}
		if rowDate, ok := dateFromTimestamp(normalized.Hour); !ok || rowDate != lookupDate {
			continue
		}
		if normalized.Hour == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Hour < filtered[j].Hour
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

	mergedByKey := map[string]semanticcontracts.Phase1WorksetRecord{}
	for _, row := range existing {
		normalized := normalizePhase1WorksetRecord(row)
		key := phase1WorksetKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizePhase1WorksetRecord(row)
		key := phase1WorksetKey(normalized)
		if key == "" {
			return fmt.Errorf("phase1 workset record missing row_id")
		}
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
		if normalized.RowID == "" {
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

	mergedByKey := map[string]semanticcontracts.Phase1NormalizedRecord{}
	for _, row := range existing {
		normalized := normalizePhase1NormalizedRecord(row)
		key := phase1WorksetKey(semanticcontracts.Phase1WorksetRecord{RowID: normalized.RowID})
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizePhase1NormalizedRecord(row)
		key := phase1WorksetKey(semanticcontracts.Phase1WorksetRecord{RowID: normalized.RowID})
		if key == "" {
			return fmt.Errorf("phase1 normalized record missing row_id")
		}
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.Phase1NormalizedRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
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
		if normalized.RowID == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
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

	mergedByKey := map[string]semanticcontracts.Phase1AssignmentRecord{}
	for _, row := range existing {
		normalized := normalizePhase1AssignmentRecord(row)
		key := strings.TrimSpace(normalized.RowID)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizePhase1AssignmentRecord(row)
		key := strings.TrimSpace(normalized.RowID)
		if key == "" {
			return fmt.Errorf("phase1 assignment record missing row_id")
		}
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.Phase1AssignmentRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
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
		if normalized.RowID == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
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

	mergedByKey := map[string]semanticcontracts.TestClusterRecord{}
	for _, row := range existing {
		normalized := normalizeTestClusterRecord(row)
		key := strings.TrimSpace(normalized.Phase1ClusterID)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizeTestClusterRecord(row)
		key := strings.TrimSpace(normalized.Phase1ClusterID)
		if key == "" {
			return fmt.Errorf("test cluster record missing phase1_cluster_id")
		}
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
		if normalized.Phase1ClusterID == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return testClusterLess(filtered[i], filtered[j])
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

	mergedByKey := map[string]semanticcontracts.ReviewItemRecord{}
	for _, row := range existing {
		normalized := normalizeReviewItemRecord(row)
		key := strings.TrimSpace(normalized.ReviewItemID)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizeReviewItemRecord(row)
		key := strings.TrimSpace(normalized.ReviewItemID)
		if key == "" {
			return fmt.Errorf("review item record missing review_item_id")
		}
		mergedByKey[key] = normalized
	}

	merged := make([]semanticcontracts.ReviewItemRecord, 0, len(mergedByKey))
	for _, row := range mergedByKey {
		merged = append(merged, row)
	}
	sort.Slice(merged, func(i, j int) bool {
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
		if normalized.ReviewItemID == "" {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sort.Slice(filtered, func(i, j int) bool {
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

func (s *Store) runCountsHourlyPath() string {
	return filepath.Join(s.dataDirectory, factsDirectory, runCountsHourlyFilename)
}

func (s *Store) phase1WorksetPath() string {
	return filepath.Join(s.dataDirectory, semanticDirectory, phase1WorksetFilename)
}

func (s *Store) phase1NormalizedPath() string {
	return filepath.Join(s.dataDirectory, semanticDirectory, phase1NormalizedFilename)
}

func (s *Store) phase1AssignmentsPath() string {
	return filepath.Join(s.dataDirectory, semanticDirectory, phase1AssignmentsFile)
}

func (s *Store) testClustersPath() string {
	return filepath.Join(s.dataDirectory, semanticDirectory, testClustersFilename)
}

func (s *Store) reviewQueuePath() string {
	return filepath.Join(s.dataDirectory, semanticDirectory, reviewQueueFilename)
}

func (s *Store) checkpointsPath() string {
	return filepath.Join(s.dataDirectory, stateDirectory, checkpointsFilename)
}

func (s *Store) deadLettersPath() string {
	return filepath.Join(s.dataDirectory, stateDirectory, deadLettersFilename)
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
	postGoodCommitFailures := row.PostGoodCommitFailures
	if postGoodCommitFailures < 0 {
		postGoodCommitFailures = 0
	}
	return contracts.RawFailureRecord{
		Environment:            normalizeEnvironment(row.Environment),
		RowID:                  strings.TrimSpace(row.RowID),
		RunURL:                 strings.TrimSpace(row.RunURL),
		NonArtifactBacked:      row.NonArtifactBacked,
		TestName:               strings.TrimSpace(row.TestName),
		TestSuite:              strings.TrimSpace(row.TestSuite),
		MergedPR:               row.MergedPR,
		PostGoodCommitFailures: postGoodCommitFailures,
		SignatureID:            strings.TrimSpace(row.SignatureID),
		OccurredAt:             strings.TrimSpace(row.OccurredAt),
		RawText:                strings.TrimSpace(row.RawText),
		NormalizedText:         strings.TrimSpace(row.NormalizedText),
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

func normalizeRunCountHourlyRecord(row contracts.RunCountHourlyRecord) contracts.RunCountHourlyRecord {
	hour := strings.TrimSpace(row.Hour)
	if ts, err := time.Parse(time.RFC3339Nano, hour); err == nil {
		hour = ts.UTC().Truncate(time.Hour).Format(time.RFC3339)
	} else if ts, err := time.Parse(time.RFC3339, hour); err == nil {
		hour = ts.UTC().Truncate(time.Hour).Format(time.RFC3339)
	}

	return contracts.RunCountHourlyRecord{
		Environment:    normalizeEnvironment(row.Environment),
		Hour:           hour,
		TotalRuns:      row.TotalRuns,
		FailedRuns:     row.FailedRuns,
		SuccessfulRuns: row.SuccessfulRuns,
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
		RunURL:         strings.TrimSpace(row.RunURL),
		OccurredAt:     strings.TrimSpace(row.OccurredAt),
		SignatureID:    strings.TrimSpace(row.SignatureID),
		PRNumber:       prNumber,
		PostGoodCommit: row.PostGoodCommit,
		RawTextExcerpt: strings.TrimSpace(row.RawTextExcerpt),
	}
}

func normalizeReferenceSlice(rows []semanticcontracts.ReferenceRecord) []semanticcontracts.ReferenceRecord {
	if len(rows) == 0 {
		return nil
	}
	out := make([]semanticcontracts.ReferenceRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizeReferenceRecord(row)
		if normalized.RunURL == "" && normalized.SignatureID == "" && normalized.OccurredAt == "" && normalized.RawTextExcerpt == "" {
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
		if out[i].SignatureID != out[j].SignatureID {
			return out[i].SignatureID < out[j].SignatureID
		}
		return out[i].RawTextExcerpt < out[j].RawTextExcerpt
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

func normalizeReviewItemRecord(row semanticcontracts.ReviewItemRecord) semanticcontracts.ReviewItemRecord {
	return semanticcontracts.ReviewItemRecord{
		SchemaVersion:                        strings.TrimSpace(row.SchemaVersion),
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

func runCountHourlyKey(row contracts.RunCountHourlyRecord) string {
	if row.Environment == "" || row.Hour == "" {
		return ""
	}
	return row.Environment + "|" + row.Hour
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
	return strings.TrimSpace(row.RowID)
}

func phase1WorksetLess(a semanticcontracts.Phase1WorksetRecord, b semanticcontracts.Phase1WorksetRecord) bool {
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
