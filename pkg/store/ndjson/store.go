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

	"ci-failure-atlas/pkg/store/contracts"
)

const (
	factsDirectory = "facts"
	stateDirectory = "state"

	runsFilename             = "runs.ndjson"
	artifactFailuresFilename = "artifact_failures.ndjson"
	rawFailuresFilename      = "raw_failures.ndjson"
	metricsDailyFilename     = "metrics_daily.ndjson"
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

	mergedByRowID := map[string]contracts.RawFailureRecord{}
	for _, row := range existing {
		normalized := normalizeRawFailureRecord(row)
		key := rawFailureKey(normalized)
		if key == "" {
			continue
		}
		mergedByRowID[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizeRawFailureRecord(row)
		key := rawFailureKey(normalized)
		if key == "" {
			return fmt.Errorf("raw failure record missing environment and/or row_id")
		}
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

	mergedByKey := map[string]contracts.MetricDailyRecord{}
	for _, row := range existing {
		normalized := normalizeMetricDailyRecord(row)
		key := metricDailyKey(normalized)
		if key == "" {
			continue
		}
		mergedByKey[key] = normalized
	}
	for _, row := range rows {
		normalized := normalizeMetricDailyRecord(row)
		key := metricDailyKey(normalized)
		if key == "" {
			return fmt.Errorf("metric daily record missing environment, date, and/or metric")
		}
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

func (s *Store) runsPath() string {
	return filepath.Join(s.dataDirectory, factsDirectory, runsFilename)
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

func (s *Store) checkpointsPath() string {
	return filepath.Join(s.dataDirectory, stateDirectory, checkpointsFilename)
}

func (s *Store) deadLettersPath() string {
	return filepath.Join(s.dataDirectory, stateDirectory, deadLettersFilename)
}

func normalizeRunRecord(row contracts.RunRecord) contracts.RunRecord {
	return contracts.RunRecord{
		Environment: normalizeEnvironment(row.Environment),
		RunURL:      strings.TrimSpace(row.RunURL),
		JobName:     strings.TrimSpace(row.JobName),
		OccurredAt:  strings.TrimSpace(row.OccurredAt),
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
		Environment:    normalizeEnvironment(row.Environment),
		RowID:          strings.TrimSpace(row.RowID),
		RunURL:         strings.TrimSpace(row.RunURL),
		SignatureID:    strings.TrimSpace(row.SignatureID),
		OccurredAt:     strings.TrimSpace(row.OccurredAt),
		RawText:        strings.TrimSpace(row.RawText),
		NormalizedText: strings.TrimSpace(row.NormalizedText),
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

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
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
