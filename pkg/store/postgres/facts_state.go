package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	storecontracts "ci-failure-atlas/pkg/store/contracts"

	"github.com/jackc/pgx/v5"
)

func (s *Store) upsertMetricsDailyImpl(ctx context.Context, rows []storecontracts.MetricDailyRecord) error {
	if len(rows) == 0 {
		return nil
	}

	type touchedDateEnvironment struct {
		environment string
		date        string
	}
	normalizedRows := make([]storecontracts.MetricDailyRecord, 0, len(rows))
	touched := map[string]touchedDateEnvironment{}
	for _, row := range rows {
		normalized := normalizeMetricDailyRecord(row)
		if metricDailyKey(normalized) == "" {
			return fmt.Errorf("metric daily record missing environment, date, and/or metric")
		}
		normalizedRows = append(normalizedRows, normalized)
		key := normalized.Environment + "|" + normalized.Date
		touched[key] = touchedDateEnvironment{
			environment: normalized.Environment,
			date:        normalized.Date,
		}
	}

	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, partition := range touched {
			_, err := tx.Exec(ctx, `
DELETE FROM cfa_metrics_daily
WHERE environment = $1 AND date = $2
`, partition.environment, partition.date)
			if err != nil {
				return fmt.Errorf("delete touched metrics partition (%s,%s): %w", partition.environment, partition.date, err)
			}
		}
		for _, row := range normalizedRows {
			_, err := tx.Exec(ctx, `
INSERT INTO cfa_metrics_daily (environment, date, metric, value)
VALUES ($1, $2, $3, $4)
ON CONFLICT (environment, date, metric)
DO UPDATE SET value = EXCLUDED.value
`, row.Environment, row.Date, row.Metric, row.Value)
			if err != nil {
				return fmt.Errorf("upsert metric row (%s,%s,%s): %w", row.Environment, row.Date, row.Metric, err)
			}
		}
		return nil
	})
}

func (s *Store) listMetricsDailyImpl(ctx context.Context) ([]storecontracts.MetricDailyRecord, error) {
	rows, err := s.pool.Query(ctx, `SELECT environment, date, metric, value FROM cfa_metrics_daily`)
	if err != nil {
		return nil, fmt.Errorf("query metrics daily: %w", err)
	}
	defer rows.Close()

	out := make([]storecontracts.MetricDailyRecord, 0)
	for rows.Next() {
		var row storecontracts.MetricDailyRecord
		if err := rows.Scan(&row.Environment, &row.Date, &row.Metric, &row.Value); err != nil {
			return nil, fmt.Errorf("scan metric daily row: %w", err)
		}
		normalized := normalizeMetricDailyRecord(row)
		if normalized.Environment == "" || normalized.Date == "" || normalized.Metric == "" {
			continue
		}
		out = append(out, normalized)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate metrics daily rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Environment != out[j].Environment {
			return out[i].Environment < out[j].Environment
		}
		if out[i].Date != out[j].Date {
			return out[i].Date < out[j].Date
		}
		return out[i].Metric < out[j].Metric
	})
	return out, nil
}

func (s *Store) listMetricsDailyByDateImpl(ctx context.Context, environment string, date string) ([]storecontracts.MetricDailyRecord, error) {
	lookupEnv := normalizeEnvironment(environment)
	if lookupEnv == "" {
		return nil, fmt.Errorf("metric daily lookup requires environment")
	}
	lookupDate, err := normalizeDate(date)
	if err != nil {
		return nil, fmt.Errorf("metric daily lookup requires valid date (YYYY-MM-DD): %w", err)
	}

	rows, err := s.pool.Query(ctx, `
SELECT environment, date, metric, value
FROM cfa_metrics_daily
WHERE environment = $1 AND date = $2
`, lookupEnv, lookupDate)
	if err != nil {
		return nil, fmt.Errorf("query metrics daily by date: %w", err)
	}
	defer rows.Close()

	out := make([]storecontracts.MetricDailyRecord, 0)
	for rows.Next() {
		var row storecontracts.MetricDailyRecord
		if err := rows.Scan(&row.Environment, &row.Date, &row.Metric, &row.Value); err != nil {
			return nil, fmt.Errorf("scan metric daily by date row: %w", err)
		}
		normalized := normalizeMetricDailyRecord(row)
		if normalized.Environment != lookupEnv || normalized.Date != lookupDate {
			continue
		}
		if normalized.Metric == "" {
			continue
		}
		out = append(out, normalized)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate metrics daily by date rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Metric < out[j].Metric
	})
	return out, nil
}

func (s *Store) listMetricDatesImpl(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT date FROM cfa_metrics_daily`)
	if err != nil {
		return nil, fmt.Errorf("query metric dates: %w", err)
	}
	defer rows.Close()

	set := map[string]struct{}{}
	for rows.Next() {
		var date string
		if err := rows.Scan(&date); err != nil {
			return nil, fmt.Errorf("scan metric date row: %w", err)
		}
		trimmed := strings.TrimSpace(date)
		if trimmed == "" {
			continue
		}
		set[trimmed] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate metric date rows: %w", err)
	}

	out := make([]string, 0, len(set))
	for date := range set {
		out = append(out, date)
	}
	sort.Strings(out)
	return out, nil
}

func (s *Store) upsertTestMetadataDailyImpl(ctx context.Context, rows []storecontracts.TestMetadataDailyRecord) error {
	if len(rows) == 0 {
		return nil
	}

	type touchedPartition struct {
		environment string
		date        string
		period      string
	}
	normalizedRows := make([]storecontracts.TestMetadataDailyRecord, 0, len(rows))
	touched := map[string]touchedPartition{}
	for _, row := range rows {
		normalized := normalizeTestMetadataDailyRecord(row)
		if testMetadataDailyKey(normalized) == "" {
			return fmt.Errorf("test metadata daily record missing environment, date, period, and/or test_name")
		}
		normalizedRows = append(normalizedRows, normalized)
		key := normalized.Environment + "|" + normalized.Date + "|" + normalized.Period
		touched[key] = touchedPartition{
			environment: normalized.Environment,
			date:        normalized.Date,
			period:      normalized.Period,
		}
	}

	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, partition := range touched {
			_, err := tx.Exec(ctx, `
DELETE FROM cfa_test_metadata_daily
WHERE environment = $1 AND date = $2 AND period = $3
`, partition.environment, partition.date, partition.period)
			if err != nil {
				return fmt.Errorf("delete touched test metadata partition (%s,%s,%s): %w", partition.environment, partition.date, partition.period, err)
			}
		}
		for _, row := range normalizedRows {
			_, err := tx.Exec(ctx, `
INSERT INTO cfa_test_metadata_daily (
  environment, date, release, period, test_name, test_suite,
  current_pass_percentage, current_runs, previous_pass_percentage, previous_runs,
  net_improvement, ingested_at
) VALUES (
  $1, $2, $3, $4, $5, $6,
  $7, $8, $9, $10,
  $11, $12
)
ON CONFLICT (environment, date, period, test_suite, test_name)
DO UPDATE SET
  release = EXCLUDED.release,
  current_pass_percentage = EXCLUDED.current_pass_percentage,
  current_runs = EXCLUDED.current_runs,
  previous_pass_percentage = EXCLUDED.previous_pass_percentage,
  previous_runs = EXCLUDED.previous_runs,
  net_improvement = EXCLUDED.net_improvement,
  ingested_at = EXCLUDED.ingested_at
`, row.Environment, row.Date, row.Release, row.Period, row.TestName, row.TestSuite, row.CurrentPassPercentage, row.CurrentRuns, row.PreviousPassPercentage, row.PreviousRuns, row.NetImprovement, row.IngestedAt)
			if err != nil {
				return fmt.Errorf("upsert test metadata row (%s,%s,%s,%s,%s): %w", row.Environment, row.Date, row.Period, row.TestSuite, row.TestName, err)
			}
		}
		return nil
	})
}

func (s *Store) listTestMetadataDailyByDateImpl(ctx context.Context, environment string, date string) ([]storecontracts.TestMetadataDailyRecord, error) {
	lookupEnv := normalizeEnvironment(environment)
	if lookupEnv == "" {
		return nil, fmt.Errorf("test metadata daily lookup requires environment")
	}
	lookupDate, err := normalizeDate(date)
	if err != nil {
		return nil, fmt.Errorf("test metadata daily lookup requires valid date (YYYY-MM-DD): %w", err)
	}

	rows, err := s.pool.Query(ctx, `
SELECT environment, date, release, period, test_name, test_suite, current_pass_percentage, current_runs, previous_pass_percentage, previous_runs, net_improvement, ingested_at
FROM cfa_test_metadata_daily
WHERE environment = $1 AND date = $2
`, lookupEnv, lookupDate)
	if err != nil {
		return nil, fmt.Errorf("query test metadata by date: %w", err)
	}
	defer rows.Close()

	out := make([]storecontracts.TestMetadataDailyRecord, 0)
	for rows.Next() {
		var row storecontracts.TestMetadataDailyRecord
		if err := rows.Scan(
			&row.Environment,
			&row.Date,
			&row.Release,
			&row.Period,
			&row.TestName,
			&row.TestSuite,
			&row.CurrentPassPercentage,
			&row.CurrentRuns,
			&row.PreviousPassPercentage,
			&row.PreviousRuns,
			&row.NetImprovement,
			&row.IngestedAt,
		); err != nil {
			return nil, fmt.Errorf("scan test metadata by date row: %w", err)
		}
		normalized := normalizeTestMetadataDailyRecord(row)
		if normalized.Environment != lookupEnv || normalized.Date != lookupDate {
			continue
		}
		if normalized.TestName == "" {
			continue
		}
		out = append(out, normalized)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate test metadata by date rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Period != out[j].Period {
			return out[i].Period < out[j].Period
		}
		if out[i].TestSuite != out[j].TestSuite {
			return out[i].TestSuite < out[j].TestSuite
		}
		return out[i].TestName < out[j].TestName
	})
	return out, nil
}

func (s *Store) upsertCheckpointsImpl(ctx context.Context, rows []storecontracts.CheckpointRecord) error {
	if len(rows) == 0 {
		return nil
	}
	normalizedRows := make([]storecontracts.CheckpointRecord, 0, len(rows))
	now := time.Now().UTC().Format(time.RFC3339)
	for _, row := range rows {
		normalized := normalizeCheckpointRecord(row)
		if normalized.Name == "" {
			return fmt.Errorf("checkpoint record missing name")
		}
		if normalized.UpdatedAt == "" {
			normalized.UpdatedAt = now
		}
		normalizedRows = append(normalizedRows, normalized)
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, row := range normalizedRows {
			_, err := tx.Exec(ctx, `
INSERT INTO cfa_checkpoints (name, value, updated_at)
VALUES ($1, $2, $3)
ON CONFLICT (name)
DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
`, row.Name, row.Value, row.UpdatedAt)
			if err != nil {
				return fmt.Errorf("upsert checkpoint %q: %w", row.Name, err)
			}
		}
		return nil
	})
}

func (s *Store) getCheckpointImpl(ctx context.Context, name string) (storecontracts.CheckpointRecord, bool, error) {
	targetName := strings.TrimSpace(name)
	if targetName == "" {
		return storecontracts.CheckpointRecord{}, false, fmt.Errorf("checkpoint name is required")
	}
	var row storecontracts.CheckpointRecord
	if err := s.pool.QueryRow(ctx, `
SELECT name, value, updated_at
FROM cfa_checkpoints
WHERE name = $1
`, targetName).Scan(&row.Name, &row.Value, &row.UpdatedAt); err != nil {
		if err == pgx.ErrNoRows {
			return storecontracts.CheckpointRecord{}, false, nil
		}
		return storecontracts.CheckpointRecord{}, false, fmt.Errorf("query checkpoint %q: %w", targetName, err)
	}
	return normalizeCheckpointRecord(row), true, nil
}

func (s *Store) appendDeadLettersImpl(ctx context.Context, rows []storecontracts.DeadLetterRecord) error {
	if len(rows) == 0 {
		return nil
	}
	normalizedRows := make([]storecontracts.DeadLetterRecord, 0, len(rows))
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
		normalizedRows = append(normalizedRows, normalized)
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, row := range normalizedRows {
			_, err := tx.Exec(ctx, `
INSERT INTO cfa_dead_letters (controller, key, error, failed_at)
VALUES ($1, $2, $3, $4)
`, row.Controller, row.Key, row.Error, row.FailedAt)
			if err != nil {
				return fmt.Errorf("append dead letter (%s,%s): %w", row.Controller, row.Key, err)
			}
		}
		return nil
	})
}

func (s *Store) listDeadLettersImpl(ctx context.Context, limit int) ([]storecontracts.DeadLetterRecord, error) {
	query := `
SELECT controller, key, error, failed_at
FROM cfa_dead_letters
ORDER BY failed_at DESC, controller ASC, key ASC, error ASC
`
	var (
		rows pgx.Rows
		err  error
	)
	if limit > 0 {
		rows, err = s.pool.Query(ctx, query+` LIMIT $1`, limit)
	} else {
		rows, err = s.pool.Query(ctx, query)
	}
	if err != nil {
		return nil, fmt.Errorf("query dead letters: %w", err)
	}
	defer rows.Close()

	out := make([]storecontracts.DeadLetterRecord, 0)
	for rows.Next() {
		var row storecontracts.DeadLetterRecord
		if err := rows.Scan(&row.Controller, &row.Key, &row.Error, &row.FailedAt); err != nil {
			return nil, fmt.Errorf("scan dead letter row: %w", err)
		}
		out = append(out, normalizeDeadLetterRecord(row))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letter rows: %w", err)
	}
	return out, nil
}
