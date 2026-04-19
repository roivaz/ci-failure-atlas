package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"

	"github.com/jackc/pgx/v5"
)

func (s *Store) replaceMaterializedWeekImpl(ctx context.Context, week storecontracts.MaterializedWeek) error {
	globalRows, err := normalizeGlobalClusterRows(week.FailurePatterns)
	if err != nil {
		return err
	}
	reviewRows, err := normalizeReviewQueueRows(week.ReviewQueue)
	if err != nil {
		return err
	}
	if _, err := semanticcontracts.InferWeekSchemaVersion(globalRows, reviewRows); err != nil {
		return fmt.Errorf("validate semantic schema version: %w", err)
	}
	currentWeek := weekScope(s.week)
	if currentWeek == "" {
		return fmt.Errorf("week is required to replace materialized week")
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
DELETE FROM cfa_sem_review_queue
WHERE semantic_subdir = $1
`, currentWeek); err != nil {
			return fmt.Errorf("delete review queue for week %q: %w", currentWeek, err)
		}
		if _, err := tx.Exec(ctx, `
DELETE FROM cfa_sem_global_clusters
WHERE semantic_subdir = $1
`, currentWeek); err != nil {
			return fmt.Errorf("delete failure patterns for week %q: %w", currentWeek, err)
		}
		if err := insertFailurePatternsTx(ctx, tx, currentWeek, globalRows); err != nil {
			return err
		}
		if err := insertReviewQueueTx(ctx, tx, currentWeek, reviewRows); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) listFailurePatternsImpl(ctx context.Context) ([]semanticcontracts.FailurePatternRecord, error) {
	rows, err := s.pool.Query(ctx, `
SELECT payload
FROM cfa_sem_global_clusters
WHERE semantic_subdir = $1
ORDER BY support_count DESC, contributing_tests_count DESC, environment, phase2_cluster_id
`, weekScope(s.week))
	if err != nil {
		return nil, fmt.Errorf("query failure patterns: %w", err)
	}
	defer rows.Close()

	out := make([]semanticcontracts.FailurePatternRecord, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan failure-pattern payload: %w", err)
		}
		var row semanticcontracts.FailurePatternRecord
		if err := json.Unmarshal(payload, &row); err != nil {
			return nil, fmt.Errorf("decode failure-pattern payload: %w", err)
		}
		normalized := normalizeFailurePatternRecord(row)
		if globalClusterKey(normalized) == "" {
			continue
		}
		out = append(out, normalized)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate failure patterns: %w", err)
	}
	return out, nil
}

func normalizeGlobalClusterRows(rows []semanticcontracts.FailurePatternRecord) ([]semanticcontracts.FailurePatternRecord, error) {
	normalizedRows := make([]semanticcontracts.FailurePatternRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizeFailurePatternRecord(row)
		if globalClusterKey(normalized) == "" {
			return nil, fmt.Errorf("failure-pattern record missing environment and/or phase2_cluster_id")
		}
		normalizedRows = append(normalizedRows, normalized)
	}
	return normalizedRows, nil
}

func normalizeReviewQueueRows(rows []semanticcontracts.ReviewItemRecord) ([]semanticcontracts.ReviewItemRecord, error) {
	normalizedRows := make([]semanticcontracts.ReviewItemRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizeReviewItemRecord(row)
		if reviewItemKey(normalized) == "" {
			return nil, fmt.Errorf("review item record missing environment and/or review_item_id")
		}
		normalizedRows = append(normalizedRows, normalized)
	}
	return normalizedRows, nil
}

func insertFailurePatternsTx(ctx context.Context, tx pgx.Tx, currentWeek string, rows []semanticcontracts.FailurePatternRecord) error {
	for _, row := range rows {
		payload, err := marshalPayload(row)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
INSERT INTO cfa_sem_global_clusters (
  semantic_subdir, environment, phase2_cluster_id, support_count, contributing_tests_count, payload
) VALUES (
  $1, $2, $3, $4, $5, $6
)
ON CONFLICT (semantic_subdir, environment, phase2_cluster_id)
DO UPDATE SET
  support_count = EXCLUDED.support_count,
  contributing_tests_count = EXCLUDED.contributing_tests_count,
  payload = EXCLUDED.payload
`, currentWeek, row.Environment, row.Phase2ClusterID, row.SupportCount, row.ContributingTestsCount, payload)
		if err != nil {
			return fmt.Errorf("upsert failure pattern (%s,%s): %w", row.Environment, row.Phase2ClusterID, err)
		}
	}
	return nil
}

func insertReviewQueueTx(ctx context.Context, tx pgx.Tx, currentWeek string, rows []semanticcontracts.ReviewItemRecord) error {
	for _, row := range rows {
		payload, err := marshalPayload(row)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
INSERT INTO cfa_sem_review_queue (
  semantic_subdir, environment, review_item_id, phase, reason, payload
) VALUES (
  $1, $2, $3, $4, $5, $6
)
ON CONFLICT (semantic_subdir, environment, review_item_id)
DO UPDATE SET
  phase = EXCLUDED.phase,
  reason = EXCLUDED.reason,
  payload = EXCLUDED.payload
`, currentWeek, row.Environment, row.ReviewItemID, row.Phase, row.Reason, payload)
		if err != nil {
			return fmt.Errorf("upsert review queue item (%s,%s): %w", row.Environment, row.ReviewItemID, err)
		}
	}
	return nil
}

func (s *Store) listReviewQueueImpl(ctx context.Context) ([]semanticcontracts.ReviewItemRecord, error) {
	rows, err := s.pool.Query(ctx, `
SELECT payload
FROM cfa_sem_review_queue
WHERE semantic_subdir = $1
ORDER BY environment, phase, reason, review_item_id
`, weekScope(s.week))
	if err != nil {
		return nil, fmt.Errorf("query review queue: %w", err)
	}
	defer rows.Close()

	out := make([]semanticcontracts.ReviewItemRecord, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan review queue payload: %w", err)
		}
		var row semanticcontracts.ReviewItemRecord
		if err := json.Unmarshal(payload, &row); err != nil {
			return nil, fmt.Errorf("decode review queue payload: %w", err)
		}
		normalized := normalizeReviewItemRecord(row)
		if reviewItemKey(normalized) == "" {
			continue
		}
		out = append(out, normalized)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate review queue rows: %w", err)
	}
	return out, nil
}

func (s *Store) getSemanticWeekSummaryImpl(ctx context.Context) (storecontracts.SemanticWeekSummary, error) {
	currentWeek := weekScope(s.week)
	summary := storecontracts.SemanticWeekSummary{
		TestClusterCountsByEnv:    map[string]int{},
		ReviewQueueCountsByEnv:    map[string]int{},
		FailurePatternCountsByEnv: map[string]int{},
		OccurrenceTotalsByEnv:     map[string]int{},
		AvailableEnvironments:     []string{},
	}

	globalRows, err := s.pool.Query(ctx, `
SELECT environment, COUNT(*), COALESCE(SUM(support_count), 0)
FROM cfa_sem_global_clusters
WHERE semantic_subdir = $1
GROUP BY environment
ORDER BY environment
`, currentWeek)
	if err != nil {
		return storecontracts.SemanticWeekSummary{}, fmt.Errorf("query semantic global summary: %w", err)
	}
	for globalRows.Next() {
		var environment string
		var clusterCount, supportTotal int64
		if err := globalRows.Scan(&environment, &clusterCount, &supportTotal); err != nil {
			globalRows.Close()
			return storecontracts.SemanticWeekSummary{}, fmt.Errorf("scan semantic global summary row: %w", err)
		}
		normalizedEnvironment := normalizeSemanticEnvironment(environment)
		summary.FailurePatternCountsByEnv[normalizedEnvironment] = int(clusterCount)
		summary.OccurrenceTotalsByEnv[normalizedEnvironment] = int(supportTotal)
		summary.TestClusterCountsByEnv[normalizedEnvironment] = 0
	}
	if err := globalRows.Err(); err != nil {
		globalRows.Close()
		return storecontracts.SemanticWeekSummary{}, fmt.Errorf("iterate semantic global summary rows: %w", err)
	}
	globalRows.Close()

	testClusterRows, err := s.pool.Query(ctx, `
SELECT environment, COUNT(DISTINCT phase1_cluster_id)
FROM (
  SELECT
    environment,
    BTRIM(phase1_ids.value) AS phase1_cluster_id
  FROM cfa_sem_global_clusters
  CROSS JOIN LATERAL jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(payload -> 'member_phase1_cluster_ids') = 'array' THEN payload -> 'member_phase1_cluster_ids'
      ELSE '[]'::jsonb
    END
  ) AS phase1_ids(value)
  WHERE semantic_subdir = $1
) expanded
WHERE phase1_cluster_id <> ''
GROUP BY environment
ORDER BY environment
`, currentWeek)
	if err != nil {
		return storecontracts.SemanticWeekSummary{}, fmt.Errorf("query semantic test cluster summary: %w", err)
	}
	for testClusterRows.Next() {
		var environment string
		var testClusterCount int64
		if err := testClusterRows.Scan(&environment, &testClusterCount); err != nil {
			testClusterRows.Close()
			return storecontracts.SemanticWeekSummary{}, fmt.Errorf("scan semantic test cluster summary row: %w", err)
		}
		normalizedEnvironment := normalizeSemanticEnvironment(environment)
		summary.TestClusterCountsByEnv[normalizedEnvironment] = int(testClusterCount)
	}
	if err := testClusterRows.Err(); err != nil {
		testClusterRows.Close()
		return storecontracts.SemanticWeekSummary{}, fmt.Errorf("iterate semantic test cluster summary rows: %w", err)
	}
	testClusterRows.Close()

	reviewRows, err := s.pool.Query(ctx, `
SELECT environment, COUNT(*)
FROM cfa_sem_review_queue
WHERE semantic_subdir = $1
GROUP BY environment
ORDER BY environment
`, currentWeek)
	if err != nil {
		return storecontracts.SemanticWeekSummary{}, fmt.Errorf("query semantic review summary: %w", err)
	}
	for reviewRows.Next() {
		var environment string
		var reviewCount int64
		if err := reviewRows.Scan(&environment, &reviewCount); err != nil {
			reviewRows.Close()
			return storecontracts.SemanticWeekSummary{}, fmt.Errorf("scan semantic review summary row: %w", err)
		}
		normalizedEnvironment := normalizeSemanticEnvironment(environment)
		summary.ReviewQueueCountsByEnv[normalizedEnvironment] = int(reviewCount)
	}
	if err := reviewRows.Err(); err != nil {
		reviewRows.Close()
		return storecontracts.SemanticWeekSummary{}, fmt.Errorf("iterate semantic review summary rows: %w", err)
	}
	reviewRows.Close()

	availableEnvironmentSet := map[string]struct{}{}
	for environment := range summary.FailurePatternCountsByEnv {
		availableEnvironmentSet[environment] = struct{}{}
	}
	for environment := range summary.ReviewQueueCountsByEnv {
		availableEnvironmentSet[environment] = struct{}{}
	}
	summary.AvailableEnvironments = make([]string, 0, len(availableEnvironmentSet))
	for environment := range availableEnvironmentSet {
		summary.AvailableEnvironments = append(summary.AvailableEnvironments, environment)
	}
	sort.Strings(summary.AvailableEnvironments)

	return summary, nil
}

func (s *Store) upsertPhase3IssuesImpl(ctx context.Context, rows []semanticcontracts.Phase3IssueRecord) error {
	if len(rows) == 0 {
		return nil
	}
	normalizedRows := make([]semanticcontracts.Phase3IssueRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase3IssueRecord(row)
		if phase3IssueKey(normalized) == "" {
			return fmt.Errorf("phase3 issue record missing issue_id")
		}
		normalizedRows = append(normalizedRows, normalized)
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, row := range normalizedRows {
			payload, err := marshalPayload(row)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `
INSERT INTO cfa_phase3_issues (issue_id, payload)
VALUES ($1, $2)
ON CONFLICT (issue_id)
DO UPDATE SET payload = EXCLUDED.payload
`, row.IssueID, payload)
			if err != nil {
				return fmt.Errorf("upsert phase3 issue %q: %w", row.IssueID, err)
			}
		}
		return nil
	})
}

func (s *Store) listPhase3IssuesImpl(ctx context.Context) ([]semanticcontracts.Phase3IssueRecord, error) {
	rows, err := s.pool.Query(ctx, `SELECT payload FROM cfa_phase3_issues`)
	if err != nil {
		return nil, fmt.Errorf("query phase3 issues: %w", err)
	}
	defer rows.Close()

	out := make([]semanticcontracts.Phase3IssueRecord, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan phase3 issue payload: %w", err)
		}
		var row semanticcontracts.Phase3IssueRecord
		if err := json.Unmarshal(payload, &row); err != nil {
			return nil, fmt.Errorf("decode phase3 issue payload: %w", err)
		}
		normalized := normalizePhase3IssueRecord(row)
		if phase3IssueKey(normalized) == "" {
			continue
		}
		out = append(out, normalized)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate phase3 issues: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].IssueID < out[j].IssueID
	})
	return out, nil
}

func (s *Store) upsertPhase3LinksImpl(ctx context.Context, rows []semanticcontracts.Phase3LinkRecord) error {
	if len(rows) == 0 {
		return nil
	}
	normalizedRows := make([]semanticcontracts.Phase3LinkRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase3LinkRecord(row)
		if phase3LinkKey(normalized) == "" {
			return fmt.Errorf("phase3 link record missing environment and/or run_url and/or row_id")
		}
		if strings.TrimSpace(normalized.IssueID) == "" {
			return fmt.Errorf("phase3 link record missing issue_id")
		}
		normalizedRows = append(normalizedRows, normalized)
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, row := range normalizedRows {
			payload, err := marshalPayload(row)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `
INSERT INTO cfa_phase3_links (
  environment, run_url, row_id, issue_id, updated_at, payload
) VALUES (
  $1, $2, $3, $4, $5, $6
)
ON CONFLICT (environment, run_url, row_id)
DO UPDATE SET
  issue_id = EXCLUDED.issue_id,
  updated_at = EXCLUDED.updated_at,
  payload = EXCLUDED.payload
`, row.Environment, row.RunURL, row.RowID, row.IssueID, row.UpdatedAt, payload)
			if err != nil {
				return fmt.Errorf("upsert phase3 link (%s,%s,%s): %w", row.Environment, row.RunURL, row.RowID, err)
			}
		}
		return nil
	})
}

func (s *Store) deletePhase3LinksImpl(ctx context.Context, rows []semanticcontracts.Phase3LinkRecord) error {
	if len(rows) == 0 {
		return nil
	}
	normalizedRows := make([]semanticcontracts.Phase3LinkRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase3LinkRecord(row)
		if phase3LinkKey(normalized) == "" {
			return fmt.Errorf("phase3 link deletion record missing environment and/or run_url and/or row_id")
		}
		normalizedRows = append(normalizedRows, normalized)
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, row := range normalizedRows {
			_, err := tx.Exec(ctx, `
DELETE FROM cfa_phase3_links
WHERE environment = $1 AND run_url = $2 AND row_id = $3
`, row.Environment, row.RunURL, row.RowID)
			if err != nil {
				return fmt.Errorf("delete phase3 link (%s,%s,%s): %w", row.Environment, row.RunURL, row.RowID, err)
			}
		}
		return nil
	})
}

func (s *Store) listPhase3LinksImpl(ctx context.Context) ([]semanticcontracts.Phase3LinkRecord, error) {
	rows, err := s.pool.Query(ctx, `SELECT payload FROM cfa_phase3_links`)
	if err != nil {
		return nil, fmt.Errorf("query phase3 links: %w", err)
	}
	defer rows.Close()

	out := make([]semanticcontracts.Phase3LinkRecord, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan phase3 link payload: %w", err)
		}
		var row semanticcontracts.Phase3LinkRecord
		if err := json.Unmarshal(payload, &row); err != nil {
			return nil, fmt.Errorf("decode phase3 link payload: %w", err)
		}
		normalized := normalizePhase3LinkRecord(row)
		if phase3LinkKey(normalized) == "" || strings.TrimSpace(normalized.IssueID) == "" {
			continue
		}
		out = append(out, normalized)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate phase3 links: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Environment != out[j].Environment {
			return out[i].Environment < out[j].Environment
		}
		if out[i].RunURL != out[j].RunURL {
			return out[i].RunURL < out[j].RunURL
		}
		if out[i].RowID != out[j].RowID {
			return out[i].RowID < out[j].RowID
		}
		return out[i].IssueID < out[j].IssueID
	})
	return out, nil
}

func (s *Store) appendPhase3EventsImpl(ctx context.Context, rows []semanticcontracts.Phase3EventRecord) error {
	if len(rows) == 0 {
		return nil
	}
	normalizedRows := make([]semanticcontracts.Phase3EventRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizePhase3EventRecord(row)
		if phase3EventKey(normalized) == "" {
			return fmt.Errorf("phase3 event record missing event_id")
		}
		normalizedRows = append(normalizedRows, normalized)
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, row := range normalizedRows {
			payload, err := marshalPayload(row)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `
INSERT INTO cfa_phase3_events (event_id, at, payload)
VALUES ($1, $2, $3)
ON CONFLICT (event_id)
DO UPDATE SET at = EXCLUDED.at, payload = EXCLUDED.payload
`, row.EventID, row.At, payload)
			if err != nil {
				return fmt.Errorf("append phase3 event %q: %w", row.EventID, err)
			}
		}
		return nil
	})
}

func (s *Store) listPhase3EventsImpl(ctx context.Context, limit int) ([]semanticcontracts.Phase3EventRecord, error) {
	query := `SELECT payload FROM cfa_phase3_events`
	var (
		rows pgx.Rows
		err  error
	)
	if limit > 0 {
		rows, err = s.pool.Query(ctx, query+` LIMIT $1`, limit*2)
	} else {
		rows, err = s.pool.Query(ctx, query)
	}
	if err != nil {
		return nil, fmt.Errorf("query phase3 events: %w", err)
	}
	defer rows.Close()

	out := make([]semanticcontracts.Phase3EventRecord, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan phase3 event payload: %w", err)
		}
		var row semanticcontracts.Phase3EventRecord
		if err := json.Unmarshal(payload, &row); err != nil {
			return nil, fmt.Errorf("decode phase3 event payload: %w", err)
		}
		normalized := normalizePhase3EventRecord(row)
		if phase3EventKey(normalized) == "" {
			continue
		}
		out = append(out, normalized)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate phase3 events: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].At != out[j].At {
			return out[i].At > out[j].At
		}
		return out[i].EventID > out[j].EventID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
