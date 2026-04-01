package postgres

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ListWeeks returns distinct materialized weeks currently present in semantic tables.
func ListWeeks(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, fmt.Errorf("postgres pool is required")
	}

	rows, err := pool.Query(ctx, `
SELECT DISTINCT semantic_subdir
FROM (
  SELECT semantic_subdir FROM cfa_sem_global_clusters
  UNION ALL
  SELECT semantic_subdir FROM cfa_sem_review_queue
) semantic_rows
WHERE semantic_subdir IS NOT NULL AND semantic_subdir <> ''
`)
	if err != nil {
		return nil, fmt.Errorf("query semantic weeks: %w", err)
	}
	defer rows.Close()

	out := make([]string, 0)
	seen := map[string]struct{}{}
	for rows.Next() {
		var week string
		if err := rows.Scan(&week); err != nil {
			return nil, fmt.Errorf("scan semantic week: %w", err)
		}
		normalizedWeek, err := NormalizeWeek(week)
		if err != nil {
			return nil, fmt.Errorf("scan semantic week %q: %w", week, err)
		}
		if normalizedWeek == "" {
			continue
		}
		if _, exists := seen[normalizedWeek]; exists {
			continue
		}
		seen[normalizedWeek] = struct{}{}
		out = append(out, normalizedWeek)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate semantic weeks: %w", err)
	}

	sort.Strings(out)
	return out, nil
}
