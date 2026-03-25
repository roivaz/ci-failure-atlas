package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ListSemanticSubdirectories returns distinct semantic subdirectory partitions
// currently present in semantic tables.
func ListSemanticSubdirectories(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
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
  UNION ALL
  SELECT semantic_subdir FROM cfa_sem_phase1_workset
  UNION ALL
  SELECT semantic_subdir FROM cfa_sem_test_clusters
) semantic_rows
WHERE semantic_subdir IS NOT NULL AND semantic_subdir <> ''
`)
	if err != nil {
		return nil, fmt.Errorf("query semantic subdirectories: %w", err)
	}
	defer rows.Close()

	out := make([]string, 0)
	seen := map[string]struct{}{}
	for rows.Next() {
		var subdir string
		if err := rows.Scan(&subdir); err != nil {
			return nil, fmt.Errorf("scan semantic subdirectory: %w", err)
		}
		trimmed := strings.TrimSpace(subdir)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate semantic subdirectories: %w", err)
	}

	sort.Strings(out)
	return out, nil
}
