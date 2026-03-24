package migrations

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	migrationStateTable = "cfa_meta_migrations"
	advisoryLockKey     = int64(810260915632847)
)

var ErrDirtyMigrations = errors.New("postgres migrations: dirty migration state detected")

//go:embed *.up.sql
var migrationFiles embed.FS

type migration struct {
	name string
	sql  string
}

func Run(ctx context.Context, pool *pgxpool.Pool) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if pool == nil {
		return fmt.Errorf("postgres pool is required")
	}

	migrations, err := loadUpMigrations()
	if err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire postgres connection for migrations: %w", err)
	}
	defer conn.Release()

	if err := ensureMigrationStateTable(ctx, conn); err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("acquire postgres advisory lock for migrations: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", advisoryLockKey)
	}()

	for _, m := range migrations {
		if _, err := applyMigration(ctx, conn, m); err != nil {
			return fmt.Errorf("apply migration %q: %w", m.name, err)
		}
	}
	return nil
}

func loadUpMigrations() ([]migration, error) {
	matches, err := fs.Glob(migrationFiles, "*.up.sql")
	if err != nil {
		return nil, fmt.Errorf("glob postgres up migrations: %w", err)
	}
	sort.Strings(matches)

	out := make([]migration, 0, len(matches))
	for _, name := range matches {
		content, err := migrationFiles.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read migration file %q: %w", name, err)
		}
		migrationName := strings.TrimSuffix(filepath.Base(name), ".up.sql")
		if migrationName == "" {
			return nil, fmt.Errorf("invalid migration name for file %q", name)
		}
		out = append(out, migration{
			name: migrationName,
			sql:  string(content),
		})
	}
	return out, nil
}

func applyMigration(ctx context.Context, conn *pgxpool.Conn, m migration) (bool, error) {
	alreadyApplied := false
	err := pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		dirtyMigrations, err := listDirtyMigrations(ctx, tx)
		if err != nil {
			return err
		}
		if len(dirtyMigrations) > 0 {
			return fmt.Errorf("%w: %s", ErrDirtyMigrations, strings.Join(dirtyMigrations, ", "))
		}

		dirty, exists, err := migrationState(ctx, tx, m.name)
		if err != nil {
			return err
		}
		if exists && !dirty {
			alreadyApplied = true
			return nil
		}

		if err := markMigrationState(ctx, tx, m.name, true); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("execute migration SQL: %w", err)
		}
		if err := markMigrationState(ctx, tx, m.name, false); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return !alreadyApplied, nil
}

func ensureMigrationStateTable(ctx context.Context, conn *pgxpool.Conn) error {
	statement := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  name TEXT PRIMARY KEY,
  dirty BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`, migrationStateTable)
	if _, err := conn.Exec(ctx, statement); err != nil {
		return fmt.Errorf("ensure migration state table: %w", err)
	}
	return nil
}

func listDirtyMigrations(ctx context.Context, tx pgx.Tx) ([]string, error) {
	query := fmt.Sprintf("SELECT name FROM %s WHERE dirty = TRUE ORDER BY name ASC", migrationStateTable)
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query dirty migrations: %w", err)
	}
	defer rows.Close()

	out := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan dirty migration row: %w", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dirty migration rows: %w", err)
	}
	return out, nil
}

func migrationState(ctx context.Context, tx pgx.Tx, name string) (bool, bool, error) {
	query := fmt.Sprintf("SELECT dirty FROM %s WHERE name = $1", migrationStateTable)
	var dirty bool
	if err := tx.QueryRow(ctx, query, name).Scan(&dirty); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("query migration state: %w", err)
	}
	return dirty, true, nil
}

func markMigrationState(ctx context.Context, tx pgx.Tx, name string, dirty bool) error {
	query := fmt.Sprintf(`
INSERT INTO %s (name, dirty, updated_at)
VALUES ($1, $2, NOW())
ON CONFLICT (name)
DO UPDATE SET dirty = EXCLUDED.dirty, updated_at = EXCLUDED.updated_at
`, migrationStateTable)
	if _, err := tx.Exec(ctx, query, name, dirty); err != nil {
		return fmt.Errorf("upsert migration state: %w", err)
	}
	return nil
}
