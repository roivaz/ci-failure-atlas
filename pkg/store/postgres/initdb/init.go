package initdb

import (
	"context"
	"fmt"

	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var ddl string

func Initialize(ctx context.Context, pool *pgxpool.Pool) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if pool == nil {
		return fmt.Errorf("postgres pool is required")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire postgres connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("initialize postgres schema: %w", err)
	}
	return nil
}
