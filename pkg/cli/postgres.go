package cli

import (
	"context"
	"fmt"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

func completePostgresForCommand(ctx context.Context, raw *postgresoptions.RawOptions) (*postgresoptions.Options, error) {
	if raw == nil {
		raw = postgresoptions.DefaultCLIOptions()
	}
	validated, err := raw.Validate()
	if err != nil {
		return nil, err
	}
	if !validated.Enabled {
		return nil, fmt.Errorf("--storage.postgres.enabled must be true")
	}
	completed, err := validated.Complete(ctx)
	if err != nil {
		return nil, err
	}
	return completed, nil
}

func openPostgresStoreForCommand(ctx context.Context, raw *postgresoptions.RawOptions, opts postgresstore.Options) (*postgresoptions.Options, storecontracts.Store, error) {
	postgresCompleted, err := completePostgresForCommand(ctx, raw)
	if err != nil {
		return nil, nil, err
	}
	store, err := postgresstore.New(postgresCompleted.Connection, opts)
	if err != nil {
		postgresCompleted.Cleanup()
		return nil, nil, fmt.Errorf("create postgres store: %w", err)
	}
	return postgresCompleted, store, nil
}
