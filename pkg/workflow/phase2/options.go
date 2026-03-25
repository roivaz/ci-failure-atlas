package phase2

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	phase2engine "ci-failure-atlas/pkg/semantic/engine/phase2"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

func DefaultOptions() *RawOptions {
	postgresRaw := postgresoptions.DefaultOptions()
	postgresRaw.Enabled = true
	postgresRaw.Embedded = true
	postgresRaw.Initialize = true
	return &RawOptions{
		PostgresOptions: postgresRaw,
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	if opts.PostgresOptions == nil {
		opts.PostgresOptions = postgresoptions.DefaultOptions()
	}
	opts.PostgresOptions.Enabled = true
	return postgresoptions.BindOptions(opts.PostgresOptions, cmd)
}

type RawOptions struct {
	PostgresOptions *postgresoptions.RawOptions
}

type validatedOptions struct {
	*RawOptions
	PostgresValidated *postgresoptions.ValidatedOptions
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	Postgres *postgresoptions.Options
	Store    storecontracts.Store
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	if o.PostgresOptions == nil {
		o.PostgresOptions = postgresoptions.DefaultOptions()
	}
	o.PostgresOptions.Enabled = true
	postgresValidated, err := o.PostgresOptions.Validate()
	if err != nil {
		return nil, err
	}
	if !postgresValidated.Enabled {
		return nil, fmt.Errorf("postgres storage is required")
	}
	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:        o,
			PostgresValidated: postgresValidated,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(ctx context.Context) (*Options, error) {
	var (
		postgresCompleted *postgresoptions.Options
		store             storecontracts.Store
		err               error
	)
	postgresCompleted, err = o.PostgresValidated.Complete(ctx)
	if err != nil {
		return nil, err
	}
	store, err = postgresstore.New(postgresCompleted.Connection, postgresstore.Options{
		SemanticSubdirectory: postgresCompleted.SemanticSubdirectory,
	})
	if err != nil {
		postgresCompleted.Cleanup()
		return nil, fmt.Errorf("create postgres store: %w", err)
	}
	return &Options{
		completedOptions: &completedOptions{
			Postgres: postgresCompleted,
			Store:    store,
		},
	}, nil
}

func (o *Options) Cleanup() {
	if o.Store != nil {
		_ = o.Store.Close()
	}
	if o.Postgres != nil {
		o.Postgres.Cleanup()
	}
}

func (o *Options) Run(ctx context.Context) error {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}

	testClusters, err := o.Store.ListTestClusters(ctx)
	if err != nil {
		return fmt.Errorf("list test clusters: %w", err)
	}
	reviewItems, err := o.Store.ListReviewQueue(ctx)
	if err != nil {
		return fmt.Errorf("list review queue: %w", err)
	}

	globalClusters, mergedReviewQueue, err := phase2engine.Merge(testClusters, reviewItems)
	if err != nil {
		return fmt.Errorf("merge phase2 clusters: %w", err)
	}

	if err := o.Store.UpsertGlobalClusters(ctx, globalClusters); err != nil {
		return fmt.Errorf("upsert global clusters: %w", err)
	}
	if err := o.Store.UpsertReviewQueue(ctx, mergedReviewQueue); err != nil {
		return fmt.Errorf("upsert merged review queue: %w", err)
	}

	logger.Info(
		"Completed workflow phase2 global merge.",
		"phase1_clusters", len(testClusters),
		"phase1_review_items", len(reviewItems),
		"phase2_clusters", len(globalClusters),
		"merged_review_items", len(mergedReviewQueue),
	)
	return nil
}
