package phase2

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/ndjsonoptions"
	phase2engine "ci-failure-atlas/pkg/semantic/engine/phase2"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

func DefaultOptions() *RawOptions {
	return &RawOptions{
		NDJSONOptions:   ndjsonoptions.DefaultOptions(),
		PostgresOptions: postgresoptions.DefaultOptions(),
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	if opts.NDJSONOptions == nil {
		opts.NDJSONOptions = ndjsonoptions.DefaultOptions()
	}
	if opts.PostgresOptions == nil {
		opts.PostgresOptions = postgresoptions.DefaultOptions()
	}
	if err := ndjsonoptions.BindNDJSONOptions(opts.NDJSONOptions, cmd); err != nil {
		return err
	}
	return postgresoptions.BindOptions(opts.PostgresOptions, cmd)
}

type RawOptions struct {
	NDJSONOptions   *ndjsonoptions.RawOptions
	PostgresOptions *postgresoptions.RawOptions
}

type validatedOptions struct {
	*RawOptions
	NDJSONValidated   *ndjsonoptions.ValidatedOptions
	PostgresValidated *postgresoptions.ValidatedOptions
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	NDJSON   *ndjsonoptions.Options
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
	postgresValidated, err := o.PostgresOptions.Validate()
	if err != nil {
		return nil, err
	}

	var ndjsonValidated *ndjsonoptions.ValidatedOptions
	if !postgresValidated.Enabled {
		if o.NDJSONOptions == nil {
			o.NDJSONOptions = ndjsonoptions.DefaultOptions()
		}
		ndjsonValidated, err = o.NDJSONOptions.Validate()
		if err != nil {
			return nil, err
		}
	}
	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:        o,
			NDJSONValidated:   ndjsonValidated,
			PostgresValidated: postgresValidated,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(ctx context.Context) (*Options, error) {
	var (
		ndjsonCompleted   *ndjsonoptions.Options
		postgresCompleted *postgresoptions.Options
		store             storecontracts.Store
		err               error
	)
	if o.PostgresValidated != nil && o.PostgresValidated.Enabled {
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
	} else {
		ndjsonCompleted, err = o.NDJSONValidated.Complete(ctx)
		if err != nil {
			return nil, err
		}
		store, err = ndjson.NewWithOptions(ndjsonCompleted.DataDirectory, ndjson.Options{
			SemanticSubdirectory: ndjsonCompleted.SemanticSubdirectory,
		})
		if err != nil {
			return nil, fmt.Errorf("create NDJSON store: %w", err)
		}
	}
	return &Options{
		completedOptions: &completedOptions{
			NDJSON:   ndjsonCompleted,
			Postgres: postgresCompleted,
			Store:    store,
		},
	}, nil
}

func (o *Options) Cleanup() {
	if o.Postgres != nil {
		o.Postgres.Cleanup()
		return
	}
	if o.Store != nil {
		_ = o.Store.Close()
	}
}

func (o *Options) Run(ctx context.Context) error {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer o.Cleanup()

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
