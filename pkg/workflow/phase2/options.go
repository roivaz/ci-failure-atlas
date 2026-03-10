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
)

func DefaultOptions() *RawOptions {
	return &RawOptions{
		NDJSONOptions: ndjsonoptions.DefaultOptions(),
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	return ndjsonoptions.BindNDJSONOptions(opts.NDJSONOptions, cmd)
}

type RawOptions struct {
	NDJSONOptions *ndjsonoptions.RawOptions
}

type validatedOptions struct {
	*RawOptions
	NDJSONValidated *ndjsonoptions.ValidatedOptions
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	NDJSON *ndjsonoptions.Options
	Store  storecontracts.Store
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	ndjsonValidated, err := o.NDJSONOptions.Validate()
	if err != nil {
		return nil, err
	}
	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:      o,
			NDJSONValidated: ndjsonValidated,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(ctx context.Context) (*Options, error) {
	ndjsonCompleted, err := o.NDJSONValidated.Complete(ctx)
	if err != nil {
		return nil, err
	}
	store, err := ndjson.NewWithOptions(ndjsonCompleted.DataDirectory, ndjson.Options{
		SemanticSubdirectory: ndjsonCompleted.SemanticSubdirectory,
	})
	if err != nil {
		return nil, fmt.Errorf("create NDJSON store: %w", err)
	}
	return &Options{
		completedOptions: &completedOptions{
			NDJSON: ndjsonCompleted,
			Store:  store,
		},
	}, nil
}

func (o *Options) Cleanup() {
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
