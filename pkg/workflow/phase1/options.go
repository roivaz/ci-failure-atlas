package phase1

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/ndjsonoptions"
	phase1engine "ci-failure-atlas/pkg/semantic/engine/phase1"
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
	store, err := ndjson.New(ndjsonCompleted.DataDirectory)
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

	rawFailures, runs, err := o.loadInputs(ctx)
	if err != nil {
		return err
	}

	workset := phase1engine.BuildWorkset(rawFailures, runs)
	normalized := phase1engine.Normalize(workset)
	assignments := phase1engine.Classify(normalized)
	testClusters, reviewItems, err := phase1engine.Compile(workset, assignments)
	if err != nil {
		return fmt.Errorf("compile phase1 outputs: %w", err)
	}

	if err := o.Store.UpsertPhase1Workset(ctx, workset); err != nil {
		return fmt.Errorf("upsert phase1 workset: %w", err)
	}
	if err := o.Store.UpsertPhase1Normalized(ctx, normalized); err != nil {
		return fmt.Errorf("upsert phase1 normalized rows: %w", err)
	}
	if err := o.Store.UpsertPhase1Assignments(ctx, assignments); err != nil {
		return fmt.Errorf("upsert phase1 assignments: %w", err)
	}
	if err := o.Store.UpsertTestClusters(ctx, testClusters); err != nil {
		return fmt.Errorf("upsert test clusters: %w", err)
	}
	if err := o.Store.UpsertReviewQueue(ctx, reviewItems); err != nil {
		return fmt.Errorf("upsert review queue: %w", err)
	}

	logger.Info(
		"Completed workflow phase1 semantic pipeline.",
		"raw_rows", len(rawFailures),
		"runs", len(runs),
		"workset_rows", len(workset),
		"normalized_rows", len(normalized),
		"assignments", len(assignments),
		"test_clusters", len(testClusters),
		"review_items", len(reviewItems),
	)
	return nil
}

func (o *Options) loadInputs(ctx context.Context) ([]storecontracts.RawFailureRecord, []storecontracts.RunRecord, error) {
	keys, err := o.Store.ListRawFailureRunKeys(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list raw failure run keys: %w", err)
	}

	rawFailures := make([]storecontracts.RawFailureRecord, 0)
	runsByKey := map[string]storecontracts.RunRecord{}

	for _, key := range keys {
		environment, runURL, err := splitEnvironmentRunKey(key)
		if err != nil {
			return nil, nil, err
		}

		rows, err := o.Store.ListRawFailuresByRun(ctx, environment, runURL)
		if err != nil {
			return nil, nil, fmt.Errorf("list raw failures by run for key %q: %w", key, err)
		}
		rawFailures = append(rawFailures, rows...)

		run, found, err := o.Store.GetRun(ctx, environment, runURL)
		if err != nil {
			return nil, nil, fmt.Errorf("get run for key %q: %w", key, err)
		}
		if !found {
			continue
		}
		runsByKey[strings.ToLower(strings.TrimSpace(environment))+"|"+strings.TrimSpace(runURL)] = run
	}

	runs := make([]storecontracts.RunRecord, 0, len(runsByKey))
	for _, run := range runsByKey {
		runs = append(runs, run)
	}
	return rawFailures, runs, nil
}

func splitEnvironmentRunKey(key string) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(key), "|", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid key %q: expected environment|run_url", key)
	}
	environment := strings.ToLower(strings.TrimSpace(parts[0]))
	runURL := strings.TrimSpace(parts[1])
	if environment == "" {
		return "", "", fmt.Errorf("invalid key %q: missing environment", key)
	}
	if runURL == "" {
		return "", "", fmt.Errorf("invalid key %q: missing run_url", key)
	}
	return environment, runURL, nil
}
