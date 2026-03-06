package run

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/controllers"
	"ci-failure-atlas/pkg/ndjsonoptions"
	"ci-failure-atlas/pkg/sourceoptions"
	"ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func DefaultOptions() *RawOptions {
	return &RawOptions{
		SourceOptions: sourceoptions.DefaultOptions(),
		NDJSONOptions: ndjsonoptions.DefaultOptions(),

		SourceSippyRunsControllerThreads:    1,
		SourceProwFailuresControllerThreads: 1,
		FactsRawFailuresControllerThreads:   1,
		MetricsRollupDailyControllerThreads: 1,
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	if err := sourceoptions.BindSourceOptions(opts.SourceOptions, cmd); err != nil {
		return err
	}
	if err := ndjsonoptions.BindNDJSONOptions(opts.NDJSONOptions, cmd); err != nil {
		return err
	}

	cmd.Flags().IntVar(&opts.SourceSippyRunsControllerThreads, "controllers.source.sippy.runs.threads", opts.SourceSippyRunsControllerThreads, "Number of threads for controller source.sippy.runs.")
	cmd.Flags().IntVar(&opts.SourceProwFailuresControllerThreads, "controllers.source.prow.failures.threads", opts.SourceProwFailuresControllerThreads, "Number of threads for controller source.prow.failures.")
	cmd.Flags().IntVar(&opts.FactsRawFailuresControllerThreads, "controllers.facts.raw-failures.threads", opts.FactsRawFailuresControllerThreads, "Number of threads for controller facts.raw-failures.")
	cmd.Flags().IntVar(&opts.MetricsRollupDailyControllerThreads, "controllers.metrics.rollup.daily.threads", opts.MetricsRollupDailyControllerThreads, "Number of threads for controller metrics.rollup.daily.")
	return nil
}

type RawOptions struct {
	SourceOptions *sourceoptions.RawOptions
	NDJSONOptions *ndjsonoptions.RawOptions

	SourceSippyRunsControllerThreads    int
	SourceProwFailuresControllerThreads int
	FactsRawFailuresControllerThreads   int
	MetricsRollupDailyControllerThreads int
}

type validatedOptions struct {
	*RawOptions
	SourceValidated *sourceoptions.ValidatedOptions
	NDJSONValidated *ndjsonoptions.ValidatedOptions

	SourceSippyRunsControllerThreads    int
	SourceProwFailuresControllerThreads int
	FactsRawFailuresControllerThreads   int
	MetricsRollupDailyControllerThreads int
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	Source *sourceoptions.Options
	NDJSON *ndjsonoptions.Options

	Store contracts.Store

	SourceSippyRunsControllerThreads    int
	SourceProwFailuresControllerThreads int
	FactsRawFailuresControllerThreads   int
	MetricsRollupDailyControllerThreads int
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	sourceValidated, err := o.SourceOptions.Validate()
	if err != nil {
		return nil, err
	}
	ndjsonValidated, err := o.NDJSONOptions.Validate()
	if err != nil {
		return nil, err
	}

	if o.SourceSippyRunsControllerThreads <= 0 {
		o.SourceSippyRunsControllerThreads = 1
	}
	if o.SourceProwFailuresControllerThreads <= 0 {
		o.SourceProwFailuresControllerThreads = 1
	}
	if o.FactsRawFailuresControllerThreads <= 0 {
		o.FactsRawFailuresControllerThreads = 1
	}
	if o.MetricsRollupDailyControllerThreads <= 0 {
		o.MetricsRollupDailyControllerThreads = 1
	}

	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:                          o,
			SourceValidated:                     sourceValidated,
			NDJSONValidated:                     ndjsonValidated,
			SourceSippyRunsControllerThreads:    o.SourceSippyRunsControllerThreads,
			SourceProwFailuresControllerThreads: o.SourceProwFailuresControllerThreads,
			FactsRawFailuresControllerThreads:   o.FactsRawFailuresControllerThreads,
			MetricsRollupDailyControllerThreads: o.MetricsRollupDailyControllerThreads,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(ctx context.Context) (*Options, error) {
	sourceCompleted, err := o.SourceValidated.Complete(ctx)
	if err != nil {
		return nil, err
	}
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
			Source:                              sourceCompleted,
			NDJSON:                              ndjsonCompleted,
			Store:                               store,
			SourceSippyRunsControllerThreads:    o.SourceSippyRunsControllerThreads,
			SourceProwFailuresControllerThreads: o.SourceProwFailuresControllerThreads,
			FactsRawFailuresControllerThreads:   o.FactsRawFailuresControllerThreads,
			MetricsRollupDailyControllerThreads: o.MetricsRollupDailyControllerThreads,
		},
	}, nil
}

func (opts *Options) Cleanup() {
	if opts.Store != nil {
		_ = opts.Store.Close()
	}
}

func (opts *Options) Run(ctx context.Context) error {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer opts.Cleanup()

	logger.Info("Starting controllers.")

	deps := controllers.Dependencies{
		Store:  opts.Store,
		Source: opts.Source,
	}
	sourceSippyRunsController, err := controllers.NewSourceSippyRuns(logger, deps)
	if err != nil {
		return err
	}
	sourceProwFailuresController, err := controllers.NewSourceProwFailures(logger, deps)
	if err != nil {
		return err
	}
	factsRawFailuresController, err := controllers.NewFactsRawFailures(logger, deps)
	if err != nil {
		return err
	}
	metricsRollupDailyController, err := controllers.NewMetricsRollupDaily(logger, deps)
	if err != nil {
		return err
	}

	controllersToRun := []struct {
		controller controllers.Controller
		threads    int
	}{
		{
			controller: sourceSippyRunsController,
			threads:    opts.SourceSippyRunsControllerThreads,
		},
		{
			controller: sourceProwFailuresController,
			threads:    opts.SourceProwFailuresControllerThreads,
		},
		{
			controller: factsRawFailuresController,
			threads:    opts.FactsRawFailuresControllerThreads,
		},
		{
			controller: metricsRollupDailyController,
			threads:    opts.MetricsRollupDailyControllerThreads,
		},
	}

	var wg sync.WaitGroup
	for _, item := range controllersToRun {
		wg.Add(1)
		go func(controller controllers.Controller, threads int) {
			defer wg.Done()
			controller.Run(ctx, threads)
		}(item.controller, item.threads)
	}

	wg.Wait()
	logger.Info("Exiting.")
	return nil
}
