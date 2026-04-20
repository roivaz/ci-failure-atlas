package run

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/controllers"
	sourceoptions "ci-failure-atlas/pkg/source/options"
	"ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

func DefaultOptions() *RawOptions {
	return &RawOptions{
		SourceOptions:   sourceoptions.DefaultOptions(),
		PostgresOptions: postgresoptions.DefaultCLIOptions(),

		SourceSippyRunsControllerThreads:       1,
		SourceProwRunsControllerThreads:        1,
		SourceSippyTestsDailyControllerThreads: 1,
		SourceGitHubPullRequestsThreads:        1,
		SourceProwFailuresControllerThreads:    1,
		FactsRunsControllerThreads:             1,
		FactsRawFailuresControllerThreads:      1,
		MetricsRollupDailyControllerThreads:    1,
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	if opts.SourceOptions == nil {
		opts.SourceOptions = sourceoptions.DefaultOptions()
	}
	if opts.PostgresOptions == nil {
		opts.PostgresOptions = postgresoptions.DefaultCLIOptions()
	}
	opts.PostgresOptions.Enabled = true
	if err := sourceoptions.BindSourceOptions(opts.SourceOptions, cmd); err != nil {
		return err
	}
	if err := postgresoptions.BindOptions(opts.PostgresOptions, cmd); err != nil {
		return err
	}

	cmd.Flags().IntVar(&opts.SourceSippyRunsControllerThreads, "controllers.source.sippy.runs.threads", opts.SourceSippyRunsControllerThreads, "Number of threads for controller source.sippy.runs.")
	cmd.Flags().IntVar(&opts.SourceProwRunsControllerThreads, "controllers.source.prow.runs.threads", opts.SourceProwRunsControllerThreads, "Number of threads for controller source.prow.runs.")
	cmd.Flags().IntVar(&opts.SourceSippyTestsDailyControllerThreads, "controllers.source.sippy.tests-daily.threads", opts.SourceSippyTestsDailyControllerThreads, "Number of threads for controller source.sippy.tests-daily.")
	cmd.Flags().IntVar(&opts.SourceGitHubPullRequestsThreads, "controllers.source.github.pull-requests.threads", opts.SourceGitHubPullRequestsThreads, "Number of threads for controller source.github.pull-requests.")
	cmd.Flags().IntVar(&opts.SourceProwFailuresControllerThreads, "controllers.source.prow.failures.threads", opts.SourceProwFailuresControllerThreads, "Number of threads for controller source.prow.failures.")
	cmd.Flags().IntVar(&opts.FactsRunsControllerThreads, "controllers.facts.runs.threads", opts.FactsRunsControllerThreads, "Number of threads for controller facts.runs.")
	cmd.Flags().IntVar(&opts.FactsRawFailuresControllerThreads, "controllers.facts.raw-failures.threads", opts.FactsRawFailuresControllerThreads, "Number of threads for controller facts.raw-failures.")
	cmd.Flags().IntVar(&opts.MetricsRollupDailyControllerThreads, "controllers.metrics.rollup.daily.threads", opts.MetricsRollupDailyControllerThreads, "Number of threads for controller metrics.rollup.daily.")
	return nil
}

type RawOptions struct {
	SourceOptions   *sourceoptions.RawOptions
	PostgresOptions *postgresoptions.RawOptions

	SourceSippyRunsControllerThreads       int
	SourceProwRunsControllerThreads        int
	SourceSippyTestsDailyControllerThreads int
	SourceGitHubPullRequestsThreads        int
	SourceProwFailuresControllerThreads    int
	FactsRunsControllerThreads             int
	FactsRawFailuresControllerThreads      int
	MetricsRollupDailyControllerThreads    int
}

type validatedOptions struct {
	*RawOptions
	SourceValidated   *sourceoptions.ValidatedOptions
	PostgresValidated *postgresoptions.ValidatedOptions

	SourceSippyRunsControllerThreads       int
	SourceProwRunsControllerThreads        int
	SourceSippyTestsDailyControllerThreads int
	SourceGitHubPullRequestsThreads        int
	SourceProwFailuresControllerThreads    int
	FactsRunsControllerThreads             int
	FactsRawFailuresControllerThreads      int
	MetricsRollupDailyControllerThreads    int
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	Source   *sourceoptions.Options
	Postgres *postgresoptions.Options

	Store contracts.Store

	SourceSippyRunsControllerThreads       int
	SourceProwRunsControllerThreads        int
	SourceSippyTestsDailyControllerThreads int
	SourceGitHubPullRequestsThreads        int
	SourceProwFailuresControllerThreads    int
	FactsRunsControllerThreads             int
	FactsRawFailuresControllerThreads      int
	MetricsRollupDailyControllerThreads    int
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	if o.SourceOptions == nil {
		o.SourceOptions = sourceoptions.DefaultOptions()
	}
	sourceValidated, err := o.SourceOptions.Validate()
	if err != nil {
		return nil, err
	}

	if o.PostgresOptions == nil {
		o.PostgresOptions = postgresoptions.DefaultCLIOptions()
	}
	o.PostgresOptions.Enabled = true
	postgresValidated, err := o.PostgresOptions.Validate()
	if err != nil {
		return nil, err
	}
	if !postgresValidated.Enabled {
		return nil, fmt.Errorf("postgres storage is required")
	}

	if o.SourceSippyRunsControllerThreads <= 0 {
		o.SourceSippyRunsControllerThreads = 1
	}
	if o.SourceProwRunsControllerThreads <= 0 {
		o.SourceProwRunsControllerThreads = 1
	}
	if o.SourceSippyTestsDailyControllerThreads <= 0 {
		o.SourceSippyTestsDailyControllerThreads = 1
	}
	if o.SourceGitHubPullRequestsThreads <= 0 {
		o.SourceGitHubPullRequestsThreads = 1
	}
	if o.SourceProwFailuresControllerThreads <= 0 {
		o.SourceProwFailuresControllerThreads = 1
	}
	if o.FactsRunsControllerThreads <= 0 {
		o.FactsRunsControllerThreads = 1
	}
	if o.FactsRawFailuresControllerThreads <= 0 {
		o.FactsRawFailuresControllerThreads = 1
	}
	if o.MetricsRollupDailyControllerThreads <= 0 {
		o.MetricsRollupDailyControllerThreads = 1
	}

	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:                             o,
			SourceValidated:                        sourceValidated,
			PostgresValidated:                      postgresValidated,
			SourceSippyRunsControllerThreads:       o.SourceSippyRunsControllerThreads,
			SourceProwRunsControllerThreads:        o.SourceProwRunsControllerThreads,
			SourceSippyTestsDailyControllerThreads: o.SourceSippyTestsDailyControllerThreads,
			SourceGitHubPullRequestsThreads:        o.SourceGitHubPullRequestsThreads,
			SourceProwFailuresControllerThreads:    o.SourceProwFailuresControllerThreads,
			FactsRunsControllerThreads:             o.FactsRunsControllerThreads,
			FactsRawFailuresControllerThreads:      o.FactsRawFailuresControllerThreads,
			MetricsRollupDailyControllerThreads:    o.MetricsRollupDailyControllerThreads,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(ctx context.Context) (*Options, error) {
	sourceCompleted, err := o.SourceValidated.Complete(ctx)
	if err != nil {
		return nil, err
	}

	var (
		postgresCompleted *postgresoptions.Options
		store             contracts.Store
	)
	postgresCompleted, err = o.PostgresValidated.Complete(ctx)
	if err != nil {
		return nil, err
	}
	store, err = postgresstore.New(postgresCompleted.Connection, postgresstore.Options{})
	if err != nil {
		postgresCompleted.Cleanup()
		return nil, fmt.Errorf("create postgres store: %w", err)
	}

	return &Options{
		completedOptions: &completedOptions{
			Source:                                 sourceCompleted,
			Postgres:                               postgresCompleted,
			Store:                                  store,
			SourceSippyRunsControllerThreads:       o.SourceSippyRunsControllerThreads,
			SourceProwRunsControllerThreads:        o.SourceProwRunsControllerThreads,
			SourceSippyTestsDailyControllerThreads: o.SourceSippyTestsDailyControllerThreads,
			SourceGitHubPullRequestsThreads:        o.SourceGitHubPullRequestsThreads,
			SourceProwFailuresControllerThreads:    o.SourceProwFailuresControllerThreads,
			FactsRunsControllerThreads:             o.FactsRunsControllerThreads,
			FactsRawFailuresControllerThreads:      o.FactsRawFailuresControllerThreads,
			MetricsRollupDailyControllerThreads:    o.MetricsRollupDailyControllerThreads,
		},
	}, nil
}

func (opts *Options) Cleanup() {
	if opts.Store != nil {
		_ = opts.Store.Close()
	}
	if opts.Postgres != nil {
		opts.Postgres.Cleanup()
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
	sourceProwRunsController, err := controllers.NewSourceProwRuns(logger, deps)
	if err != nil {
		return err
	}
	sourceSippyTestsDailyController, err := controllers.NewSourceSippyTestsDaily(logger, deps)
	if err != nil {
		return err
	}
	sourceGitHubPullRequestsController, err := controllers.NewSourceGitHubPullRequests(logger, deps)
	if err != nil {
		return err
	}
	sourceProwFailuresController, err := controllers.NewSourceProwFailures(logger, deps)
	if err != nil {
		return err
	}
	factsRunsController, err := controllers.NewFactsRuns(logger, deps)
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
			controller: sourceProwRunsController,
			threads:    opts.SourceProwRunsControllerThreads,
		},
		{
			controller: sourceSippyTestsDailyController,
			threads:    opts.SourceSippyTestsDailyControllerThreads,
		},
		{
			controller: sourceGitHubPullRequestsController,
			threads:    opts.SourceGitHubPullRequestsThreads,
		},
		{
			controller: sourceProwFailuresController,
			threads:    opts.SourceProwFailuresControllerThreads,
		},
		{
			controller: factsRunsController,
			threads:    opts.FactsRunsControllerThreads,
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
