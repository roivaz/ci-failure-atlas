package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/controllers"
	sourceoptions "ci-failure-atlas/pkg/source/options"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

type controllerCommandOptions struct {
	SourceOptions   *sourceoptions.RawOptions
	PostgresOptions *postgresoptions.RawOptions

	ControllerName string
	ControllerKey  string
}

type controllerCommandRuntime struct {
	Source         *sourceoptions.Options
	Postgres       *postgresoptions.Options
	Store          storecontracts.Store
	ControllerName string
	ControllerKey  string
}

func defaultControllerCommandOptions() *controllerCommandOptions {
	return &controllerCommandOptions{
		SourceOptions:   sourceoptions.DefaultOptions(),
		PostgresOptions: postgresoptions.DefaultCLIOptions(),
	}
}

func bindControllerCommandOptions(opts *controllerCommandOptions, cmd *cobra.Command, includeKey bool) error {
	if opts.SourceOptions == nil {
		opts.SourceOptions = sourceoptions.DefaultOptions()
	}
	if opts.PostgresOptions == nil {
		opts.PostgresOptions = postgresoptions.DefaultCLIOptions()
	}
	if err := sourceoptions.BindSourceOptions(opts.SourceOptions, cmd); err != nil {
		return err
	}
	if err := postgresoptions.BindOptions(opts.PostgresOptions, cmd); err != nil {
		return err
	}
	cmd.Flags().StringVar(&opts.ControllerName, "controllers.name", opts.ControllerName, "Name of the controller to execute.")
	if includeKey {
		cmd.Flags().StringVar(&opts.ControllerKey, "controllers.key", opts.ControllerKey, "Controller key to process.")
	}
	return nil
}

func (opts *controllerCommandOptions) complete(ctx context.Context, requireKey bool) (*controllerCommandRuntime, error) {
	if opts == nil {
		opts = defaultControllerCommandOptions()
	}

	controllerName := strings.TrimSpace(opts.ControllerName)
	if controllerName == "" {
		return nil, fmt.Errorf("the controller name must be provided with --controllers.name")
	}
	controllerKey := strings.TrimSpace(opts.ControllerKey)
	if requireKey && controllerKey == "" {
		return nil, fmt.Errorf("the controller key must be provided with --controllers.key")
	}

	if opts.SourceOptions == nil {
		opts.SourceOptions = sourceoptions.DefaultOptions()
	}
	sourceValidated, err := opts.SourceOptions.Validate()
	if err != nil {
		return nil, err
	}
	sourceCompleted, err := sourceValidated.Complete(ctx)
	if err != nil {
		return nil, err
	}

	postgresCompleted, store, err := openPostgresStoreForCommand(ctx, opts.PostgresOptions, postgresstore.Options{})
	if err != nil {
		return nil, err
	}

	return &controllerCommandRuntime{
		Source:         sourceCompleted,
		Postgres:       postgresCompleted,
		Store:          store,
		ControllerName: controllerName,
		ControllerKey:  controllerKey,
	}, nil
}

func (runtime *controllerCommandRuntime) Cleanup() {
	if runtime == nil {
		return
	}
	if runtime.Store != nil {
		_ = runtime.Store.Close()
	}
	if runtime.Postgres != nil {
		runtime.Postgres.Cleanup()
	}
}

func runNamedControllerCommand(ctx context.Context, opts *controllerCommandOptions, requireKey bool, runner func(context.Context, controllers.Controller, *controllerCommandRuntime) error) error {
	runtime, err := opts.complete(ctx, requireKey)
	if err != nil {
		return err
	}
	defer runtime.Cleanup()

	logger, err := logr.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	controller, err := controllers.NewByName(runtime.ControllerName, logger, controllers.Dependencies{
		Store:  runtime.Store,
		Source: runtime.Source,
	})
	if err != nil {
		return err
	}
	return runner(ctx, controller, runtime)
}
