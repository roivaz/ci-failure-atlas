package run_once

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/controllers"
	"ci-failure-atlas/pkg/sourceoptions"
	"ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

func DefaultOptions() *RawOptions {
	postgresRaw := postgresoptions.DefaultOptions()
	postgresRaw.Enabled = true
	postgresRaw.Embedded = true
	postgresRaw.Initialize = true
	return &RawOptions{
		SourceOptions:   sourceoptions.DefaultOptions(),
		PostgresOptions: postgresRaw,
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	if opts.SourceOptions == nil {
		opts.SourceOptions = sourceoptions.DefaultOptions()
	}
	if opts.PostgresOptions == nil {
		opts.PostgresOptions = postgresoptions.DefaultOptions()
	}
	opts.PostgresOptions.Enabled = true
	if err := sourceoptions.BindSourceOptions(opts.SourceOptions, cmd); err != nil {
		return err
	}
	if err := postgresoptions.BindOptions(opts.PostgresOptions, cmd); err != nil {
		return err
	}
	cmd.Flags().StringVar(&opts.ControllerName, "controllers.name", opts.ControllerName, "Name of the controller to run once.")
	cmd.Flags().StringVar(&opts.ControllerKey, "controllers.key", opts.ControllerKey, "Controller key to process.")
	return nil
}

type RawOptions struct {
	SourceOptions   *sourceoptions.RawOptions
	PostgresOptions *postgresoptions.RawOptions

	ControllerName string
	ControllerKey  string
}

type validatedOptions struct {
	*RawOptions
	SourceValidated   *sourceoptions.ValidatedOptions
	PostgresValidated *postgresoptions.ValidatedOptions

	ControllerName string
	ControllerKey  string
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	Source   *sourceoptions.Options
	Postgres *postgresoptions.Options
	Store    contracts.Store

	ControllerName string
	ControllerKey  string
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	if o.ControllerName == "" {
		return nil, fmt.Errorf("the controller name must be provided with --controllers.name")
	}
	if o.ControllerKey == "" {
		return nil, fmt.Errorf("the controller key must be provided with --controllers.key")
	}
	if o.SourceOptions == nil {
		o.SourceOptions = sourceoptions.DefaultOptions()
	}
	sourceValidated, err := o.SourceOptions.Validate()
	if err != nil {
		return nil, err
	}

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
			SourceValidated:   sourceValidated,
			PostgresValidated: postgresValidated,
			ControllerName:    o.ControllerName,
			ControllerKey:     o.ControllerKey,
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
	store, err = postgresstore.New(postgresCompleted.Connection, postgresstore.Options{
		SemanticSubdirectory: postgresCompleted.SemanticSubdirectory,
	})
	if err != nil {
		postgresCompleted.Cleanup()
		return nil, fmt.Errorf("create postgres store: %w", err)
	}
	return &Options{
		completedOptions: &completedOptions{
			Source:         sourceCompleted,
			Postgres:       postgresCompleted,
			Store:          store,
			ControllerName: o.ControllerName,
			ControllerKey:  o.ControllerKey,
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

	controller, err := controllers.NewByName(opts.ControllerName, logger, controllers.Dependencies{
		Store:  opts.Store,
		Source: opts.Source,
	})
	if err != nil {
		return err
	}
	return controller.RunOnce(ctx, opts.ControllerKey)
}
