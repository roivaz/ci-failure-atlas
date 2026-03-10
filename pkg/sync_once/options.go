package sync_once

import (
	"context"
	"fmt"

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
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	if err := sourceoptions.BindSourceOptions(opts.SourceOptions, cmd); err != nil {
		return err
	}
	if err := ndjsonoptions.BindNDJSONOptions(opts.NDJSONOptions, cmd); err != nil {
		return err
	}
	cmd.Flags().StringVar(&opts.ControllerName, "controllers.name", opts.ControllerName, "Name of the controller to sync once.")
	return nil
}

type RawOptions struct {
	SourceOptions *sourceoptions.RawOptions
	NDJSONOptions *ndjsonoptions.RawOptions

	ControllerName string
}

type validatedOptions struct {
	*RawOptions
	SourceValidated *sourceoptions.ValidatedOptions
	NDJSONValidated *ndjsonoptions.ValidatedOptions

	ControllerName string
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	Source *sourceoptions.Options
	NDJSON *ndjsonoptions.Options
	Store  contracts.Store

	ControllerName string
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	if o.ControllerName == "" {
		return nil, fmt.Errorf("the controller name must be provided with --controllers.name")
	}
	sourceValidated, err := o.SourceOptions.Validate()
	if err != nil {
		return nil, err
	}
	ndjsonValidated, err := o.NDJSONOptions.Validate()
	if err != nil {
		return nil, err
	}
	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:      o,
			SourceValidated: sourceValidated,
			NDJSONValidated: ndjsonValidated,
			ControllerName:  o.ControllerName,
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
	store, err := ndjson.NewWithOptions(ndjsonCompleted.DataDirectory, ndjson.Options{
		SemanticSubdirectory: ndjsonCompleted.SemanticSubdirectory,
	})
	if err != nil {
		return nil, fmt.Errorf("create NDJSON store: %w", err)
	}
	return &Options{
		completedOptions: &completedOptions{
			Source:         sourceCompleted,
			NDJSON:         ndjsonCompleted,
			Store:          store,
			ControllerName: o.ControllerName,
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

	controller, err := controllers.NewByName(opts.ControllerName, logger, controllers.Dependencies{
		Store:  opts.Store,
		Source: opts.Source,
	})
	if err != nil {
		return err
	}
	return controller.SyncOnce(ctx)
}
