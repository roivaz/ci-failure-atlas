package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/controllers"
	"ci-failure-atlas/pkg/run"
)

func NewRunCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "run",
		Short:         "Run CI Failure Atlas controllers continuously.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	opts := run.DefaultOptions()
	if err := run.BindOptions(opts, cmd); err != nil {
		return nil, fmt.Errorf("failed to bind options: %w", err)
	}

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		validated, err := opts.Validate()
		if err != nil {
			return err
		}
		completed, err := validated.Complete(cmd.Context())
		if err != nil {
			return err
		}
		return completed.Run(cmd.Context())
	}

	return cmd, nil
}

func NewRunOnceCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "run-once",
		Short:         "Run one controller reconciliation for one key.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	opts := defaultControllerCommandOptions()
	if err := bindControllerCommandOptions(opts, cmd, true); err != nil {
		return nil, fmt.Errorf("failed to bind options: %w", err)
	}

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		return runNamedControllerCommand(cmd.Context(), opts, true, func(ctx context.Context, controller controllers.Controller, runtime *controllerCommandRuntime) error {
			return controller.RunOnce(ctx, runtime.ControllerKey)
		})
	}

	return cmd, nil
}

func NewSyncOnceCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "sync-once",
		Short:         "Run one full controller sync for all keys.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	opts := defaultControllerCommandOptions()
	if err := bindControllerCommandOptions(opts, cmd, false); err != nil {
		return nil, fmt.Errorf("failed to bind options: %w", err)
	}

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		return runNamedControllerCommand(cmd.Context(), opts, false, func(ctx context.Context, controller controllers.Controller, _ *controllerCommandRuntime) error {
			return controller.SyncOnce(ctx)
		})
	}

	return cmd, nil
}
