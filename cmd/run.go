package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/run"
	"ci-failure-atlas/pkg/run_once"
	"ci-failure-atlas/pkg/sync_once"
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

	opts := run_once.DefaultOptions()
	if err := run_once.BindOptions(opts, cmd); err != nil {
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

func NewSyncOnceCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "sync-once",
		Short:         "Run one full controller sync for all keys.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	opts := sync_once.DefaultOptions()
	if err := sync_once.BindOptions(opts, cmd); err != nil {
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
