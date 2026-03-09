package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	workflowphase1 "ci-failure-atlas/pkg/workflow/phase1"
	workflowphase2 "ci-failure-atlas/pkg/workflow/phase2"
)

func NewWorkflowCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "workflow",
		Short:         "Semantic workflow commands.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	phase1Opts := workflowphase1.DefaultOptions()
	phase1Cmd := &cobra.Command{
		Use:   "phase1",
		Short: "Run semantic phase1 workflow.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			validated, err := phase1Opts.Validate()
			if err != nil {
				return err
			}
			completed, err := validated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			return completed.Run(cmd.Context())
		},
	}
	if err := workflowphase1.BindOptions(phase1Opts, phase1Cmd); err != nil {
		return nil, fmt.Errorf("failed to bind workflow phase1 options: %w", err)
	}
	cmd.AddCommand(phase1Cmd)

	phase2Opts := workflowphase2.DefaultOptions()
	phase2Cmd := &cobra.Command{
		Use:   "phase2",
		Short: "Run semantic phase2 workflow.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			validated, err := phase2Opts.Validate()
			if err != nil {
				return err
			}
			completed, err := validated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			return completed.Run(cmd.Context())
		},
	}
	if err := workflowphase2.BindOptions(phase2Opts, phase2Cmd); err != nil {
		return nil, fmt.Errorf("failed to bind workflow phase2 options: %w", err)
	}
	cmd.AddCommand(phase2Cmd)

	for _, sub := range []string{
		"validate",
		"canary",
		"promote-rules",
	} {
		subCmd := &cobra.Command{
			Use:   sub,
			Short: "Workflow stage: " + sub,
			RunE: func(cmd *cobra.Command, _ []string) error {
				return fmt.Errorf("workflow %s not implemented yet", cmd.Name())
			},
		}
		cmd.AddCommand(subCmd)
	}

	return cmd, nil
}
