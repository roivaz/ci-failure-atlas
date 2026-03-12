package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	semhistory "ci-failure-atlas/pkg/semantic/history"
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

	buildOpts := workflowphase1.DefaultOptions()
	buildCmd := &cobra.Command{
		Use:   "build",
		Short: "Run semantic workflow build (phase1 + phase2).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			phase1Validated, err := buildOpts.Validate()
			if err != nil {
				return err
			}
			phase1Completed, err := phase1Validated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			if err := phase1Completed.Run(cmd.Context()); err != nil {
				return fmt.Errorf("workflow build phase1: %w", err)
			}

			phase2Opts := workflowphase2.DefaultOptions()
			phase2Opts.NDJSONOptions.DataDirectory = buildOpts.NDJSONOptions.DataDirectory
			phase2Opts.NDJSONOptions.SemanticSubdirectory = buildOpts.NDJSONOptions.SemanticSubdirectory
			phase2Validated, err := phase2Opts.Validate()
			if err != nil {
				return err
			}
			phase2Completed, err := phase2Validated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			if err := phase2Completed.Run(cmd.Context()); err != nil {
				return fmt.Errorf("workflow build phase2: %w", err)
			}
			if strings.TrimSpace(buildOpts.NDJSONOptions.SemanticSubdirectory) != "" && phase1Completed.WindowStart != nil && phase1Completed.WindowEnd != nil {
				if err := semhistory.WriteWindowMetadata(
					buildOpts.NDJSONOptions.DataDirectory,
					buildOpts.NDJSONOptions.SemanticSubdirectory,
					phase1Completed.WindowStart.UTC(),
					phase1Completed.WindowEnd.UTC(),
				); err != nil {
					return fmt.Errorf("workflow build metadata: %w", err)
				}
			}
			return nil
		},
	}
	if err := workflowphase1.BindOptions(buildOpts, buildCmd); err != nil {
		return nil, fmt.Errorf("failed to bind workflow build options: %w", err)
	}
	cmd.AddCommand(buildCmd)

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
