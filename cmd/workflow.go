package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func NewWorkflowCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "workflow",
		Short:         "Semantic workflow commands.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	for _, sub := range []string{
		"phase1",
		"phase2",
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
