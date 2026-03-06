package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func NewReportCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "report",
		Short:         "Report generation commands.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "summary",
			Short: "Generate triage summary report.",
			RunE: func(_ *cobra.Command, _ []string) error {
				return fmt.Errorf("report summary not implemented yet")
			},
		},
		&cobra.Command{
			Use:   "test-summary",
			Short: "Generate per-test summary report.",
			RunE: func(_ *cobra.Command, _ []string) error {
				return fmt.Errorf("report test-summary not implemented yet")
			},
		},
	)

	return cmd, nil
}
