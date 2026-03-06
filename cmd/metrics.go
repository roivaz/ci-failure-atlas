package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func NewMetricsCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "metrics",
		Short:         "Metrics generation commands.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	var date string
	var window string

	rollupCmd := &cobra.Command{
		Use:   "rollup-daily",
		Short: "Compute or refresh daily metrics rollups.",
		RunE: func(_ *cobra.Command, _ []string) error {
			if date == "" {
				return fmt.Errorf("missing required --date")
			}
			return fmt.Errorf("metrics rollup-daily not implemented yet")
		},
	}
	rollupCmd.Flags().StringVar(&date, "date", "", "rollup date in YYYY-MM-DD format")

	trendCmd := &cobra.Command{
		Use:   "trend",
		Short: "Generate trend outputs over a time window.",
		RunE: func(_ *cobra.Command, _ []string) error {
			if window == "" {
				return fmt.Errorf("missing required --window")
			}
			return fmt.Errorf("metrics trend not implemented yet")
		},
	}
	trendCmd.Flags().StringVar(&window, "window", "30d", "trend window (for example 7d, 30d, 90d)")

	cmd.AddCommand(rollupCmd, trendCmd)
	return cmd, nil
}
