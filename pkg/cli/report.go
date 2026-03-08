package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/ndjsonoptions"
	reporttestsummary "ci-failure-atlas/pkg/report/testsummary"
	reportweekly "ci-failure-atlas/pkg/report/weekly"
	"ci-failure-atlas/pkg/store/ndjson"
)

func NewReportCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "report",
		Short:         "Report generation commands.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	testSummaryOpts := reporttestsummary.DefaultOptions()
	testSummaryNDJSONOpts := ndjsonoptions.DefaultOptions()
	testSummaryCmd := &cobra.Command{
		Use:   "test-summary",
		Short: "Generate per-test summary report.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			validated, err := testSummaryNDJSONOpts.Validate()
			if err != nil {
				return err
			}
			completed, err := validated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			store, err := ndjson.New(completed.DataDirectory)
			if err != nil {
				return fmt.Errorf("create NDJSON store: %w", err)
			}
			defer func() {
				_ = store.Close()
			}()

			return reporttestsummary.Generate(cmd.Context(), store, testSummaryOpts)
		},
	}
	if err := ndjsonoptions.BindNDJSONOptions(testSummaryNDJSONOpts, testSummaryCmd); err != nil {
		return nil, fmt.Errorf("bind NDJSON options for report test-summary: %w", err)
	}
	testSummaryCmd.Flags().StringVar(&testSummaryOpts.OutputPath, "output", testSummaryOpts.OutputPath, "path to output markdown report")
	testSummaryCmd.Flags().IntVar(&testSummaryOpts.TopTests, "top", testSummaryOpts.TopTests, "max number of tests to render (0 = all)")
	testSummaryCmd.Flags().IntVar(&testSummaryOpts.RecentRuns, "recent", testSummaryOpts.RecentRuns, "recent failing runs to render per signature")
	testSummaryCmd.Flags().IntVar(&testSummaryOpts.MinRuns, "min-runs", testSummaryOpts.MinRuns, "minimum runs threshold for including a test")

	weeklyOpts := reportweekly.DefaultOptions()
	weeklyNDJSONOpts := ndjsonoptions.DefaultOptions()
	weeklyCmd := &cobra.Command{
		Use:   "weekly",
		Short: "Generate weekly metrics HTML report.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			validated, err := weeklyNDJSONOpts.Validate()
			if err != nil {
				return err
			}
			completed, err := validated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			store, err := ndjson.New(completed.DataDirectory)
			if err != nil {
				return fmt.Errorf("create NDJSON store: %w", err)
			}
			defer func() {
				_ = store.Close()
			}()
			return reportweekly.Generate(cmd.Context(), store, weeklyOpts)
		},
	}
	if err := ndjsonoptions.BindNDJSONOptions(weeklyNDJSONOpts, weeklyCmd); err != nil {
		return nil, fmt.Errorf("bind NDJSON options for report weekly: %w", err)
	}
	weeklyCmd.Flags().StringVar(&weeklyOpts.OutputPath, "output", weeklyOpts.OutputPath, "path to output HTML report")
	weeklyCmd.Flags().StringVar(&weeklyOpts.StartDate, "start-date", weeklyOpts.StartDate, "weekly window start date (YYYY-MM-DD)")
	if err := weeklyCmd.MarkFlagRequired("start-date"); err != nil {
		return nil, fmt.Errorf("mark --start-date required for report weekly: %w", err)
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "summary",
			Short: "Generate triage summary report.",
			RunE: func(_ *cobra.Command, _ []string) error {
				return fmt.Errorf("report summary not implemented yet (pending global cross-test merge phase)")
			},
		},
		testSummaryCmd,
		weeklyCmd,
	)

	return cmd, nil
}
