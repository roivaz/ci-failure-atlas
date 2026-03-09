package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/ndjsonoptions"
	reportsummary "ci-failure-atlas/pkg/report/summary"
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

	summaryOpts := reportsummary.DefaultOptions()
	summaryNDJSONOpts := ndjsonoptions.DefaultOptions()
	summaryCmd := &cobra.Command{
		Use:   "summary",
		Short: "Generate triage summary report.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			validated, err := summaryNDJSONOpts.Validate()
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

			return reportsummary.Generate(cmd.Context(), store, summaryOpts)
		},
	}
	if err := ndjsonoptions.BindNDJSONOptions(summaryNDJSONOpts, summaryCmd); err != nil {
		return nil, fmt.Errorf("bind NDJSON options for report summary: %w", err)
	}
	summaryCmd.Flags().StringVar(&summaryOpts.OutputPath, "output", summaryOpts.OutputPath, "path to output markdown summary")
	summaryCmd.Flags().IntVar(&summaryOpts.Top, "top", summaryOpts.Top, "number of top rows to include in sections")
	summaryCmd.Flags().Float64Var(&summaryOpts.MinPercent, "min-percent", summaryOpts.MinPercent, "minimum percent threshold for including rows")
	summaryCmd.Flags().StringSliceVar(&summaryOpts.Environments, "source.envs", summaryOpts.Environments, "environments to include (e.g. dev,int,stg,prod)")
	summaryCmd.Flags().BoolVar(&summaryOpts.SplitByEnvironment, "split-by-env", summaryOpts.SplitByEnvironment, "write one output file per environment using <output>.<env>.<ext>")

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
	testSummaryCmd.Flags().StringSliceVar(&testSummaryOpts.Environments, "source.envs", testSummaryOpts.Environments, "environments to include (e.g. dev,int,stg,prod)")
	testSummaryCmd.Flags().BoolVar(&testSummaryOpts.SplitByEnvironment, "split-by-env", testSummaryOpts.SplitByEnvironment, "write one output file per environment using <output>.<env>.<ext>")

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
		summaryCmd,
		testSummaryCmd,
		weeklyCmd,
	)

	return cmd, nil
}
