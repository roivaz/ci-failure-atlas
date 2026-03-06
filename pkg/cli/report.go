package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/ndjsonoptions"
	reporttestsummary "ci-failure-atlas/pkg/report/testsummary"
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
	ndjsonOpts := ndjsonoptions.DefaultOptions()
	testSummaryCmd := &cobra.Command{
		Use:   "test-summary",
		Short: "Generate per-test summary report.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			validated, err := ndjsonOpts.Validate()
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
	if err := ndjsonoptions.BindNDJSONOptions(ndjsonOpts, testSummaryCmd); err != nil {
		return nil, fmt.Errorf("bind NDJSON options for report test-summary: %w", err)
	}
	testSummaryCmd.Flags().StringVar(&testSummaryOpts.OutputPath, "output", testSummaryOpts.OutputPath, "path to output markdown report")
	testSummaryCmd.Flags().IntVar(&testSummaryOpts.TopTests, "top", testSummaryOpts.TopTests, "max number of tests to render (0 = all)")
	testSummaryCmd.Flags().IntVar(&testSummaryOpts.RecentRuns, "recent", testSummaryOpts.RecentRuns, "recent failing runs to render per signature")
	testSummaryCmd.Flags().IntVar(&testSummaryOpts.MinRuns, "min-runs", testSummaryOpts.MinRuns, "minimum runs threshold for including a test")

	cmd.AddCommand(
		&cobra.Command{
			Use:   "summary",
			Short: "Generate triage summary report.",
			RunE: func(_ *cobra.Command, _ []string) error {
				return fmt.Errorf("report summary not implemented yet (pending global cross-test merge phase)")
			},
		},
		testSummaryCmd,
	)

	return cmd, nil
}
