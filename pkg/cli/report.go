package cli

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/ndjsonoptions"
	reportsummary "ci-failure-atlas/pkg/report/summary"
	reporttestsummary "ci-failure-atlas/pkg/report/testsummary"
	reportweekly "ci-failure-atlas/pkg/report/weekly"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func NewReportCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "report",
		Short:         "Report generation commands.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	reportsSubdirectory := ""
	cmd.PersistentFlags().StringVar(
		&reportsSubdirectory,
		"reports.subdir",
		reportsSubdirectory,
		"Optional subdirectory under reports/ for generated report files. Defaults to --storage.ndjson.semantic-subdir when unset.",
	)

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
			store, err := ndjson.NewWithOptions(completed.DataDirectory, ndjson.Options{
				SemanticSubdirectory: completed.SemanticSubdirectory,
			})
			if err != nil {
				return fmt.Errorf("create NDJSON store: %w", err)
			}
			defer func() {
				_ = store.Close()
			}()

			resolvedOutputPath, err := resolveReportOutputPath(
				completed.DataDirectory,
				completed.SemanticSubdirectory,
				reportsSubdirectory,
				summaryOpts.OutputPath,
				cmd.Flags().Changed("output"),
			)
			if err != nil {
				return err
			}
			runOpts := summaryOpts
			runOpts.OutputPath = resolvedOutputPath
			return reportsummary.Generate(cmd.Context(), store, runOpts)
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
			store, err := ndjson.NewWithOptions(completed.DataDirectory, ndjson.Options{
				SemanticSubdirectory: completed.SemanticSubdirectory,
			})
			if err != nil {
				return fmt.Errorf("create NDJSON store: %w", err)
			}
			defer func() {
				_ = store.Close()
			}()

			resolvedOutputPath, err := resolveReportOutputPath(
				completed.DataDirectory,
				completed.SemanticSubdirectory,
				reportsSubdirectory,
				testSummaryOpts.OutputPath,
				cmd.Flags().Changed("output"),
			)
			if err != nil {
				return err
			}
			runOpts := testSummaryOpts
			runOpts.OutputPath = resolvedOutputPath
			return reporttestsummary.Generate(cmd.Context(), store, runOpts)
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
	weeklyCompareSemanticSubdirectory := ""
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
			store, err := ndjson.NewWithOptions(completed.DataDirectory, ndjson.Options{
				SemanticSubdirectory: completed.SemanticSubdirectory,
			})
			if err != nil {
				return fmt.Errorf("create NDJSON store: %w", err)
			}
			defer func() {
				_ = store.Close()
			}()
			resolvedOutputPath, err := resolveReportOutputPath(
				completed.DataDirectory,
				completed.SemanticSubdirectory,
				reportsSubdirectory,
				weeklyOpts.OutputPath,
				cmd.Flags().Changed("output"),
			)
			if err != nil {
				return err
			}

			var previousSemanticStore storecontracts.Store
			if compareSubdir := strings.TrimSpace(weeklyCompareSemanticSubdirectory); compareSubdir != "" {
				previousStore, previousStoreErr := ndjson.NewWithOptions(completed.DataDirectory, ndjson.Options{
					SemanticSubdirectory: compareSubdir,
				})
				if previousStoreErr != nil {
					return fmt.Errorf("create previous semantic NDJSON store: %w", previousStoreErr)
				}
				previousSemanticStore = previousStore
				defer func() {
					_ = previousStore.Close()
				}()
			}
			runOpts := weeklyOpts
			runOpts.OutputPath = resolvedOutputPath
			return reportweekly.GenerateWithComparison(cmd.Context(), store, previousSemanticStore, runOpts)
		},
	}
	if err := ndjsonoptions.BindNDJSONOptions(weeklyNDJSONOpts, weeklyCmd); err != nil {
		return nil, fmt.Errorf("bind NDJSON options for report weekly: %w", err)
	}
	weeklyCmd.Flags().StringVar(&weeklyOpts.OutputPath, "output", weeklyOpts.OutputPath, "path to output HTML report")
	weeklyCmd.Flags().StringVar(&weeklyOpts.StartDate, "start-date", weeklyOpts.StartDate, "weekly window start date (YYYY-MM-DD)")
	weeklyCmd.Flags().Float64Var(&weeklyOpts.TargetRate, "target-rate", weeklyOpts.TargetRate, "target success rate percentage for management status")
	weeklyCmd.Flags().StringVar(&weeklyCompareSemanticSubdirectory, "semantic.compare-subdir", weeklyCompareSemanticSubdirectory, "semantic subdirectory for comparison week (for example 2026-02-22)")
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

func resolveReportOutputPath(
	dataDirectory string,
	semanticSubdirectory string,
	reportSubdirectory string,
	outputPath string,
	outputFlagChanged bool,
) (string, error) {
	trimmedOutputPath := strings.TrimSpace(outputPath)
	if trimmedOutputPath == "" {
		return "", fmt.Errorf("report output path must not be empty")
	}
	if outputFlagChanged {
		return trimmedOutputPath, nil
	}

	effectiveSubdirectory := strings.TrimSpace(reportSubdirectory)
	if effectiveSubdirectory == "" {
		effectiveSubdirectory = strings.TrimSpace(semanticSubdirectory)
	}
	normalizedSubdirectory, err := normalizeReportSubdirectory(effectiveSubdirectory)
	if err != nil {
		return "", fmt.Errorf("invalid --reports.subdir: %w", err)
	}

	baseName := strings.TrimSpace(filepath.Base(trimmedOutputPath))
	if baseName == "" || baseName == "." {
		return "", fmt.Errorf("invalid report output file name %q", trimmedOutputPath)
	}
	if normalizedSubdirectory == "" {
		return filepath.Join(dataDirectory, "reports", baseName), nil
	}
	return filepath.Join(dataDirectory, "reports", normalizedSubdirectory, baseName), nil
}

func normalizeReportSubdirectory(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", nil
	}
	cleaned := filepath.Clean(trimmed)
	if cleaned == "." {
		return "", nil
	}
	if filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("must be a relative path")
	}
	parts := strings.Split(cleaned, string(filepath.Separator))
	for _, part := range parts {
		switch part {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("must not contain '..'")
		}
	}
	return cleaned, nil
}
