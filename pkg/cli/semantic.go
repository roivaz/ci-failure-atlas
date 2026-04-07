package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	semanticworkflow "ci-failure-atlas/pkg/semantic/workflow"
	sourceoptions "ci-failure-atlas/pkg/source/options"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

const semanticWeekFormat = "2006-01-02"

type materializeScope struct {
	Week      string
	WeekStart time.Time
	WeekEnd   time.Time
}

func NewSemanticCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "semantic",
		Short:         "Semantic week workflows.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	week := ""
	postgresRaw := postgresoptions.DefaultCLIOptions()

	materializeCmd := &cobra.Command{
		Use:   "materialize",
		Short: "Materialize one semantic week from facts in PostgreSQL.",
		Args:  cobra.NoArgs,
		Example: strings.TrimSpace(`
cfa semantic materialize --week 2026-03-29
`),
		RunE: func(cmd *cobra.Command, _ []string) error {
			scope, err := resolveMaterializeScope(week, time.Now().UTC())
			if err != nil {
				return err
			}

			postgresCompleted, store, err := openPostgresStoreForCommand(cmd.Context(), postgresRaw, postgresstore.Options{
				Week: scope.Week,
			})
			if err != nil {
				return err
			}
			defer postgresCompleted.Cleanup()
			defer func() {
				_ = store.Close()
			}()

			result, err := semanticworkflow.MaterializeWeek(cmd.Context(), store, scope.WeekStart)
			if err != nil {
				return fmt.Errorf("materialize semantic week %q: %w", scope.Week, err)
			}

			printMaterializeSummary(cmd, scope, result)
			return nil
		},
	}
	materializeCmd.Flags().StringVar(&week, "week", week, "week start date to materialize (YYYY-MM-DD). Defaults to the current week.")
	if err := postgresoptions.BindOptions(postgresRaw, materializeCmd); err != nil {
		return nil, err
	}

	cmd.AddCommand(materializeCmd)
	return cmd, nil
}

func resolveMaterializeScope(rawWeek string, now time.Time) (materializeScope, error) {
	weekLabel := strings.TrimSpace(rawWeek)
	if weekLabel == "" {
		weekLabel = currentMaterializeWeekLabel(now)
	}
	normalizedWeek, err := postgresstore.NormalizeWeek(weekLabel)
	if err != nil {
		return materializeScope{}, fmt.Errorf("invalid --week %q: %w", weekLabel, err)
	}
	weekStart, err := time.Parse(semanticWeekFormat, normalizedWeek)
	if err != nil {
		return materializeScope{}, fmt.Errorf("parse week %q: %w", normalizedWeek, err)
	}
	weekStart = weekStart.UTC()
	return materializeScope{
		Week:      normalizedWeek,
		WeekStart: weekStart,
		WeekEnd:   weekStart.AddDate(0, 0, 7).UTC(),
	}, nil
}

func printMaterializeSummary(cmd *cobra.Command, scope materializeScope, result semanticworkflow.Result) {
	diagnostics := result.Phase1.Diagnostics
	cmd.Printf("Materialized semantic week %s\n", scope.Week)
	cmd.Printf("  window: %s .. %s\n", scope.WeekStart.Format(time.RFC3339), scope.WeekEnd.Format(time.RFC3339))
	cmd.Printf("  environments: %s\n", strings.Join(sourceoptions.SupportedEnvironments(), ","))
	cmd.Printf("  raw failures: %d included of %d total\n", diagnostics.RowsIncluded, diagnostics.RawRowsTotal)
	cmd.Printf("  test clusters: %d\n", len(result.Phase1.TestClusters))
	cmd.Printf("  review queue: %d\n", len(result.Phase2.ReviewQueue))
	cmd.Printf("  global clusters: %d\n", len(result.Phase2.GlobalClusters))
	if diagnostics.RowsSkippedOutsideWindow > 0 || diagnostics.RowsSkippedNonArtifact > 0 || diagnostics.RowsSkippedInvalid > 0 {
		cmd.Printf(
			"  skipped: outside_window=%d non_artifact=%d invalid=%d\n",
			diagnostics.RowsSkippedOutsideWindow,
			diagnostics.RowsSkippedNonArtifact,
			diagnostics.RowsSkippedInvalid,
		)
	}
}

func currentMaterializeWeekLabel(now time.Time) string {
	normalized := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
	offset := int(normalized.Weekday())
	return normalized.AddDate(0, 0, -offset).Format(semanticWeekFormat)
}
