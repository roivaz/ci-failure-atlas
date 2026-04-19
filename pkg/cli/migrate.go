package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"

	"github.com/spf13/cobra"
)

func NewMigrateCommand() (*cobra.Command, error) {
	dataDirectory := "data"
	postgresRaw := postgresoptions.DefaultCLIOptions()
	postgresRaw.Embedded = false

	importCmd := &cobra.Command{
		Use:     "import-legacy-data",
		Aliases: []string{"ndjson-to-postgres"},
		Short:   "Import legacy facts/state snapshots into PostgreSQL.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			postgresCompleted, store, err := openPostgresStoreForCommand(cmd.Context(), postgresRaw, postgresstore.Options{})
			if err != nil {
				return err
			}
			defer postgresCompleted.Cleanup()
			defer func() {
				_ = store.Close()
			}()

			counts, err := importLegacyFactsAndState(cmd.Context(), dataDirectory, store)
			if err != nil {
				return err
			}

			cmd.Printf("Legacy snapshot import completed.\n")
			cmd.Printf("  facts.runs: %d\n", counts.Runs)
			cmd.Printf("  facts.pull_requests: %d\n", counts.PullRequests)
			cmd.Printf("  facts.artifact_failures: %d\n", counts.ArtifactFailures)
			cmd.Printf("  facts.raw_failures: %d\n", counts.RawFailures)
			cmd.Printf("  facts.metrics_daily: %d\n", counts.MetricsDaily)
			cmd.Printf("  facts.test_metadata_daily: %d\n", counts.TestMetadataDaily)
			cmd.Printf("  state.checkpoints: %d\n", counts.Checkpoints)
			cmd.Printf("  skipped: state.dead_letters, state.phase3\n")
			return nil
		},
	}
	importCmd.Flags().StringVar(&dataDirectory, "legacy.data-dir", dataDirectory, "root directory for legacy facts/state snapshots to import")
	importCmd.Flags().StringVar(&dataDirectory, "ndjson.data-dir", dataDirectory, "deprecated alias for --legacy.data-dir")
	_ = importCmd.Flags().MarkHidden("ndjson.data-dir")
	if err := postgresoptions.BindOptions(postgresRaw, importCmd); err != nil {
		return nil, err
	}

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "One-off migration helpers for storage backends.",
	}
	cmd.AddCommand(importCmd)
	return cmd, nil
}

type legacyImportCounts struct {
	Runs              int
	PullRequests      int
	ArtifactFailures  int
	RawFailures       int
	MetricsDaily      int
	TestMetadataDaily int
	Checkpoints       int
}

func importLegacyFactsAndState(ctx context.Context, dataDirectory string, dst storecontracts.Store) (legacyImportCounts, error) {
	counts := legacyImportCounts{}
	if dst == nil {
		return counts, fmt.Errorf("destination store is required")
	}
	if ctx == nil {
		return counts, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return counts, err
	}
	dataDirectory = strings.TrimSpace(dataDirectory)
	if dataDirectory == "" {
		return counts, fmt.Errorf("data directory is required")
	}

	factsPath := filepath.Join(dataDirectory, "facts")
	statePath := filepath.Join(dataDirectory, "state")

	runs, err := readJSONLinesFile[storecontracts.RunRecord](filepath.Join(factsPath, "runs.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertRuns(ctx, runs); err != nil {
		return counts, fmt.Errorf("import facts.runs: %w", err)
	}
	counts.Runs = len(runs)

	pullRequests, err := readJSONLinesFile[storecontracts.PullRequestRecord](filepath.Join(factsPath, "pull_requests.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertPullRequests(ctx, pullRequests); err != nil {
		return counts, fmt.Errorf("import facts.pull_requests: %w", err)
	}
	counts.PullRequests = len(pullRequests)

	artifactFailures, err := readJSONLinesFile[storecontracts.ArtifactFailureRecord](filepath.Join(factsPath, "artifact_failures.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertArtifactFailures(ctx, artifactFailures); err != nil {
		return counts, fmt.Errorf("import facts.artifact_failures: %w", err)
	}
	counts.ArtifactFailures = len(artifactFailures)

	rawFailures, err := readJSONLinesFile[storecontracts.RawFailureRecord](filepath.Join(factsPath, "raw_failures.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertRawFailures(ctx, rawFailures); err != nil {
		return counts, fmt.Errorf("import facts.raw_failures: %w", err)
	}
	counts.RawFailures = len(rawFailures)

	metricsDaily, err := readJSONLinesFile[storecontracts.MetricDailyRecord](filepath.Join(factsPath, "metrics_daily.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertMetricsDaily(ctx, metricsDaily); err != nil {
		return counts, fmt.Errorf("import facts.metrics_daily: %w", err)
	}
	counts.MetricsDaily = len(metricsDaily)

	testMetadataDaily, err := readJSONLinesFile[storecontracts.TestMetadataDailyRecord](filepath.Join(factsPath, "test_metadata_daily.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertTestMetadataDaily(ctx, testMetadataDaily); err != nil {
		return counts, fmt.Errorf("import facts.test_metadata_daily: %w", err)
	}
	counts.TestMetadataDaily = len(testMetadataDaily)

	checkpoints, err := readJSONLinesFile[storecontracts.CheckpointRecord](filepath.Join(statePath, "checkpoints.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertCheckpoints(ctx, checkpoints); err != nil {
		return counts, fmt.Errorf("import state.checkpoints: %w", err)
	}
	counts.Checkpoints = len(checkpoints)

	return counts, nil
}

func readJSONLinesFile[T any](path string) ([]T, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	out := make([]T, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 50*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row T
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("decode JSON-lines row from %q: %w", path, err)
		}
		out = append(out, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	return out, nil
}
