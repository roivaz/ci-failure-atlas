package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"ci-failure-atlas/pkg/ndjsonoptions"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"

	"github.com/spf13/cobra"
)

func NewMigrateCommand() (*cobra.Command, error) {
	ndjsonRaw := ndjsonoptions.DefaultOptions()
	postgresRaw := postgresoptions.DefaultOptions()
	postgresRaw.Enabled = true
	postgresRaw.Initialize = true

	importCmd := &cobra.Command{
		Use:   "ndjson-to-postgres",
		Short: "Import facts/state data from NDJSON into PostgreSQL.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ndjsonValidated, err := ndjsonRaw.Validate()
			if err != nil {
				return err
			}
			ndjsonCompleted, err := ndjsonValidated.Complete(cmd.Context())
			if err != nil {
				return err
			}

			postgresValidated, err := postgresRaw.Validate()
			if err != nil {
				return err
			}
			if !postgresValidated.Enabled {
				return fmt.Errorf("--storage.postgres.enabled must be true")
			}
			postgresCompleted, err := postgresValidated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			defer postgresCompleted.Cleanup()

			store, err := postgresstore.New(postgresCompleted.Connection, postgresstore.Options{})
			if err != nil {
				return fmt.Errorf("create postgres store: %w", err)
			}
			defer func() {
				_ = store.Close()
			}()

			counts, err := importNDJSONFactsAndState(cmd.Context(), ndjsonCompleted.DataDirectory, store)
			if err != nil {
				return err
			}

			cmd.Printf("NDJSON -> PostgreSQL import completed.\n")
			cmd.Printf("  facts.runs: %d\n", counts.Runs)
			cmd.Printf("  facts.pull_requests: %d\n", counts.PullRequests)
			cmd.Printf("  facts.artifact_failures: %d\n", counts.ArtifactFailures)
			cmd.Printf("  facts.raw_failures: %d\n", counts.RawFailures)
			cmd.Printf("  facts.metrics_daily: %d\n", counts.MetricsDaily)
			cmd.Printf("  facts.test_metadata_daily: %d\n", counts.TestMetadataDaily)
			cmd.Printf("  state.checkpoints: %d\n", counts.Checkpoints)
			cmd.Printf("  state.phase3.issues: %d\n", counts.Phase3Issues)
			cmd.Printf("  state.phase3.links: %d\n", counts.Phase3Links)
			cmd.Printf("  skipped: state.dead_letters, state.phase3.events\n")
			return nil
		},
	}
	importCmd.Flags().StringVar(&ndjsonRaw.SemanticSubdirectory, "storage.ndjson.semantic-subdir", ndjsonRaw.SemanticSubdirectory, "unused by this importer; kept for consistency with storage flags")
	importCmd.Flags().StringVar(&ndjsonRaw.DataDirectory, "storage.ndjson.data-dir", ndjsonRaw.DataDirectory, "root directory for NDJSON facts/state data to import")
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

type ndjsonImportCounts struct {
	Runs              int
	PullRequests      int
	ArtifactFailures  int
	RawFailures       int
	MetricsDaily      int
	TestMetadataDaily int
	Checkpoints       int
	Phase3Issues      int
	Phase3Links       int
}

func importNDJSONFactsAndState(ctx context.Context, dataDirectory string, dst storecontracts.Store) (ndjsonImportCounts, error) {
	counts := ndjsonImportCounts{}
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

	runs, err := readNDJSONFile[storecontracts.RunRecord](filepath.Join(factsPath, "runs.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertRuns(ctx, runs); err != nil {
		return counts, fmt.Errorf("import facts.runs: %w", err)
	}
	counts.Runs = len(runs)

	pullRequests, err := readNDJSONFile[storecontracts.PullRequestRecord](filepath.Join(factsPath, "pull_requests.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertPullRequests(ctx, pullRequests); err != nil {
		return counts, fmt.Errorf("import facts.pull_requests: %w", err)
	}
	counts.PullRequests = len(pullRequests)

	artifactFailures, err := readNDJSONFile[storecontracts.ArtifactFailureRecord](filepath.Join(factsPath, "artifact_failures.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertArtifactFailures(ctx, artifactFailures); err != nil {
		return counts, fmt.Errorf("import facts.artifact_failures: %w", err)
	}
	counts.ArtifactFailures = len(artifactFailures)

	rawFailures, err := readNDJSONFile[storecontracts.RawFailureRecord](filepath.Join(factsPath, "raw_failures.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertRawFailures(ctx, rawFailures); err != nil {
		return counts, fmt.Errorf("import facts.raw_failures: %w", err)
	}
	counts.RawFailures = len(rawFailures)

	metricsDaily, err := readNDJSONFile[storecontracts.MetricDailyRecord](filepath.Join(factsPath, "metrics_daily.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertMetricsDaily(ctx, metricsDaily); err != nil {
		return counts, fmt.Errorf("import facts.metrics_daily: %w", err)
	}
	counts.MetricsDaily = len(metricsDaily)

	testMetadataDaily, err := readNDJSONFile[storecontracts.TestMetadataDailyRecord](filepath.Join(factsPath, "test_metadata_daily.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertTestMetadataDaily(ctx, testMetadataDaily); err != nil {
		return counts, fmt.Errorf("import facts.test_metadata_daily: %w", err)
	}
	counts.TestMetadataDaily = len(testMetadataDaily)

	checkpoints, err := readNDJSONFile[storecontracts.CheckpointRecord](filepath.Join(statePath, "checkpoints.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertCheckpoints(ctx, checkpoints); err != nil {
		return counts, fmt.Errorf("import state.checkpoints: %w", err)
	}
	counts.Checkpoints = len(checkpoints)

	phase3Issues, err := readNDJSONFile[semanticcontracts.Phase3IssueRecord](filepath.Join(statePath, "phase3", "issues.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertPhase3Issues(ctx, phase3Issues); err != nil {
		return counts, fmt.Errorf("import state.phase3.issues: %w", err)
	}
	counts.Phase3Issues = len(phase3Issues)

	phase3Links, err := readNDJSONFile[semanticcontracts.Phase3LinkRecord](filepath.Join(statePath, "phase3", "links.ndjson"))
	if err != nil {
		return counts, err
	}
	if err := dst.UpsertPhase3Links(ctx, phase3Links); err != nil {
		return counts, fmt.Errorf("import state.phase3.links: %w", err)
	}
	counts.Phase3Links = len(phase3Links)

	return counts, nil
}

func readNDJSONFile[T any](path string) ([]T, error) {
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
			return nil, fmt.Errorf("decode ndjson row from %q: %w", path, err)
		}
		out = append(out, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	return out, nil
}
