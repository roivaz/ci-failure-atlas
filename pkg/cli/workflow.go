package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	phase2engine "ci-failure-atlas/pkg/semantic/engine/phase2"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	workflowphase1 "ci-failure-atlas/pkg/workflow/phase1"
)

type workflowBuildInputDiagnostics struct {
	RawRowsTotal             int `json:"raw_rows_total"`
	RowsIncluded             int `json:"rows_included"`
	RowsSkippedOutsideWindow int `json:"rows_skipped_outside_window"`
	RowsSkippedNonArtifact   int `json:"rows_skipped_non_artifact"`
	RowsSkippedInvalid       int `json:"rows_skipped_invalid"`
	MissingRunMetadata       int `json:"missing_run_metadata"`
	MissingOccurredAt        int `json:"missing_occurred_at"`
	MissingJobName           int `json:"missing_job_name"`
	MissingRowID             int `json:"missing_row_id"`
	MissingSignatureID       int `json:"missing_signature_id"`
	MissingRawText           int `json:"missing_raw_text"`
	MissingNormalizedText    int `json:"missing_normalized_text"`
}

type workflowBuildProfile struct {
	GeneratedAt            string                        `json:"generated_at"`
	DataDirectory          string                        `json:"data_directory"`
	SemanticSubdirectory   string                        `json:"semantic_subdirectory,omitempty"`
	WindowStart            string                        `json:"window_start,omitempty"`
	WindowEnd              string                        `json:"window_end,omitempty"`
	DebugDumpIntermediates bool                          `json:"debug_dump_intermediates"`
	StageDurationsMS       map[string]float64            `json:"stage_durations_ms"`
	TotalDurationMS        float64                       `json:"total_duration_ms"`
	Counts                 map[string]int                `json:"counts"`
	InputDiagnostics       workflowBuildInputDiagnostics `json:"input_diagnostics"`
}

func NewWorkflowCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "workflow",
		Short:         "Semantic workflow commands.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	buildOpts := workflowphase1.DefaultOptions()
	workflowProfileOutput := ""
	workflowDumpIntermediate := false
	buildCmd := &cobra.Command{
		Use:   "build",
		Short: "Run semantic workflow build (phase1 + phase2).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			totalStart := time.Now()
			stageDurations := map[string]time.Duration{}

			phase1Validated, err := buildOpts.Validate()
			if err != nil {
				return err
			}
			phase1Completed, err := phase1Validated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			defer phase1Completed.Cleanup()

			pipeline, err := phase1Completed.RunPipeline(cmd.Context())
			if err != nil {
				return fmt.Errorf("workflow build phase1 pipeline: %w", err)
			}
			stageDurations["phase1_enrich_input"] = pipeline.Timings.EnrichInput
			stageDurations["phase1_workset"] = pipeline.Timings.Workset
			stageDurations["phase1_normalize"] = pipeline.Timings.Normalize
			stageDurations["phase1_classify"] = pipeline.Timings.Classify
			stageDurations["phase1_compile"] = pipeline.Timings.Compile

			stageStart := time.Now()
			globalClusters, mergedReviewQueue, err := phase2engine.Merge(pipeline.TestClusters, pipeline.ReviewItems)
			if err != nil {
				return fmt.Errorf("workflow build phase2 merge: %w", err)
			}
			stageDurations["phase2_merge"] = time.Since(stageStart)

			stageStart = time.Now()
			if err := phase1Completed.Store.UpsertPhase1Workset(cmd.Context(), pipeline.Workset); err != nil {
				return fmt.Errorf("workflow build persist phase1 workset: %w", err)
			}
			stageDurations["persist_phase1_workset"] = time.Since(stageStart)

			if workflowDumpIntermediate {
				stageStart = time.Now()
				if err := phase1Completed.Store.UpsertPhase1Normalized(cmd.Context(), pipeline.Normalized); err != nil {
					return fmt.Errorf("workflow build persist phase1 normalized rows: %w", err)
				}
				stageDurations["persist_phase1_normalized"] = time.Since(stageStart)

				stageStart = time.Now()
				if err := phase1Completed.Store.UpsertPhase1Assignments(cmd.Context(), pipeline.Assignments); err != nil {
					return fmt.Errorf("workflow build persist phase1 assignments: %w", err)
				}
				stageDurations["persist_phase1_assignments"] = time.Since(stageStart)
			}

			stageStart = time.Now()
			if err := phase1Completed.Store.UpsertTestClusters(cmd.Context(), pipeline.TestClusters); err != nil {
				return fmt.Errorf("workflow build persist test clusters: %w", err)
			}
			stageDurations["persist_test_clusters"] = time.Since(stageStart)

			stageStart = time.Now()
			if err := phase1Completed.Store.UpsertReviewQueue(cmd.Context(), mergedReviewQueue); err != nil {
				return fmt.Errorf("workflow build persist review queue: %w", err)
			}
			stageDurations["persist_review_queue"] = time.Since(stageStart)

			stageStart = time.Now()
			if err := phase1Completed.Store.UpsertGlobalClusters(cmd.Context(), globalClusters); err != nil {
				return fmt.Errorf("workflow build persist global clusters: %w", err)
			}
			stageDurations["persist_global_clusters"] = time.Since(stageStart)

			if strings.TrimSpace(buildOpts.NDJSONOptions.SemanticSubdirectory) != "" && phase1Completed.WindowStart != nil && phase1Completed.WindowEnd != nil {
				stageStart = time.Now()
				if err := semhistory.WriteWindowMetadata(
					buildOpts.NDJSONOptions.DataDirectory,
					buildOpts.NDJSONOptions.SemanticSubdirectory,
					phase1Completed.WindowStart.UTC(),
					phase1Completed.WindowEnd.UTC(),
				); err != nil {
					return fmt.Errorf("workflow build metadata: %w", err)
				}
				stageDurations["persist_window_metadata"] = time.Since(stageStart)
			}

			logger, loggerErr := logr.FromContext(cmd.Context())
			if loggerErr == nil {
				logger.Info(
					"Completed workflow build pipeline.",
					"envs", strings.Join(phase1Completed.Environments, ","),
					"window_start", formatOptionalWorkflowTime(phase1Completed.WindowStart),
					"window_end", formatOptionalWorkflowTime(phase1Completed.WindowEnd),
					"debug_dump_intermediate", workflowDumpIntermediate,
					"workset_rows", len(pipeline.Workset),
					"normalized_rows", len(pipeline.Normalized),
					"assignments", len(pipeline.Assignments),
					"test_clusters", len(pipeline.TestClusters),
					"phase2_clusters", len(globalClusters),
					"phase2_review_items", len(mergedReviewQueue),
					"duration_total_ms", time.Since(totalStart).Milliseconds(),
					"duration_phase1_enrich_ms", pipeline.Timings.EnrichInput.Milliseconds(),
					"duration_phase1_workset_ms", pipeline.Timings.Workset.Milliseconds(),
					"duration_phase1_normalize_ms", pipeline.Timings.Normalize.Milliseconds(),
					"duration_phase1_classify_ms", pipeline.Timings.Classify.Milliseconds(),
					"duration_phase1_compile_ms", pipeline.Timings.Compile.Milliseconds(),
					"duration_phase2_merge_ms", stageDurations["phase2_merge"].Milliseconds(),
				)
			}

			profileOutputPath := strings.TrimSpace(workflowProfileOutput)
			if profileOutputPath != "" {
				profile := workflowBuildProfile{
					GeneratedAt:            time.Now().UTC().Format(time.RFC3339),
					DataDirectory:          buildOpts.NDJSONOptions.DataDirectory,
					SemanticSubdirectory:   buildOpts.NDJSONOptions.SemanticSubdirectory,
					WindowStart:            formatOptionalWorkflowTime(phase1Completed.WindowStart),
					WindowEnd:              formatOptionalWorkflowTime(phase1Completed.WindowEnd),
					DebugDumpIntermediates: workflowDumpIntermediate,
					StageDurationsMS:       workflowDurationsMilliseconds(stageDurations),
					TotalDurationMS:        workflowDurationMilliseconds(time.Since(totalStart)),
					Counts: map[string]int{
						"workset_rows":        len(pipeline.Workset),
						"normalized_rows":     len(pipeline.Normalized),
						"assignments":         len(pipeline.Assignments),
						"test_clusters":       len(pipeline.TestClusters),
						"phase2_clusters":     len(globalClusters),
						"phase2_review_items": len(mergedReviewQueue),
					},
					InputDiagnostics: workflowBuildInputDiagnostics{
						RawRowsTotal:             pipeline.Diagnostics.RawRowsTotal,
						RowsIncluded:             pipeline.Diagnostics.RowsIncluded,
						RowsSkippedOutsideWindow: pipeline.Diagnostics.RowsSkippedOutsideWindow,
						RowsSkippedNonArtifact:   pipeline.Diagnostics.RowsSkippedNonArtifact,
						RowsSkippedInvalid:       pipeline.Diagnostics.RowsSkippedInvalid,
						MissingRunMetadata:       pipeline.Diagnostics.MissingRunMetadata,
						MissingOccurredAt:        pipeline.Diagnostics.MissingOccurredAt,
						MissingJobName:           pipeline.Diagnostics.MissingJobName,
						MissingRowID:             pipeline.Diagnostics.MissingRowID,
						MissingSignatureID:       pipeline.Diagnostics.MissingSignatureID,
						MissingRawText:           pipeline.Diagnostics.MissingRawText,
						MissingNormalizedText:    pipeline.Diagnostics.MissingNormalizedText,
					},
				}
				if err := writeWorkflowBuildProfile(profileOutputPath, profile); err != nil {
					return fmt.Errorf("write workflow build profile: %w", err)
				}
			}
			return nil
		},
	}
	if err := workflowphase1.BindOptions(buildOpts, buildCmd); err != nil {
		return nil, fmt.Errorf("failed to bind workflow build options: %w", err)
	}
	buildCmd.Flags().BoolVar(&workflowDumpIntermediate, "workflow.debug-dump-intermediate", workflowDumpIntermediate, "persist phase1 normalized and assignment intermediate artifacts for debugging")
	buildCmd.Flags().StringVar(&workflowProfileOutput, "workflow.profile-output", workflowProfileOutput, "optional path to write workflow build stage timings/profile as JSON")
	cmd.AddCommand(buildCmd)

	for _, sub := range []string{
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

func workflowDurationMilliseconds(duration time.Duration) float64 {
	return float64(duration.Nanoseconds()) / float64(time.Millisecond)
}

func workflowDurationsMilliseconds(values map[string]time.Duration) map[string]float64 {
	out := make(map[string]float64, len(values))
	for key, value := range values {
		out[key] = workflowDurationMilliseconds(value)
	}
	return out
}

func writeWorkflowBuildProfile(path string, profile workflowBuildProfile) error {
	trimmedPath := strings.TrimSpace(path)
	if trimmedPath == "" {
		return fmt.Errorf("profile output path is required")
	}
	if err := os.MkdirAll(filepath.Dir(trimmedPath), 0o755); err != nil {
		return fmt.Errorf("create profile output directory: %w", err)
	}
	payload, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal workflow build profile: %w", err)
	}
	payload = append(payload, '\n')
	if err := os.WriteFile(trimmedPath, payload, 0o644); err != nil {
		return fmt.Errorf("write workflow build profile %q: %w", trimmedPath, err)
	}
	return nil
}

func formatOptionalWorkflowTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}
