package phase1

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/ndjsonoptions"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	phase1engine "ci-failure-atlas/pkg/semantic/engine/phase1"
	semanticinput "ci-failure-atlas/pkg/semantic/input"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

func DefaultOptions() *RawOptions {
	return &RawOptions{
		NDJSONOptions:   ndjsonoptions.DefaultOptions(),
		PostgresOptions: postgresoptions.DefaultOptions(),
		Environments:    []string{"dev"},
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	if opts.NDJSONOptions == nil {
		opts.NDJSONOptions = ndjsonoptions.DefaultOptions()
	}
	if opts.PostgresOptions == nil {
		opts.PostgresOptions = postgresoptions.DefaultOptions()
	}
	if err := ndjsonoptions.BindNDJSONOptions(opts.NDJSONOptions, cmd); err != nil {
		return err
	}
	if err := postgresoptions.BindOptions(opts.PostgresOptions, cmd); err != nil {
		return err
	}
	cmd.Flags().StringSliceVar(&opts.Environments, "source.envs", opts.Environments, "Environments to include (allowed: dev,int,stg,prod).")
	cmd.Flags().StringVar(&opts.WindowStart, "workflow.window.start", opts.WindowStart, "Inclusive start of semantic window (RFC3339 or YYYY-MM-DD at 00:00:00Z).")
	cmd.Flags().StringVar(&opts.WindowEnd, "workflow.window.end", opts.WindowEnd, "Exclusive end of semantic window (RFC3339 or YYYY-MM-DD interpreted as next-day 00:00:00Z).")
	return nil
}

type RawOptions struct {
	NDJSONOptions   *ndjsonoptions.RawOptions
	PostgresOptions *postgresoptions.RawOptions
	Environments    []string
	WindowStart     string
	WindowEnd       string
}

type validatedOptions struct {
	*RawOptions
	NDJSONValidated   *ndjsonoptions.ValidatedOptions
	PostgresValidated *postgresoptions.ValidatedOptions
	Environments      []string
	EnvironmentSet    map[string]struct{}
	WindowStart       *time.Time
	WindowEnd         *time.Time
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	NDJSON         *ndjsonoptions.Options
	Postgres       *postgresoptions.Options
	Store          storecontracts.Store
	Environments   []string
	EnvironmentSet map[string]struct{}
	WindowStart    *time.Time
	WindowEnd      *time.Time
}

type Options struct {
	*completedOptions
}

type PipelineTimings struct {
	EnrichInput time.Duration
	Workset     time.Duration
	Normalize   time.Duration
	Classify    time.Duration
	Compile     time.Duration
}

type PipelineResult struct {
	Diagnostics  semanticinput.Diagnostics
	Workset      []semanticcontracts.Phase1WorksetRecord
	Normalized   []semanticcontracts.Phase1NormalizedRecord
	Assignments  []semanticcontracts.Phase1AssignmentRecord
	TestClusters []semanticcontracts.TestClusterRecord
	ReviewItems  []semanticcontracts.ReviewItemRecord
	Timings      PipelineTimings
}

var supportedWorkflowEnvironments = []string{"dev", "int", "stg", "prod"}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	if o.PostgresOptions == nil {
		o.PostgresOptions = postgresoptions.DefaultOptions()
	}
	postgresValidated, err := o.PostgresOptions.Validate()
	if err != nil {
		return nil, err
	}

	var ndjsonValidated *ndjsonoptions.ValidatedOptions
	if !postgresValidated.Enabled {
		if o.NDJSONOptions == nil {
			o.NDJSONOptions = ndjsonoptions.DefaultOptions()
		}
		ndjsonValidated, err = o.NDJSONOptions.Validate()
		if err != nil {
			return nil, err
		}
	}
	environments, environmentSet, err := normalizeWorkflowEnvironments(o.Environments)
	if err != nil {
		return nil, err
	}
	windowStart, windowEnd, err := parseWorkflowWindow(o.WindowStart, o.WindowEnd)
	if err != nil {
		return nil, err
	}
	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:        o,
			NDJSONValidated:   ndjsonValidated,
			PostgresValidated: postgresValidated,
			Environments:      environments,
			EnvironmentSet:    environmentSet,
			WindowStart:       windowStart,
			WindowEnd:         windowEnd,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(ctx context.Context) (*Options, error) {
	var (
		ndjsonCompleted   *ndjsonoptions.Options
		postgresCompleted *postgresoptions.Options
		store             storecontracts.Store
		err               error
	)
	if o.PostgresValidated != nil && o.PostgresValidated.Enabled {
		postgresCompleted, err = o.PostgresValidated.Complete(ctx)
		if err != nil {
			return nil, err
		}
		store, err = postgresstore.New(postgresCompleted.Connection, postgresstore.Options{
			SemanticSubdirectory: postgresCompleted.SemanticSubdirectory,
		})
		if err != nil {
			postgresCompleted.Cleanup()
			return nil, fmt.Errorf("create postgres store: %w", err)
		}
	} else {
		ndjsonCompleted, err = o.NDJSONValidated.Complete(ctx)
		if err != nil {
			return nil, err
		}
		store, err = ndjson.NewWithOptions(ndjsonCompleted.DataDirectory, ndjson.Options{
			SemanticSubdirectory: ndjsonCompleted.SemanticSubdirectory,
		})
		if err != nil {
			return nil, fmt.Errorf("create NDJSON store: %w", err)
		}
	}
	return &Options{
		completedOptions: &completedOptions{
			NDJSON:         ndjsonCompleted,
			Postgres:       postgresCompleted,
			Store:          store,
			Environments:   append([]string(nil), o.Environments...),
			EnvironmentSet: copyStringSet(o.EnvironmentSet),
			WindowStart:    cloneTimePointer(o.WindowStart),
			WindowEnd:      cloneTimePointer(o.WindowEnd),
		},
	}, nil
}

func (o *Options) Cleanup() {
	if o.Postgres != nil {
		o.Postgres.Cleanup()
		return
	}
	if o.Store != nil {
		_ = o.Store.Close()
	}
}

func (o *Options) RunPipeline(ctx context.Context) (PipelineResult, error) {
	if o == nil {
		return PipelineResult{}, fmt.Errorf("options are required")
	}
	start := time.Now()
	enriched, err := semanticinput.BuildEnrichedFailures(ctx, o.Store, semanticinput.BuildOptions{
		EnvironmentSet: copyStringSet(o.EnvironmentSet),
		WindowStart:    cloneTimePointer(o.WindowStart),
		WindowEnd:      cloneTimePointer(o.WindowEnd),
	})
	if err != nil {
		return PipelineResult{}, fmt.Errorf("build enriched semantic input rows: %w", err)
	}
	enrichDuration := time.Since(start)

	start = time.Now()
	workset := phase1engine.BuildWorkset(enriched.Rows)
	worksetDuration := time.Since(start)

	start = time.Now()
	normalized := phase1engine.Normalize(workset)
	normalizeDuration := time.Since(start)

	start = time.Now()
	assignments := phase1engine.Classify(normalized)
	classifyDuration := time.Since(start)

	start = time.Now()
	testClusters, reviewItems, err := phase1engine.Compile(workset, assignments)
	if err != nil {
		return PipelineResult{}, fmt.Errorf("compile phase1 outputs: %w", err)
	}
	compileDuration := time.Since(start)

	return PipelineResult{
		Diagnostics:  enriched.Diagnostics,
		Workset:      workset,
		Normalized:   normalized,
		Assignments:  assignments,
		TestClusters: testClusters,
		ReviewItems:  reviewItems,
		Timings: PipelineTimings{
			EnrichInput: enrichDuration,
			Workset:     worksetDuration,
			Normalize:   normalizeDuration,
			Classify:    classifyDuration,
			Compile:     compileDuration,
		},
	}, nil
}

func (o *Options) Run(ctx context.Context) error {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer o.Cleanup()

	pipeline, err := o.RunPipeline(ctx)
	if err != nil {
		return err
	}

	if err := o.Store.UpsertPhase1Workset(ctx, pipeline.Workset); err != nil {
		return fmt.Errorf("upsert phase1 workset: %w", err)
	}
	if err := o.Store.UpsertPhase1Normalized(ctx, pipeline.Normalized); err != nil {
		return fmt.Errorf("upsert phase1 normalized rows: %w", err)
	}
	if err := o.Store.UpsertPhase1Assignments(ctx, pipeline.Assignments); err != nil {
		return fmt.Errorf("upsert phase1 assignments: %w", err)
	}
	if err := o.Store.UpsertTestClusters(ctx, pipeline.TestClusters); err != nil {
		return fmt.Errorf("upsert test clusters: %w", err)
	}
	if err := o.Store.UpsertReviewQueue(ctx, pipeline.ReviewItems); err != nil {
		return fmt.Errorf("upsert review queue: %w", err)
	}

	logger.Info(
		"Completed workflow phase1 semantic pipeline.",
		"envs", strings.Join(o.Environments, ","),
		"window_start", formatOptionalTime(o.WindowStart),
		"window_end", formatOptionalTime(o.WindowEnd),
		"raw_rows_total", pipeline.Diagnostics.RawRowsTotal,
		"rows_included", pipeline.Diagnostics.RowsIncluded,
		"rows_skipped_outside_window", pipeline.Diagnostics.RowsSkippedOutsideWindow,
		"rows_skipped_non_artifact", pipeline.Diagnostics.RowsSkippedNonArtifact,
		"rows_skipped_invalid", pipeline.Diagnostics.RowsSkippedInvalid,
		"workset_rows", len(pipeline.Workset),
		"normalized_rows", len(pipeline.Normalized),
		"assignments", len(pipeline.Assignments),
		"test_clusters", len(pipeline.TestClusters),
		"review_items", len(pipeline.ReviewItems),
		"duration_enrich_input_ms", pipeline.Timings.EnrichInput.Milliseconds(),
		"duration_workset_ms", pipeline.Timings.Workset.Milliseconds(),
		"duration_normalize_ms", pipeline.Timings.Normalize.Milliseconds(),
		"duration_classify_ms", pipeline.Timings.Classify.Milliseconds(),
		"duration_compile_ms", pipeline.Timings.Compile.Milliseconds(),
	)
	return nil
}

func normalizeWorkflowEnvironments(raw []string) ([]string, map[string]struct{}, error) {
	set := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, value := range raw {
		normalized := workflowNormalizeEnvironment(value)
		if normalized == "" {
			continue
		}
		if !slices.Contains(supportedWorkflowEnvironments, normalized) {
			return nil, nil, fmt.Errorf("unsupported environment %q for --source.envs (allowed: %s)", value, strings.Join(supportedWorkflowEnvironments, ","))
		}
		if _, exists := set[normalized]; exists {
			continue
		}
		set[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return nil, nil, fmt.Errorf("at least one environment must be provided with --source.envs (allowed: %s)", strings.Join(supportedWorkflowEnvironments, ","))
	}
	sort.Strings(out)
	return out, set, nil
}

func parseWorkflowWindow(rawStart, rawEnd string) (*time.Time, *time.Time, error) {
	startRaw := strings.TrimSpace(rawStart)
	endRaw := strings.TrimSpace(rawEnd)
	if startRaw == "" && endRaw == "" {
		return nil, nil, nil
	}
	if startRaw == "" || endRaw == "" {
		return nil, nil, fmt.Errorf("both --workflow.window.start and --workflow.window.end must be set together")
	}
	start, err := parseWorkflowBoundary(startRaw, false)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid --workflow.window.start value: %w", err)
	}
	end, err := parseWorkflowBoundary(endRaw, true)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid --workflow.window.end value: %w", err)
	}
	if !start.Before(end) {
		return nil, nil, fmt.Errorf("workflow window start must be before end (start=%s end=%s)", start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	return &start, &end, nil
}

func parseWorkflowBoundary(raw string, endBoundary bool) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("empty boundary")
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse("2006-01-02", trimmed); err == nil {
		utc := parsed.UTC()
		if endBoundary {
			utc = utc.Add(24 * time.Hour)
		}
		return utc, nil
	}
	return time.Time{}, fmt.Errorf("unsupported time format %q (use RFC3339 or YYYY-MM-DD)", raw)
}

func copyStringSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for key := range in {
		out[key] = struct{}{}
	}
	return out
}

func cloneTimePointer(in *time.Time) *time.Time {
	if in == nil {
		return nil
	}
	cloned := in.UTC()
	return &cloned
}

func workflowNormalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}
