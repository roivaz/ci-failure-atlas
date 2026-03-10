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
	phase1engine "ci-failure-atlas/pkg/semantic/engine/phase1"
	semanticinput "ci-failure-atlas/pkg/semantic/input"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func DefaultOptions() *RawOptions {
	return &RawOptions{
		NDJSONOptions: ndjsonoptions.DefaultOptions(),
		Environments:  []string{"dev"},
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	if err := ndjsonoptions.BindNDJSONOptions(opts.NDJSONOptions, cmd); err != nil {
		return err
	}
	cmd.Flags().StringSliceVar(&opts.Environments, "source.envs", opts.Environments, "Environments to include (allowed: dev,int,stg,prod).")
	cmd.Flags().StringVar(&opts.WindowStart, "workflow.window.start", opts.WindowStart, "Inclusive start of semantic window (RFC3339 or YYYY-MM-DD at 00:00:00Z).")
	cmd.Flags().StringVar(&opts.WindowEnd, "workflow.window.end", opts.WindowEnd, "Exclusive end of semantic window (RFC3339 or YYYY-MM-DD interpreted as next-day 00:00:00Z).")
	return nil
}

type RawOptions struct {
	NDJSONOptions *ndjsonoptions.RawOptions
	Environments  []string
	WindowStart   string
	WindowEnd     string
}

type validatedOptions struct {
	*RawOptions
	NDJSONValidated *ndjsonoptions.ValidatedOptions
	Environments    []string
	EnvironmentSet  map[string]struct{}
	WindowStart     *time.Time
	WindowEnd       *time.Time
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	NDJSON         *ndjsonoptions.Options
	Store          storecontracts.Store
	Environments   []string
	EnvironmentSet map[string]struct{}
	WindowStart    *time.Time
	WindowEnd      *time.Time
}

type Options struct {
	*completedOptions
}

var supportedWorkflowEnvironments = []string{"dev", "int", "stg", "prod"}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	ndjsonValidated, err := o.NDJSONOptions.Validate()
	if err != nil {
		return nil, err
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
			RawOptions:      o,
			NDJSONValidated: ndjsonValidated,
			Environments:    environments,
			EnvironmentSet:  environmentSet,
			WindowStart:     windowStart,
			WindowEnd:       windowEnd,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(ctx context.Context) (*Options, error) {
	ndjsonCompleted, err := o.NDJSONValidated.Complete(ctx)
	if err != nil {
		return nil, err
	}
	store, err := ndjson.New(ndjsonCompleted.DataDirectory)
	if err != nil {
		return nil, fmt.Errorf("create NDJSON store: %w", err)
	}
	return &Options{
		completedOptions: &completedOptions{
			NDJSON:         ndjsonCompleted,
			Store:          store,
			Environments:   append([]string(nil), o.Environments...),
			EnvironmentSet: copyStringSet(o.EnvironmentSet),
			WindowStart:    cloneTimePointer(o.WindowStart),
			WindowEnd:      cloneTimePointer(o.WindowEnd),
		},
	}, nil
}

func (o *Options) Cleanup() {
	if o.Store != nil {
		_ = o.Store.Close()
	}
}

func (o *Options) Run(ctx context.Context) error {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer o.Cleanup()

	enriched, err := semanticinput.BuildEnrichedFailures(ctx, o.Store, semanticinput.BuildOptions{
		EnvironmentSet: copyStringSet(o.EnvironmentSet),
		WindowStart:    cloneTimePointer(o.WindowStart),
		WindowEnd:      cloneTimePointer(o.WindowEnd),
	})
	if err != nil {
		return fmt.Errorf("build enriched semantic input rows: %w", err)
	}

	workset := phase1engine.BuildWorkset(enriched.Rows)
	normalized := phase1engine.Normalize(workset)
	assignments := phase1engine.Classify(normalized)
	testClusters, reviewItems, err := phase1engine.Compile(workset, assignments)
	if err != nil {
		return fmt.Errorf("compile phase1 outputs: %w", err)
	}

	if err := o.Store.UpsertPhase1Workset(ctx, workset); err != nil {
		return fmt.Errorf("upsert phase1 workset: %w", err)
	}
	if err := o.Store.UpsertPhase1Normalized(ctx, normalized); err != nil {
		return fmt.Errorf("upsert phase1 normalized rows: %w", err)
	}
	if err := o.Store.UpsertPhase1Assignments(ctx, assignments); err != nil {
		return fmt.Errorf("upsert phase1 assignments: %w", err)
	}
	if err := o.Store.UpsertTestClusters(ctx, testClusters); err != nil {
		return fmt.Errorf("upsert test clusters: %w", err)
	}
	if err := o.Store.UpsertReviewQueue(ctx, reviewItems); err != nil {
		return fmt.Errorf("upsert review queue: %w", err)
	}

	logger.Info(
		"Completed workflow phase1 semantic pipeline.",
		"envs", strings.Join(o.Environments, ","),
		"window_start", formatOptionalTime(o.WindowStart),
		"window_end", formatOptionalTime(o.WindowEnd),
		"raw_rows_total", enriched.Diagnostics.RawRowsTotal,
		"rows_included", enriched.Diagnostics.RowsIncluded,
		"rows_skipped_outside_window", enriched.Diagnostics.RowsSkippedOutsideWindow,
		"rows_skipped_non_artifact", enriched.Diagnostics.RowsSkippedNonArtifact,
		"rows_skipped_invalid", enriched.Diagnostics.RowsSkippedInvalid,
		"workset_rows", len(workset),
		"normalized_rows", len(normalized),
		"assignments", len(assignments),
		"test_clusters", len(testClusters),
		"review_items", len(reviewItems),
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
