package workflow

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	phase1engine "ci-failure-atlas/pkg/semantic/engine/phase1"
	phase2engine "ci-failure-atlas/pkg/semantic/engine/phase2"
	semanticinput "ci-failure-atlas/pkg/semantic/input"
	sourceoptions "ci-failure-atlas/pkg/source/options"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

var supportedEnvironments = sourceoptions.SupportedEnvironments()

type RunOptions struct {
	Environments []string
	WindowStart  *time.Time
	WindowEnd    *time.Time
}

type Phase1Timings struct {
	EnrichInput time.Duration
	Workset     time.Duration
	Normalize   time.Duration
	Classify    time.Duration
	Compile     time.Duration
}

type Phase1Result struct {
	Diagnostics  semanticinput.Diagnostics
	Workset      []semanticcontracts.Phase1WorksetRecord
	Normalized   []semanticcontracts.Phase1NormalizedRecord
	Assignments  []semanticcontracts.Phase1AssignmentRecord
	TestClusters []semanticcontracts.TestClusterRecord
	ReviewQueue  []semanticcontracts.ReviewItemRecord
	Timings      Phase1Timings
}

type Phase2Result struct {
	FailurePatterns []semanticcontracts.FailurePatternRecord
	ReviewQueue     []semanticcontracts.ReviewItemRecord
}

type Result struct {
	Phase1 Phase1Result
	Phase2 Phase2Result
}

func NormalizeEnvironments(raw []string) ([]string, map[string]struct{}, error) {
	set := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, value := range raw {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if !slices.Contains(supportedEnvironments, normalized) {
			return nil, nil, fmt.Errorf("unsupported environment %q (allowed: %s)", value, strings.Join(supportedEnvironments, ","))
		}
		if _, exists := set[normalized]; exists {
			continue
		}
		set[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return nil, nil, fmt.Errorf("at least one environment must be provided (allowed: %s)", strings.Join(supportedEnvironments, ","))
	}
	sort.Strings(out)
	return out, set, nil
}

func RunPhase1(ctx context.Context, store storecontracts.Store, opts RunOptions) (Phase1Result, error) {
	if store == nil {
		return Phase1Result{}, fmt.Errorf("store is required")
	}
	_, environmentSet, err := resolveEnvironmentSet(opts.Environments)
	if err != nil {
		return Phase1Result{}, err
	}

	start := time.Now()
	enriched, err := semanticinput.BuildEnrichedFailures(ctx, store, semanticinput.BuildOptions{
		EnvironmentSet: environmentSet,
		WindowStart:    cloneTimePointer(opts.WindowStart),
		WindowEnd:      cloneTimePointer(opts.WindowEnd),
	})
	if err != nil {
		return Phase1Result{}, fmt.Errorf("build enriched semantic input rows: %w", err)
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
	testClusters, reviewQueue, err := phase1engine.Compile(workset, assignments)
	if err != nil {
		return Phase1Result{}, fmt.Errorf("compile phase1 outputs: %w", err)
	}
	compileDuration := time.Since(start)
	return Phase1Result{
		Diagnostics:  enriched.Diagnostics,
		Workset:      workset,
		Normalized:   normalized,
		Assignments:  assignments,
		TestClusters: testClusters,
		ReviewQueue:  reviewQueue,
		Timings: Phase1Timings{
			EnrichInput: enrichDuration,
			Workset:     worksetDuration,
			Normalize:   normalizeDuration,
			Classify:    classifyDuration,
			Compile:     compileDuration,
		},
	}, nil
}

func RunPhase2(phase1Result Phase1Result) (Phase2Result, error) {
	globalClusters, mergedReviewQueue, err := phase2engine.Merge(phase1Result.TestClusters, phase1Result.ReviewQueue)
	if err != nil {
		return Phase2Result{}, fmt.Errorf("merge phase2 clusters: %w", err)
	}
	return Phase2Result{
		FailurePatterns: globalClusters,
		ReviewQueue:     mergedReviewQueue,
	}, nil
}

func Run(ctx context.Context, store storecontracts.Store, opts RunOptions) (Result, error) {
	phase1Result, err := RunPhase1(ctx, store, opts)
	if err != nil {
		return Result{}, err
	}
	phase2Result, err := RunPhase2(phase1Result)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Phase1: phase1Result,
		Phase2: phase2Result,
	}, nil
}

func MaterializeWeek(ctx context.Context, store storecontracts.Store, weekStart time.Time) (Result, error) {
	if store == nil {
		return Result{}, fmt.Errorf("store is required")
	}
	normalizedWeekStart, err := normalizeWeekStart(weekStart)
	if err != nil {
		return Result{}, err
	}
	weekEnd := normalizedWeekStart.AddDate(0, 0, 7)
	result, err := Run(ctx, store, RunOptions{
		Environments: append([]string(nil), supportedEnvironments...),
		WindowStart:  &normalizedWeekStart,
		WindowEnd:    &weekEnd,
	})
	if err != nil {
		return Result{}, err
	}
	if err := store.ReplaceMaterializedWeek(ctx, storecontracts.MaterializedWeek{
		FailurePatterns: result.Phase2.FailurePatterns,
		ReviewQueue:     result.Phase2.ReviewQueue,
	}); err != nil {
		return Result{}, fmt.Errorf("replace materialized week: %w", err)
	}
	return result, nil
}

func resolveEnvironmentSet(raw []string) ([]string, map[string]struct{}, error) {
	if len(raw) == 0 {
		raw = supportedEnvironments
	}
	return NormalizeEnvironments(raw)
}

func normalizeWeekStart(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, fmt.Errorf("week start is required")
	}
	normalized := time.Date(value.UTC().Year(), value.UTC().Month(), value.UTC().Day(), 0, 0, 0, 0, time.UTC)
	if normalized.Weekday() != time.Monday {
		return time.Time{}, fmt.Errorf("week start %q must be a Monday", normalized.Format("2006-01-02"))
	}
	return normalized, nil
}

func cloneTimePointer(in *time.Time) *time.Time {
	if in == nil {
		return nil
	}
	cloned := in.UTC()
	return &cloned
}
