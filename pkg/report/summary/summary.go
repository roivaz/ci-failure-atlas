package summary

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-logr/logr"

	frontservice "ci-failure-atlas/pkg/frontend/service"
	"ci-failure-atlas/pkg/report/triagehtml"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
)

type Options struct {
	TriagePath          string
	TestPath            string
	ReviewPath          string
	OutputPath          string
	Format              string
	Top                 int
	MinPercent          float64
	Environments        []string
	SplitByEnvironment  bool
	Week                string
	HistoryHorizonWeeks int
	HistoryResolver     semhistory.GlobalSignatureResolver
	Chrome              triagehtml.ReportChromeOptions
}

const (
	reportFormatHTML = "html"
)

func DefaultOptions() Options {
	return Options{
		OutputPath:          "data/reports/triage-report.html",
		Format:              reportFormatHTML,
		Top:                 10,
		MinPercent:          1.0,
		SplitByEnvironment:  false,
		HistoryHorizonWeeks: 4,
	}
}

type triageReference = frontservice.TriageReportReference
type triageContributingTest = frontservice.TriageReportContributingTest
type triageCluster = frontservice.TriageReportCluster

func Run(ctx context.Context, args []string) error {
	_ = ctx
	_ = args
	return fmt.Errorf("report summary Run(args) is not wired; use Generate with an injected store")
}

func Generate(ctx context.Context, store storecontracts.Store, opts Options) error {
	validated, err := validateOptions(opts)
	if err != nil {
		return err
	}
	if store == nil {
		return errors.New("store is required")
	}

	logger := loggerFromContext(ctx).WithValues("component", "report.summary")

	data, err := frontservice.BuildTriageReportData(ctx, store, frontservice.TriageReportBuildOptions{
		Week:                validated.Week,
		Environments:        validated.Environments,
		HistoryHorizonWeeks: validated.HistoryHorizonWeeks,
		HistoryResolver:     validated.HistoryResolver,
	})
	if err != nil {
		return err
	}

	var report string
	report = buildTriageReportHTML(
		data.TriageClusters,
		validated.Top,
		validated.MinPercent,
		data.GeneratedAt,
		validated.Environments,
		data.OverallJobsByEnvironment,
		data.WindowStartRaw,
		data.WindowEndRaw,
		data.HistoryResolver,
		validated.Chrome,
	)
	if validated.SplitByEnvironment {
		targetEnvs := append([]string(nil), data.TargetEnvironments...)
		if len(targetEnvs) == 0 {
			targetEnvs = []string{"unknown"}
		}
		for _, environment := range targetEnvs {
			filteredTriageRows := filterTriageClustersByEnvironment(data.TriageClusters, environment)
			report := buildTriageReportHTML(
				filteredTriageRows,
				validated.Top,
				validated.MinPercent,
				data.GeneratedAt,
				[]string{environment},
				data.OverallJobsByEnvironment,
				data.WindowStartRaw,
				data.WindowEndRaw,
				data.HistoryResolver,
				validated.Chrome,
			)
			outputPath := outputPathForEnvironment(validated.OutputPath, environment)
			if err := writeSummary(outputPath, report); err != nil {
				return err
			}
			logger.Info(
				"Wrote triage summary report.",
				"output", outputPath,
				"format", reportFormatHTML,
				"environment", environment,
				"triageClusters", len(filteredTriageRows),
				"testClusters", data.TestClusterCountsByEnvironment[environment],
				"reviewItems", data.ReviewItemCountsByEnvironment[environment],
				"top", validated.Top,
				"minPercent", validated.MinPercent,
			)
		}
		return nil
	}

	filteredTriageRows := data.TriageClusters
	if len(validated.Environments) > 0 {
		envSet := make(map[string]struct{}, len(validated.Environments))
		for _, environment := range validated.Environments {
			envSet[normalizeReportEnvironment(environment)] = struct{}{}
		}
		filteredTriageRows = filterTriageClustersByEnvironmentSet(data.TriageClusters, envSet)
		report = buildTriageReportHTML(
			filteredTriageRows,
			validated.Top,
			validated.MinPercent,
			data.GeneratedAt,
			validated.Environments,
			data.OverallJobsByEnvironment,
			data.WindowStartRaw,
			data.WindowEndRaw,
			data.HistoryResolver,
			validated.Chrome,
		)
	}
	if err := writeSummary(validated.OutputPath, report); err != nil {
		return err
	}
	logger.Info(
		"Wrote triage summary report.",
		"output", validated.OutputPath,
		"format", reportFormatHTML,
		"triageClusters", len(filteredTriageRows),
		"testClusters", totalCountForEnvironments(data.TestClusterCountsByEnvironment, validated.Environments),
		"reviewItems", totalCountForEnvironments(data.ReviewItemCountsByEnvironment, validated.Environments),
		"top", validated.Top,
		"minPercent", validated.MinPercent,
	)
	return nil
}

func GenerateHTML(ctx context.Context, store storecontracts.Store, opts Options) (string, error) {
	tmp, err := os.CreateTemp("", "cfa-summary-*.html")
	if err != nil {
		return "", fmt.Errorf("create temp summary output: %w", err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	opts.OutputPath = tmpPath
	opts.Format = reportFormatHTML
	if err := Generate(ctx, store, opts); err != nil {
		return "", err
	}
	content, err := os.ReadFile(tmpPath)
	if err != nil {
		return "", fmt.Errorf("read generated summary output: %w", err)
	}
	return string(content), nil
}

func validateOptions(opts Options) (Options, error) {
	if strings.TrimSpace(opts.OutputPath) == "" {
		return Options{}, errors.New("missing --output path")
	}
	switch strings.ToLower(strings.TrimSpace(opts.Format)) {
	case "", reportFormatHTML:
		opts.Format = reportFormatHTML
	default:
		return Options{}, fmt.Errorf("invalid --format %q (expected html)", strings.TrimSpace(opts.Format))
	}
	if opts.Top <= 0 {
		return Options{}, errors.New("--top must be > 0")
	}
	if opts.MinPercent < 0 {
		return Options{}, errors.New("--min-percent must be >= 0")
	}
	week, err := postgresstore.NormalizeWeek(opts.Week)
	if err != nil {
		return Options{}, fmt.Errorf("invalid week %q: %w", strings.TrimSpace(opts.Week), err)
	}
	if week == "" {
		return Options{}, errors.New("missing week (expected YYYY-MM-DD Sunday start)")
	}
	if opts.HistoryHorizonWeeks <= 0 {
		opts.HistoryHorizonWeeks = 4
	}
	opts.Environments = normalizeReportEnvironments(opts.Environments)
	opts.Week = week
	return opts, nil
}

func writeSummary(outputPath string, report string) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create summary output directory: %w", err)
	}
	if err := os.WriteFile(outputPath, []byte(report), 0o644); err != nil {
		return fmt.Errorf("write summary report: %w", err)
	}
	return nil
}

func normalizeReportEnvironments(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		normalized := normalizeReportEnvironment(value)
		if normalized == "" {
			continue
		}
		if _, exists := set[normalized]; exists {
			continue
		}
		set[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func normalizeReportEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func totalCountForEnvironments(counts map[string]int, environments []string) int {
	if len(counts) == 0 {
		return 0
	}
	normalizedEnvironments := normalizeReportEnvironments(environments)
	if len(normalizedEnvironments) == 0 {
		total := 0
		for _, count := range counts {
			total += count
		}
		return total
	}
	total := 0
	for _, environment := range normalizedEnvironments {
		total += counts[environment]
	}
	return total
}

func outputPathForEnvironment(outputPath, environment string) string {
	base := strings.TrimSpace(outputPath)
	env := normalizeReportEnvironment(environment)
	if base == "" || env == "" {
		return base
	}
	ext := filepath.Ext(base)
	baseWithoutExt := strings.TrimSuffix(base, ext)
	if strings.HasSuffix(baseWithoutExt, "."+env) {
		return base
	}
	if ext == "" {
		return base + "." + env
	}
	return baseWithoutExt + "." + env + ext
}

func filterTriageClustersByEnvironment(rows []triageCluster, environment string) []triageCluster {
	envSet := map[string]struct{}{normalizeReportEnvironment(environment): {}}
	return filterTriageClustersByEnvironmentSet(rows, envSet)
}

func filterTriageClustersByEnvironmentSet(rows []triageCluster, envSet map[string]struct{}) []triageCluster {
	if len(envSet) == 0 {
		return append([]triageCluster(nil), rows...)
	}
	out := make([]triageCluster, 0, len(rows))
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		if _, ok := envSet[environment]; !ok {
			continue
		}
		out = append(out, row)
	}
	return out
}

func loggerFromContext(ctx context.Context) logr.Logger {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return logr.Discard()
	}
	return logger
}

func pct(value, total int) float64 {
	if total <= 0 {
		return 0
	}
	return (float64(value) * 100.0) / float64(total)
}
