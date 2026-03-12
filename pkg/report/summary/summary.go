package summary

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/report/triagehtml"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

type Options struct {
	GlobalPath           string
	TestPath             string
	ReviewPath           string
	OutputPath           string
	Format               string
	WindowStart          string
	WindowEnd            string
	Top                  int
	MinPercent           float64
	Environments         []string
	SplitByEnvironment   bool
	DataDirectory        string
	SemanticSubdirectory string
	FlakeLookbackDays    int
	Chrome               triagehtml.ReportChromeOptions
}

const (
	reportFormatHTML              = "html"
	summaryFullErrorExamplesLimit = 3
)

func DefaultOptions() Options {
	return Options{
		OutputPath:         "data/reports/global-signature-triage.html",
		Format:             reportFormatHTML,
		Top:                10,
		MinPercent:         1.0,
		SplitByEnvironment: false,
		FlakeLookbackDays:  30,
	}
}

type reference struct {
	RunURL         string `json:"run_url"`
	OccurredAt     string `json:"occurred_at"`
	SignatureID    string `json:"signature_id"`
	PRNumber       int    `json:"pr_number"`
	PostGoodCommit bool   `json:"post_good_commit"`
}

type contributingTest struct {
	Lane         string `json:"lane"`
	JobName      string `json:"job_name"`
	TestName     string `json:"test_name"`
	SupportCount int    `json:"support_count"`
}

type globalCluster struct {
	Environment             string             `json:"environment"`
	SchemaVersion           string             `json:"schema_version"`
	Phase2ClusterID         string             `json:"phase2_cluster_id"`
	CanonicalEvidencePhrase string             `json:"canonical_evidence_phrase"`
	SearchQueryPhrase       string             `json:"search_query_phrase"`
	SupportCount            int                `json:"support_count"`
	SeenPostGoodCommit      bool               `json:"seen_post_good_commit"`
	PostGoodCommitCount     int                `json:"post_good_commit_count"`
	ContributingTestsCount  int                `json:"contributing_tests_count"`
	ContributingTests       []contributingTest `json:"contributing_tests"`
	MemberPhase1ClusterIDs  []string           `json:"member_phase1_cluster_ids"`
	MemberSignatureIDs      []string           `json:"member_signature_ids"`
	References              []reference        `json:"references"`
	FullErrorSamples        []string           `json:"full_error_samples,omitempty"`
}

type testCluster struct {
	SchemaVersion           string      `json:"schema_version"`
	Phase1ClusterID         string      `json:"phase1_cluster_id"`
	Lane                    string      `json:"lane"`
	JobName                 string      `json:"job_name"`
	TestName                string      `json:"test_name"`
	TestSuite               string      `json:"test_suite"`
	CanonicalEvidencePhrase string      `json:"canonical_evidence_phrase"`
	SearchQueryPhrase       string      `json:"search_query_phrase"`
	SupportCount            int         `json:"support_count"`
	SeenPostGoodCommit      bool        `json:"seen_post_good_commit"`
	PostGoodCommitCount     int         `json:"post_good_commit_count"`
	MemberSignatureIDs      []string    `json:"member_signature_ids"`
	References              []reference `json:"references"`
}

type reviewItem struct {
	SchemaVersion string `json:"schema_version"`
	ReviewItemID  string `json:"review_item_id"`
	Phase         string `json:"phase"`
	Reason        string `json:"reason"`
}

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

	globalRows, err := store.ListGlobalClusters(ctx)
	if err != nil {
		return fmt.Errorf("list global clusters: %w", err)
	}
	reportGlobalRows := toReportGlobalClusters(globalRows)
	testRows, err := store.ListTestClusters(ctx)
	if err != nil {
		return fmt.Errorf("list test clusters: %w", err)
	}
	reviewRows, err := store.ListReviewQueue(ctx)
	if err != nil {
		return fmt.Errorf("list review queue: %w", err)
	}

	var report string
	historyResolver, err := semhistory.BuildGlobalSignatureResolver(ctx, semhistory.BuildOptions{
		DataDirectory:               validated.DataDirectory,
		CurrentSemanticSubdir:       validated.SemanticSubdirectory,
		GlobalSignatureLookbackDays: validated.FlakeLookbackDays,
	})
	if err != nil {
		return fmt.Errorf("build global signature history resolver: %w", err)
	}
	htmlGlobalRows, htmlRowsErr := attachGlobalFullErrorSamples(ctx, store, reportGlobalRows, summaryFullErrorExamplesLimit)
	if htmlRowsErr != nil {
		return fmt.Errorf("attach global full-error samples: %w", htmlRowsErr)
	}
	report = buildGlobalTriageHTML(
		htmlGlobalRows,
		validated.Top,
		validated.MinPercent,
		time.Now().UTC(),
		validated.Environments,
		validated.WindowStart,
		validated.WindowEnd,
		historyResolver,
		validated.Chrome,
	)
	if validated.SplitByEnvironment {
		targetEnvs := resolveSummaryTargetEnvironments(validated.Environments, globalRows, testRows, reviewRows)
		if len(targetEnvs) == 0 {
			targetEnvs = []string{"unknown"}
		}
		for _, environment := range targetEnvs {
			filteredGlobalRows := filterGlobalClustersByEnvironment(globalRows, environment)
			filteredTestRows := filterTestClustersByEnvironment(testRows, environment)
			filteredReviewRows := filterReviewItemsByEnvironment(reviewRows, environment)
			reportFilteredGlobalRows := toReportGlobalClusters(filteredGlobalRows)
			htmlGlobalRows, htmlRowsErr := attachGlobalFullErrorSamples(ctx, store, reportFilteredGlobalRows, summaryFullErrorExamplesLimit)
			if htmlRowsErr != nil {
				return fmt.Errorf("attach global full-error samples for env=%q: %w", environment, htmlRowsErr)
			}
			report := buildGlobalTriageHTML(
				htmlGlobalRows,
				validated.Top,
				validated.MinPercent,
				time.Now().UTC(),
				[]string{environment},
				validated.WindowStart,
				validated.WindowEnd,
				historyResolver,
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
				"globalClusters", len(filteredGlobalRows),
				"testClusters", len(filteredTestRows),
				"reviewItems", len(filteredReviewRows),
				"top", validated.Top,
				"minPercent", validated.MinPercent,
			)
		}
		return nil
	}
	filteredGlobalRows := globalRows
	filteredTestRows := testRows
	filteredReviewRows := reviewRows
	if len(validated.Environments) > 0 {
		envSet := make(map[string]struct{}, len(validated.Environments))
		for _, environment := range validated.Environments {
			envSet[normalizeReportEnvironment(environment)] = struct{}{}
		}
		filteredGlobalRows = filterGlobalClustersByEnvironmentSet(globalRows, envSet)
		filteredTestRows = filterTestClustersByEnvironmentSet(testRows, envSet)
		filteredReviewRows = filterReviewItemsByEnvironmentSet(reviewRows, envSet)
		reportFilteredGlobalRows := toReportGlobalClusters(filteredGlobalRows)
		htmlGlobalRows, htmlRowsErr := attachGlobalFullErrorSamples(ctx, store, reportFilteredGlobalRows, summaryFullErrorExamplesLimit)
		if htmlRowsErr != nil {
			return fmt.Errorf("attach global full-error samples for selected environments: %w", htmlRowsErr)
		}
		report = buildGlobalTriageHTML(
			htmlGlobalRows,
			validated.Top,
			validated.MinPercent,
			time.Now().UTC(),
			validated.Environments,
			validated.WindowStart,
			validated.WindowEnd,
			historyResolver,
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
		"globalClusters", len(filteredGlobalRows),
		"testClusters", len(filteredTestRows),
		"reviewItems", len(filteredReviewRows),
		"top", validated.Top,
		"minPercent", validated.MinPercent,
	)
	return nil
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
	windowStart, windowEnd, err := normalizeReportWindow(opts.WindowStart, opts.WindowEnd)
	if err != nil {
		return Options{}, err
	}
	if opts.FlakeLookbackDays <= 0 {
		opts.FlakeLookbackDays = 30
	}
	opts.WindowStart = windowStart
	opts.WindowEnd = windowEnd
	opts.Environments = normalizeReportEnvironments(opts.Environments)
	opts.DataDirectory = strings.TrimSpace(opts.DataDirectory)
	opts.SemanticSubdirectory = strings.TrimSpace(opts.SemanticSubdirectory)
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

func resolveSummaryTargetEnvironments(
	configured []string,
	globalRows []semanticcontracts.GlobalClusterRecord,
	testRows []semanticcontracts.TestClusterRecord,
	reviewRows []semanticcontracts.ReviewItemRecord,
) []string {
	normalizedConfigured := normalizeReportEnvironments(configured)
	if len(normalizedConfigured) > 0 {
		return normalizedConfigured
	}
	set := map[string]struct{}{}
	for _, row := range globalRows {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		set[environment] = struct{}{}
	}
	for _, row := range testRows {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		set[environment] = struct{}{}
	}
	for _, row := range reviewRows {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		set[environment] = struct{}{}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for environment := range set {
		out = append(out, environment)
	}
	sort.Strings(out)
	return out
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

func filterGlobalClustersByEnvironment(rows []semanticcontracts.GlobalClusterRecord, environment string) []semanticcontracts.GlobalClusterRecord {
	envSet := map[string]struct{}{normalizeReportEnvironment(environment): {}}
	return filterGlobalClustersByEnvironmentSet(rows, envSet)
}

func filterGlobalClustersByEnvironmentSet(rows []semanticcontracts.GlobalClusterRecord, envSet map[string]struct{}) []semanticcontracts.GlobalClusterRecord {
	if len(envSet) == 0 {
		return append([]semanticcontracts.GlobalClusterRecord(nil), rows...)
	}
	out := make([]semanticcontracts.GlobalClusterRecord, 0, len(rows))
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		if _, ok := envSet[environment]; !ok {
			continue
		}
		out = append(out, row)
	}
	return out
}

func filterTestClustersByEnvironment(rows []semanticcontracts.TestClusterRecord, environment string) []semanticcontracts.TestClusterRecord {
	envSet := map[string]struct{}{normalizeReportEnvironment(environment): {}}
	return filterTestClustersByEnvironmentSet(rows, envSet)
}

func filterTestClustersByEnvironmentSet(rows []semanticcontracts.TestClusterRecord, envSet map[string]struct{}) []semanticcontracts.TestClusterRecord {
	if len(envSet) == 0 {
		return append([]semanticcontracts.TestClusterRecord(nil), rows...)
	}
	out := make([]semanticcontracts.TestClusterRecord, 0, len(rows))
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		if _, ok := envSet[environment]; !ok {
			continue
		}
		out = append(out, row)
	}
	return out
}

func filterReviewItemsByEnvironment(rows []semanticcontracts.ReviewItemRecord, environment string) []semanticcontracts.ReviewItemRecord {
	envSet := map[string]struct{}{normalizeReportEnvironment(environment): {}}
	return filterReviewItemsByEnvironmentSet(rows, envSet)
}

func filterReviewItemsByEnvironmentSet(rows []semanticcontracts.ReviewItemRecord, envSet map[string]struct{}) []semanticcontracts.ReviewItemRecord {
	if len(envSet) == 0 {
		return append([]semanticcontracts.ReviewItemRecord(nil), rows...)
	}
	out := make([]semanticcontracts.ReviewItemRecord, 0, len(rows))
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		if _, ok := envSet[environment]; !ok {
			continue
		}
		out = append(out, row)
	}
	return out
}

func toReportGlobalClusters(rows []semanticcontracts.GlobalClusterRecord) []globalCluster {
	out := make([]globalCluster, 0, len(rows))
	for _, row := range rows {
		out = append(out, globalCluster{
			Environment:             normalizeReportEnvironment(row.Environment),
			SchemaVersion:           strings.TrimSpace(row.SchemaVersion),
			Phase2ClusterID:         strings.TrimSpace(row.Phase2ClusterID),
			CanonicalEvidencePhrase: strings.TrimSpace(row.CanonicalEvidencePhrase),
			SearchQueryPhrase:       strings.TrimSpace(row.SearchQueryPhrase),
			SupportCount:            row.SupportCount,
			SeenPostGoodCommit:      row.SeenPostGoodCommit,
			PostGoodCommitCount:     row.PostGoodCommitCount,
			ContributingTestsCount:  row.ContributingTestsCount,
			ContributingTests:       toReportContributingTests(row.ContributingTests),
			MemberPhase1ClusterIDs:  append([]string(nil), row.MemberPhase1ClusterIDs...),
			MemberSignatureIDs:      append([]string(nil), row.MemberSignatureIDs...),
			References:              toReportReferences(row.References),
		})
	}
	return out
}

func toReportContributingTests(rows []semanticcontracts.ContributingTestRecord) []contributingTest {
	out := make([]contributingTest, 0, len(rows))
	for _, row := range rows {
		out = append(out, contributingTest{
			Lane:         strings.TrimSpace(row.Lane),
			JobName:      strings.TrimSpace(row.JobName),
			TestName:     strings.TrimSpace(row.TestName),
			SupportCount: row.SupportCount,
		})
	}
	return out
}

func toReportTestClusters(rows []semanticcontracts.TestClusterRecord) []testCluster {
	out := make([]testCluster, 0, len(rows))
	for _, row := range rows {
		out = append(out, testCluster{
			SchemaVersion:           strings.TrimSpace(row.SchemaVersion),
			Phase1ClusterID:         strings.TrimSpace(row.Phase1ClusterID),
			Lane:                    strings.TrimSpace(row.Lane),
			JobName:                 strings.TrimSpace(row.JobName),
			TestName:                strings.TrimSpace(row.TestName),
			TestSuite:               strings.TrimSpace(row.TestSuite),
			CanonicalEvidencePhrase: strings.TrimSpace(row.CanonicalEvidencePhrase),
			SearchQueryPhrase:       strings.TrimSpace(row.SearchQueryPhrase),
			SupportCount:            row.SupportCount,
			SeenPostGoodCommit:      row.SeenPostGoodCommit,
			PostGoodCommitCount:     row.PostGoodCommitCount,
			MemberSignatureIDs:      append([]string(nil), row.MemberSignatureIDs...),
			References:              toReportReferences(row.References),
		})
	}
	return out
}

func toReportReferences(rows []semanticcontracts.ReferenceRecord) []reference {
	out := make([]reference, 0, len(rows))
	for _, row := range rows {
		out = append(out, reference{
			RunURL:         strings.TrimSpace(row.RunURL),
			OccurredAt:     strings.TrimSpace(row.OccurredAt),
			SignatureID:    strings.TrimSpace(row.SignatureID),
			PRNumber:       row.PRNumber,
			PostGoodCommit: row.PostGoodCommit,
		})
	}
	return out
}

func toReportReviewItems(rows []semanticcontracts.ReviewItemRecord) []reviewItem {
	out := make([]reviewItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, reviewItem{
			SchemaVersion: strings.TrimSpace(row.SchemaVersion),
			ReviewItemID:  strings.TrimSpace(row.ReviewItemID),
			Phase:         strings.TrimSpace(row.Phase),
			Reason:        strings.TrimSpace(row.Reason),
		})
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

func cleanInline(input string, max int) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(input, "\n", " "), "\r", " "), "\t", " "))
	normalized = strings.Join(strings.Fields(normalized), " ")
	normalized = strings.ReplaceAll(normalized, "`", "'")
	if max <= 0 {
		return normalized
	}
	runes := []rune(normalized)
	if len(runes) <= max {
		return normalized
	}
	return string(runes[:max-1]) + "…"
}

func pct(value, total int) float64 {
	if total <= 0 {
		return 0
	}
	return (float64(value) * 100.0) / float64(total)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func attachGlobalFullErrorSamples(
	ctx context.Context,
	store storecontracts.Store,
	clusters []globalCluster,
	limit int,
) ([]globalCluster, error) {
	if len(clusters) == 0 || limit <= 0 {
		return append([]globalCluster(nil), clusters...), nil
	}
	out := append([]globalCluster(nil), clusters...)
	runFailureCache := map[string][]storecontracts.RawFailureRecord{}
	for index := range out {
		cluster := out[index]
		signatureIDs := map[string]struct{}{}
		for _, signatureID := range cluster.MemberSignatureIDs {
			trimmed := strings.TrimSpace(signatureID)
			if trimmed == "" {
				continue
			}
			signatureIDs[trimmed] = struct{}{}
		}
		for _, ref := range cluster.References {
			trimmed := strings.TrimSpace(ref.SignatureID)
			if trimmed == "" {
				continue
			}
			signatureIDs[trimmed] = struct{}{}
		}

		samples := make([]string, 0, limit)
		orderedRefs := append([]reference(nil), cluster.References...)
		sort.Slice(orderedRefs, func(i, j int) bool {
			ti, okI := parseReferenceTimestamp(orderedRefs[i].OccurredAt)
			tj, okJ := parseReferenceTimestamp(orderedRefs[j].OccurredAt)
			switch {
			case okI && okJ && !ti.Equal(tj):
				return ti.After(tj)
			case okI != okJ:
				return okI
			}
			return strings.TrimSpace(orderedRefs[i].RunURL) < strings.TrimSpace(orderedRefs[j].RunURL)
		})

		environment := normalizeReportEnvironment(cluster.Environment)
		for _, ref := range orderedRefs {
			if len(samples) >= limit {
				break
			}
			runURL := strings.TrimSpace(ref.RunURL)
			if runURL == "" || environment == "" {
				continue
			}
			cacheKey := environment + "|" + runURL
			runRows, ok := runFailureCache[cacheKey]
			if !ok {
				loadedRows, err := store.ListRawFailuresByRun(ctx, environment, runURL)
				if err != nil {
					return nil, fmt.Errorf("list raw failures by run env=%q run=%q: %w", environment, runURL, err)
				}
				runRows = loadedRows
				runFailureCache[cacheKey] = runRows
			}
			for _, runRow := range runRows {
				if len(samples) >= limit {
					break
				}
				signatureID := strings.TrimSpace(runRow.SignatureID)
				if len(signatureIDs) > 0 {
					if _, ok := signatureIDs[signatureID]; !ok {
						continue
					}
				}
				sample := strings.TrimSpace(runRow.RawText)
				if sample == "" {
					sample = strings.TrimSpace(runRow.NormalizedText)
				}
				samples = appendUniqueLimitedSample(samples, sample, limit)
			}
		}
		out[index].FullErrorSamples = samples
	}
	return out, nil
}

func appendUniqueLimitedSample(existing []string, candidate string, limit int) []string {
	trimmedCandidate := strings.TrimSpace(candidate)
	if trimmedCandidate == "" {
		return existing
	}
	for _, value := range existing {
		if value == trimmedCandidate {
			return existing
		}
	}
	if limit > 0 && len(existing) >= limit {
		return existing
	}
	return append(existing, trimmedCandidate)
}

func normalizeReportWindow(rawStart string, rawEnd string) (string, string, error) {
	startRaw := strings.TrimSpace(rawStart)
	endRaw := strings.TrimSpace(rawEnd)
	if startRaw == "" && endRaw == "" {
		return "", "", nil
	}
	if startRaw == "" || endRaw == "" {
		return "", "", fmt.Errorf("both --workflow.window.start and --workflow.window.end must be set together")
	}
	start, err := parseReportWindowBoundary(startRaw, false)
	if err != nil {
		return "", "", fmt.Errorf("invalid --workflow.window.start value: %w", err)
	}
	end, err := parseReportWindowBoundary(endRaw, true)
	if err != nil {
		return "", "", fmt.Errorf("invalid --workflow.window.end value: %w", err)
	}
	if !start.Before(end) {
		return "", "", fmt.Errorf("workflow window start must be before end (start=%s end=%s)", start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	return start.Format(time.RFC3339), end.Format(time.RFC3339), nil
}

func parseReportWindowBoundary(raw string, endBoundary bool) (time.Time, error) {
	_ = endBoundary
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
		return parsed.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unsupported time format %q (use RFC3339 or YYYY-MM-DD)", raw)
}
