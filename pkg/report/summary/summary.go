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
	semanticquery "ci-failure-atlas/pkg/semantic/query"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
)

type Options struct {
	GlobalPath           string
	TestPath             string
	ReviewPath           string
	OutputPath           string
	Format               string
	Top                  int
	MinPercent           float64
	Environments         []string
	SplitByEnvironment   bool
	Week                 string
	HistoryHorizonWeeks  int
	HistoryResolver      semhistory.GlobalSignatureResolver
	Chrome               triagehtml.ReportChromeOptions
}

const (
	reportFormatHTML              = "html"
	metricRunCount                = "run_count"
	summaryFullErrorExamplesLimit = 3
)

func DefaultOptions() Options {
	return Options{
		OutputPath:          "data/reports/global-signature-triage.html",
		Format:              reportFormatHTML,
		Top:                 10,
		MinPercent:          1.0,
		SplitByEnvironment:  false,
		HistoryHorizonWeeks: 4,
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
	LinkedChildren          []globalCluster    `json:"linked_children,omitempty"`
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

	weekData, err := semanticquery.LoadWeekData(ctx, store, semanticquery.LoadWeekDataOptions{
		IncludeRawFailures: true,
	})
	if err != nil {
		return err
	}
	sourceGlobalRows := append([]semanticcontracts.GlobalClusterRecord(nil), weekData.SourceGlobalClusters...)
	phase3Links := append([]semanticcontracts.Phase3LinkRecord(nil), weekData.Phase3Links...)
	globalRows := append([]semanticcontracts.GlobalClusterRecord(nil), weekData.GlobalClusters...)
	reviewRows := append([]semanticcontracts.ReviewItemRecord(nil), weekData.ReviewQueue...)
	linkedChildrenByClusterKey, err := linkedChildrenByMergedClusterKey(sourceGlobalRows, phase3Links)
	if err != nil {
		return fmt.Errorf("build linked child clusters: %w", err)
	}
	reportGlobalRows := toReportGlobalClusters(globalRows)
	reportLinkedChildrenByClusterKey := toReportGlobalClusterGroupMap(linkedChildrenByClusterKey)
	rawFailuresByRun := indexRawFailuresByEnvironmentRun(weekData.RawFailures)
	reportLinkedChildrenByClusterKey = attachGlobalFullErrorSamplesByGroup(
		reportLinkedChildrenByClusterKey,
		summaryFullErrorExamplesLimit,
		rawFailuresByRun,
	)
	targetEnvs := semanticquery.ResolveTargetEnvironments(validated.Environments, weekData)
	metricWindowStart, metricWindowEnd := summaryMetricWindowBounds(validated)
	windowStartRaw, windowEndRaw := summaryMetricWindowStrings(metricWindowStart, metricWindowEnd)
	overallJobsByEnvironment, err := metricRunTotalsByEnvironment(
		ctx,
		store,
		targetEnvs,
		metricWindowStart,
		metricWindowEnd,
	)
	if err != nil {
		return fmt.Errorf("load overall metric run counts: %w", err)
	}

	var report string
	historyResolver := validated.HistoryResolver
	if historyResolver == nil {
		historyResolver, err = semhistory.BuildGlobalSignatureResolver(ctx, semhistory.BuildOptions{
			CurrentWeek:                  validated.Week,
			GlobalSignatureLookbackWeeks: validated.HistoryHorizonWeeks,
		})
		if err != nil {
			return fmt.Errorf("build global signature history resolver: %w", err)
		}
	}
	htmlGlobalRows := attachGlobalFullErrorSamples(reportGlobalRows, summaryFullErrorExamplesLimit, rawFailuresByRun)
	htmlGlobalRows = attachLinkedChildrenToGlobalRows(htmlGlobalRows, reportLinkedChildrenByClusterKey)
	report = buildGlobalTriageHTML(
		htmlGlobalRows,
		validated.Top,
		validated.MinPercent,
		time.Now().UTC(),
		validated.Environments,
		overallJobsByEnvironment,
		windowStartRaw,
		windowEndRaw,
		historyResolver,
		validated.Chrome,
	)
	if validated.SplitByEnvironment {
		if len(targetEnvs) == 0 {
			targetEnvs = []string{"unknown"}
		}
		for _, environment := range targetEnvs {
			filteredGlobalRows := filterGlobalClustersByEnvironment(globalRows, environment)
			filteredReviewRows := filterReviewItemsByEnvironment(reviewRows, environment)
			reportFilteredGlobalRows := toReportGlobalClusters(filteredGlobalRows)
			htmlGlobalRows := attachGlobalFullErrorSamples(reportFilteredGlobalRows, summaryFullErrorExamplesLimit, rawFailuresByRun)
			htmlGlobalRows = attachLinkedChildrenToGlobalRows(htmlGlobalRows, reportLinkedChildrenByClusterKey)
			report := buildGlobalTriageHTML(
				htmlGlobalRows,
				validated.Top,
				validated.MinPercent,
				time.Now().UTC(),
				[]string{environment},
				overallJobsByEnvironment,
				windowStartRaw,
				windowEndRaw,
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
				"testClusters", weekData.TestClusterCountsByEnv[environment],
				"reviewItems", len(filteredReviewRows),
				"top", validated.Top,
				"minPercent", validated.MinPercent,
			)
		}
		return nil
	}
	filteredGlobalRows := globalRows
	filteredReviewRows := reviewRows
	if len(validated.Environments) > 0 {
		envSet := make(map[string]struct{}, len(validated.Environments))
		for _, environment := range validated.Environments {
			envSet[normalizeReportEnvironment(environment)] = struct{}{}
		}
		filteredGlobalRows = filterGlobalClustersByEnvironmentSet(globalRows, envSet)
		filteredReviewRows = filterReviewItemsByEnvironmentSet(reviewRows, envSet)
		reportFilteredGlobalRows := toReportGlobalClusters(filteredGlobalRows)
		htmlGlobalRows := attachGlobalFullErrorSamples(reportFilteredGlobalRows, summaryFullErrorExamplesLimit, rawFailuresByRun)
		htmlGlobalRows = attachLinkedChildrenToGlobalRows(htmlGlobalRows, reportLinkedChildrenByClusterKey)
		report = buildGlobalTriageHTML(
			htmlGlobalRows,
			validated.Top,
			validated.MinPercent,
			time.Now().UTC(),
			validated.Environments,
			overallJobsByEnvironment,
			windowStartRaw,
			windowEndRaw,
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
		"phase3Links", len(phase3Links),
		"globalClusters", len(filteredGlobalRows),
		"testClusters", totalCountForEnvironments(weekData.TestClusterCountsByEnv, validated.Environments),
		"reviewItems", len(filteredReviewRows),
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

func metricRunTotalsByEnvironment(
	ctx context.Context,
	store storecontracts.Store,
	environments []string,
	windowStart time.Time,
	windowEnd time.Time,
) (map[string]int, error) {
	totals := map[string]int{}
	normalizedEnvironments := normalizeReportEnvironments(environments)
	if len(normalizedEnvironments) == 0 {
		return totals, nil
	}
	environmentSet := make(map[string]struct{}, len(normalizedEnvironments))
	for _, environment := range normalizedEnvironments {
		environmentSet[environment] = struct{}{}
	}
	rows, err := store.ListMetricsDaily(ctx)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		if _, ok := environmentSet[environment]; !ok {
			continue
		}
		if strings.TrimSpace(row.Metric) != metricRunCount {
			continue
		}
		trimmedDate := strings.TrimSpace(row.Date)
		if !windowStart.IsZero() && !windowEnd.IsZero() {
			dateValue, ok := parseMetricDate(trimmedDate)
			if !ok {
				continue
			}
			if dateValue.Before(windowStart) || !dateValue.Before(windowEnd) {
				continue
			}
		}
		value := int(row.Value)
		if value <= 0 {
			continue
		}
		totals[environment] += value
	}
	return totals, nil
}

func summaryMetricWindowBounds(opts Options) (time.Time, time.Time) {
	week, err := postgresstore.NormalizeWeek(opts.Week)
	if err != nil || week == "" {
		return time.Time{}, time.Time{}
	}
	start, err := time.Parse("2006-01-02", week)
	if err != nil {
		return time.Time{}, time.Time{}
	}
	start = start.UTC()
	return start, start.AddDate(0, 0, 7)
}

func summaryMetricWindowStrings(start time.Time, end time.Time) (string, string) {
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return "", ""
	}
	return start.Format(time.RFC3339), end.Format(time.RFC3339)
}

func parseMetricDate(value string) (time.Time, bool) {
	parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
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

func linkedChildrenByMergedClusterKey(
	globalClusters []semanticcontracts.GlobalClusterRecord,
	phase3Links []semanticcontracts.Phase3LinkRecord,
) (map[string][]semanticcontracts.GlobalClusterRecord, error) {
	phase3ClusterByAnchor := map[string]string{}
	for _, row := range phase3Links {
		phase3ClusterID := strings.TrimSpace(row.IssueID)
		if phase3ClusterID == "" {
			continue
		}
		key := phase3AnchorKey(row.Environment, row.RunURL, row.RowID)
		if key == "" {
			continue
		}
		phase3ClusterByAnchor[key] = phase3ClusterID
	}
	grouped := map[string][]semanticcontracts.GlobalClusterRecord{}
	for _, cluster := range globalClusters {
		environment := normalizeReportEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		if environment == "" || clusterID == "" {
			return nil, fmt.Errorf("global cluster record missing environment and/or phase2_cluster_id")
		}
		phase3ClusterIDs := phase3ClusterIDsForGlobalCluster(cluster, phase3ClusterByAnchor)
		if len(phase3ClusterIDs) > 1 {
			return nil, fmt.Errorf(
				"phase3 conflict: semantic cluster %s resolves to multiple phase3 cluster IDs (%s)",
				clusterID,
				strings.Join(phase3ClusterIDs, ", "),
			)
		}
		if len(phase3ClusterIDs) == 0 {
			continue
		}
		mergedClusterID := phase3ClusterIDs[0]
		groupKey := reportGlobalClusterKey(environment, mergedClusterID)
		grouped[groupKey] = append(grouped[groupKey], cluster)
	}
	for key := range grouped {
		rows := grouped[key]
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].SupportCount != rows[j].SupportCount {
				return rows[i].SupportCount > rows[j].SupportCount
			}
			if strings.TrimSpace(rows[i].CanonicalEvidencePhrase) != strings.TrimSpace(rows[j].CanonicalEvidencePhrase) {
				return strings.TrimSpace(rows[i].CanonicalEvidencePhrase) < strings.TrimSpace(rows[j].CanonicalEvidencePhrase)
			}
			return strings.TrimSpace(rows[i].Phase2ClusterID) < strings.TrimSpace(rows[j].Phase2ClusterID)
		})
		grouped[key] = rows
	}
	return grouped, nil
}

func phase3ClusterIDsForGlobalCluster(
	cluster semanticcontracts.GlobalClusterRecord,
	phase3ClusterByAnchor map[string]string,
) []string {
	set := map[string]struct{}{}
	environment := normalizeReportEnvironment(cluster.Environment)
	for _, reference := range cluster.References {
		key := phase3AnchorKey(environment, reference.RunURL, reference.RowID)
		if key == "" {
			continue
		}
		phase3ClusterID := strings.TrimSpace(phase3ClusterByAnchor[key])
		if phase3ClusterID == "" {
			continue
		}
		set[phase3ClusterID] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for phase3ClusterID := range set {
		out = append(out, phase3ClusterID)
	}
	sort.Strings(out)
	return out
}

func phase3AnchorKey(environment string, runURL string, rowID string) string {
	normalizedEnvironment := normalizeReportEnvironment(environment)
	trimmedRunURL := strings.TrimSpace(runURL)
	trimmedRowID := strings.TrimSpace(rowID)
	if normalizedEnvironment == "" || trimmedRunURL == "" || trimmedRowID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedRunURL + "|" + trimmedRowID
}

func reportGlobalClusterKey(environment string, clusterID string) string {
	normalizedEnvironment := normalizeReportEnvironment(environment)
	trimmedClusterID := strings.TrimSpace(clusterID)
	if normalizedEnvironment == "" || trimmedClusterID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedClusterID
}

func toReportGlobalClusterGroupMap(
	groups map[string][]semanticcontracts.GlobalClusterRecord,
) map[string][]globalCluster {
	if len(groups) == 0 {
		return nil
	}
	out := make(map[string][]globalCluster, len(groups))
	for key, rows := range groups {
		out[key] = toReportGlobalClusters(rows)
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

func indexRawFailuresByEnvironmentRun(rows []storecontracts.RawFailureRecord) map[string][]storecontracts.RawFailureRecord {
	byRun := map[string][]storecontracts.RawFailureRecord{}
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		runURL := strings.TrimSpace(row.RunURL)
		if environment == "" || runURL == "" {
			continue
		}
		key := environment + "|" + runURL
		byRun[key] = append(byRun[key], row)
	}
	for key := range byRun {
		runRows := byRun[key]
		sort.Slice(runRows, func(i, j int) bool {
			if runRows[i].OccurredAt != runRows[j].OccurredAt {
				return runRows[i].OccurredAt < runRows[j].OccurredAt
			}
			if runRows[i].RowID != runRows[j].RowID {
				return runRows[i].RowID < runRows[j].RowID
			}
			return runRows[i].SignatureID < runRows[j].SignatureID
		})
		byRun[key] = runRows
	}
	return byRun
}

func attachGlobalFullErrorSamples(
	clusters []globalCluster,
	limit int,
	runFailuresByRun map[string][]storecontracts.RawFailureRecord,
) []globalCluster {
	if len(clusters) == 0 || limit <= 0 {
		return append([]globalCluster(nil), clusters...)
	}
	out := append([]globalCluster(nil), clusters...)
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
			runRows := runFailuresByRun[cacheKey]
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
	return out
}

func attachGlobalFullErrorSamplesByGroup(
	groups map[string][]globalCluster,
	limit int,
	runFailuresByRun map[string][]storecontracts.RawFailureRecord,
) map[string][]globalCluster {
	if len(groups) == 0 {
		return nil
	}
	out := make(map[string][]globalCluster, len(groups))
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out[key] = attachGlobalFullErrorSamples(groups[key], limit, runFailuresByRun)
	}
	return out
}

func attachLinkedChildrenToGlobalRows(
	rows []globalCluster,
	linkedChildrenByClusterKey map[string][]globalCluster,
) []globalCluster {
	if len(rows) == 0 || len(linkedChildrenByClusterKey) == 0 {
		return rows
	}
	out := append([]globalCluster(nil), rows...)
	for index := range out {
		key := reportGlobalClusterKey(out[index].Environment, out[index].Phase2ClusterID)
		children := linkedChildrenByClusterKey[key]
		if len(children) == 0 {
			continue
		}
		out[index].LinkedChildren = append([]globalCluster(nil), children...)
	}
	return out
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
