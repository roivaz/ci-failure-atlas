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

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

type Options struct {
	GlobalPath         string
	TestPath           string
	ReviewPath         string
	OutputPath         string
	Top                int
	MinPercent         float64
	Environments       []string
	SplitByEnvironment bool
}

func DefaultOptions() Options {
	return Options{
		OutputPath:         "data/reports/triage-summary.md",
		Top:                10,
		MinPercent:         1.0,
		SplitByEnvironment: false,
	}
}

type reference struct {
	RunURL         string `json:"run_url"`
	OccurredAt     string `json:"occurred_at"`
	SignatureID    string `json:"signature_id"`
	PRNumber       int    `json:"pr_number"`
	PostGoodCommit bool   `json:"post_good_commit"`
	RawTextExcerpt string `json:"raw_text_excerpt"`
}

type contributingTest struct {
	Lane         string `json:"lane"`
	JobName      string `json:"job_name"`
	TestName     string `json:"test_name"`
	SupportCount int    `json:"support_count"`
}

type globalCluster struct {
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
	testRows, err := store.ListTestClusters(ctx)
	if err != nil {
		return fmt.Errorf("list test clusters: %w", err)
	}
	reviewRows, err := store.ListReviewQueue(ctx)
	if err != nil {
		return fmt.Errorf("list review queue: %w", err)
	}

	report := buildMarkdown(
		toReportGlobalClusters(globalRows),
		toReportTestClusters(testRows),
		toReportReviewItems(reviewRows),
		validated.Top,
		validated.MinPercent,
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
			report := buildMarkdown(
				toReportGlobalClusters(filteredGlobalRows),
				toReportTestClusters(filteredTestRows),
				toReportReviewItems(filteredReviewRows),
				validated.Top,
				validated.MinPercent,
			)
			outputPath := outputPathForEnvironment(validated.OutputPath, environment)
			if err := writeSummary(outputPath, report); err != nil {
				return err
			}
			logger.Info(
				"Wrote triage summary markdown.",
				"output", outputPath,
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
		report = buildMarkdown(
			toReportGlobalClusters(filteredGlobalRows),
			toReportTestClusters(filteredTestRows),
			toReportReviewItems(filteredReviewRows),
			validated.Top,
			validated.MinPercent,
		)
	}
	if err := writeSummary(validated.OutputPath, report); err != nil {
		return err
	}
	logger.Info(
		"Wrote triage summary markdown.",
		"output", validated.OutputPath,
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
	if opts.Top <= 0 {
		return Options{}, errors.New("--top must be > 0")
	}
	if opts.MinPercent < 0 {
		return Options{}, errors.New("--min-percent must be >= 0")
	}
	opts.Environments = normalizeReportEnvironments(opts.Environments)
	return opts, nil
}

func writeSummary(outputPath string, report string) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create summary output directory: %w", err)
	}
	if err := os.WriteFile(outputPath, []byte(report), 0o644); err != nil {
		return fmt.Errorf("write summary markdown: %w", err)
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
			RawTextExcerpt: strings.TrimSpace(row.RawTextExcerpt),
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

func buildMarkdown(globalClusters []globalCluster, testClusters []testCluster, reviewItems []reviewItem, top int, minPercent float64) string {
	totalFailures := 0
	postGoodTotal := 0
	for _, cluster := range globalClusters {
		totalFailures += cluster.SupportCount
		postGoodTotal += cluster.PostGoodCommitCount
	}
	if totalFailures == 0 {
		for _, cluster := range testClusters {
			totalFailures += cluster.SupportCount
			postGoodTotal += cluster.PostGoodCommitCount
		}
	}

	minTS, maxTS := findObservedWindow(globalClusters, testClusters)

	laneCounts := make(map[string]int)
	laneTests := make(map[string]map[string]struct{})
	for _, cluster := range testClusters {
		lane := strings.TrimSpace(cluster.Lane)
		if lane == "" {
			lane = "unknown"
		}
		laneCounts[lane] += cluster.SupportCount
		if _, exists := laneTests[lane]; !exists {
			laneTests[lane] = map[string]struct{}{}
		}
		key := cluster.JobName + "|" + cluster.TestName
		laneTests[lane][key] = struct{}{}
	}
	laneNames := make([]string, 0, len(laneCounts))
	for lane := range laneCounts {
		laneNames = append(laneNames, lane)
	}
	sort.Strings(laneNames)

	globalSorted := append([]globalCluster(nil), globalClusters...)
	sort.Slice(globalSorted, func(i, j int) bool {
		if globalSorted[i].SupportCount != globalSorted[j].SupportCount {
			return globalSorted[i].SupportCount > globalSorted[j].SupportCount
		}
		if globalSorted[i].ContributingTestsCount != globalSorted[j].ContributingTestsCount {
			return globalSorted[i].ContributingTestsCount > globalSorted[j].ContributingTestsCount
		}
		return globalSorted[i].Phase2ClusterID < globalSorted[j].Phase2ClusterID
	})

	filterGlobal := func(items []globalCluster) []globalCluster {
		if totalFailures <= 0 || minPercent <= 0 {
			return items
		}
		filtered := make([]globalCluster, 0, len(items))
		for _, item := range items {
			if pct(item.SupportCount, totalFailures) < minPercent {
				continue
			}
			filtered = append(filtered, item)
		}
		return filtered
	}

	type testAgg struct {
		Lane             string
		JobName          string
		TestName         string
		Failures         int
		PostGoodFailures int
		TopPhrase        string
		TopPhraseCount   int
	}
	testAggMap := map[string]*testAgg{}
	for _, cluster := range testClusters {
		key := cluster.Lane + "|" + cluster.JobName + "|" + cluster.TestName
		entry, exists := testAggMap[key]
		if !exists {
			entry = &testAgg{
				Lane:     cluster.Lane,
				JobName:  cluster.JobName,
				TestName: cluster.TestName,
			}
			testAggMap[key] = entry
		}
		entry.Failures += cluster.SupportCount
		entry.PostGoodFailures += cluster.PostGoodCommitCount
		if cluster.SupportCount > entry.TopPhraseCount {
			entry.TopPhraseCount = cluster.SupportCount
			entry.TopPhrase = cluster.CanonicalEvidencePhrase
		}
	}
	testAggs := make([]testAgg, 0, len(testAggMap))
	for _, value := range testAggMap {
		testAggs = append(testAggs, *value)
	}
	sort.Slice(testAggs, func(i, j int) bool {
		if testAggs[i].Failures != testAggs[j].Failures {
			return testAggs[i].Failures > testAggs[j].Failures
		}
		if testAggs[i].Lane != testAggs[j].Lane {
			return testAggs[i].Lane < testAggs[j].Lane
		}
		if testAggs[i].JobName != testAggs[j].JobName {
			return testAggs[i].JobName < testAggs[j].JobName
		}
		return testAggs[i].TestName < testAggs[j].TestName
	})

	filterTestAggs := func(items []testAgg) []testAgg {
		if totalFailures <= 0 || minPercent <= 0 {
			return items
		}
		filtered := make([]testAgg, 0, len(items))
		for _, item := range items {
			if pct(item.Failures, totalFailures) < minPercent {
				continue
			}
			filtered = append(filtered, item)
		}
		return filtered
	}

	postGoodClusters := make([]globalCluster, 0)
	for _, cluster := range globalSorted {
		if cluster.PostGoodCommitCount > 0 {
			postGoodClusters = append(postGoodClusters, cluster)
		}
	}
	sort.Slice(postGoodClusters, func(i, j int) bool {
		if postGoodClusters[i].PostGoodCommitCount != postGoodClusters[j].PostGoodCommitCount {
			return postGoodClusters[i].PostGoodCommitCount > postGoodClusters[j].PostGoodCommitCount
		}
		if postGoodClusters[i].SupportCount != postGoodClusters[j].SupportCount {
			return postGoodClusters[i].SupportCount > postGoodClusters[j].SupportCount
		}
		return postGoodClusters[i].Phase2ClusterID < postGoodClusters[j].Phase2ClusterID
	})

	globalHighSignal := filterGlobal(globalSorted)
	testHighSignal := filterTestAggs(testAggs)
	postGoodHighSignal := filterGlobal(postGoodClusters)

	reasonCounts := map[string]int{}
	for _, item := range reviewItems {
		reason := strings.TrimSpace(item.Reason)
		if reason == "" {
			reason = "(unspecified)"
		}
		reasonCounts[reason]++
	}
	type reasonCount struct {
		Reason string
		Count  int
	}
	reasons := make([]reasonCount, 0, len(reasonCounts))
	for reason, count := range reasonCounts {
		reasons = append(reasons, reasonCount{Reason: reason, Count: count})
	}
	sort.Slice(reasons, func(i, j int) bool {
		if reasons[i].Count != reasons[j].Count {
			return reasons[i].Count > reasons[j].Count
		}
		return reasons[i].Reason < reasons[j].Reason
	})

	var b strings.Builder
	b.WriteString("# CI Failure Triage Summary\n\n")
	b.WriteString(fmt.Sprintf("_Generated: %s UTC_\n\n", time.Now().UTC().Format(time.RFC3339)))

	b.WriteString("## Overview\n\n")
	b.WriteString(fmt.Sprintf("- Total failure records analyzed: **%d**\n", totalFailures))
	b.WriteString(fmt.Sprintf("- Global clusters: **%d**\n", len(globalClusters)))
	b.WriteString(fmt.Sprintf("- Per-test clusters: **%d**\n", len(testClusters)))
	b.WriteString(fmt.Sprintf("- Review queue items: **%d**\n", len(reviewItems)))
	if totalFailures > 0 {
		b.WriteString(fmt.Sprintf("- Post-good-commit failures: **%d** (%.1f%%)\n", postGoodTotal, pct(postGoodTotal, totalFailures)))
	}
	if !minTS.IsZero() && !maxTS.IsZero() {
		b.WriteString(fmt.Sprintf("- Observed failure window: `%s` -> `%s`\n", minTS.Format(time.RFC3339), maxTS.Format(time.RFC3339)))
	}
	b.WriteString(fmt.Sprintf("- Markdown focus: top **%d** rows with at least **%.2f%%** of total failures\n", top, minPercent))
	b.WriteString("\n")

	b.WriteString("## Top Global Failure Signatures\n\n")
	topGlobal := min(top, len(globalHighSignal))
	if topGlobal == 0 {
		b.WriteString(fmt.Sprintf("No global clusters at or above %.2f%% of total failures.\n\n", minPercent))
	} else {
		for i := 0; i < topGlobal; i++ {
			cluster := globalHighSignal[i]
			b.WriteString(fmt.Sprintf("### %d) %d failures (%.1f%%)\n\n", i+1, cluster.SupportCount, pct(cluster.SupportCount, totalFailures)))
			b.WriteString(fmt.Sprintf("- Evidence: `%s`\n", cleanInline(cluster.CanonicalEvidencePhrase, 220)))
			b.WriteString(fmt.Sprintf("- Query seed: `%s`\n", cleanInline(cluster.SearchQueryPhrase, 180)))
			b.WriteString(fmt.Sprintf("- Tests affected: **%d**\n", cluster.ContributingTestsCount))
			b.WriteString(fmt.Sprintf("- Post-good-commit count: **%d**\n", cluster.PostGoodCommitCount))
			sample := sampleContributingTests(cluster.ContributingTests, 3)
			if sample != "" {
				b.WriteString(fmt.Sprintf("- Sample tests: %s\n", sample))
			}
			if len(cluster.References) > 0 {
				b.WriteString(fmt.Sprintf("- Example run: `%s`\n", cluster.References[0].RunURL))
			}
			b.WriteString("\n")
		}
	}

	b.WriteString("## Top Failing Tests\n\n")
	topTests := min(top, len(testHighSignal))
	if topTests == 0 {
		b.WriteString(fmt.Sprintf("No failing tests at or above %.2f%% of total failures.\n\n", minPercent))
	} else {
		b.WriteString("| Rank | Lane | Job | Test | Failures | Post-good | Top evidence |\n")
		b.WriteString("|---|---|---|---|---:|---:|---|\n")
		for i := 0; i < topTests; i++ {
			row := testHighSignal[i]
			b.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %d | %s |\n",
				i+1,
				escapePipe(row.Lane),
				escapePipe(cleanInline(row.JobName, 48)),
				escapePipe(cleanInline(row.TestName, 64)),
				row.Failures,
				row.PostGoodFailures,
				escapePipe(cleanInline(row.TopPhrase, 84)),
			))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Lane Breakdown\n\n")
	if len(laneNames) == 0 {
		b.WriteString("No lane data available.\n\n")
	} else {
		b.WriteString("| Lane | Failures | % of total | Distinct tests |\n")
		b.WriteString("|---|---:|---:|---:|\n")
		for _, lane := range laneNames {
			failures := laneCounts[lane]
			b.WriteString(fmt.Sprintf("| %s | %d | %.1f%% | %d |\n",
				escapePipe(lane),
				failures,
				pct(failures, totalFailures),
				len(laneTests[lane]),
			))
		}
		b.WriteString("\n")
	}

	b.WriteString("## High-Impact Post-Good-Commit Signatures\n\n")
	topPostGood := min(top, len(postGoodHighSignal))
	if topPostGood == 0 {
		b.WriteString(fmt.Sprintf("No post-good-commit clusters at or above %.2f%% of total failures.\n\n", minPercent))
	} else {
		for i := 0; i < topPostGood; i++ {
			cluster := postGoodHighSignal[i]
			b.WriteString(fmt.Sprintf("- **%d** post-good / **%d** total: `%s`\n",
				cluster.PostGoodCommitCount,
				cluster.SupportCount,
				cleanInline(cluster.CanonicalEvidencePhrase, 140),
			))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Review Queue\n\n")
	if len(reviewItems) == 0 {
		b.WriteString("No review items.\n")
	} else {
		b.WriteString(fmt.Sprintf("Total review items: **%d**\n\n", len(reviewItems)))
		for _, reason := range reasons {
			b.WriteString(fmt.Sprintf("- **%d**: %s\n", reason.Count, cleanInline(reason.Reason, 160)))
		}
	}

	return b.String()
}

func sampleContributingTests(items []contributingTest, limit int) string {
	if len(items) == 0 || limit <= 0 {
		return ""
	}
	sorted := append([]contributingTest(nil), items...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].SupportCount != sorted[j].SupportCount {
			return sorted[i].SupportCount > sorted[j].SupportCount
		}
		if sorted[i].Lane != sorted[j].Lane {
			return sorted[i].Lane < sorted[j].Lane
		}
		if sorted[i].JobName != sorted[j].JobName {
			return sorted[i].JobName < sorted[j].JobName
		}
		return sorted[i].TestName < sorted[j].TestName
	})

	parts := make([]string, 0, min(limit, len(sorted)))
	for i := 0; i < len(sorted) && i < limit; i++ {
		parts = append(parts, fmt.Sprintf("`%s (%d)`", cleanInline(sorted[i].TestName, 80), sorted[i].SupportCount))
	}
	return strings.Join(parts, ", ")
}

func findObservedWindow(globalClusters []globalCluster, testClusters []testCluster) (time.Time, time.Time) {
	var minTS time.Time
	var maxTS time.Time
	apply := func(raw string) {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return
		}
		ts, err := time.Parse(time.RFC3339, trimmed)
		if err != nil {
			return
		}
		if minTS.IsZero() || ts.Before(minTS) {
			minTS = ts
		}
		if maxTS.IsZero() || ts.After(maxTS) {
			maxTS = ts
		}
	}

	for _, cluster := range globalClusters {
		for _, ref := range cluster.References {
			apply(ref.OccurredAt)
		}
	}
	if minTS.IsZero() || maxTS.IsZero() {
		for _, cluster := range testClusters {
			for _, ref := range cluster.References {
				apply(ref.OccurredAt)
			}
		}
	}
	return minTS, maxTS
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

func escapePipe(input string) string {
	return strings.ReplaceAll(input, "|", "\\|")
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
