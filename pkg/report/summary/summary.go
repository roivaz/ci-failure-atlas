package summary

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Options struct {
	GlobalPath string
	TestPath   string
	ReviewPath string
	OutputPath string
	Top        int
	MinPercent float64
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
	return fmt.Errorf("report summary not implemented yet (pending global cross-test merge phase)")
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
