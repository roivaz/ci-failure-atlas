package testsummary

import (
	"encoding/json"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"ci-failure-atlas/pkg/report/triagehtml"
)

type qualitySignatureRow struct {
	Environment         string
	Lane                string
	JobName             string
	TestName            string
	TestSuite           string
	Phase1ClusterID     string
	Phrase              string
	SupportCount        int
	PostGoodCommitCount int
	PassRate            *float64
	Runs                int
	ReviewReasons       []string
	IssueCodes          []string
	IssueLabels         []string
	QualityScore        int
	RecentRuns          []reference
	FullErrorSamples    []string
	ContributingTests   []qualityContributingTest
	Sparkline           string
	SparkCounts         []int
	SparkRange          string
	References          []reference
	SearchIndex         string
}

type qualityContributingTest struct {
	Lane         string
	JobName      string
	TestName     string
	SupportCount int
}

type qualityFlaggedSignatureExport struct {
	Environment             string             `json:"environment"`
	Lane                    string             `json:"lane"`
	JobName                 string             `json:"job_name"`
	TestName                string             `json:"test_name"`
	TestSuite               string             `json:"test_suite"`
	Phase1ClusterID         string             `json:"phase1_cluster_id"`
	CanonicalEvidencePhrase string             `json:"canonical_evidence_phrase"`
	SupportCount            int                `json:"support_count"`
	PostGoodCommitCount     int                `json:"post_good_commit_count"`
	CurrentPassPercentage   *float64           `json:"current_pass_percentage,omitempty"`
	CurrentRuns             int                `json:"current_runs"`
	QualityIssueCodes       []string           `json:"quality_issue_codes"`
	QualityIssueLabels      []string           `json:"quality_issue_labels"`
	ReviewReasons           []string           `json:"review_reasons,omitempty"`
	Score                   int                `json:"score"`
	RecentRuns              []qualityExportRun `json:"recent_runs,omitempty"`
	FullErrorSamples        []string           `json:"full_error_samples,omitempty"`
}

type qualityExportRun struct {
	RunURL     string `json:"run_url"`
	OccurredAt string `json:"occurred_at"`
	PRNumber   int    `json:"pr_number,omitempty"`
}

const (
	defaultGitHubRepoOwner = "Azure"
	defaultGitHubRepoName  = "ARO-HCP"
)

func buildQualitySignatureRows(
	testClusters []testCluster,
	metadataByFull map[testKey]testMetadata,
	metadataByNoSuite map[testKeyNoSuite]testMetadata,
	fullErrorsByReference map[referenceKey]string,
	reviewIndex reviewSignalIndex,
	generatedAt time.Time,
	topTests int,
	recentRuns int,
	minRuns int,
) []qualitySignatureRow {
	aggregates := prepareSortedTestAggregates(testClusters, metadataByFull, metadataByNoSuite, minRuns)

	limit := len(aggregates)
	if topTests > 0 && topTests < limit {
		limit = topTests
	}

	rows := make([]qualitySignatureRow, 0, limit)
	for i := 0; i < limit; i++ {
		aggregate := aggregates[i]
		for _, cluster := range aggregate.Clusters {
			reviewReasons := qualityReviewReasonsForCluster(cluster, reviewIndex)
			signatureRuns := recentRunsForCluster(cluster, recentRuns)
			fullErrorSamples := fullErrorSamplesForCluster(cluster, signatureRuns, fullErrorsByReference, 2)
			issueCodes := qualityIssueCodes(cluster, fullErrorSamples)
			issueLabels := make([]string, 0, len(issueCodes))
			for _, code := range issueCodes {
				issueLabels = append(issueLabels, qualityIssueLabel(code))
			}
			sparkline, sparkCounts, sparkRange, hasSparkline := clusterDailyDensitySparkline(cluster, sparklineWindowDays, generatedAt)
			row := qualitySignatureRow{
				Environment:         normalizeReportEnvironment(cluster.Environment),
				Lane:                strings.TrimSpace(cluster.Lane),
				JobName:             strings.TrimSpace(cluster.JobName),
				TestName:            strings.TrimSpace(cluster.TestName),
				TestSuite:           strings.TrimSpace(cluster.TestSuite),
				Phase1ClusterID:     strings.TrimSpace(cluster.Phase1ClusterID),
				Phrase:              strings.TrimSpace(cluster.CanonicalEvidencePhrase),
				SupportCount:        cluster.SupportCount,
				PostGoodCommitCount: cluster.PostGoodCommitCount,
				PassRate:            aggregate.Metadata.PassRate,
				Runs:                aggregate.Metadata.Runs,
				ReviewReasons:       append([]string(nil), reviewReasons...),
				IssueCodes:          append([]string(nil), issueCodes...),
				IssueLabels:         append([]string(nil), issueLabels...),
				QualityScore:        qualityIssueScore(issueCodes, reviewReasons),
				RecentRuns:          append([]reference(nil), signatureRuns...),
				FullErrorSamples:    append([]string(nil), fullErrorSamples...),
				References:          append([]reference(nil), cluster.References...),
			}
			if hasSparkline {
				row.Sparkline = sparkline
				row.SparkCounts = append([]int(nil), sparkCounts...)
				row.SparkRange = sparkRange
			}
			row.SearchIndex = strings.ToLower(strings.Join([]string{
				row.Environment,
				row.Lane,
				row.JobName,
				row.TestName,
				row.TestSuite,
				row.Phrase,
				strings.Join(row.IssueLabels, " "),
				strings.Join(row.ReviewReasons, " "),
			}, " "))
			rows = append(rows, row)
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].QualityScore != rows[j].QualityScore {
			return rows[i].QualityScore > rows[j].QualityScore
		}
		if rows[i].SupportCount != rows[j].SupportCount {
			return rows[i].SupportCount > rows[j].SupportCount
		}
		if rows[i].Environment != rows[j].Environment {
			return rows[i].Environment < rows[j].Environment
		}
		if rows[i].Lane != rows[j].Lane {
			return rows[i].Lane < rows[j].Lane
		}
		if rows[i].JobName != rows[j].JobName {
			return rows[i].JobName < rows[j].JobName
		}
		if rows[i].TestName != rows[j].TestName {
			return rows[i].TestName < rows[j].TestName
		}
		return rows[i].Phase1ClusterID < rows[j].Phase1ClusterID
	})
	return rows
}

func qualityReviewReasonsForCluster(cluster testCluster, index reviewSignalIndex) []string {
	reasons := reviewReasonsForCluster(cluster, index)
	return filterQualityReviewReasonsForPhrase(reasons, cluster.CanonicalEvidencePhrase)
}

func qualityReviewReasonsForGlobalCluster(cluster globalCluster, index reviewSignalIndex) []string {
	reasonSet := map[string]struct{}{}
	for _, phase1ClusterID := range cluster.MemberPhase1ClusterIDs {
		if values, ok := index.ByPhase1Cluster[strings.TrimSpace(phase1ClusterID)]; ok {
			for _, reason := range values.UnsortedList() {
				if isInformationalReviewReason(reason) {
					continue
				}
				reasonSet[reason] = struct{}{}
			}
		}
	}
	for _, signatureID := range cluster.MemberSignatureIDs {
		if values, ok := index.BySignatureID[strings.TrimSpace(signatureID)]; ok {
			for _, reason := range values.UnsortedList() {
				if isInformationalReviewReason(reason) {
					continue
				}
				reasonSet[reason] = struct{}{}
			}
		}
	}
	ordered := make([]string, 0, len(reasonSet))
	for reason := range reasonSet {
		ordered = append(ordered, reason)
	}
	sort.Strings(ordered)
	return filterQualityReviewReasonsForPhrase(ordered, cluster.CanonicalEvidencePhrase)
}

func filterQualityReviewReasonsForPhrase(reasons []string, phrase string) []string {
	if !isLikelyGlobalCancellationPhrase(phrase) {
		return reasons
	}
	filtered := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		switch strings.ToLower(strings.TrimSpace(reason)) {
		case "insufficient_inner_error", "low_confidence_evidence":
			// Cancellation signatures often have little inner context in
			// artifacts. Keep them visible in the inspector without classifying
			// them as semantic-quality defects by default.
			continue
		default:
			filtered = append(filtered, reason)
		}
	}
	return filtered
}

func buildQualitySignatureRowsFromGlobalClusters(
	globalClusters []globalCluster,
	fullErrorsByReference map[referenceKey]string,
	reviewIndex reviewSignalIndex,
	generatedAt time.Time,
	topClusters int,
	recentRuns int,
) []qualitySignatureRow {
	sortedClusters := append([]globalCluster(nil), globalClusters...)
	sort.Slice(sortedClusters, func(i, j int) bool {
		if sortedClusters[i].SupportCount != sortedClusters[j].SupportCount {
			return sortedClusters[i].SupportCount > sortedClusters[j].SupportCount
		}
		if sortedClusters[i].PostGoodCommitCount != sortedClusters[j].PostGoodCommitCount {
			return sortedClusters[i].PostGoodCommitCount > sortedClusters[j].PostGoodCommitCount
		}
		if sortedClusters[i].Environment != sortedClusters[j].Environment {
			return sortedClusters[i].Environment < sortedClusters[j].Environment
		}
		return sortedClusters[i].Phase2ClusterID < sortedClusters[j].Phase2ClusterID
	})
	if topClusters > 0 && topClusters < len(sortedClusters) {
		sortedClusters = sortedClusters[:topClusters]
	}

	rows := make([]qualitySignatureRow, 0, len(sortedClusters))
	for _, cluster := range sortedClusters {
		primary := primaryContributingTest(cluster.ContributingTests)
		reviewReasons := qualityReviewReasonsForGlobalCluster(cluster, reviewIndex)
		signatureRuns := recentRunsForReferences(cluster.References, recentRuns)
		fullErrorSamples := fullErrorSamplesForReferences(cluster.References, signatureRuns, fullErrorsByReference, 2)
		issueCodes := qualityIssueCodesForPhrase(cluster.CanonicalEvidencePhrase, fullErrorSamples)
		issueLabels := make([]string, 0, len(issueCodes))
		for _, code := range issueCodes {
			issueLabels = append(issueLabels, qualityIssueLabel(code))
		}
		sparkline, sparkCounts, sparkRange, hasSparkline := referencesDailyDensitySparkline(cluster.References, sparklineWindowDays, generatedAt)
		row := qualitySignatureRow{
			Environment:         normalizeReportEnvironment(cluster.Environment),
			Lane:                strings.TrimSpace(primary.Lane),
			JobName:             strings.TrimSpace(primary.JobName),
			TestName:            strings.TrimSpace(primary.TestName),
			TestSuite:           "",
			Phase1ClusterID:     strings.TrimSpace(cluster.Phase2ClusterID),
			Phrase:              strings.TrimSpace(cluster.CanonicalEvidencePhrase),
			SupportCount:        cluster.SupportCount,
			PostGoodCommitCount: cluster.PostGoodCommitCount,
			PassRate:            nil,
			Runs:                0,
			ReviewReasons:       append([]string(nil), reviewReasons...),
			IssueCodes:          append([]string(nil), issueCodes...),
			IssueLabels:         append([]string(nil), issueLabels...),
			QualityScore:        qualityIssueScore(issueCodes, reviewReasons),
			RecentRuns:          append([]reference(nil), signatureRuns...),
			FullErrorSamples:    append([]string(nil), fullErrorSamples...),
			ContributingTests:   append([]qualityContributingTest(nil), cluster.ContributingTests...),
			References:          append([]reference(nil), cluster.References...),
		}
		if hasSparkline {
			row.Sparkline = sparkline
			row.SparkCounts = append([]int(nil), sparkCounts...)
			row.SparkRange = sparkRange
		}
		row.SearchIndex = strings.ToLower(strings.Join([]string{
			row.Environment,
			row.Lane,
			row.JobName,
			row.TestName,
			row.Phrase,
			strings.Join(issueLabels, " "),
			strings.Join(reviewReasons, " "),
			qualityContributingSearchIndex(cluster.ContributingTests),
		}, " "))
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].QualityScore != rows[j].QualityScore {
			return rows[i].QualityScore > rows[j].QualityScore
		}
		if rows[i].SupportCount != rows[j].SupportCount {
			return rows[i].SupportCount > rows[j].SupportCount
		}
		if rows[i].Environment != rows[j].Environment {
			return rows[i].Environment < rows[j].Environment
		}
		if rows[i].Lane != rows[j].Lane {
			return rows[i].Lane < rows[j].Lane
		}
		if rows[i].JobName != rows[j].JobName {
			return rows[i].JobName < rows[j].JobName
		}
		if rows[i].TestName != rows[j].TestName {
			return rows[i].TestName < rows[j].TestName
		}
		return rows[i].Phase1ClusterID < rows[j].Phase1ClusterID
	})
	return rows
}

func primaryContributingTest(items []qualityContributingTest) qualityContributingTest {
	if len(items) == 0 {
		return qualityContributingTest{}
	}
	sorted := append([]qualityContributingTest(nil), items...)
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
	return sorted[0]
}

func qualityContributingSearchIndex(items []qualityContributingTest) string {
	if len(items) == 0 {
		return ""
	}
	parts := make([]string, 0, len(items)*3)
	for _, item := range items {
		if lane := strings.TrimSpace(item.Lane); lane != "" {
			parts = append(parts, lane)
		}
		if jobName := strings.TrimSpace(item.JobName); jobName != "" {
			parts = append(parts, jobName)
		}
		if testName := strings.TrimSpace(item.TestName); testName != "" {
			parts = append(parts, testName)
		}
	}
	return strings.Join(parts, " ")
}

func isLikelyGlobalCancellationPhrase(phrase string) bool {
	normalized := strings.ToLower(strings.TrimSpace(phrase))
	if normalized == "" {
		return false
	}
	if strings.Contains(normalized, "interrupted by user") {
		return true
	}
	if strings.Contains(normalized, "context canceled") || strings.Contains(normalized, "context cancelled") {
		return true
	}
	if strings.Contains(normalized, "cancelled by user") || strings.Contains(normalized, "canceled by user") {
		return true
	}
	return false
}

func prepareSortedTestAggregates(
	testClusters []testCluster,
	metadataByFull map[testKey]testMetadata,
	metadataByNoSuite map[testKeyNoSuite]testMetadata,
	minRuns int,
) []testAggregate {
	aggregates := mergeUnknownJobAggregates(aggregateByTest(testClusters))
	for i := range aggregates {
		aggregates[i].Metadata = lookupMetadata(aggregates[i].Key, metadataByFull, metadataByNoSuite)
		sortClusters(aggregates[i].Clusters)
	}
	if minRuns > 0 {
		filtered := make([]testAggregate, 0, len(aggregates))
		for _, aggregate := range aggregates {
			if aggregate.Metadata.Runs < minRuns {
				continue
			}
			filtered = append(filtered, aggregate)
		}
		aggregates = filtered
	}

	sort.Slice(aggregates, func(i, j int) bool {
		pi := aggregates[i].Metadata.PassRate
		pj := aggregates[j].Metadata.PassRate
		switch {
		case pi != nil && pj != nil && *pi != *pj:
			return *pi < *pj
		case pi != nil && pj == nil:
			return true
		case pi == nil && pj != nil:
			return false
		}
		if aggregates[i].Metadata.Runs != aggregates[j].Metadata.Runs {
			return aggregates[i].Metadata.Runs > aggregates[j].Metadata.Runs
		}
		if aggregates[i].TotalFailures != aggregates[j].TotalFailures {
			return aggregates[i].TotalFailures > aggregates[j].TotalFailures
		}
		if aggregates[i].DistinctSignatures != aggregates[j].DistinctSignatures {
			return aggregates[i].DistinctSignatures > aggregates[j].DistinctSignatures
		}
		if !aggregates[i].LatestFailure.Equal(aggregates[j].LatestFailure) {
			return aggregates[i].LatestFailure.After(aggregates[j].LatestFailure)
		}
		if aggregates[i].Key.Lane != aggregates[j].Key.Lane {
			return aggregates[i].Key.Lane < aggregates[j].Key.Lane
		}
		if aggregates[i].Key.JobName != aggregates[j].Key.JobName {
			return aggregates[i].Key.JobName < aggregates[j].Key.JobName
		}
		return aggregates[i].Key.TestName < aggregates[j].Key.TestName
	})
	return aggregates
}

func buildHTML(
	rows []qualitySignatureRow,
	testPath string,
	rawPath string,
	generatedAt time.Time,
	configuredWindowStart string,
	configuredWindowEnd string,
	impactTotalJobs int,
) string {
	var b strings.Builder
	windowStart, windowEnd, hasWindow := resolvedQualityWindow(rows, configuredWindowStart, configuredWindowEnd)
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Semantic Quality Report</title>\n")
	b.WriteString(triagehtml.ThemeInitScriptTag())
	b.WriteString("  <style>\n")
	b.WriteString("    body { font-family: Arial, sans-serif; margin: 20px; color: #1f2937; }\n")
	b.WriteString("    h1 { margin-bottom: 6px; }\n")
	b.WriteString("    h2 { margin-top: 24px; }\n")
	b.WriteString("    .meta { color: #4b5563; margin-bottom: 8px; }\n")
	b.WriteString("    .cards { display: flex; flex-wrap: wrap; gap: 10px; margin: 14px 0 18px; }\n")
	b.WriteString("    .card { border: 1px solid #e5e7eb; border-radius: 8px; background: #f9fafb; padding: 10px 12px; min-width: 180px; }\n")
	b.WriteString("    .card .label { font-size: 12px; color: #6b7280; margin-bottom: 3px; }\n")
	b.WriteString("    .card .value { font-size: 20px; font-weight: 700; }\n")
	b.WriteString("    .filters { border: 1px solid #e5e7eb; border-radius: 8px; background: #f9fafb; padding: 10px; display: flex; flex-wrap: wrap; gap: 10px; align-items: end; margin-bottom: 12px; }\n")
	b.WriteString("    .filters label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: #374151; }\n")
	b.WriteString("    .filters input, .filters select { padding: 4px 6px; border: 1px solid #d1d5db; border-radius: 4px; min-width: 120px; font-size: 12px; }\n")
	b.WriteString("    .filters .inline { flex-direction: row; align-items: center; gap: 6px; }\n")
	b.WriteString("    .filters .results { margin-left: auto; font-size: 12px; color: #6b7280; }\n")
	b.WriteString("    .quality-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0 16px; }\n")
	b.WriteString("    .quality-table th, .quality-table td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; vertical-align: top; }\n")
	b.WriteString("    .quality-table th { background: #f3f4f6; color: #374151; font-weight: 700; }\n")
	b.WriteString("    .quality-table tr.inspector-errors-row td { background: #eff6ff; }\n")
	b.WriteString("    .badge { display: inline-block; border-radius: 999px; padding: 2px 8px; font-size: 11px; margin: 1px 2px 1px 0; }\n")
	b.WriteString("    .badge-issue { background: #fee2e2; color: #991b1b; }\n")
	b.WriteString("    .badge-review { background: #fef3c7; color: #92400e; }\n")
	b.WriteString("    .badge-ok { background: #dcfce7; color: #166534; }\n")
	b.WriteString("    .score-high { color: #991b1b; font-weight: 700; }\n")
	b.WriteString("    .score-low { color: #374151; }\n")
	b.WriteString("    .empty { color: #6b7280; font-size: 12px; }\n")
	b.WriteString("    details { margin: 2px 0; }\n")
	b.WriteString("    details summary { cursor: pointer; color: #1d4ed8; }\n")
	b.WriteString("    .inspector-errors-row .inspector-detail-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-start; }\n")
	b.WriteString("    .inspector-errors-row details.full-errors-toggle, .inspector-errors-row details.affected-runs-toggle { margin: 0; }\n")
	b.WriteString("    .inspector-errors-row details.full-errors-toggle > summary, .inspector-errors-row details.affected-runs-toggle > summary { display: inline-flex; align-items: center; gap: 6px; font-size: 9px; font-weight: 600; color: #1e3a8a; background: #dbeafe; border: 1px solid #93c5fd; border-radius: 999px; padding: 2px 10px; }\n")
	b.WriteString("    .inspector-errors-row details.full-errors-toggle[open] > summary, .inspector-errors-row details.affected-runs-toggle[open] > summary { background: #bfdbfe; border-color: #60a5fa; color: #1e40af; }\n")
	b.WriteString("    .inspector-errors-row .runs-scroll { margin-top: 6px; max-height: 172px; overflow-y: auto; border: 1px solid #bfdbfe; border-radius: 6px; background: #eff6ff; }\n")
	b.WriteString("    .inspector-errors-row .runs-table { border-collapse: collapse; width: 100%; font-size: 11px; }\n")
	b.WriteString("    .inspector-errors-row .runs-table th, .inspector-errors-row .runs-table td { padding: 4px 6px; border-bottom: 1px solid #dbeafe; text-align: left; vertical-align: top; }\n")
	b.WriteString("    .inspector-errors-row .runs-table th { position: sticky; top: 0; background: #dbeafe; z-index: 1; }\n")
	b.WriteString("    .signature-text { font-size: 13px; font-weight: 700; color: #111827; }\n")
	b.WriteString("    pre { white-space: pre-wrap; word-break: break-word; background: #111827; color: #f9fafb; padding: 8px; border-radius: 6px; font-size: 11px; }\n")
	b.WriteString("    .muted { color: #6b7280; }\n")
	b.WriteString(triagehtml.StylesCSS())
	b.WriteString(triagehtml.ThemeCSS())
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString(triagehtml.ThemeToggleHTML())
	b.WriteString("  <h1>CI Semantic Quality Report</h1>\n")
	if hasWindow {
		windowDays := inclusiveWindowDays(windowStart, windowEnd)
		b.WriteString(fmt.Sprintf(
			"  <p class=\"meta\">Window: <strong>%s</strong> to <strong>%s</strong> (%d days)</p>\n",
			html.EscapeString(windowStart.Format("2006-01-02")),
			html.EscapeString(windowEnd.Format("2006-01-02")),
			windowDays,
		))
	}
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Generated: <strong>%s</strong></p>\n", html.EscapeString(generatedAt.Format(time.RFC3339))))
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Source semantic clusters: <code>%s</code></p>\n", html.EscapeString(strings.TrimSpace(testPath))))
	if strings.TrimSpace(rawPath) != "" {
		b.WriteString(fmt.Sprintf("  <p class=\"meta\">Source raw failures: <code>%s</code></p>\n", html.EscapeString(strings.TrimSpace(rawPath))))
	}

	totalRows := len(rows)
	flaggedRows := make([]qualitySignatureRow, 0, len(rows))
	reviewFlaggedCount := 0
	withSamplesCount := 0
	uniqueTests := map[string]struct{}{}
	for _, row := range rows {
		if row.isFlagged() {
			flaggedRows = append(flaggedRows, row)
		}
		if row.hasReviewSignals() {
			reviewFlaggedCount++
		}
		if len(row.FullErrorSamples) > 0 {
			withSamplesCount++
		}
		uniqueTests[row.Environment+"|"+row.TestName+"|"+row.TestSuite] = struct{}{}
	}
	flaggedPct := qualityPct(len(flaggedRows), totalRows)
	withSamplesPct := qualityPct(withSamplesCount, totalRows)

	b.WriteString("  <div class=\"cards\">\n")
	b.WriteString(qualityCardHTML("Signature rows", fmt.Sprintf("%d", totalRows)))
	b.WriteString(qualityCardHTML("Suspicious signatures", fmt.Sprintf("%d (%.1f%%)", len(flaggedRows), flaggedPct)))
	b.WriteString(qualityCardHTML("Review-flagged signatures", fmt.Sprintf("%d", reviewFlaggedCount)))
	b.WriteString(qualityCardHTML("Rows with full error samples", fmt.Sprintf("%d (%.1f%%)", withSamplesCount, withSamplesPct)))
	b.WriteString(qualityCardHTML("Distinct tests in scope", fmt.Sprintf("%d", len(uniqueTests))))
	b.WriteString("  </div>\n")

	envOptions := sortedValuesFromRows(rows, func(row qualitySignatureRow) string {
		return row.Environment
	})
	laneOptions := sortedValuesFromRows(rows, func(row qualitySignatureRow) string {
		return row.Lane
	})

	b.WriteString("  <h2>Signature Inspector</h2>\n")
	b.WriteString("  <div class=\"filters\">\n")
	b.WriteString("    <label>Environment<select id=\"filter-env\"><option value=\"\">All</option>")
	for _, option := range envOptions {
		b.WriteString(fmt.Sprintf("<option value=\"%s\">%s</option>", html.EscapeString(option), html.EscapeString(strings.ToUpper(option))))
	}
	b.WriteString("</select></label>\n")
	b.WriteString("    <label>Lane<select id=\"filter-lane\"><option value=\"\">All</option>")
	for _, option := range laneOptions {
		b.WriteString(fmt.Sprintf("<option value=\"%s\">%s</option>", html.EscapeString(option), html.EscapeString(option)))
	}
	b.WriteString("</select></label>\n")
	b.WriteString("    <label>Min support<input id=\"filter-min-support\" type=\"number\" min=\"0\" value=\"0\" /></label>\n")
	b.WriteString("    <label class=\"inline\"><input id=\"filter-flagged-only\" type=\"checkbox\" /> Suspicious only</label>\n")
	b.WriteString("    <label class=\"inline\"><input id=\"filter-review-only\" type=\"checkbox\" /> Needs review only</label>\n")
	b.WriteString("    <label>Search<input id=\"filter-search\" type=\"text\" placeholder=\"phrase, test, job, reason\" /></label>\n")
	b.WriteString("    <span class=\"results\" id=\"filter-count\"></span>\n")
	b.WriteString("  </div>\n")

	b.WriteString("  <h3>All Signatures (Inspector)</h3>\n")
	if len(rows) == 0 {
		b.WriteString("  <p class=\"empty\">No signatures available for the selected scope.</p>\n")
	} else {
		triageRows := qualityRowsToTriageRows(rows)
		b.WriteString(triagehtml.RenderTable(triageRows, triagehtml.TableOptions{
			ShowQualityFlags:   true,
			ShowReviewFlags:    true,
			ShowQualityScore:   true,
			IncludeTrend:       true,
			GitHubRepoOwner:    defaultGitHubRepoOwner,
			GitHubRepoName:     defaultGitHubRepoName,
			ImpactTotalJobs:    impactTotalJobs,
			LoadedRowsLimit:    -1,
			InitialVisibleRows: -1,
		}))
	}

	b.WriteString(triagehtml.ThemeToggleScriptTag())
	b.WriteString("<script>\n")
	b.WriteString("(function(){\n")
	b.WriteString("  var envSelect = document.getElementById('filter-env');\n")
	b.WriteString("  var laneSelect = document.getElementById('filter-lane');\n")
	b.WriteString("  var minSupportInput = document.getElementById('filter-min-support');\n")
	b.WriteString("  var flaggedOnly = document.getElementById('filter-flagged-only');\n")
	b.WriteString("  var reviewOnly = document.getElementById('filter-review-only');\n")
	b.WriteString("  var searchInput = document.getElementById('filter-search');\n")
	b.WriteString("  var countEl = document.getElementById('filter-count');\n")
	b.WriteString("  var table = document.querySelector('table.triage-table');\n")
	b.WriteString("  function parseSupport(value) { var v = parseInt(value || '0', 10); return isNaN(v) ? 0 : v; }\n")
	b.WriteString("  function rowMatches(row) {\n")
	b.WriteString("    var env = envSelect ? envSelect.value : '';\n")
	b.WriteString("    var lane = laneSelect ? laneSelect.value : '';\n")
	b.WriteString("    var minSupport = minSupportInput ? parseSupport(minSupportInput.value) : 0;\n")
	b.WriteString("    var flagged = flaggedOnly && flaggedOnly.checked;\n")
	b.WriteString("    var review = reviewOnly && reviewOnly.checked;\n")
	b.WriteString("    var search = searchInput ? searchInput.value.toLowerCase().trim() : '';\n")
	b.WriteString("    if (env && (row.getAttribute('data-filter-env') || '') !== env) { return false; }\n")
	b.WriteString("    if (lane && (row.getAttribute('data-filter-lane') || '') !== lane) { return false; }\n")
	b.WriteString("    if (parseSupport(row.getAttribute('data-sort-count')) < minSupport) { return false; }\n")
	b.WriteString("    if (flagged && (row.getAttribute('data-filter-flagged') || '') !== 'true') { return false; }\n")
	b.WriteString("    if (review && (row.getAttribute('data-filter-review') || '') !== 'true') { return false; }\n")
	b.WriteString("    if (search) {\n")
	b.WriteString("      var haystack = (row.getAttribute('data-filter-search') || '').toLowerCase();\n")
	b.WriteString("      if (haystack.indexOf(search) === -1) { return false; }\n")
	b.WriteString("    }\n")
	b.WriteString("    return true;\n")
	b.WriteString("  }\n")
	b.WriteString("  function applyFilters() {\n")
	b.WriteString("    if (!table) {\n")
	b.WriteString("      if (countEl) { countEl.textContent = '0 signatures shown'; }\n")
	b.WriteString("      return;\n")
	b.WriteString("    }\n")
	b.WriteString("    var inspectorRows = table.querySelectorAll('tbody tr.triage-row');\n")
	b.WriteString("    var detailRows = table.querySelectorAll('tbody tr.triage-errors-row');\n")
	b.WriteString("    var detailByParent = {};\n")
	b.WriteString("    for (var d = 0; d < detailRows.length; d++) {\n")
	b.WriteString("      var detail = detailRows[d];\n")
	b.WriteString("      var parentId = detail.getAttribute('data-parent-row-id') || '';\n")
	b.WriteString("      if (parentId) { detailByParent[parentId] = detail; }\n")
	b.WriteString("    }\n")
	b.WriteString("    var visibleInspector = 0;\n")
	b.WriteString("    for (var i = 0; i < inspectorRows.length; i++) {\n")
	b.WriteString("      var row = inspectorRows[i];\n")
	b.WriteString("      var show = rowMatches(row);\n")
	b.WriteString("      row.style.display = show ? '' : 'none';\n")
	b.WriteString("      var rowId = row.getAttribute('data-row-id') || '';\n")
	b.WriteString("      var detail = detailByParent[rowId];\n")
	b.WriteString("      if (detail) { detail.style.display = show ? '' : 'none'; }\n")
	b.WriteString("      if (show) { visibleInspector++; }\n")
	b.WriteString("    }\n")
	b.WriteString("    if (countEl) { countEl.textContent = visibleInspector + ' signatures shown'; }\n")
	b.WriteString("  }\n")
	b.WriteString("  var controls = [envSelect, laneSelect, minSupportInput, flaggedOnly, reviewOnly, searchInput];\n")
	b.WriteString("  for (var k = 0; k < controls.length; k++) {\n")
	b.WriteString("    var control = controls[k];\n")
	b.WriteString("    if (!control) { continue; }\n")
	b.WriteString("    control.addEventListener('input', applyFilters);\n")
	b.WriteString("    control.addEventListener('change', applyFilters);\n")
	b.WriteString("  }\n")
	b.WriteString("  if (table) {\n")
	b.WriteString("    var sortButtons = table.querySelectorAll('button.triage-sort-button');\n")
	b.WriteString("    for (var s = 0; s < sortButtons.length; s++) {\n")
	b.WriteString("      sortButtons[s].addEventListener('click', function () { window.setTimeout(applyFilters, 0); });\n")
	b.WriteString("    }\n")
	b.WriteString("  }\n")
	b.WriteString("  applyFilters();\n")
	b.WriteString("})();\n")
	b.WriteString("</script>\n")
	b.WriteString("</body>\n")
	b.WriteString("</html>\n")
	return b.String()
}

func observedQualityWindow(rows []qualitySignatureRow) (time.Time, time.Time, bool) {
	var minTS time.Time
	var maxTS time.Time
	for _, row := range rows {
		for _, ref := range row.References {
			ts, ok := parseTimestamp(ref.OccurredAt)
			if !ok {
				continue
			}
			ts = ts.UTC()
			if minTS.IsZero() || ts.Before(minTS) {
				minTS = ts
			}
			if maxTS.IsZero() || ts.After(maxTS) {
				maxTS = ts
			}
		}
	}
	if minTS.IsZero() || maxTS.IsZero() {
		return time.Time{}, time.Time{}, false
	}
	return minTS, maxTS, true
}

func resolvedQualityWindow(rows []qualitySignatureRow, configuredStart string, configuredEnd string) (time.Time, time.Time, bool) {
	if strings.TrimSpace(configuredStart) != "" && strings.TrimSpace(configuredEnd) != "" {
		start, end, ok := configuredReportWindowDisplayBounds(configuredStart, configuredEnd)
		if ok {
			return start, end, true
		}
	}
	return observedQualityWindow(rows)
}

func configuredReportWindowDisplayBounds(configuredStart string, configuredEnd string) (time.Time, time.Time, bool) {
	start, err := time.Parse(time.RFC3339, strings.TrimSpace(configuredStart))
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	endExclusive, err := time.Parse(time.RFC3339, strings.TrimSpace(configuredEnd))
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	if !start.Before(endExclusive) {
		return time.Time{}, time.Time{}, false
	}
	endInclusive := endExclusive.Add(-time.Nanosecond)
	return start.UTC(), endInclusive.UTC(), true
}

func inclusiveWindowDays(start time.Time, end time.Time) int {
	startDay := start.UTC().Truncate(24 * time.Hour)
	endDay := end.UTC().Truncate(24 * time.Hour)
	if endDay.Before(startDay) {
		return 0
	}
	days := int(endDay.Sub(startDay)/(24*time.Hour)) + 1
	if days < 1 {
		return 1
	}
	return days
}

func sortedValuesFromRows(rows []qualitySignatureRow, valueFn func(qualitySignatureRow) string) []string {
	set := map[string]struct{}{}
	for _, row := range rows {
		value := strings.TrimSpace(valueFn(row))
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func qualityRowsToTriageRows(rows []qualitySignatureRow) []triagehtml.SignatureRow {
	supportByEnvironment := map[string]int{}
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		supportByEnvironment[environment] += maxInt(row.SupportCount, 0)
	}

	out := make([]triagehtml.SignatureRow, 0, len(rows))
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		share := qualityPct(maxInt(row.SupportCount, 0), supportByEnvironment[environment])
		searchQuery := fmt.Sprintf(
			"lane=%s; job=%s; test=%s; suite=%s; success=%s",
			cleanInline(row.Lane, 40),
			cleanInline(row.JobName, 80),
			cleanInline(row.TestName, 120),
			cleanInline(row.TestSuite, 80),
			qualitySuccessLabel(row.PassRate, row.Runs),
		)
		contributingTests := qualityContributingTestsToTriage(row.ContributingTests)
		if len(contributingTests) == 0 {
			contributingTests = []triagehtml.ContributingTest{
				{
					Lane:         strings.TrimSpace(row.Lane),
					JobName:      strings.TrimSpace(row.JobName),
					TestName:     strings.TrimSpace(row.TestName),
					SupportCount: row.SupportCount,
				},
			}
		}
		triageRow := triagehtml.SignatureRow{
			Environment:       environment,
			Lane:              strings.TrimSpace(row.Lane),
			JobName:           strings.TrimSpace(row.JobName),
			TestName:          strings.TrimSpace(row.TestName),
			TestSuite:         strings.TrimSpace(row.TestSuite),
			Phrase:            strings.TrimSpace(row.Phrase),
			ClusterID:         strings.TrimSpace(row.Phase1ClusterID),
			SearchQuery:       searchQuery,
			SearchIndex:       strings.TrimSpace(row.SearchIndex),
			SupportCount:      row.SupportCount,
			SupportShare:      share,
			PostGoodCount:     row.PostGoodCommitCount,
			QualityScore:      row.QualityScore,
			QualityNoteLabels: append([]string(nil), row.IssueLabels...),
			ReviewNoteLabels:  append([]string(nil), row.ReviewReasons...),
			ContributingTests: contributingTests,
			FullErrorSamples:  append([]string(nil), row.FullErrorSamples...),
			References:        toTriageRunReferences(row.References),
		}
		if len(row.SparkCounts) > 0 {
			triageRow.TrendCounts = append([]int(nil), row.SparkCounts...)
			triageRow.TrendRange = strings.TrimSpace(row.SparkRange)
			triageRow.TrendSparkline = strings.TrimSpace(row.Sparkline)
		}
		out = append(out, triageRow)
	}
	return out
}

func qualityContributingTestsToTriage(rows []qualityContributingTest) []triagehtml.ContributingTest {
	if len(rows) == 0 {
		return nil
	}
	out := make([]triagehtml.ContributingTest, 0, len(rows))
	for _, row := range rows {
		out = append(out, triagehtml.ContributingTest{
			Lane:         strings.TrimSpace(row.Lane),
			JobName:      strings.TrimSpace(row.JobName),
			TestName:     strings.TrimSpace(row.TestName),
			SupportCount: row.SupportCount,
		})
	}
	return out
}

func qualitySuccessLabel(passRate *float64, runs int) string {
	if passRate == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.2f%% (%d runs)", *passRate, runs)
}

func maxInt(value int, minimum int) int {
	if value < minimum {
		return minimum
	}
	return value
}

func signatureRowHTML(row qualitySignatureRow, suspiciousOnly bool) string {
	var b strings.Builder
	rowClass := "quality-row suspicious-row"
	search := html.EscapeString(row.SearchIndex)
	b.WriteString(fmt.Sprintf(
		"      <tr class=\"%s\" data-env=\"%s\" data-lane=\"%s\" data-support=\"%d\" data-flagged=\"%t\" data-review=\"%t\" data-search=\"%s\">",
		rowClass,
		html.EscapeString(row.Environment),
		html.EscapeString(row.Lane),
		row.SupportCount,
		row.isFlagged(),
		row.hasReviewSignals(),
		search,
	))
	scoreClass := "score-low"
	if row.QualityScore >= 5 {
		scoreClass = "score-high"
	}
	b.WriteString(fmt.Sprintf("<td><span class=\"%s\">%d</span></td>", scoreClass, row.QualityScore))
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(strings.ToUpper(row.Environment))))
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(row.Lane)))
	b.WriteString(fmt.Sprintf("<td>%d</td>", row.SupportCount))
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(strings.Join(row.combinedFlagLabels(), ", "))))
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(cleanInline(row.Phrase, 220))))
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(row.exampleRunSummary())))
	if suspiciousOnly {
		b.WriteString("</tr>\n")
		return b.String()
	}
	b.WriteString("</tr>\n")
	return b.String()
}

func signatureInspectorRowHTML(row qualitySignatureRow, rowID string) string {
	var b strings.Builder
	phraseSortValue := strings.ToLower(strings.TrimSpace(row.Phrase))
	jobSortValue := strings.ToLower(strings.TrimSpace(row.JobName))
	testSortValue := strings.ToLower(strings.TrimSpace(row.TestName))
	jobTestSortValue := strings.TrimSpace(testSortValue + "|" + jobSortValue)
	phase1SortValue := strings.ToLower(strings.TrimSpace(row.Phase1ClusterID))
	b.WriteString(fmt.Sprintf(
		"      <tr class=\"quality-row inspector-row\" data-row-id=\"%s\" data-env=\"%s\" data-lane=\"%s\" data-support=\"%d\" data-score=\"%d\" data-phrase=\"%s\" data-job=\"%s\" data-test=\"%s\" data-jobtest=\"%s\" data-phase1=\"%s\" data-flagged=\"%t\" data-review=\"%t\" data-search=\"%s\">",
		html.EscapeString(strings.TrimSpace(rowID)),
		html.EscapeString(row.Environment),
		html.EscapeString(row.Lane),
		row.SupportCount,
		row.QualityScore,
		html.EscapeString(phraseSortValue),
		html.EscapeString(jobSortValue),
		html.EscapeString(testSortValue),
		html.EscapeString(jobTestSortValue),
		html.EscapeString(phase1SortValue),
		row.isFlagged(),
		row.hasReviewSignals(),
		html.EscapeString(row.SearchIndex),
	))
	scoreClass := "score-low"
	if row.QualityScore >= 5 {
		scoreClass = "score-high"
	}
	b.WriteString(fmt.Sprintf("<td><span class=\"%s\">%d</span></td>", scoreClass, row.QualityScore))
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(strings.ToUpper(row.Environment))))
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(row.Lane)))
	b.WriteString(fmt.Sprintf("<td>%d</td>", row.SupportCount))
	if row.PassRate != nil {
		b.WriteString(fmt.Sprintf("<td>%.2f%% (%d runs)</td>", *row.PassRate, row.Runs))
	} else {
		b.WriteString("<td>n/a</td>")
	}
	if row.Sparkline != "" {
		b.WriteString(fmt.Sprintf("<td title=\"%s (%s)\">%s</td>", html.EscapeString(formatCounts(row.SparkCounts)), html.EscapeString(row.SparkRange), html.EscapeString(row.Sparkline)))
	} else {
		b.WriteString("<td>n/a</td>")
	}
	b.WriteString(fmt.Sprintf("<td><span class=\"signature-text\">%s</span></td>", html.EscapeString(cleanInline(row.Phrase, 240))))
	b.WriteString("<td>")
	if len(row.IssueLabels) == 0 {
		b.WriteString("<span class=\"badge badge-ok\">ok</span>")
	} else {
		for _, label := range row.IssueLabels {
			b.WriteString(fmt.Sprintf("<span class=\"badge badge-issue\">%s</span>", html.EscapeString(label)))
		}
	}
	b.WriteString("</td>")
	b.WriteString("<td>")
	if len(row.ReviewReasons) == 0 {
		b.WriteString("<span class=\"badge badge-ok\">none</span>")
	} else {
		for _, reason := range row.ReviewReasons {
			b.WriteString(fmt.Sprintf("<span class=\"badge badge-review\">%s</span>", html.EscapeString(reason)))
		}
	}
	b.WriteString("</td>")
	b.WriteString(fmt.Sprintf("<td><div><strong>%s</strong></div><div class=\"muted\">%s</div><div class=\"muted\">%s</div></td>",
		html.EscapeString(cleanInline(row.TestName, 120)),
		html.EscapeString(cleanInline(row.JobName, 80)),
		html.EscapeString(cleanInline(row.TestSuite, 80)),
	))
	b.WriteString("</tr>\n")
	return b.String()
}

func signatureInspectorDetailRowHTML(row qualitySignatureRow, rowID string, colSpan int) string {
	var b strings.Builder
	if colSpan <= 0 {
		colSpan = 1
	}
	b.WriteString(fmt.Sprintf(
		"      <tr class=\"inspector-errors-row\" data-parent-id=\"%s\"><td colspan=\"%d\">",
		html.EscapeString(strings.TrimSpace(rowID)),
		colSpan,
	))
	b.WriteString("<div class=\"inspector-detail-actions\">")
	if len(row.FullErrorSamples) == 0 {
		b.WriteString("<span class=\"muted\">Full errors: n/a</span>")
	} else {
		b.WriteString(fmt.Sprintf("<details class=\"full-errors-toggle\"><summary>Full errors (%d)</summary>", len(row.FullErrorSamples)))
		for _, sample := range row.FullErrorSamples {
			b.WriteString("<pre>")
			b.WriteString(html.EscapeString(sample))
			b.WriteString("</pre>")
		}
		b.WriteString("</details>")
	}
	b.WriteString(renderInspectorAffectedRunsDetails(row.References))
	b.WriteString("</div>")
	b.WriteString("</td></tr>\n")
	return b.String()
}

func renderInspectorAffectedRunsDetails(rows []reference) string {
	runs := triagehtml.OrderedUniqueReferences(toTriageRunReferences(rows))
	var b strings.Builder
	b.WriteString(fmt.Sprintf("<details class=\"affected-runs-toggle\"><summary>Affected runs (%d)</summary>", len(runs)))
	if len(runs) == 0 {
		b.WriteString("<span class=\"muted\">No affected runs available.</span>")
		b.WriteString("</details>")
		return b.String()
	}
	b.WriteString("<div class=\"runs-scroll\"><table class=\"runs-table\"><thead><tr><th>Date</th><th>Associated PR</th><th>Prow job</th></tr></thead><tbody>")
	for _, row := range runs {
		b.WriteString("<tr>")
		b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(triagehtml.FormatReferenceTimestampLabel(row.OccurredAt))))
		b.WriteString(fmt.Sprintf("<td>%s</td>", renderInspectorAssociatedPR(row)))
		b.WriteString(fmt.Sprintf("<td>%s</td>", renderInspectorProwJobLink(row)))
		b.WriteString("</tr>")
	}
	b.WriteString("</tbody></table></div></details>")
	return b.String()
}

func renderInspectorAssociatedPR(row triagehtml.RunReference) string {
	if row.PRNumber <= 0 {
		return "<span class=\"muted\">n/a</span>"
	}
	label := fmt.Sprintf("PR #%d", row.PRNumber)
	if prURL, ok := resolveInspectorGitHubPRURLFromProwRun(strings.TrimSpace(row.RunURL), row.PRNumber); ok {
		return fmt.Sprintf(
			"<a href=\"%s\" target=\"_blank\" rel=\"noopener noreferrer\">%s</a>",
			html.EscapeString(prURL),
			html.EscapeString(label),
		)
	}
	return html.EscapeString(label)
}

func renderInspectorProwJobLink(row triagehtml.RunReference) string {
	runURL := strings.TrimSpace(row.RunURL)
	if runURL == "" {
		return "<span class=\"muted\">n/a</span>"
	}
	return fmt.Sprintf(
		"<a href=\"%s\" target=\"_blank\" rel=\"noopener noreferrer\">prow job</a>",
		html.EscapeString(runURL),
	)
}

func toTriageRunReferences(rows []reference) []triagehtml.RunReference {
	out := make([]triagehtml.RunReference, 0, len(rows))
	for _, row := range rows {
		out = append(out, triagehtml.RunReference{
			RunURL:      strings.TrimSpace(row.RunURL),
			OccurredAt:  strings.TrimSpace(row.OccurredAt),
			SignatureID: strings.TrimSpace(row.SignatureID),
			PRNumber:    row.PRNumber,
		})
	}
	return out
}

func resolveInspectorGitHubPRURLFromProwRun(runURL string, prNumber int) (string, bool) {
	return triagehtml.ResolveGitHubPRURLFromProwRun(
		runURL,
		prNumber,
		defaultGitHubRepoOwner,
		defaultGitHubRepoName,
	)
}

func (row qualitySignatureRow) isFlagged() bool {
	return len(row.IssueCodes) > 0 || len(row.ReviewReasons) > 0
}

func (row qualitySignatureRow) hasReviewSignals() bool {
	return len(row.ReviewReasons) > 0
}

func (row qualitySignatureRow) combinedFlagLabels() []string {
	out := make([]string, 0, len(row.IssueLabels)+len(row.ReviewReasons))
	out = append(out, row.IssueLabels...)
	for _, reason := range row.ReviewReasons {
		out = append(out, "review:"+reason)
	}
	if len(out) == 0 {
		out = append(out, "none")
	}
	return out
}

func (row qualitySignatureRow) exampleRunSummary() string {
	if len(row.RecentRuns) == 0 {
		return "n/a"
	}
	run := row.RecentRuns[0]
	label := run.OccurredAt
	if strings.TrimSpace(label) == "" {
		label = "unknown-time"
	}
	if run.PRNumber > 0 {
		label = fmt.Sprintf("%s PR#%d", label, run.PRNumber)
	}
	return label
}

func qualityIssueCodes(cluster testCluster, fullErrorSamples []string) []string {
	return qualityIssueCodesForPhrase(cluster.CanonicalEvidencePhrase, fullErrorSamples)
}

func qualityIssueCodesForPhrase(rawPhrase string, fullErrorSamples []string) []string {
	phrase := strings.TrimSpace(rawPhrase)
	normalized := strings.ToLower(phrase)
	set := map[string]struct{}{}
	add := func(code string) {
		if strings.TrimSpace(code) == "" {
			return
		}
		set[code] = struct{}{}
	}

	if phrase == "" {
		add("empty_phrase")
	}
	if isGenericFailurePhrase(phrase) {
		add("generic_failure_phrase")
	}
	if len([]rune(strings.TrimSpace(phrase))) > 0 && len([]rune(strings.TrimSpace(phrase))) <= 3 {
		add("too_short_phrase")
	}
	if strings.Contains(normalized, "<context.") {
		add("context_type_stub")
	}
	if strings.Contains(normalized, "errorcode:\"\"") || strings.Contains(normalized, "errorcode: \"\"") || strings.Contains(normalized, "errorcode:''") || strings.Contains(normalized, "errorcode: ''") {
		add("empty_error_code")
	}
	if isLikelyStructFragment(phrase) {
		add("struct_fragment")
	}
	if isMostlyPunctuation(phrase) {
		add("mostly_punctuation")
	}
	if isSourceDeserializationNoOutput(phrase, fullErrorSamples) {
		add("source_deserialization_no_output")
	}
	if len(fullErrorSamples) == 0 {
		add("missing_full_error_sample")
	}

	out := make([]string, 0, len(set))
	for code := range set {
		out = append(out, code)
	}
	sort.Slice(out, func(i, j int) bool {
		if qualityIssueWeight(out[i]) != qualityIssueWeight(out[j]) {
			return qualityIssueWeight(out[i]) > qualityIssueWeight(out[j])
		}
		return out[i] < out[j]
	})
	return out
}

func qualityIssueLabel(code string) string {
	switch strings.TrimSpace(code) {
	case "empty_phrase":
		return "empty phrase"
	case "too_short_phrase":
		return "very short phrase"
	case "generic_failure_phrase":
		return "generic fallback phrase"
	case "context_type_stub":
		return "context type stub leaked"
	case "empty_error_code":
		return "contains empty ErrorCode"
	case "struct_fragment":
		return "struct/object fragment"
	case "mostly_punctuation":
		return "mostly punctuation"
	case "source_deserialization_no_output":
		return "source deserialization/no-output error"
	case "missing_full_error_sample":
		return "missing full error sample"
	default:
		return code
	}
}

func qualityIssueScore(issueCodes []string, reviewReasons []string) int {
	score := 0
	for _, code := range issueCodes {
		score += qualityIssueWeight(code)
	}
	score += len(reviewReasons) * 2
	return score
}

func qualityIssueWeight(code string) int {
	switch strings.TrimSpace(code) {
	case "empty_phrase":
		return 6
	case "struct_fragment":
		return 5
	case "context_type_stub":
		return 4
	case "empty_error_code":
		return 4
	case "too_short_phrase":
		return 3
	case "generic_failure_phrase":
		return 5
	case "mostly_punctuation":
		return 3
	case "source_deserialization_no_output":
		return 9
	case "missing_full_error_sample":
		return 1
	default:
		return 1
	}
}

func isLikelyStructFragment(input string) bool {
	trimmed := strings.TrimSpace(input)
	lower := strings.ToLower(trimmed)
	switch lower {
	case "{", "}", "[]", "{}", "{},", "null":
		return true
	}
	if strings.HasPrefix(trimmed, "{") && strings.Contains(trimmed, ":") {
		return true
	}
	if strings.HasSuffix(trimmed, "},") || strings.HasSuffix(trimmed, "{}") || strings.HasSuffix(trimmed, ">{},") {
		return true
	}
	if strings.Contains(trimmed, "ErrorCode:") {
		return true
	}
	return false
}

func isMostlyPunctuation(input string) bool {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return false
	}
	alphaNumericCount := 0
	punctuationCount := 0
	for _, char := range trimmed {
		switch {
		case unicode.IsLetter(char), unicode.IsDigit(char):
			alphaNumericCount++
		case unicode.IsSpace(char):
			continue
		default:
			punctuationCount++
		}
	}
	if alphaNumericCount == 0 && punctuationCount > 0 {
		return true
	}
	wordCount := len(strings.Fields(trimmed))
	return punctuationCount >= (alphaNumericCount*2) && wordCount <= 4
}

func isGenericFailurePhrase(input string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(input)), " "))
	switch normalized {
	case "failure", "failure occurred", "unknown failure":
		return true
	default:
		return false
	}
}

func isSourceDeserializationNoOutput(phrase string, fullErrorSamples []string) bool {
	hasDeserializationNoOutput := containsDeserializationNoOutputSignal(phrase)
	for _, sample := range fullErrorSamples {
		if containsDeserializationNoOutputSignal(sample) {
			hasDeserializationNoOutput = true
			break
		}
	}
	if !hasDeserializationNoOutput {
		return false
	}

	// If command execution details are present, the signature has concrete
	// semantic context and should not be treated as a no-output defect.
	if containsCompanionCommandErrorSignal(phrase) {
		return false
	}
	for _, sample := range fullErrorSamples {
		if containsCompanionCommandErrorSignal(sample) {
			return false
		}
	}
	return true
}

func containsDeserializationNoOutputSignal(value string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
	if normalized == "" {
		return false
	}
	hasDeserialization := strings.Contains(normalized, "deserializaion error") || strings.Contains(normalized, "deserialization error")
	return hasDeserialization && strings.Contains(normalized, "no output from command")
}

func containsCompanionCommandErrorSignal(value string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
	if normalized == "" {
		return false
	}
	if !strings.Contains(normalized, "command error:") {
		return false
	}
	if strings.Contains(normalized, "command error: no output from command") {
		return false
	}
	if strings.Contains(normalized, "command error: exit status ") {
		return true
	}
	parts := strings.SplitN(normalized, "command error:", 2)
	if len(parts) < 2 {
		return false
	}
	detail := strings.TrimSpace(parts[1])
	return detail != "" && detail != "no output from command"
}

func toQualityFlaggedSignatureExports(rows []qualitySignatureRow) []qualityFlaggedSignatureExport {
	out := make([]qualityFlaggedSignatureExport, 0, len(rows))
	for _, row := range rows {
		if !row.isFlagged() {
			continue
		}
		exportRow := qualityFlaggedSignatureExport{
			Environment:             row.Environment,
			Lane:                    row.Lane,
			JobName:                 row.JobName,
			TestName:                row.TestName,
			TestSuite:               row.TestSuite,
			Phase1ClusterID:         row.Phase1ClusterID,
			CanonicalEvidencePhrase: row.Phrase,
			SupportCount:            row.SupportCount,
			PostGoodCommitCount:     row.PostGoodCommitCount,
			CurrentRuns:             row.Runs,
			QualityIssueCodes:       append([]string(nil), row.IssueCodes...),
			QualityIssueLabels:      append([]string(nil), row.IssueLabels...),
			ReviewReasons:           append([]string(nil), row.ReviewReasons...),
			Score:                   row.QualityScore,
			FullErrorSamples:        append([]string(nil), row.FullErrorSamples...),
		}
		if row.PassRate != nil {
			exportRow.CurrentPassPercentage = float64Ptr(*row.PassRate)
		}
		for _, run := range row.RecentRuns {
			exportRow.RecentRuns = append(exportRow.RecentRuns, qualityExportRun{
				RunURL:     strings.TrimSpace(run.RunURL),
				OccurredAt: strings.TrimSpace(run.OccurredAt),
				PRNumber:   run.PRNumber,
			})
		}
		out = append(out, exportRow)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		if out[i].SupportCount != out[j].SupportCount {
			return out[i].SupportCount > out[j].SupportCount
		}
		if out[i].Environment != out[j].Environment {
			return out[i].Environment < out[j].Environment
		}
		if out[i].Lane != out[j].Lane {
			return out[i].Lane < out[j].Lane
		}
		if out[i].TestName != out[j].TestName {
			return out[i].TestName < out[j].TestName
		}
		return out[i].Phase1ClusterID < out[j].Phase1ClusterID
	})
	return out
}

func writeQualityFlaggedSignatures(outputPath string, rows []qualityFlaggedSignatureExport) error {
	trimmedOutputPath := strings.TrimSpace(outputPath)
	if trimmedOutputPath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(trimmedOutputPath), 0o755); err != nil {
		return fmt.Errorf("create quality export directory: %w", err)
	}

	var b strings.Builder
	for _, row := range rows {
		payload, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("marshal quality export row: %w", err)
		}
		b.Write(payload)
		b.WriteString("\n")
	}
	if err := os.WriteFile(trimmedOutputPath, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write quality export artifact: %w", err)
	}
	return nil
}

func qualityCardHTML(label string, value string) string {
	return fmt.Sprintf(
		"    <div class=\"card\"><div class=\"label\">%s</div><div class=\"value\">%s</div></div>\n",
		html.EscapeString(strings.TrimSpace(label)),
		html.EscapeString(strings.TrimSpace(value)),
	)
}

func qualityPct(value int, total int) float64 {
	if total <= 0 || value <= 0 {
		return 0
	}
	return (float64(value) * 100.0) / float64(total)
}
