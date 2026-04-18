package ui

import (
	"fmt"
	"html"
	"net/url"
	"sort"
	"strings"
	"time"

	frontreadmodel "ci-failure-atlas/pkg/frontend/readmodel"
	sourceoptions "ci-failure-atlas/pkg/source/options"
)

type RunReference = frontreadmodel.RunReference

type ContributingTest = frontreadmodel.ContributingTest

type FailurePatternRow = frontreadmodel.FailurePatternRow

func OrderedUniqueReferences(rows []RunReference) []RunReference {
	return frontreadmodel.OrderedUniqueReferences(rows)
}

func OrderedContributingTests(items []ContributingTest) []ContributingTest {
	return frontreadmodel.OrderedContributingTests(items)
}

func ParseReferenceTimestamp(value string) (time.Time, bool) {
	return frontreadmodel.ParseReferenceTimestamp(value)
}

func QualityIssueCodes(phrase string) []string {
	return frontreadmodel.QualityIssueCodes(phrase)
}

func QualityScore(issueCodes []string) int {
	return frontreadmodel.QualityScore(issueCodes)
}

func QualityIssueLabel(code string) string {
	return frontreadmodel.QualityIssueLabel(code)
}

func DailyDensitySparkline(references []RunReference, windowDays int, endAnchor time.Time) (string, []int, string, bool) {
	return frontreadmodel.DailyDensitySparkline(references, windowDays, endAnchor)
}

func BadPRScoreAndReasons(row FailurePatternRow) (int, []string) {
	return frontreadmodel.BadPRScoreAndReasons(row)
}

func FlakeScoreAndReasons(row FailurePatternRow) (int, []string) {
	return frontreadmodel.FlakeScoreAndReasons(row)
}

func SortRowsByDefaultPriority(rows []FailurePatternRow) {
	frontreadmodel.SortRowsByDefaultPriority(rows)
}

func SortRowsByBadPRScore(rows []FailurePatternRow) {
	frontreadmodel.SortRowsByBadPRScore(rows)
}

func affectedJobCount(row FailurePatternRow) int {
	return len(OrderedUniqueReferences(row.AffectedRuns))
}

func rowAffectedReferences(row FailurePatternRow) []RunReference {
	combined := append([]RunReference(nil), row.AffectedRuns...)
	for _, child := range row.LinkedPatterns {
		combined = append(combined, child.AffectedRuns...)
	}
	return OrderedUniqueReferences(combined)
}

func rowPostGoodCount(row FailurePatternRow) int {
	if len(row.LinkedPatterns) == 0 {
		return row.AfterLastPushCount
	}
	total := 0
	for _, child := range row.LinkedPatterns {
		total += child.AfterLastPushCount
	}
	if total > 0 {
		return total
	}
	return row.AfterLastPushCount
}

func rowJobsAffected(row FailurePatternRow) int {
	if refs := rowAffectedReferences(row); len(refs) > 0 {
		return len(refs)
	}
	return affectedJobCount(row)
}

func totalAffectedJobs(rows []FailurePatternRow) int {
	seenRuns := map[string]struct{}{}
	for _, row := range rows {
		for _, reference := range rowAffectedReferences(row) {
			runURL := strings.TrimSpace(reference.RunURL)
			if runURL == "" {
				continue
			}
			seenRuns[runURL] = struct{}{}
		}
	}
	return len(seenRuns)
}

func impactShare(jobsAffected int, impactTotalJobs int) float64 {
	if impactTotalJobs <= 0 {
		return 0
	}
	return (float64(jobsAffected) * 100.0) / float64(impactTotalJobs)
}

func impactScoreClass(percent float64) string {
	switch {
	case percent >= 20:
		return "impact-high"
	case percent >= 8:
		return "impact-medium"
	default:
		return "impact-low"
	}
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func flakeScoreClass(score int) string {
	switch {
	case score >= 10:
		return "flake-high"
	case score >= 5:
		return "flake-medium"
	default:
		return "flake-low"
	}
}

func flakeScoreLabel(score int) string {
	switch {
	case score >= 10:
		return "High"
	case score >= 5:
		return "Medium"
	default:
		return "Low"
	}
}

func sortRowsByDefaultPriorityWithImpact(rows []FailurePatternRow, impactTotalJobs int) {
	sort.Slice(rows, func(i, j int) bool {
		impactI := impactShare(rowJobsAffected(rows[i]), impactTotalJobs)
		impactJ := impactShare(rowJobsAffected(rows[j]), impactTotalJobs)
		if impactI != impactJ {
			return impactI > impactJ
		}
		jobsI := rowJobsAffected(rows[i])
		jobsJ := rowJobsAffected(rows[j])
		if jobsI != jobsJ {
			return jobsI > jobsJ
		}
		flakeI, _ := FlakeScoreAndReasons(rows[i])
		flakeJ, _ := FlakeScoreAndReasons(rows[j])
		if flakeI != flakeJ {
			return flakeI > flakeJ
		}
		if rows[i].OccurrenceShare != rows[j].OccurrenceShare {
			return rows[i].OccurrenceShare > rows[j].OccurrenceShare
		}
		if rows[i].Occurrences != rows[j].Occurrences {
			return rows[i].Occurrences > rows[j].Occurrences
		}
		postGoodI := rowPostGoodCount(rows[i])
		postGoodJ := rowPostGoodCount(rows[j])
		if postGoodI != postGoodJ {
			return postGoodI > postGoodJ
		}
		if strings.TrimSpace(rows[i].Environment) != strings.TrimSpace(rows[j].Environment) {
			return strings.TrimSpace(rows[i].Environment) < strings.TrimSpace(rows[j].Environment)
		}
		if strings.TrimSpace(rows[i].FailurePattern) != strings.TrimSpace(rows[j].FailurePattern) {
			return strings.TrimSpace(rows[i].FailurePattern) < strings.TrimSpace(rows[j].FailurePattern)
		}
		return strings.TrimSpace(rows[i].FailurePatternID) < strings.TrimSpace(rows[j].FailurePatternID)
	})
}

type TableOptions struct {
	// Deprecated: prefer ShowQualityFlags.
	IncludeQualityNotes bool
	// Deprecated: prefer ShowQualityScore.
	HideQualityScore         bool
	ShowQualityFlags         bool
	ShowReviewFlags          bool
	ShowQualityScore         bool
	IncludeTrend             bool
	TrendHeaderLabel         string
	GitHubRepoOwner          string
	GitHubRepoName           string
	FullErrorsSummaryLabel   string
	ContributingSummaryLabel string
	AffectedRunsSummaryLabel string
	LoadedRowsLimit          int
	InitialVisibleRows       int
	InitialSortKey           string
	InitialSortDirection     string
	ImpactTotalJobs          int
	ShowLinkedChildQuality   bool
	ShowLinkedChildReview    bool
	ShowLinkedChildRemove    bool
	ShowCount                bool
	ShowAfterLastPush        bool
	ShowShare                bool
	ShowManualIssue          bool
	IncludeSelection         bool
	SelectionInputName       string
}

type ReportView string

const (
	ReportViewRolling         ReportView = "rolling"
	ReportViewReport          ReportView = "report"
	ReportViewFailurePatterns ReportView = "failure-patterns"
	ReportViewRunLog          ReportView = "run-log"
)

type ReportChromeOptions struct {
	WindowLabel         string
	CurrentWeek         string
	CurrentView         ReportView
	PreviousWeek        string
	PreviousHref        string
	NextWeek            string
	NextHref            string
	RollingHref         string
	ReportHref          string
	FailurePatternsHref string
	RunLogHref          string
	ArchiveHref         string
}

const (
	defaultLoadedRowsLimit  = 50
	defaultSortKey          = sortKeyImpact
	defaultSortDirection    = "desc"
	sortDirectionAscending  = "asc"
	sortDirectionDescending = "desc"
	sortKeyCount            = "count"
	sortKeyJobsAffected     = "jobs_affected"
	sortKeyAfterLastPush    = "after_last_push"
	sortKeyFlakeScore       = "flake_score"
	sortKeyShare            = "share"
	sortKeyImpact           = "impact"
	sortKeyManualCluster    = "manual_cluster"
)

func StylesCSS() string {
	return strings.Join([]string{
		"    .failure-patterns-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0 12px; }",
		"    .failure-patterns-table th, .failure-patterns-table td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; vertical-align: top; }",
		"    .failure-patterns-table thead th:first-child { width: 36%; max-width: 360px; }",
		"    .failure-patterns-table tbody tr:not(.failure-patterns-errors-row) > td:first-child { width: 36%; max-width: 360px; }",
		"    .failure-patterns-table th { background: #f3f4f6; color: #374151; font-weight: 700; }",
		"    .failure-patterns-select-col { width: 38px; text-align: center; }",
		"    .failure-patterns-row-select { width: 14px; height: 14px; cursor: pointer; }",
		"    .failure-patterns-table th.failure-patterns-sortable { white-space: nowrap; }",
		"    .failure-patterns-sort-button { all: unset; display: inline-flex; align-items: center; gap: 4px; cursor: pointer; color: inherit; font: inherit; font-weight: 700; }",
		"    .failure-patterns-sort-indicator { display: inline-block; min-width: 10px; text-align: center; font-size: 10px; color: #6b7280; }",
		"    .failure-patterns-table tr.failure-patterns-errors-row td { background: #eff6ff; }",
		"    .badge { display: inline-block; border-radius: 999px; padding: 2px 8px; font-size: 11px; margin: 1px 2px 1px 0; }",
		"    .badge-quality { background: #fee2e2; color: #991b1b; }",
		"    .badge-review { background: #fef3c7; color: #92400e; }",
		"    .badge-ok { background: #dcfce7; color: #166534; }",
		"    .quality-high { color: #991b1b; font-weight: 700; }",
		"    .quality-low { color: #374151; }",
		"    .impact-score { font-weight: 700; color: inherit; }",
		"    .impact-high { color: inherit; }",
		"    .impact-medium { color: inherit; }",
		"    .impact-low { color: inherit; }",
		"    .flake-score { font-weight: 700; }",
		"    .flake-high { color: #166534; }",
		"    .flake-medium { color: #92400e; }",
		"    .flake-low { color: #6b7280; }",
		"    .bad-pr-flag { display: inline-flex; align-items: center; justify-content: center; margin-right: 6px; color: #dc2626; font-weight: 700; }",
		"    .failure-patterns-header-help { display: inline-flex; align-items: center; justify-content: center; margin-left: 5px; width: 14px; height: 14px; border-radius: 999px; border: 1px solid #93c5fd; color: #1d4ed8; background: #eff6ff; font-size: 10px; font-weight: 700; cursor: help; vertical-align: middle; }",
		"    .trend-svg { display: block; }",
		"    details { margin: 2px 0; }",
		"    details summary { cursor: pointer; color: #1d4ed8; }",
		"    details.failure-pattern-toggle > summary { font-size: 13px; font-weight: 700; color: #111827; }",
		"    .failure-patterns-errors-row .failure-pattern-detail-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-start; }",
		"    .failure-patterns-errors-row details.full-errors-toggle, .failure-patterns-errors-row details.affected-runs-toggle, .failure-patterns-errors-row details.contributing-tests-toggle { margin: 0; }",
		"    .failure-patterns-errors-row details.full-errors-toggle > summary, .failure-patterns-errors-row details.affected-runs-toggle > summary, .failure-patterns-errors-row details.contributing-tests-toggle > summary { display: inline-flex; align-items: center; gap: 6px; font-size: 9px; font-weight: 600; color: #1e3a8a; background: #dbeafe; border: 1px solid #93c5fd; border-radius: 999px; padding: 2px 10px; }",
		"    .failure-patterns-errors-row details.full-errors-toggle[open] > summary, .failure-patterns-errors-row details.affected-runs-toggle[open] > summary, .failure-patterns-errors-row details.contributing-tests-toggle[open] > summary { background: #bfdbfe; border-color: #60a5fa; color: #1e40af; }",
		"    .failure-patterns-errors-row details.linked-failure-patterns-toggle > summary, .failure-patterns-errors-row details.linked-child-toggle > summary { display: inline-flex; align-items: center; gap: 6px; font-size: 11px; font-weight: 700; color: #1e3a8a; background: #dbeafe; border: 1px solid #93c5fd; border-radius: 8px; padding: 4px 10px; }",
		"    .failure-patterns-errors-row details.linked-failure-patterns-toggle[open] > summary, .failure-patterns-errors-row details.linked-child-toggle[open] > summary { background: #bfdbfe; border-color: #60a5fa; color: #1e40af; }",
		"    .linked-failure-pattern-list { display: flex; flex-direction: column; gap: 8px; margin-top: 8px; }",
		"    .linked-failure-pattern-item { border: 1px solid #bfdbfe; border-radius: 8px; background: #eff6ff; padding: 6px 8px; }",
		"    .linked-failure-pattern-item-remove { display: inline-flex; align-items: center; justify-content: center; width: 18px; height: 18px; margin-right: 6px; border: 1px solid #93c5fd; border-radius: 999px; background: #fff; color: #1e40af; font-size: 12px; font-weight: 700; line-height: 1; cursor: pointer; }",
		"    .linked-failure-pattern-item-remove:hover { background: #dbeafe; }",
		"    .linked-failure-pattern-item-summary { display: inline-flex; flex-wrap: wrap; align-items: center; gap: 8px; }",
		"    .linked-failure-pattern-item-meta { color: #4b5563; font-size: 11px; }",
		"    .linked-failure-pattern-item-flags { margin: 6px 0 6px; }",
		"    .linked-failure-pattern-item-header { margin-top: 4px; }",
		"    .failure-patterns-errors-row .runs-scroll { margin-top: 6px; max-height: 172px; overflow-y: auto; border: 1px solid #bfdbfe; border-radius: 6px; background: #eff6ff; }",
		"    .failure-patterns-errors-row .runs-table { border-collapse: collapse; width: 100%; font-size: 11px; }",
		"    .failure-patterns-errors-row .runs-table th, .failure-patterns-errors-row .runs-table td { padding: 4px 6px; border-bottom: 1px solid #dbeafe; text-align: left; vertical-align: top; }",
		"    .failure-patterns-errors-row .runs-table th { position: sticky; top: 0; background: #dbeafe; z-index: 1; }",
		"    .failure-patterns-errors-row .tests-scroll { margin-top: 6px; max-height: 172px; overflow-y: auto; border: 1px solid #bfdbfe; border-radius: 6px; background: #eff6ff; }",
		"    .failure-patterns-errors-row .tests-table { border-collapse: collapse; width: 100%; font-size: 11px; }",
		"    .failure-patterns-errors-row .tests-table th, .failure-patterns-errors-row .tests-table td { padding: 4px 6px; border-bottom: 1px solid #dbeafe; text-align: left; vertical-align: top; }",
		"    .failure-patterns-errors-row .tests-table th { position: sticky; top: 0; background: #dbeafe; z-index: 1; }",
		"    pre { white-space: pre-wrap; word-break: break-word; background: #111827; color: #f9fafb; padding: 8px; border-radius: 6px; font-size: 11px; margin: 6px 0 0; }",
	}, "\n") + "\n"
}

func ThemeCSS() string {
	return strings.Join([]string{
		"    .theme-toggle-wrap { position: fixed; top: 12px; right: 12px; z-index: 999; }",
		"    .theme-toggle { border: 1px solid #d1d5db; border-radius: 999px; background: #ffffff; color: #1f2937; font-size: 12px; font-weight: 600; padding: 4px 10px; cursor: pointer; box-shadow: 0 1px 2px rgba(0,0,0,0.08); }",
		"    .theme-toggle:hover { background: #f3f4f6; }",
		"    :root[data-theme=\"dark\"] body { background: #0b1220; color: #e2e8f0; }",
		"    :root[data-theme=\"dark\"] a { color: #93c5fd; }",
		"    :root[data-theme=\"dark\"] .theme-toggle { background: #111827; border-color: #334155; color: #e2e8f0; }",
		"    :root[data-theme=\"dark\"] .theme-toggle:hover { background: #1f2937; }",
		"    :root[data-theme=\"dark\"] .meta, :root[data-theme=\"dark\"] .muted, :root[data-theme=\"dark\"] .label, :root[data-theme=\"dark\"] .legend, :root[data-theme=\"dark\"] .section-note, :root[data-theme=\"dark\"] .outcome-values, :root[data-theme=\"dark\"] .filters label, :root[data-theme=\"dark\"] .filters .results { color: #94a3b8; }",
		"    :root[data-theme=\"dark\"] .env, :root[data-theme=\"dark\"] .section, :root[data-theme=\"dark\"] .card, :root[data-theme=\"dark\"] .filters, :root[data-theme=\"dark\"] .drill-tab, :root[data-theme=\"dark\"] .outcome-bar, :root[data-theme=\"dark\"] .outcome-bar-empty { background: #111827; border-color: #334155; color: #e2e8f0; }",
		"    :root[data-theme=\"dark\"] .drill-tab.active { background: #2563eb; border-color: #2563eb; color: #e2e8f0; }",
		"    :root[data-theme=\"dark\"] .chart-controls { color: #cbd5e1; }",
		"    :root[data-theme=\"dark\"] .detail-table th, :root[data-theme=\"dark\"] .overview-table th, :root[data-theme=\"dark\"] .quality-table th, :root[data-theme=\"dark\"] .failure-patterns-table th { background: #1f2937; color: #e2e8f0; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .detail-table td, :root[data-theme=\"dark\"] .overview-table td, :root[data-theme=\"dark\"] .quality-table td, :root[data-theme=\"dark\"] .failure-patterns-table td { border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-table tr.failure-patterns-errors-row td, :root[data-theme=\"dark\"] .quality-table tr.inspector-errors-row td { background: #0f172a; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-errors-row .runs-scroll, :root[data-theme=\"dark\"] .failure-patterns-errors-row .tests-scroll, :root[data-theme=\"dark\"] .inspector-errors-row .runs-scroll { background: #0f172a; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-errors-row .runs-table th, :root[data-theme=\"dark\"] .failure-patterns-errors-row .tests-table th, :root[data-theme=\"dark\"] .inspector-errors-row .runs-table th { background: #1e293b; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-errors-row .runs-table th, :root[data-theme=\"dark\"] .failure-patterns-errors-row .runs-table td, :root[data-theme=\"dark\"] .failure-patterns-errors-row .tests-table th, :root[data-theme=\"dark\"] .failure-patterns-errors-row .tests-table td, :root[data-theme=\"dark\"] .inspector-errors-row .runs-table th, :root[data-theme=\"dark\"] .inspector-errors-row .runs-table td { border-bottom-color: #334155; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-errors-row details.full-errors-toggle > summary, :root[data-theme=\"dark\"] .failure-patterns-errors-row details.affected-runs-toggle > summary, :root[data-theme=\"dark\"] .failure-patterns-errors-row details.contributing-tests-toggle > summary { color: #e2e8f0; background: #1f2937; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-errors-row details.full-errors-toggle[open] > summary, :root[data-theme=\"dark\"] .failure-patterns-errors-row details.affected-runs-toggle[open] > summary, :root[data-theme=\"dark\"] .failure-patterns-errors-row details.contributing-tests-toggle[open] > summary { color: #e2e8f0; background: #2563eb; border-color: #2563eb; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-errors-row details.linked-failure-patterns-toggle > summary, :root[data-theme=\"dark\"] .failure-patterns-errors-row details.linked-child-toggle > summary { color: #e2e8f0; background: #1f2937; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-errors-row details.linked-failure-patterns-toggle[open] > summary, :root[data-theme=\"dark\"] .failure-patterns-errors-row details.linked-child-toggle[open] > summary { color: #e2e8f0; background: #2563eb; border-color: #2563eb; }",
		"    :root[data-theme=\"dark\"] .linked-failure-pattern-item { background: #0f172a; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .linked-failure-pattern-item-remove { background: #111827; border-color: #334155; color: #93c5fd; }",
		"    :root[data-theme=\"dark\"] .linked-failure-pattern-item-remove:hover { background: #1f2937; }",
		"    :root[data-theme=\"dark\"] .linked-failure-pattern-item-meta { color: #94a3b8; }",
		"    :root[data-theme=\"dark\"] pre { background: #020617; color: #e2e8f0; border: 1px solid #334155; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-header-help { border-color: #334155; color: #93c5fd; background: #1e293b; }",
		"    :root[data-theme=\"dark\"] .failure-patterns-sort-indicator { color: #94a3b8; }",
		"    :root[data-theme=\"dark\"] .impact-high { color: inherit; }",
		"    :root[data-theme=\"dark\"] .impact-medium { color: inherit; }",
		"    :root[data-theme=\"dark\"] .impact-low { color: inherit; }",
		"    :root[data-theme=\"dark\"] .flake-high { color: #34d399; }",
		"    :root[data-theme=\"dark\"] .flake-medium { color: #fbbf24; }",
		"    :root[data-theme=\"dark\"] .flake-low { color: #94a3b8; }",
		"    :root[data-theme=\"dark\"] details summary { color: #93c5fd; }",
	}, "\n") + "\n"
}

func ThemeToggleHTML() string {
	return fmt.Sprintf("  <div class=\"theme-toggle-wrap\">%s</div>\n", ThemeToggleButtonHTML())
}

func ThemeToggleButtonHTML() string {
	return "<button id=\"theme-toggle\" class=\"theme-toggle\" type=\"button\" aria-label=\"Toggle theme mode\">Theme: Auto</button>"
}

func ReportChromeCSS() string {
	return strings.Join([]string{
		"    .report-chrome { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; margin: 0 0 14px; padding: 10px 12px; border: 1px solid #e5e7eb; border-radius: 8px; background: #f9fafb; }",
		"    .report-chrome-nav { display: inline-flex; align-items: center; gap: 8px; flex-wrap: wrap; }",
		"    .report-nav-btn, .report-view-link { display: inline-flex; align-items: center; justify-content: center; border: 1px solid #d1d5db; border-radius: 999px; padding: 4px 10px; font-size: 12px; font-weight: 600; color: #1f2937; background: #ffffff; text-decoration: none; }",
		"    .report-nav-btn:hover, .report-view-link:hover { background: #f3f4f6; }",
		"    .report-nav-btn.disabled { color: #9ca3af; background: #f3f4f6; border-color: #e5e7eb; cursor: not-allowed; }",
		"    .report-view-link.active { background: #111827; border-color: #111827; color: #ffffff; }",
		"    .report-context-label { font-size: 12px; font-weight: 600; color: #4b5563; margin-right: 2px; }",
		"    .report-theme-slot { margin-left: auto; }",
		"    .report-theme-slot .theme-toggle { box-shadow: none; }",
		"    :root[data-theme=\"dark\"] .report-chrome { background: #111827; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .report-context-label { color: #94a3b8; }",
		"    :root[data-theme=\"dark\"] .report-nav-btn, :root[data-theme=\"dark\"] .report-view-link { background: #1f2937; border-color: #334155; color: #e2e8f0; }",
		"    :root[data-theme=\"dark\"] .report-nav-btn:hover, :root[data-theme=\"dark\"] .report-view-link:hover { background: #0f172a; }",
		"    :root[data-theme=\"dark\"] .report-nav-btn.disabled { background: #0f172a; border-color: #334155; color: #64748b; }",
		"    :root[data-theme=\"dark\"] .report-view-link.active { background: #2563eb; border-color: #2563eb; color: #e2e8f0; }",
	}, "\n") + "\n"
}

func ReportChromeHTML(options ReportChromeOptions) string {
	normalized := normalizedReportChromeOptions(options)
	if !hasReportChromeNavigation(normalized) {
		return ThemeToggleHTML()
	}
	var b strings.Builder
	b.WriteString("  <div class=\"report-chrome\">\n")
	b.WriteString("    <div class=\"report-chrome-nav\">\n")
	b.WriteString(renderReportChromeNavButton(normalized.PreviousHref, "&larr; Older", normalized.PreviousWeek, true))
	b.WriteString(renderReportChromeNavButton(normalized.NextHref, "Newer &rarr;", normalized.NextWeek, false))
	b.WriteString("    </div>\n")
	b.WriteString("    <div class=\"report-chrome-nav\">\n")
	if normalized.WindowLabel != "" {
		b.WriteString(fmt.Sprintf("      <span class=\"report-context-label\">%s</span>\n", html.EscapeString(normalized.WindowLabel)))
	} else if normalized.CurrentWeek != "" {
		b.WriteString(fmt.Sprintf("      <span class=\"report-context-label\">Week %s (UTC)</span>\n", html.EscapeString(normalized.CurrentWeek)))
	}
	if strings.TrimSpace(normalized.RollingHref) != "" {
		b.WriteString(renderReportChromeViewLink(normalized.RollingHref, "Last 7 Days", normalized.CurrentView == ReportViewRolling))
	}
	if strings.TrimSpace(normalized.ReportHref) != "" {
		b.WriteString(renderReportChromeViewLink(normalized.ReportHref, "Weekly Report", normalized.CurrentView == ReportViewReport))
	}
	if strings.TrimSpace(normalized.FailurePatternsHref) != "" {
		b.WriteString(renderReportChromeViewLink(normalized.FailurePatternsHref, "Failure Patterns", normalized.CurrentView == ReportViewFailurePatterns))
	}
	if strings.TrimSpace(normalized.RunLogHref) != "" {
		b.WriteString(renderReportChromeViewLink(normalized.RunLogHref, "Run Log", normalized.CurrentView == ReportViewRunLog))
	}
	if strings.TrimSpace(normalized.ArchiveHref) != "" {
		b.WriteString(renderReportChromeViewLink(normalized.ArchiveHref, "Archive", false))
	}
	b.WriteString("    </div>\n")
	b.WriteString("    <div class=\"report-theme-slot\">")
	b.WriteString(ThemeToggleButtonHTML())
	b.WriteString("</div>\n")
	b.WriteString("  </div>\n")
	return b.String()
}

func ThemeInitScriptTag() string {
	return strings.TrimSpace(`
<script>
(function () {
  var key = "ci-failure-report-theme-mode";
  function normalize(value) {
    return value === "light" || value === "dark" || value === "auto" ? value : "auto";
  }
  function prefersDark() {
    return !!(window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
  }
  var mode = "auto";
  try {
    mode = normalize(localStorage.getItem(key) || "auto");
  } catch (err) {
    mode = "auto";
  }
  var effective = mode === "auto" ? (prefersDark() ? "dark" : "light") : mode;
  var root = document.documentElement;
  root.setAttribute("data-theme-mode", mode);
  root.setAttribute("data-theme", effective);
})();
</script>
`) + "\n"
}

func ThemeToggleScriptTag() string {
	return strings.TrimSpace(`
<script>
(function () {
  var key = "ci-failure-report-theme-mode";
  var button = document.getElementById("theme-toggle");
  var root = document.documentElement;
  function normalize(value) {
    return value === "light" || value === "dark" || value === "auto" ? value : "auto";
  }
  function prefersDark() {
    return !!(window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
  }
  function label(mode) {
    if (mode === "light") { return "Theme: Light"; }
    if (mode === "dark") { return "Theme: Dark"; }
    return "Theme: Auto";
  }
  function apply(mode, persist) {
    var normalized = normalize(mode);
    var effective = normalized === "auto" ? (prefersDark() ? "dark" : "light") : normalized;
    root.setAttribute("data-theme-mode", normalized);
    root.setAttribute("data-theme", effective);
    if (button) {
      button.textContent = label(normalized);
      button.setAttribute("title", "Current mode: " + normalized + ". Click to cycle Auto -> Light -> Dark.");
    }
    if (persist) {
      try {
        localStorage.setItem(key, normalized);
      } catch (err) {}
    }
  }
  apply(root.getAttribute("data-theme-mode") || "auto", false);
  if (button) {
    button.addEventListener("click", function () {
      var current = normalize(root.getAttribute("data-theme-mode") || "auto");
      var next = current === "auto" ? "light" : current === "light" ? "dark" : "auto";
      apply(next, true);
    });
  }
  if (window.matchMedia) {
    var media = window.matchMedia("(prefers-color-scheme: dark)");
    var onChange = function () {
      if (normalize(root.getAttribute("data-theme-mode") || "auto") === "auto") {
        apply("auto", false);
      }
    };
    if (media.addEventListener) {
      media.addEventListener("change", onChange);
    } else if (media.addListener) {
      media.addListener(onChange);
    }
  }
})();
</script>
`) + "\n"
}

func normalizedReportChromeOptions(options ReportChromeOptions) ReportChromeOptions {
	options.WindowLabel = strings.TrimSpace(options.WindowLabel)
	options.CurrentWeek = strings.TrimSpace(options.CurrentWeek)
	options.PreviousWeek = strings.TrimSpace(options.PreviousWeek)
	options.PreviousHref = strings.TrimSpace(options.PreviousHref)
	options.NextWeek = strings.TrimSpace(options.NextWeek)
	options.NextHref = strings.TrimSpace(options.NextHref)
	options.RollingHref = strings.TrimSpace(options.RollingHref)
	options.ReportHref = strings.TrimSpace(options.ReportHref)
	options.FailurePatternsHref = strings.TrimSpace(options.FailurePatternsHref)
	options.RunLogHref = strings.TrimSpace(options.RunLogHref)
	options.ArchiveHref = strings.TrimSpace(options.ArchiveHref)
	switch options.CurrentView {
	case ReportViewRolling, ReportViewReport, ReportViewFailurePatterns, ReportViewRunLog:
	default:
		options.CurrentView = ""
	}
	return options
}

func hasReportChromeNavigation(options ReportChromeOptions) bool {
	return options.WindowLabel != "" ||
		options.CurrentWeek != "" ||
		options.PreviousHref != "" ||
		options.NextHref != "" ||
		options.RollingHref != "" ||
		options.ReportHref != "" ||
		options.FailurePatternsHref != "" ||
		options.RunLogHref != "" ||
		options.ArchiveHref != ""
}

func renderReportChromeNavButton(href string, label string, week string, older bool) string {
	trimmedHref := strings.TrimSpace(href)
	trimmedWeek := strings.TrimSpace(week)
	if trimmedHref == "" {
		disabledTitle := "No older window available"
		if !older {
			disabledTitle = "No newer window available"
		}
		return fmt.Sprintf(
			"      <span class=\"report-nav-btn disabled\" aria-disabled=\"true\" title=\"%s\">%s</span>\n",
			html.EscapeString(disabledTitle),
			label,
		)
	}
	title := "Go to older window"
	if !older {
		title = "Go to newer window"
	}
	if trimmedWeek != "" {
		title = fmt.Sprintf("%s anchored on %s (UTC)", title, trimmedWeek)
	}
	return fmt.Sprintf(
		"      <a class=\"report-nav-btn\" href=\"%s\" title=\"%s\">%s</a>\n",
		html.EscapeString(trimmedHref),
		html.EscapeString(title),
		label,
	)
}

func renderReportChromeViewLink(href string, label string, active bool) string {
	trimmedHref := strings.TrimSpace(href)
	if trimmedHref == "" {
		return ""
	}
	className := "report-view-link"
	if active {
		className += " active"
	}
	return fmt.Sprintf(
		"      <a class=\"%s\" href=\"%s\">%s</a>\n",
		className,
		html.EscapeString(trimmedHref),
		html.EscapeString(strings.TrimSpace(label)),
	)
}

func RenderTable(rows []FailurePatternRow, options TableOptions) string {
	opts := normalizedOptions(options)
	initialSortKey := strings.TrimSpace(opts.InitialSortKey)
	if !isSortableKey(initialSortKey) {
		initialSortKey = defaultSortKey
	}
	initialSortDirection := strings.TrimSpace(strings.ToLower(opts.InitialSortDirection))
	if initialSortDirection != sortDirectionAscending && initialSortDirection != sortDirectionDescending {
		initialSortDirection = defaultSortDirection
	}
	orderedRows := append([]FailurePatternRow(nil), rows...)
	impactTotalJobs := opts.ImpactTotalJobs
	if impactTotalJobs <= 0 {
		impactTotalJobs = totalAffectedJobs(orderedRows)
	}
	opts.ImpactTotalJobs = impactTotalJobs
	sortRowsByDefaultPriorityWithImpact(orderedRows, impactTotalJobs)
	if opts.LoadedRowsLimit > 0 && len(orderedRows) > opts.LoadedRowsLimit {
		orderedRows = orderedRows[:opts.LoadedRowsLimit]
	}
	initialVisibleRows := opts.InitialVisibleRows
	if initialVisibleRows <= 0 || initialVisibleRows > len(orderedRows) {
		initialVisibleRows = len(orderedRows)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(
		"    <table class=\"failure-patterns-table\" data-sortable=\"true\" data-sort-key=\"%s\" data-sort-dir=\"%s\" data-initial-visible=\"%d\">\n",
		initialSortKey,
		initialSortDirection,
		initialVisibleRows,
	))
	b.WriteString("      <thead><tr>")
	headers := make([]string, 0, 12)
	if opts.IncludeSelection {
		headers = append(headers, "<th class=\"failure-patterns-select-col\">Select</th>")
	}
	headers = append(headers, renderTooltipHeaderCell("Failure pattern", "The canonical description of a recurring CI failure, extracted and normalized from raw logs."))
	headers = append(headers, renderTooltipHeaderCell("Failed at", "The stage of the job run where this failure occurred: 'provision' (environment setup, DEV only), 'e2e' (test suite execution), or 'other' (CI infrastructure issues — no failure pattern extracted)."))
	headers = append(headers,
		renderSortableHeaderCell("Runs affected", sortKeyJobsAffected, "Number of distinct job runs where this failure pattern was detected.", initialSortKey, initialSortDirection),
		renderSortableHeaderCell("Impact", sortKeyImpact, "Percentage of all job runs in this environment affected by this failure pattern during the selected window.", initialSortKey, initialSortDirection),
		renderSortableHeaderCell("Flake signal", sortKeyFlakeScore, "Whether this failure pattern looks like an ongoing flake or a PR-specific regression. High = strong flake signal across multiple days or runs. Low = likely caused by the PR under test.", initialSortKey, initialSortDirection),
	)
	if opts.ShowCount {
		headers = append(headers, renderSortableHeaderCell("Count", sortKeyCount, "", initialSortKey, initialSortDirection))
	}
	if opts.ShowAfterLastPush {
		headers = append(headers, renderSortableHeaderCell("After last push", sortKeyAfterLastPush, "Job run occurred after last push of a PR that merges.", initialSortKey, initialSortDirection))
	}
	if opts.ShowShare {
		headers = append(headers, renderSortableHeaderCell("Share", sortKeyShare, "", initialSortKey, initialSortDirection))
	}
	if opts.ShowManualIssue {
		headers = append(headers, renderSortableHeaderCell("Linked group ID", sortKeyManualCluster, "ID of the linked failure group, assigned when patterns are manually grouped in the review workflow.", initialSortKey, initialSortDirection))
	}
	if opts.IncludeTrend {
		headers = append(headers, fmt.Sprintf("<th>%s</th>", html.EscapeString(opts.TrendHeaderLabel)))
	}
	headers = append(headers, renderTooltipHeaderCell("Also in", "Other environments where the same failure pattern was also detected during the selected window."))
	if opts.ShowQualityScore {
		headers = append(headers, "<th>Quality score</th>")
	}
	if opts.ShowQualityFlags {
		headers = append(headers, "<th>Quality flags</th>")
	}
	if opts.ShowReviewFlags {
		headers = append(headers, "<th>Review flags</th>")
	}
	for _, header := range headers {
		b.WriteString(header)
	}
	b.WriteString("</tr></thead>\n")
	b.WriteString("      <tbody>\n")
	colSpan := len(headers)
	for rowIndex, row := range orderedRows {
		rowID := fmt.Sprintf("failure-pattern-row-%d", rowIndex)
		b.WriteString(renderMainRow(row, rowID, opts))
		b.WriteString(renderDetailRow(row, rowID, colSpan, opts))
	}
	b.WriteString("      </tbody>\n")
	b.WriteString("    </table>\n")
	b.WriteString(renderTableSortScriptTag())
	return b.String()
}

func renderTooltipHeaderCell(label string, tooltip string) string {
	trimmedLabel := strings.TrimSpace(label)
	if trimmedLabel == "" {
		trimmedLabel = "n/a"
	}
	trimmedTooltip := strings.TrimSpace(tooltip)
	if trimmedTooltip == "" {
		return fmt.Sprintf("<th>%s</th>", html.EscapeString(trimmedLabel))
	}
	return fmt.Sprintf(
		"<th>%s<span class=\"failure-patterns-header-help\" title=\"%s\" aria-label=\"%s\">i</span></th>",
		html.EscapeString(trimmedLabel),
		html.EscapeString(trimmedTooltip),
		html.EscapeString(trimmedTooltip),
	)
}

func renderSortableHeaderCell(label string, sortKey string, tooltip string, activeSortKey string, activeSortDirection string) string {
	trimmedLabel := strings.TrimSpace(label)
	if trimmedLabel == "" {
		trimmedLabel = "n/a"
	}
	trimmedSortKey := strings.TrimSpace(sortKey)
	if trimmedSortKey == "" {
		return renderTooltipHeaderCell(trimmedLabel, tooltip)
	}
	ariaSort := "none"
	indicator := ""
	if trimmedSortKey == strings.TrimSpace(activeSortKey) {
		if strings.TrimSpace(activeSortDirection) == sortDirectionAscending {
			ariaSort = "ascending"
			indicator = "^"
		} else {
			ariaSort = "descending"
			indicator = "v"
		}
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf(
		"<th class=\"failure-patterns-sortable\" data-sort-key=\"%s\" aria-sort=\"%s\">",
		html.EscapeString(trimmedSortKey),
		ariaSort,
	))
	b.WriteString(fmt.Sprintf(
		"<button type=\"button\" class=\"failure-patterns-sort-button\" data-sort-key=\"%s\">%s<span class=\"failure-patterns-sort-indicator\" aria-hidden=\"true\">%s</span></button>",
		html.EscapeString(trimmedSortKey),
		html.EscapeString(trimmedLabel),
		html.EscapeString(indicator),
	))
	trimmedTooltip := strings.TrimSpace(tooltip)
	if trimmedTooltip != "" {
		b.WriteString(fmt.Sprintf(
			"<span class=\"failure-patterns-header-help\" title=\"%s\" aria-label=\"%s\">i</span>",
			html.EscapeString(trimmedTooltip),
			html.EscapeString(trimmedTooltip),
		))
	}
	b.WriteString("</th>")
	return b.String()
}

func renderTableSortScriptTag() string {
	return strings.TrimSpace(`
<script>
(function () {
  function parseNumber(value) {
    var parsed = parseFloat(value || "0");
    if (!isFinite(parsed)) {
      return 0;
    }
    return parsed;
  }
  function compareStrings(a, b) {
    var left = (a || "").toLowerCase();
    var right = (b || "").toLowerCase();
    if (left === right) {
      return 0;
    }
    return left < right ? -1 : 1;
  }
  function defaultCompareRows(leftRow, rightRow) {
    var impactDiff = parseNumber(rightRow.getAttribute("data-sort-impact")) - parseNumber(leftRow.getAttribute("data-sort-impact"));
    if (impactDiff !== 0) { return impactDiff; }
    var jobsDiff = parseNumber(rightRow.getAttribute("data-sort-jobs")) - parseNumber(leftRow.getAttribute("data-sort-jobs"));
    if (jobsDiff !== 0) { return jobsDiff; }
    var flakeDiff = parseNumber(rightRow.getAttribute("data-sort-flake")) - parseNumber(leftRow.getAttribute("data-sort-flake"));
    if (flakeDiff !== 0) { return flakeDiff; }
    var shareDiff = parseNumber(rightRow.getAttribute("data-sort-share")) - parseNumber(leftRow.getAttribute("data-sort-share"));
    if (shareDiff !== 0) { return shareDiff; }
    var countDiff = parseNumber(rightRow.getAttribute("data-sort-count")) - parseNumber(leftRow.getAttribute("data-sort-count"));
    if (countDiff !== 0) { return countDiff; }
    var afterLastPushDiff = parseNumber(rightRow.getAttribute("data-sort-post-good")) - parseNumber(leftRow.getAttribute("data-sort-post-good"));
    if (afterLastPushDiff !== 0) { return afterLastPushDiff; }
    var envDiff = compareStrings(leftRow.getAttribute("data-sort-environment"), rightRow.getAttribute("data-sort-environment"));
    if (envDiff !== 0) { return envDiff; }
    var phraseDiff = compareStrings(leftRow.getAttribute("data-sort-phrase"), rightRow.getAttribute("data-sort-phrase"));
    if (phraseDiff !== 0) { return phraseDiff; }
    return compareStrings(leftRow.getAttribute("data-sort-cluster"), rightRow.getAttribute("data-sort-cluster"));
  }
  function compareRowsByKey(leftRow, rightRow, sortKey, sortDirection) {
    var diff = 0;
    switch (sortKey) {
      case "count":
        diff = parseNumber(leftRow.getAttribute("data-sort-count")) - parseNumber(rightRow.getAttribute("data-sort-count"));
        break;
      case "jobs_affected":
        diff = parseNumber(leftRow.getAttribute("data-sort-jobs")) - parseNumber(rightRow.getAttribute("data-sort-jobs"));
        break;
      case "impact":
        diff = parseNumber(leftRow.getAttribute("data-sort-impact")) - parseNumber(rightRow.getAttribute("data-sort-impact"));
        break;
      case "after_last_push":
        diff = parseNumber(leftRow.getAttribute("data-sort-post-good")) - parseNumber(rightRow.getAttribute("data-sort-post-good"));
        break;
      case "flake_score":
        diff = parseNumber(leftRow.getAttribute("data-sort-flake")) - parseNumber(rightRow.getAttribute("data-sort-flake"));
        break;
      case "share":
        diff = parseNumber(leftRow.getAttribute("data-sort-share")) - parseNumber(rightRow.getAttribute("data-sort-share"));
        break;
      case "manual_cluster":
        diff = compareStrings(leftRow.getAttribute("data-sort-manual"), rightRow.getAttribute("data-sort-manual"));
        break;
      default:
        diff = 0;
        break;
    }
    if (diff !== 0) {
      return sortDirection === "asc" ? diff : -diff;
    }
    return defaultCompareRows(leftRow, rightRow);
  }
  function collectRowPairs(tbody) {
    var mains = Array.prototype.slice.call(tbody.querySelectorAll("tr.failure-pattern-row"));
    var pairs = [];
    for (var i = 0; i < mains.length; i++) {
      var main = mains[i];
      var rowID = main.getAttribute("data-row-id") || "";
      var detail = null;
      var sibling = main.nextElementSibling;
      if (sibling && sibling.classList.contains("failure-patterns-errors-row") && (sibling.getAttribute("data-parent-row-id") || "") === rowID) {
        detail = sibling;
      }
      pairs.push({ main: main, detail: detail });
    }
    return pairs;
  }
  function applyVisibility(table, pairs) {
    var visible = parseInt(table.getAttribute("data-initial-visible") || "0", 10);
    if (!isFinite(visible) || visible <= 0) {
      visible = pairs.length;
    }
    for (var i = 0; i < pairs.length; i++) {
      var isVisible = i < visible;
      pairs[i].main.style.display = isVisible ? "" : "none";
      if (pairs[i].detail) {
        pairs[i].detail.style.display = isVisible ? "" : "none";
      }
    }
  }
  function updateHeaderState(table, sortKey, sortDirection) {
    var headers = table.querySelectorAll("th.failure-patterns-sortable");
    for (var i = 0; i < headers.length; i++) {
      var header = headers[i];
      var headerKey = header.getAttribute("data-sort-key") || "";
      var indicator = header.querySelector(".failure-patterns-sort-indicator");
      if (headerKey === sortKey) {
        header.setAttribute("aria-sort", sortDirection === "asc" ? "ascending" : "descending");
        if (indicator) {
          indicator.textContent = sortDirection === "asc" ? "^" : "v";
        }
      } else {
        header.setAttribute("aria-sort", "none");
        if (indicator) {
          indicator.textContent = "";
        }
      }
    }
  }
  function applySort(table, sortKey, sortDirection) {
    var tbody = table.querySelector("tbody");
    if (!tbody) {
      return;
    }
    var pairs = collectRowPairs(tbody);
    pairs.sort(function (left, right) {
      return compareRowsByKey(left.main, right.main, sortKey, sortDirection);
    });
    for (var i = 0; i < pairs.length; i++) {
      tbody.appendChild(pairs[i].main);
      if (pairs[i].detail) {
        tbody.appendChild(pairs[i].detail);
      }
    }
    table.setAttribute("data-sort-key", sortKey);
    table.setAttribute("data-sort-dir", sortDirection);
    updateHeaderState(table, sortKey, sortDirection);
    applyVisibility(table, pairs);
  }
  function initSortableTable(table) {
    if (!table || table.getAttribute("data-sort-init") === "true") {
      return;
    }
    table.setAttribute("data-sort-init", "true");
    var sortKey = table.getAttribute("data-sort-key") || "impact";
    var sortDirection = table.getAttribute("data-sort-dir") || "desc";
    var buttons = table.querySelectorAll("button.failure-patterns-sort-button");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].addEventListener("click", function (event) {
        var key = (event.currentTarget && event.currentTarget.getAttribute("data-sort-key")) || "";
        if (!key) {
          return;
        }
        var currentKey = table.getAttribute("data-sort-key") || "impact";
        var currentDirection = table.getAttribute("data-sort-dir") || "desc";
        var nextDirection = "desc";
        if (key === currentKey) {
          nextDirection = currentDirection === "desc" ? "asc" : "desc";
        }
        applySort(table, key, nextDirection);
      });
    }
    applySort(table, sortKey, sortDirection);
  }
  var tables = document.querySelectorAll("table.failure-patterns-table[data-sortable=\"true\"]");
  for (var i = 0; i < tables.length; i++) {
    initSortableTable(tables[i]);
  }
})();
</script>
`) + "\n"
}

func FormatReferenceTimestampLabel(value string) string {
	label := strings.TrimSpace(value)
	if parsed, ok := ParseReferenceTimestamp(value); ok {
		label = parsed.UTC().Format("2006-01-02 15:04 UTC")
	}
	if label == "" {
		return "unknown-time"
	}
	return label
}

func ResolveGitHubPRURLFromProwRun(runURL string, prNumber int, fallbackOwner string, fallbackRepo string) (string, bool) {
	if prNumber <= 0 {
		return "", false
	}
	parsedURL, err := url.Parse(strings.TrimSpace(runURL))
	if err == nil {
		segments := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
		for i := 0; i+3 < len(segments); i++ {
			if segments[i] != "pr-logs" || segments[i+1] != "pull" {
				continue
			}
			orgRepo := strings.TrimSpace(segments[i+2])
			org, repo, ok := strings.Cut(orgRepo, "_")
			if !ok || strings.TrimSpace(org) == "" || strings.TrimSpace(repo) == "" {
				continue
			}
			return fmt.Sprintf(
				"https://github.com/%s/%s/pull/%d",
				strings.TrimSpace(org),
				strings.TrimSpace(repo),
				prNumber,
			), true
		}
	}
	fallbackOwner = strings.TrimSpace(fallbackOwner)
	fallbackRepo = strings.TrimSpace(fallbackRepo)
	if fallbackOwner == "" || fallbackRepo == "" {
		return "", false
	}
	return fmt.Sprintf("https://github.com/%s/%s/pull/%d", fallbackOwner, fallbackRepo, prNumber), true
}

func FormatCounts(values []int) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return strings.Join(parts, ",")
}

func formatBadPRReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case "post-good=0":
		return "no runs after the PR's last push"
	default:
		return reason
	}
}

func FormatBadPRTooltip(reasons []string) string {
	base := "This failure pattern appears to be caused by the PR under test"
	if len(reasons) == 0 {
		return base + "."
	}
	translated := make([]string, 0, len(reasons))
	for _, r := range reasons {
		translated = append(translated, formatBadPRReason(r))
	}
	return base + " — " + strings.Join(translated, "; ") + "."
}

func normalizedOptions(options TableOptions) TableOptions {
	opts := options
	if strings.TrimSpace(opts.GitHubRepoOwner) == "" {
		opts.GitHubRepoOwner = sourceoptions.DefaultGitHubRepoOwner()
	}
	if strings.TrimSpace(opts.GitHubRepoName) == "" {
		opts.GitHubRepoName = sourceoptions.DefaultGitHubRepoName()
	}
	if strings.TrimSpace(opts.FullErrorsSummaryLabel) == "" {
		opts.FullErrorsSummaryLabel = "Full failure examples"
	}
	if strings.TrimSpace(opts.ContributingSummaryLabel) == "" {
		opts.ContributingSummaryLabel = "Contributing tests"
	}
	if strings.TrimSpace(opts.AffectedRunsSummaryLabel) == "" {
		opts.AffectedRunsSummaryLabel = "Affected runs"
	}
	if strings.TrimSpace(opts.TrendHeaderLabel) == "" {
		opts.TrendHeaderLabel = "Trend"
	}
	if strings.TrimSpace(opts.SelectionInputName) == "" {
		opts.SelectionInputName = "cluster_id"
	}
	opts.InitialSortKey = strings.TrimSpace(opts.InitialSortKey)
	if !isSortableKey(opts.InitialSortKey) {
		opts.InitialSortKey = defaultSortKey
	}
	opts.InitialSortDirection = strings.ToLower(strings.TrimSpace(opts.InitialSortDirection))
	if opts.InitialSortDirection != sortDirectionAscending && opts.InitialSortDirection != sortDirectionDescending {
		opts.InitialSortDirection = defaultSortDirection
	}
	if opts.IncludeQualityNotes {
		opts.ShowQualityFlags = true
	}
	if opts.HideQualityScore {
		opts.ShowQualityScore = false
	}
	if opts.LoadedRowsLimit == 0 {
		opts.LoadedRowsLimit = defaultLoadedRowsLimit
	}
	if opts.InitialVisibleRows <= 0 {
		if opts.LoadedRowsLimit > 0 {
			opts.InitialVisibleRows = opts.LoadedRowsLimit
		} else {
			opts.InitialVisibleRows = 0
		}
	}
	if opts.LoadedRowsLimit > 0 && opts.InitialVisibleRows > opts.LoadedRowsLimit {
		opts.InitialVisibleRows = opts.LoadedRowsLimit
	}
	return opts
}

func isSortableKey(value string) bool {
	switch strings.TrimSpace(value) {
	case sortKeyCount, sortKeyJobsAffected, sortKeyImpact, sortKeyAfterLastPush, sortKeyFlakeScore, sortKeyShare, sortKeyManualCluster:
		return true
	default:
		return false
	}
}

func renderMainRow(row FailurePatternRow, rowID string, opts TableOptions) string {
	var b strings.Builder
	phrase := strings.TrimSpace(row.FailurePattern)
	if phrase == "" {
		phrase = "(unknown evidence)"
	}
	laneValue := rowLaneForDisplay(row)
	otherEnvironments := "none"
	if len(row.AlsoIn) > 0 {
		otherEnvironments = strings.Join(row.AlsoIn, ", ")
	}
	qualityClass := "quality-low"
	if row.QualityScore >= 8 {
		qualityClass = "quality-high"
	}
	qualityNotes := "<span class=\"badge badge-ok\">ok</span>"
	if len(row.QualityNoteLabels) > 0 {
		parts := make([]string, 0, len(row.QualityNoteLabels))
		for _, label := range row.QualityNoteLabels {
			parts = append(parts, fmt.Sprintf("<span class=\"badge badge-quality\">%s</span>", html.EscapeString(label)))
		}
		qualityNotes = strings.Join(parts, "")
	}
	reviewNotes := "<span class=\"badge badge-ok\">none</span>"
	if len(row.ReviewNoteLabels) > 0 {
		parts := make([]string, 0, len(row.ReviewNoteLabels))
		for _, label := range row.ReviewNoteLabels {
			parts = append(parts, fmt.Sprintf("<span class=\"badge badge-review\">%s</span>", html.EscapeString(label)))
		}
		reviewNotes = strings.Join(parts, "")
	}
	filterSearchValue := strings.TrimSpace(row.SearchIndex)
	if filterSearchValue == "" {
		filterSearchValue = defaultSearchIndex(row)
	}
	isFlagged := len(row.QualityNoteLabels) > 0 || len(row.ReviewNoteLabels) > 0
	hasReviewFlags := len(row.ReviewNoteLabels) > 0
	summaryText := html.EscapeString(cleanInline(phrase, 180))
	badPRScore, badPRReasons := BadPRScoreAndReasons(row)
	if badPRScore == 3 {
		tooltip := FormatBadPRTooltip(badPRReasons)
		summaryText = fmt.Sprintf(
			"<span class=\"bad-pr-flag\" title=\"%s\" aria-label=\"%s\">⚠</span>%s",
			html.EscapeString(tooltip),
			html.EscapeString(tooltip),
			summaryText,
		)
	}
	jobsAffected := rowJobsAffected(row)
	postGoodCount := rowPostGoodCount(row)
	impactPercent := impactShare(jobsAffected, opts.ImpactTotalJobs)
	impactLabel := fmt.Sprintf("%.2f%%", impactPercent)
	impactTitle := fmt.Sprintf(
		"%d of %d job runs affected",
		jobsAffected,
		maxInt(opts.ImpactTotalJobs, 0),
	)
	flakeScore, flakeReasons := FlakeScoreAndReasons(row)
	flakeLabel := flakeScoreLabel(flakeScore)
	flakeCellTitle := fmt.Sprintf("Flake signal: %s (score %d/14)", flakeLabel, flakeScore)
	if len(flakeReasons) > 0 {
		flakeCellTitle = fmt.Sprintf("%s — %s", flakeCellTitle, strings.Join(flakeReasons, "; "))
	}
	flakeClass := flakeScoreClass(flakeScore)
	manualSortValue := strings.TrimSpace(row.ManualIssueID)
	if manualSortValue == "" {
		manualSortValue = "~" + strings.ToLower(strings.TrimSpace(row.Environment)) + "|" + strings.TrimSpace(row.FailurePatternID)
	}
	b.WriteString(fmt.Sprintf(
		"        <tr class=\"failure-pattern-row\" data-row-id=\"%s\" data-sort-count=\"%d\" data-sort-post-good=\"%d\" data-sort-jobs=\"%d\" data-sort-impact=\"%.6f\" data-sort-flake=\"%d\" data-sort-share=\"%.6f\" data-sort-environment=\"%s\" data-sort-phrase=\"%s\" data-sort-cluster=\"%s\" data-sort-manual=\"%s\" data-filter-env=\"%s\" data-filter-lane=\"%s\" data-filter-search=\"%s\" data-filter-flagged=\"%t\" data-filter-review=\"%t\">",
		html.EscapeString(strings.TrimSpace(rowID)),
		row.Occurrences,
		postGoodCount,
		jobsAffected,
		impactPercent,
		flakeScore,
		row.OccurrenceShare,
		html.EscapeString(strings.ToLower(strings.TrimSpace(row.Environment))),
		html.EscapeString(strings.TrimSpace(row.FailurePattern)),
		html.EscapeString(strings.TrimSpace(row.FailurePatternID)),
		html.EscapeString(strings.ToLower(manualSortValue)),
		html.EscapeString(strings.ToLower(strings.TrimSpace(row.Environment))),
		html.EscapeString(strings.ToLower(laneValue)),
		html.EscapeString(strings.ToLower(filterSearchValue)),
		isFlagged,
		hasReviewFlags,
	))
	if opts.IncludeSelection {
		selectionValue := strings.TrimSpace(row.SelectionValue)
		if selectionValue == "" {
			selectionValue = strings.TrimSpace(row.FailurePatternID)
		}
		if selectionValue == "" {
			selectionValue = strings.TrimSpace(rowID)
		}
		b.WriteString(fmt.Sprintf(
			"<td class=\"failure-patterns-select-col\"><input class=\"failure-patterns-row-select\" type=\"checkbox\" name=\"%s\" value=\"%s\" /></td>",
			html.EscapeString(strings.TrimSpace(opts.SelectionInputName)),
			html.EscapeString(selectionValue),
		))
	}
	var signatureDetails strings.Builder
	signatureDetails.WriteString("<td><details class=\"failure-pattern-toggle\">")
	signatureDetails.WriteString(fmt.Sprintf("<summary>%s</summary>", summaryText))
	signatureDetails.WriteString("<div class=\"muted\">full failure pattern:</div>")
	signatureDetails.WriteString(fmt.Sprintf("<pre>%s</pre>", html.EscapeString(phrase)))
	if successDetails := successDetailsFromSearchQuery(row.SearchQuery); successDetails != "" {
		signatureDetails.WriteString(fmt.Sprintf("<div class=\"muted\">%s</div>", html.EscapeString(successDetails)))
	}
	signatureDetails.WriteString(fmt.Sprintf("<div class=\"muted\">Flake signal: %s</div>", html.EscapeString(flakeLabel)))
	signatureDetails.WriteString("</details></td>")
	b.WriteString(signatureDetails.String())
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(laneValue)))
	b.WriteString(fmt.Sprintf("<td>%d</td>", jobsAffected))
	b.WriteString(fmt.Sprintf("<td title=\"%s\"><span class=\"impact-score %s\">%s</span></td>", html.EscapeString(impactTitle), impactScoreClass(impactPercent), html.EscapeString(impactLabel)))
	b.WriteString(fmt.Sprintf("<td title=\"%s\"><span class=\"flake-score %s\">%s</span></td>", html.EscapeString(flakeCellTitle), flakeClass, html.EscapeString(flakeLabel)))
	if opts.ShowCount {
		b.WriteString(fmt.Sprintf("<td>%d</td>", row.Occurrences))
	}
	if opts.ShowAfterLastPush {
		b.WriteString(fmt.Sprintf("<td>%d</td>", postGoodCount))
	}
	if opts.ShowShare {
		b.WriteString(fmt.Sprintf("<td>%.2f%%</td>", row.OccurrenceShare))
	}
	if opts.ShowManualIssue {
		manualIssueLabel := strings.TrimSpace(row.ManualIssueID)
		if manualIssueLabel == "" {
			manualIssueLabel = "<span class=\"muted\">unlinked</span>"
		} else {
			manualIssueLabel = html.EscapeString(manualIssueLabel)
		}
		b.WriteString(fmt.Sprintf("<td>%s</td>", manualIssueLabel))
	}
	if opts.IncludeTrend {
		b.WriteString(renderTrendCell(row))
	}
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(otherEnvironments)))
	if opts.ShowQualityScore {
		b.WriteString(fmt.Sprintf("<td><span class=\"%s\">%d</span></td>", qualityClass, row.QualityScore))
	}
	if opts.ShowQualityFlags {
		b.WriteString(fmt.Sprintf("<td>%s</td>", qualityNotes))
	}
	if opts.ShowReviewFlags {
		b.WriteString(fmt.Sprintf("<td>%s</td>", reviewNotes))
	}
	b.WriteString("</tr>\n")
	return b.String()
}

func defaultSearchIndex(row FailurePatternRow) string {
	laneValue := rowLaneForDisplay(row)
	parts := []string{
		strings.TrimSpace(row.Environment),
		laneValue,
		strings.TrimSpace(row.JobName),
		strings.TrimSpace(row.TestName),
		strings.TrimSpace(row.TestSuite),
		strings.TrimSpace(row.FailurePattern),
		strings.TrimSpace(row.FailurePatternID),
		strings.TrimSpace(row.SearchQuery),
	}
	parts = append(parts, row.QualityNoteLabels...)
	parts = append(parts, row.ReviewNoteLabels...)
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		filtered = append(filtered, trimmed)
	}
	return strings.Join(filtered, " ")
}

func rowLaneForDisplay(row FailurePatternRow) string {
	lane := strings.TrimSpace(row.FailedAt)
	if lane != "" {
		return lane
	}
	ordered := OrderedContributingTests(row.ContributingTests)
	uniqueLanes := map[string]struct{}{}
	for _, contributing := range ordered {
		trimmedLane := strings.TrimSpace(contributing.FailedAt)
		if trimmedLane == "" {
			continue
		}
		uniqueLanes[trimmedLane] = struct{}{}
	}
	switch len(uniqueLanes) {
	case 0:
		return "unknown"
	case 1:
		for laneValue := range uniqueLanes {
			return laneValue
		}
		return "unknown"
	default:
		return "mixed"
	}
}

func successDetailsFromSearchQuery(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	for _, part := range strings.Split(trimmed, ";") {
		entry := strings.TrimSpace(part)
		if entry == "" {
			continue
		}
		const prefix = "success="
		if !strings.HasPrefix(strings.ToLower(entry), prefix) {
			continue
		}
		successValue := strings.TrimSpace(entry[len(prefix):])
		if successValue == "" {
			return ""
		}
		return "success: " + successValue
	}
	return ""
}

func renderTrendCell(row FailurePatternRow) string {
	if len(row.TrendCounts) > 0 {
		tooltip := trendTooltip(row.TrendCounts, row.TrendRange)
		return fmt.Sprintf(
			"<td title=\"%s\">%s</td>",
			html.EscapeString(tooltip),
			renderTrendBarsSVG(row.TrendCounts, tooltip),
		)
	}
	if row.TrendSparkline != "" {
		return fmt.Sprintf(
			"<td title=\"%s (%s)\">%s</td>",
			html.EscapeString(FormatCounts(row.TrendCounts)),
			html.EscapeString(row.TrendRange),
			html.EscapeString(row.TrendSparkline),
		)
	}
	return "<td>n/a</td>"
}

func trendRangeStartDate(trendRange string) (time.Time, bool) {
	parts := strings.Split(strings.TrimSpace(trendRange), "..")
	if len(parts) != 2 {
		return time.Time{}, false
	}
	startDay, err := time.Parse("2006-01-02", strings.TrimSpace(parts[0]))
	if err != nil {
		return time.Time{}, false
	}
	return startDay.UTC(), true
}

func trendTooltip(counts []int, dateRange string) string {
	if len(counts) == 0 {
		return "n/a"
	}
	startDate, ok := trendRangeStartDate(dateRange)
	if ok {
		parts := make([]string, 0, len(counts))
		for i, count := range counts {
			day := startDate.AddDate(0, 0, i)
			parts = append(parts, fmt.Sprintf("%s %d: %d", day.Format("Jan"), day.Day(), count))
		}
		return strings.Join(parts, " · ")
	}
	rangeLabel := strings.TrimSpace(dateRange)
	if rangeLabel != "" {
		return fmt.Sprintf("%s (%s)", strings.TrimSpace(FormatCounts(counts)), rangeLabel)
	}
	return strings.TrimSpace(FormatCounts(counts))
}

func renderTrendBarsSVG(counts []int, ariaLabel string) string {
	if len(counts) == 0 {
		return "<span class=\"muted\">n/a</span>"
	}
	const (
		barWidth    = 6
		barGap      = 2
		chartHeight = 18
	)
	maxCount := 0
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}
	chartWidth := len(counts)*barWidth + (len(counts)-1)*barGap
	if chartWidth < 1 {
		chartWidth = 1
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(
		"<svg class=\"trend-svg\" width=\"%d\" height=\"%d\" viewBox=\"0 0 %d %d\" role=\"img\" aria-label=\"%s\">",
		chartWidth,
		chartHeight,
		chartWidth,
		chartHeight,
		html.EscapeString(strings.TrimSpace(ariaLabel)),
	))
	b.WriteString(fmt.Sprintf(
		"<line x1=\"0\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"#d1d5db\" stroke-width=\"1\"/>",
		chartHeight-1,
		chartWidth,
		chartHeight-1,
	))
	for i, count := range counts {
		height := 1
		fill := "#e5e7eb"
		if count > 0 {
			if maxCount > 0 {
				height = 1 + (count*(chartHeight-2))/maxCount
			}
			fill = "#93c5fd"
		}
		if i == len(counts)-1 {
			if count > 0 {
				fill = "#2563eb"
			} else {
				fill = "#cbd5e1"
			}
		}
		x := i * (barWidth + barGap)
		y := chartHeight - height
		b.WriteString(fmt.Sprintf(
			"<rect x=\"%d\" y=\"%d\" width=\"%d\" height=\"%d\" rx=\"1\" ry=\"1\" fill=\"%s\"/>",
			x,
			y,
			barWidth,
			height,
			fill,
		))
	}
	b.WriteString("</svg>")
	return b.String()
}

func renderDetailRow(row FailurePatternRow, rowID string, colSpan int, opts TableOptions) string {
	if colSpan <= 0 {
		colSpan = 1
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf(
		"        <tr class=\"failure-patterns-errors-row\" data-parent-row-id=\"%s\"><td colspan=\"%d\">",
		html.EscapeString(strings.TrimSpace(rowID)),
		colSpan,
	))
	if len(row.LinkedPatterns) > 0 {
		b.WriteString(renderLinkedChildrenDetails(row.LinkedPatterns, opts))
		b.WriteString("</td></tr>\n")
		return b.String()
	}
	b.WriteString("<div class=\"failure-pattern-detail-actions\">")
	b.WriteString(renderFullErrorDetails(row.FullErrorSamples, opts.FullErrorsSummaryLabel))
	b.WriteString(renderContributingTestsDetails(row.ContributingTests, opts.ContributingSummaryLabel))
	b.WriteString(renderAffectedRunsDetails(row.AffectedRuns, opts))
	b.WriteString("</div>")
	b.WriteString("</td></tr>\n")
	return b.String()
}

func renderLinkedChildrenDetails(children []FailurePatternRow, opts TableOptions) string {
	if len(children) == 0 {
		return "<span class=\"muted\">No linked failure patterns.</span>"
	}
	ordered := append([]FailurePatternRow(nil), children...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Occurrences != ordered[j].Occurrences {
			return ordered[i].Occurrences > ordered[j].Occurrences
		}
		if ordered[i].AfterLastPushCount != ordered[j].AfterLastPushCount {
			return ordered[i].AfterLastPushCount > ordered[j].AfterLastPushCount
		}
		if strings.TrimSpace(ordered[i].FailurePattern) != strings.TrimSpace(ordered[j].FailurePattern) {
			return strings.TrimSpace(ordered[i].FailurePattern) < strings.TrimSpace(ordered[j].FailurePattern)
		}
		return strings.TrimSpace(ordered[i].FailurePatternID) < strings.TrimSpace(ordered[j].FailurePatternID)
	})
	var b strings.Builder
	b.WriteString(fmt.Sprintf("<details class=\"linked-failure-patterns-toggle\"><summary>Linked failure patterns (%d)</summary>", len(ordered)))
	b.WriteString("<div class=\"linked-failure-pattern-list\">")
	for index, child := range ordered {
		phrase := strings.TrimSpace(child.FailurePattern)
		if phrase == "" {
			phrase = "(unknown evidence)"
		}
		jobsAffected := affectedJobCount(child)
		flakeScore, flakeReasons := FlakeScoreAndReasons(child)
		childFlakeLabel := flakeScoreLabel(flakeScore)
		childFlakeTitle := fmt.Sprintf("Flake signal: %s (score %d/14)", childFlakeLabel, flakeScore)
		if len(flakeReasons) > 0 {
			childFlakeTitle = fmt.Sprintf("%s — %s", childFlakeTitle, strings.Join(flakeReasons, "; "))
		}
		b.WriteString("<details class=\"linked-child-toggle linked-failure-pattern-item\">")
		b.WriteString("<summary>")
		if opts.ShowLinkedChildRemove {
			selectionValue := strings.TrimSpace(child.SelectionValue)
			if selectionValue == "" {
				selectionValue = strings.TrimSpace(child.FailurePatternID)
			}
			if selectionValue != "" {
				b.WriteString(fmt.Sprintf(
					"<button class=\"linked-failure-pattern-item-remove\" type=\"submit\" name=\"unlink_child\" value=\"%s\" title=\"Remove this signature from the linked cluster\" aria-label=\"Remove this signature from the linked cluster\" onclick=\"event.stopPropagation();\">-</button>",
					html.EscapeString(selectionValue),
				))
			}
		}
		b.WriteString(fmt.Sprintf(
			"<span class=\"linked-failure-pattern-item-summary\"><strong>%d.</strong> %s</span><span class=\"linked-failure-pattern-item-meta\">runs affected: %d</span>",
			index+1,
			html.EscapeString(cleanInline(phrase, 220)),
			jobsAffected,
		))
		b.WriteString("</summary>")
		b.WriteString(fmt.Sprintf("<div class=\"muted\">Flake signal: %s</div>", html.EscapeString(childFlakeLabel)))
		if opts.ShowLinkedChildQuality || opts.ShowLinkedChildReview {
			b.WriteString("<div class=\"linked-failure-pattern-item-flags\">")
			if opts.ShowLinkedChildQuality {
				b.WriteString(renderQualityBadges(child.QualityNoteLabels))
			}
			if opts.ShowLinkedChildReview {
				b.WriteString(renderReviewBadges(child.ReviewNoteLabels))
			}
			b.WriteString("</div>")
		}
		b.WriteString("<div class=\"failure-pattern-detail-actions\">")
		b.WriteString(renderFullErrorDetails(child.FullErrorSamples, opts.FullErrorsSummaryLabel))
		b.WriteString(renderContributingTestsDetails(child.ContributingTests, opts.ContributingSummaryLabel))
		b.WriteString(renderAffectedRunsDetails(child.AffectedRuns, opts))
		b.WriteString("</div>")
		b.WriteString("</details>")
	}
	b.WriteString("</div></details>")
	return b.String()
}

func renderQualityBadges(labels []string) string {
	if len(labels) == 0 {
		return "<span class=\"badge badge-ok\">quality: ok</span>"
	}
	parts := make([]string, 0, len(labels))
	for _, label := range labels {
		parts = append(parts, fmt.Sprintf("<span class=\"badge badge-quality\">%s</span>", html.EscapeString(label)))
	}
	return strings.Join(parts, "")
}

func renderReviewBadges(labels []string) string {
	if len(labels) == 0 {
		return "<span class=\"badge badge-ok\">review: none</span>"
	}
	parts := make([]string, 0, len(labels))
	for _, label := range labels {
		parts = append(parts, fmt.Sprintf("<span class=\"badge badge-review\">%s</span>", html.EscapeString(label)))
	}
	return strings.Join(parts, "")
}

func renderFullErrorDetails(samples []string, summaryLabel string) string {
	summaryLabel = strings.TrimSpace(summaryLabel)
	if summaryLabel == "" {
		summaryLabel = "Full failure examples"
	}
	if len(samples) == 0 {
		return fmt.Sprintf("<span class=\"muted\">%s: n/a</span>", html.EscapeString(summaryLabel))
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("<details class=\"full-errors-toggle\"><summary>%s (%d)</summary>", html.EscapeString(summaryLabel), len(samples)))
	for _, sample := range samples {
		trimmed := strings.TrimSpace(sample)
		if trimmed == "" {
			continue
		}
		b.WriteString("<pre>")
		b.WriteString(html.EscapeString(trimmed))
		b.WriteString("</pre>")
	}
	b.WriteString("</details>")
	return b.String()
}

func renderContributingTestsDetails(items []ContributingTest, summaryLabel string) string {
	summaryLabel = strings.TrimSpace(summaryLabel)
	if summaryLabel == "" {
		summaryLabel = "Contributing tests"
	}
	ordered := OrderedContributingTests(items)
	var b strings.Builder
	b.WriteString(fmt.Sprintf("<details class=\"contributing-tests-toggle\"><summary>%s (%d)</summary>", html.EscapeString(summaryLabel), len(ordered)))
	if len(ordered) == 0 {
		b.WriteString("<span class=\"muted\">No contributing tests available.</span>")
		b.WriteString("</details>")
		return b.String()
	}
	b.WriteString("<div class=\"tests-scroll\"><table class=\"tests-table\"><thead><tr><th>Lane</th><th>Job</th><th>Test</th><th>Support</th></tr></thead><tbody>")
	for _, item := range ordered {
		lane := strings.TrimSpace(item.FailedAt)
		jobName := strings.TrimSpace(item.JobName)
		testName := strings.TrimSpace(item.TestName)
		if lane == "" {
			lane = "n/a"
		}
		if jobName == "" {
			jobName = "n/a"
		}
		if testName == "" {
			testName = "n/a"
		}
		b.WriteString("<tr>")
		b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(lane)))
		b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(jobName)))
		b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(testName)))
		b.WriteString(fmt.Sprintf("<td>%d</td>", item.Occurrences))
		b.WriteString("</tr>")
	}
	b.WriteString("</tbody></table></div></details>")
	return b.String()
}

func renderAffectedRunsDetails(rows []RunReference, opts TableOptions) string {
	runs := OrderedUniqueReferences(rows)
	var b strings.Builder
	b.WriteString(fmt.Sprintf("<details class=\"affected-runs-toggle\"><summary>%s (%d)</summary>", html.EscapeString(opts.AffectedRunsSummaryLabel), len(runs)))
	if len(runs) == 0 {
		b.WriteString("<span class=\"muted\">No affected runs available.</span>")
		b.WriteString("</details>")
		return b.String()
	}
	b.WriteString("<div class=\"runs-scroll\"><table class=\"runs-table\"><thead><tr><th>Date (UTC)</th><th>Associated PR</th><th>Prow job</th></tr></thead><tbody>")
	for _, row := range runs {
		b.WriteString("<tr>")
		b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(FormatReferenceTimestampLabel(row.OccurredAt))))
		b.WriteString(fmt.Sprintf("<td>%s</td>", renderAssociatedPRCell(row, opts)))
		b.WriteString(fmt.Sprintf("<td>%s</td>", renderProwJobCell(row)))
		b.WriteString("</tr>")
	}
	b.WriteString("</tbody></table></div></details>")
	return b.String()
}

func renderAssociatedPRCell(row RunReference, opts TableOptions) string {
	if row.PRNumber <= 0 {
		return "<span class=\"muted\">n/a</span>"
	}
	label := fmt.Sprintf("PR #%d", row.PRNumber)
	if prURL, ok := ResolveGitHubPRURLFromProwRun(strings.TrimSpace(row.RunURL), row.PRNumber, opts.GitHubRepoOwner, opts.GitHubRepoName); ok {
		return fmt.Sprintf(
			"<a href=\"%s\" target=\"_blank\" rel=\"noopener noreferrer\">%s</a>",
			html.EscapeString(prURL),
			html.EscapeString(label),
		)
	}
	return html.EscapeString(label)
}

func renderProwJobCell(row RunReference) string {
	runURL := strings.TrimSpace(row.RunURL)
	if runURL == "" {
		return "<span class=\"muted\">n/a</span>"
	}
	return fmt.Sprintf(
		"<a href=\"%s\" target=\"_blank\" rel=\"noopener noreferrer\">prow job</a>",
		html.EscapeString(runURL),
	)
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
	return string(runes[:max-1]) + "..."
}
