package triagehtml

import (
	"fmt"
	"html"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"

	sourceoptions "ci-failure-atlas/pkg/source/options"
)

type RunReference struct {
	RunURL      string
	OccurredAt  string
	SignatureID string
	PRNumber    int
}

type ContributingTest struct {
	Lane         string
	JobName      string
	TestName     string
	SupportCount int
}

type SignatureRow struct {
	Environment         string
	Lane                string
	JobName             string
	TestName            string
	TestSuite           string
	Phrase              string
	ClusterID           string
	SearchQuery         string
	SearchIndex         string
	SupportCount        int
	TrendSparkline      string
	TrendCounts         []int
	TrendRange          string
	SupportShare        float64
	PostGoodCount       int
	AlsoSeenIn          []string
	QualityScore        int
	QualityNoteLabels   []string
	ReviewNoteLabels    []string
	ContributingTests   []ContributingTest
	FullErrorSamples    []string
	References          []RunReference
	ScoringReferences   []RunReference
	PriorWeeksPresent   int
	PriorWeekStarts     []string
	PriorJobsAffected   int
	PriorLastSeenAt     string
	ManualIssueID       string
	ManualIssueConflict bool
	SelectionValue      string
	LinkedChildren      []SignatureRow
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
	ReportViewRolling ReportView = "rolling"
	ReportViewReport  ReportView = "report"
	ReportViewTriage  ReportView = "triage"
	ReportViewRuns    ReportView = "runs"
)

type ReportChromeOptions struct {
	WindowLabel  string
	CurrentWeek  string
	CurrentView  ReportView
	PreviousWeek string
	PreviousHref string
	NextWeek     string
	NextHref     string
	RollingHref  string
	ReportHref   string
	WeeklyHref   string
	TriageHref   string
	RunsHref     string
	ArchiveHref  string
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
		"    .triage-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0 12px; }",
		"    .triage-table th, .triage-table td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; vertical-align: top; }",
		"    .triage-table thead th:first-child { width: 36%; max-width: 360px; }",
		"    .triage-table tbody tr:not(.triage-errors-row) > td:first-child { width: 36%; max-width: 360px; }",
		"    .triage-table th { background: #f3f4f6; color: #374151; font-weight: 700; }",
		"    .triage-select-col { width: 38px; text-align: center; }",
		"    .triage-row-select { width: 14px; height: 14px; cursor: pointer; }",
		"    .triage-table th.triage-sortable { white-space: nowrap; }",
		"    .triage-sort-button { all: unset; display: inline-flex; align-items: center; gap: 4px; cursor: pointer; color: inherit; font: inherit; font-weight: 700; }",
		"    .triage-sort-indicator { display: inline-block; min-width: 10px; text-align: center; font-size: 10px; color: #6b7280; }",
		"    .triage-table tr.triage-errors-row td { background: #eff6ff; }",
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
		"    .triage-header-help { display: inline-flex; align-items: center; justify-content: center; margin-left: 5px; width: 14px; height: 14px; border-radius: 999px; border: 1px solid #93c5fd; color: #1d4ed8; background: #eff6ff; font-size: 10px; font-weight: 700; cursor: help; vertical-align: middle; }",
		"    .trend-svg { display: block; }",
		"    details { margin: 2px 0; }",
		"    details summary { cursor: pointer; color: #1d4ed8; }",
		"    details.signature-toggle > summary { font-size: 13px; font-weight: 700; color: #111827; }",
		"    .triage-errors-row .triage-detail-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-start; }",
		"    .triage-errors-row details.full-errors-toggle, .triage-errors-row details.affected-runs-toggle, .triage-errors-row details.contributing-tests-toggle { margin: 0; }",
		"    .triage-errors-row details.full-errors-toggle > summary, .triage-errors-row details.affected-runs-toggle > summary, .triage-errors-row details.contributing-tests-toggle > summary { display: inline-flex; align-items: center; gap: 6px; font-size: 9px; font-weight: 600; color: #1e3a8a; background: #dbeafe; border: 1px solid #93c5fd; border-radius: 999px; padding: 2px 10px; }",
		"    .triage-errors-row details.full-errors-toggle[open] > summary, .triage-errors-row details.affected-runs-toggle[open] > summary, .triage-errors-row details.contributing-tests-toggle[open] > summary { background: #bfdbfe; border-color: #60a5fa; color: #1e40af; }",
		"    .triage-errors-row details.linked-signatures-toggle > summary, .triage-errors-row details.linked-child-toggle > summary { display: inline-flex; align-items: center; gap: 6px; font-size: 11px; font-weight: 700; color: #1e3a8a; background: #dbeafe; border: 1px solid #93c5fd; border-radius: 8px; padding: 4px 10px; }",
		"    .triage-errors-row details.linked-signatures-toggle[open] > summary, .triage-errors-row details.linked-child-toggle[open] > summary { background: #bfdbfe; border-color: #60a5fa; color: #1e40af; }",
		"    .triage-linked-list { display: flex; flex-direction: column; gap: 8px; margin-top: 8px; }",
		"    .triage-linked-item { border: 1px solid #bfdbfe; border-radius: 8px; background: #eff6ff; padding: 6px 8px; }",
		"    .triage-linked-item-remove { display: inline-flex; align-items: center; justify-content: center; width: 18px; height: 18px; margin-right: 6px; border: 1px solid #93c5fd; border-radius: 999px; background: #fff; color: #1e40af; font-size: 12px; font-weight: 700; line-height: 1; cursor: pointer; }",
		"    .triage-linked-item-remove:hover { background: #dbeafe; }",
		"    .triage-linked-item-summary { display: inline-flex; flex-wrap: wrap; align-items: center; gap: 8px; }",
		"    .triage-linked-item-meta { color: #4b5563; font-size: 11px; }",
		"    .triage-linked-item-flags { margin: 6px 0 6px; }",
		"    .triage-linked-item-header { margin-top: 4px; }",
		"    .triage-errors-row .runs-scroll { margin-top: 6px; max-height: 172px; overflow-y: auto; border: 1px solid #bfdbfe; border-radius: 6px; background: #eff6ff; }",
		"    .triage-errors-row .runs-table { border-collapse: collapse; width: 100%; font-size: 11px; }",
		"    .triage-errors-row .runs-table th, .triage-errors-row .runs-table td { padding: 4px 6px; border-bottom: 1px solid #dbeafe; text-align: left; vertical-align: top; }",
		"    .triage-errors-row .runs-table th { position: sticky; top: 0; background: #dbeafe; z-index: 1; }",
		"    .triage-errors-row .tests-scroll { margin-top: 6px; max-height: 172px; overflow-y: auto; border: 1px solid #bfdbfe; border-radius: 6px; background: #eff6ff; }",
		"    .triage-errors-row .tests-table { border-collapse: collapse; width: 100%; font-size: 11px; }",
		"    .triage-errors-row .tests-table th, .triage-errors-row .tests-table td { padding: 4px 6px; border-bottom: 1px solid #dbeafe; text-align: left; vertical-align: top; }",
		"    .triage-errors-row .tests-table th { position: sticky; top: 0; background: #dbeafe; z-index: 1; }",
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
		"    :root[data-theme=\"dark\"] .detail-table th, :root[data-theme=\"dark\"] .overview-table th, :root[data-theme=\"dark\"] .quality-table th, :root[data-theme=\"dark\"] .triage-table th { background: #1f2937; color: #e2e8f0; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .detail-table td, :root[data-theme=\"dark\"] .overview-table td, :root[data-theme=\"dark\"] .quality-table td, :root[data-theme=\"dark\"] .triage-table td { border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .triage-table tr.triage-errors-row td, :root[data-theme=\"dark\"] .quality-table tr.inspector-errors-row td { background: #0f172a; }",
		"    :root[data-theme=\"dark\"] .triage-errors-row .runs-scroll, :root[data-theme=\"dark\"] .triage-errors-row .tests-scroll, :root[data-theme=\"dark\"] .inspector-errors-row .runs-scroll { background: #0f172a; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .triage-errors-row .runs-table th, :root[data-theme=\"dark\"] .triage-errors-row .tests-table th, :root[data-theme=\"dark\"] .inspector-errors-row .runs-table th { background: #1e293b; }",
		"    :root[data-theme=\"dark\"] .triage-errors-row .runs-table th, :root[data-theme=\"dark\"] .triage-errors-row .runs-table td, :root[data-theme=\"dark\"] .triage-errors-row .tests-table th, :root[data-theme=\"dark\"] .triage-errors-row .tests-table td, :root[data-theme=\"dark\"] .inspector-errors-row .runs-table th, :root[data-theme=\"dark\"] .inspector-errors-row .runs-table td { border-bottom-color: #334155; }",
		"    :root[data-theme=\"dark\"] .triage-errors-row details.full-errors-toggle > summary, :root[data-theme=\"dark\"] .triage-errors-row details.affected-runs-toggle > summary, :root[data-theme=\"dark\"] .triage-errors-row details.contributing-tests-toggle > summary { color: #e2e8f0; background: #1f2937; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .triage-errors-row details.full-errors-toggle[open] > summary, :root[data-theme=\"dark\"] .triage-errors-row details.affected-runs-toggle[open] > summary, :root[data-theme=\"dark\"] .triage-errors-row details.contributing-tests-toggle[open] > summary { color: #e2e8f0; background: #2563eb; border-color: #2563eb; }",
		"    :root[data-theme=\"dark\"] .triage-errors-row details.linked-signatures-toggle > summary, :root[data-theme=\"dark\"] .triage-errors-row details.linked-child-toggle > summary { color: #e2e8f0; background: #1f2937; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .triage-errors-row details.linked-signatures-toggle[open] > summary, :root[data-theme=\"dark\"] .triage-errors-row details.linked-child-toggle[open] > summary { color: #e2e8f0; background: #2563eb; border-color: #2563eb; }",
		"    :root[data-theme=\"dark\"] .triage-linked-item { background: #0f172a; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .triage-linked-item-remove { background: #111827; border-color: #334155; color: #93c5fd; }",
		"    :root[data-theme=\"dark\"] .triage-linked-item-remove:hover { background: #1f2937; }",
		"    :root[data-theme=\"dark\"] .triage-linked-item-meta { color: #94a3b8; }",
		"    :root[data-theme=\"dark\"] pre { background: #020617; color: #e2e8f0; border: 1px solid #334155; }",
		"    :root[data-theme=\"dark\"] .triage-header-help { border-color: #334155; color: #93c5fd; background: #1e293b; }",
		"    :root[data-theme=\"dark\"] .triage-sort-indicator { color: #94a3b8; }",
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
		b.WriteString(renderReportChromeViewLink(normalized.RollingHref, "Rolling 7d", normalized.CurrentView == ReportViewRolling))
	}
	if strings.TrimSpace(normalized.ReportHref) != "" {
		b.WriteString(renderReportChromeViewLink(normalized.ReportHref, "Report", normalized.CurrentView == ReportViewReport))
	}
	if strings.TrimSpace(normalized.TriageHref) != "" {
		b.WriteString(renderReportChromeViewLink(normalized.TriageHref, "Triage", normalized.CurrentView == ReportViewTriage))
	}
	if strings.TrimSpace(normalized.RunsHref) != "" {
		b.WriteString(renderReportChromeViewLink(normalized.RunsHref, "Runs", normalized.CurrentView == ReportViewRuns))
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
	options.WeeklyHref = strings.TrimSpace(options.WeeklyHref)
	options.TriageHref = strings.TrimSpace(options.TriageHref)
	options.RunsHref = strings.TrimSpace(options.RunsHref)
	options.ArchiveHref = strings.TrimSpace(options.ArchiveHref)
	if options.ReportHref == "" {
		options.ReportHref = options.WeeklyHref
	}
	switch options.CurrentView {
	case ReportViewRolling, ReportViewReport, ReportViewTriage, ReportViewRuns:
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
		options.TriageHref != "" ||
		options.RunsHref != "" ||
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

func RenderTable(rows []SignatureRow, options TableOptions) string {
	opts := normalizedOptions(options)
	initialSortKey := strings.TrimSpace(opts.InitialSortKey)
	if !isSortableKey(initialSortKey) {
		initialSortKey = defaultSortKey
	}
	initialSortDirection := strings.TrimSpace(strings.ToLower(opts.InitialSortDirection))
	if initialSortDirection != sortDirectionAscending && initialSortDirection != sortDirectionDescending {
		initialSortDirection = defaultSortDirection
	}
	orderedRows := append([]SignatureRow(nil), rows...)
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
		"    <table class=\"triage-table\" data-sortable=\"true\" data-sort-key=\"%s\" data-sort-dir=\"%s\" data-initial-visible=\"%d\">\n",
		initialSortKey,
		initialSortDirection,
		initialVisibleRows,
	))
	b.WriteString("      <thead><tr>")
	headers := make([]string, 0, 12)
	if opts.IncludeSelection {
		headers = append(headers, "<th class=\"triage-select-col\">Select</th>")
	}
	headers = append(headers, "<th>Signature</th>")
	headers = append(headers, "<th>Lane</th>")
	headers = append(headers,
		renderSortableHeaderCell("Jobs affected", sortKeyJobsAffected, "Unique job runs affected by this signature in the selected window.", initialSortKey, initialSortDirection),
		renderSortableHeaderCell("Impact", sortKeyImpact, "Relative impact = jobs affected / overall job count from metrics.", initialSortKey, initialSortDirection),
		renderSortableHeaderCell("Flake score", sortKeyFlakeScore, "Heuristic score for unresolved recurrent flakes (0-14). Higher means more likely ongoing flake; likely bad-PR patterns reduce this score.", initialSortKey, initialSortDirection),
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
		headers = append(headers, renderSortableHeaderCell("Phase3 cluster", sortKeyManualCluster, "Internal cluster id created automatically when linking selected signatures.", initialSortKey, initialSortDirection))
	}
	if opts.IncludeTrend {
		headers = append(headers, fmt.Sprintf("<th>%s</th>", html.EscapeString(opts.TrendHeaderLabel)))
	}
	headers = append(headers, renderTooltipHeaderCell("Seen in", "Other environments where the same canonical signature phrase appears."))
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
		rowID := fmt.Sprintf("triage-row-%d", rowIndex)
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
		"<th>%s<span class=\"triage-header-help\" title=\"%s\" aria-label=\"%s\">i</span></th>",
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
		"<th class=\"triage-sortable\" data-sort-key=\"%s\" aria-sort=\"%s\">",
		html.EscapeString(trimmedSortKey),
		ariaSort,
	))
	b.WriteString(fmt.Sprintf(
		"<button type=\"button\" class=\"triage-sort-button\" data-sort-key=\"%s\">%s<span class=\"triage-sort-indicator\" aria-hidden=\"true\">%s</span></button>",
		html.EscapeString(trimmedSortKey),
		html.EscapeString(trimmedLabel),
		html.EscapeString(indicator),
	))
	trimmedTooltip := strings.TrimSpace(tooltip)
	if trimmedTooltip != "" {
		b.WriteString(fmt.Sprintf(
			"<span class=\"triage-header-help\" title=\"%s\" aria-label=\"%s\">i</span>",
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
    var mains = Array.prototype.slice.call(tbody.querySelectorAll("tr.triage-row"));
    var pairs = [];
    for (var i = 0; i < mains.length; i++) {
      var main = mains[i];
      var rowID = main.getAttribute("data-row-id") || "";
      var detail = null;
      var sibling = main.nextElementSibling;
      if (sibling && sibling.classList.contains("triage-errors-row") && (sibling.getAttribute("data-parent-row-id") || "") === rowID) {
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
    var headers = table.querySelectorAll("th.triage-sortable");
    for (var i = 0; i < headers.length; i++) {
      var header = headers[i];
      var headerKey = header.getAttribute("data-sort-key") || "";
      var indicator = header.querySelector(".triage-sort-indicator");
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
    var buttons = table.querySelectorAll("button.triage-sort-button");
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
  var tables = document.querySelectorAll("table.triage-table[data-sortable=\"true\"]");
  for (var i = 0; i < tables.length; i++) {
    initSortableTable(tables[i]);
  }
})();
</script>
`) + "\n"
}

func OrderedUniqueReferences(rows []RunReference) []RunReference {
	if len(rows) == 0 {
		return nil
	}
	byRunURL := map[string]RunReference{}
	withoutRunURL := make([]RunReference, 0)
	for _, row := range rows {
		runURL := strings.TrimSpace(row.RunURL)
		if runURL == "" {
			withoutRunURL = append(withoutRunURL, row)
			continue
		}
		existing, ok := byRunURL[runURL]
		if !ok || referenceIsNewer(row, existing) {
			byRunURL[runURL] = row
		}
	}
	ordered := make([]RunReference, 0, len(byRunURL)+len(withoutRunURL))
	for _, row := range byRunURL {
		ordered = append(ordered, row)
	}
	ordered = append(ordered, withoutRunURL...)
	sort.Slice(ordered, func(i, j int) bool {
		ti, okI := ParseReferenceTimestamp(ordered[i].OccurredAt)
		tj, okJ := ParseReferenceTimestamp(ordered[j].OccurredAt)
		switch {
		case okI && okJ && !ti.Equal(tj):
			return ti.After(tj)
		case okI != okJ:
			return okI
		}
		runURLI := strings.TrimSpace(ordered[i].RunURL)
		runURLJ := strings.TrimSpace(ordered[j].RunURL)
		if runURLI != runURLJ {
			return runURLI < runURLJ
		}
		return strings.TrimSpace(ordered[i].SignatureID) < strings.TrimSpace(ordered[j].SignatureID)
	})
	return ordered
}

func OrderedContributingTests(items []ContributingTest) []ContributingTest {
	if len(items) == 0 {
		return nil
	}
	out := append([]ContributingTest(nil), items...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].SupportCount != out[j].SupportCount {
			return out[i].SupportCount > out[j].SupportCount
		}
		if out[i].Lane != out[j].Lane {
			return out[i].Lane < out[j].Lane
		}
		if out[i].JobName != out[j].JobName {
			return out[i].JobName < out[j].JobName
		}
		return out[i].TestName < out[j].TestName
	})
	return out
}

func ParseReferenceTimestamp(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, false
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts.UTC(), true
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts.UTC(), true
	}
	return time.Time{}, false
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

func QualityIssueCodes(phrase string) []string {
	trimmed := strings.TrimSpace(phrase)
	normalized := strings.ToLower(trimmed)
	set := map[string]struct{}{}
	add := func(code string) {
		if strings.TrimSpace(code) == "" {
			return
		}
		set[code] = struct{}{}
	}
	if trimmed == "" {
		add("empty_phrase")
	}
	if isGenericFailurePhrase(trimmed) {
		add("generic_failure_phrase")
	}
	if len([]rune(trimmed)) > 0 && len([]rune(trimmed)) <= 3 {
		add("too_short_phrase")
	}
	if strings.Contains(normalized, "<context.") {
		add("context_type_stub")
	}
	if strings.Contains(normalized, "errorcode:\"\"") || strings.Contains(normalized, "errorcode: \"\"") || strings.Contains(normalized, "errorcode:''") || strings.Contains(normalized, "errorcode: ''") {
		add("empty_error_code")
	}
	if phraseLooksLikeStructFragment(trimmed) {
		add("struct_fragment")
	}
	if phraseMostlyPunctuation(trimmed) {
		add("mostly_punctuation")
	}
	if containsDeserializationNoOutputSignal(trimmed) {
		add("source_deserialization_no_output")
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

func QualityScore(issueCodes []string) int {
	score := 0
	for _, issue := range issueCodes {
		score += qualityIssueWeight(issue)
	}
	return score
}

func QualityIssueLabel(code string) string {
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
	default:
		return code
	}
}

func DailyDensitySparkline(references []RunReference, windowDays int, endAnchor time.Time) (string, []int, string, bool) {
	if windowDays <= 0 {
		return "", nil, "", false
	}
	endDay := endAnchor.UTC().Truncate(24 * time.Hour)
	if endDay.IsZero() {
		endDay = time.Now().UTC().Truncate(24 * time.Hour)
	}
	startDay := endDay.AddDate(0, 0, -(windowDays - 1))

	counts := make([]int, windowDays)
	seenTimestamp := false
	for _, reference := range references {
		ts, ok := ParseReferenceTimestamp(reference.OccurredAt)
		if !ok {
			continue
		}
		seenTimestamp = true
		day := ts.UTC().Truncate(24 * time.Hour)
		if day.Before(startDay) || day.After(endDay) {
			continue
		}
		index := int(day.Sub(startDay).Hours() / 24)
		if index < 0 || index >= windowDays {
			continue
		}
		counts[index]++
	}
	if !seenTimestamp {
		return "", nil, "", false
	}

	maxCount := 0
	for _, value := range counts {
		if value > maxCount {
			maxCount = value
		}
	}

	unicodeLevels := []rune("▁▂▃▄▅▆▇█")
	var unicodeBuilder strings.Builder
	for _, value := range counts {
		if value <= 0 {
			unicodeBuilder.WriteRune('·')
			continue
		}
		levelIndex := len(unicodeLevels) - 1
		if maxCount > 0 {
			levelIndex = value * (len(unicodeLevels) - 1) / maxCount
		}
		unicodeBuilder.WriteRune(unicodeLevels[levelIndex])
	}

	dateRange := fmt.Sprintf("%s..%s UTC", startDay.Format("2006-01-02"), endDay.Format("2006-01-02"))
	return unicodeBuilder.String(), counts, dateRange, true
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

func BadPRScoreAndReasons(row SignatureRow) (int, []string) {
	if rowPostGoodCount(row) > 0 {
		return 0, nil
	}

	score := 1
	reasons := []string{"post-good=0"}

	if isOnlySeenInDev(row) {
		score++
		reasons = append(reasons, "only seen in DEV")
	}
	if isSingleKnownPR(row) {
		score++
		reasons = append(reasons, "only seen in one PR")
	}
	return score, reasons
}

func FlakeScoreAndReasons(row SignatureRow) (int, []string) {
	score := 0
	reasons := make([]string, 0, 7)

	jobsAffected := rowJobsAffected(row)
	jobPoints := flakeAffectedJobPoints(jobsAffected)
	if jobPoints > 0 {
		score += jobPoints
		reasons = append(reasons, fmt.Sprintf("jobs affected +%d", jobPoints))
	}

	postGoodPoints := flakePostGoodPoints(rowPostGoodCount(row))
	if postGoodPoints > 0 {
		score += postGoodPoints
		reasons = append(reasons, fmt.Sprintf("after last push +%d", postGoodPoints))
	}

	spreadPoints := flakeSpreadPoints(row.TrendCounts)
	if spreadPoints > 0 {
		score += spreadPoints
		reasons = append(reasons, fmt.Sprintf("daily spread +%d", spreadPoints))
	}

	recentPoints := flakeRecentPoints(rowScoreReferences(row), row.TrendRange)
	if recentPoints > 0 {
		score += recentPoints
		reasons = append(reasons, fmt.Sprintf("recent occurrence +%d", recentPoints))
	}

	historyPoints := flakeHistoryWeeksPoints(row.PriorWeeksPresent)
	if historyPoints > 0 {
		score += historyPoints
		reason := fmt.Sprintf("present in %d prior week", row.PriorWeeksPresent)
		if row.PriorWeeksPresent != 1 {
			reason += "s"
		}
		reasons = append(reasons, fmt.Sprintf("%s +%d", reason, historyPoints))
	}

	badPRScore, _ := BadPRScoreAndReasons(row)
	if badPRScore > 0 {
		score -= badPRScore
		reasons = append(reasons, fmt.Sprintf("likely bad PR -%d", badPRScore))
	}

	if score < 0 {
		score = 0
	}
	if score > 14 {
		score = 14
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "no strong flake signals")
	}
	return score, reasons
}

func affectedJobCount(row SignatureRow) int {
	return len(OrderedUniqueReferences(row.References))
}

func rowAffectedReferences(row SignatureRow) []RunReference {
	combined := append([]RunReference(nil), row.References...)
	for _, child := range row.LinkedChildren {
		combined = append(combined, child.References...)
	}
	return OrderedUniqueReferences(combined)
}

func rowScoreReferences(row SignatureRow) []RunReference {
	if len(row.ScoringReferences) > 0 {
		return row.ScoringReferences
	}
	return row.References
}

func rowPostGoodCount(row SignatureRow) int {
	if len(row.LinkedChildren) == 0 {
		return row.PostGoodCount
	}
	total := 0
	for _, child := range row.LinkedChildren {
		total += child.PostGoodCount
	}
	if total > 0 {
		return total
	}
	return row.PostGoodCount
}

func rowJobsAffected(row SignatureRow) int {
	if refs := rowAffectedReferences(row); len(refs) > 0 {
		return len(refs)
	}
	return affectedJobCount(row)
}

func totalAffectedJobs(rows []SignatureRow) int {
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

func flakeAffectedJobPoints(jobsAffected int) int {
	switch {
	case jobsAffected >= 12:
		return 3
	case jobsAffected >= 6:
		return 2
	case jobsAffected >= 3:
		return 1
	default:
		return 0
	}
}

func flakePostGoodPoints(postGoodCount int) int {
	switch {
	case postGoodCount >= 8:
		return 3
	case postGoodCount >= 4:
		return 2
	case postGoodCount >= 1:
		return 1
	default:
		return 0
	}
}

func flakeSpreadPoints(counts []int) int {
	if len(counts) == 0 {
		return 0
	}
	total := 0
	activeDays := 0
	maxDaily := 0
	for _, count := range counts {
		if count <= 0 {
			continue
		}
		total += count
		activeDays++
		if count > maxDaily {
			maxDaily = count
		}
	}
	if total <= 0 || activeDays <= 0 {
		return 0
	}
	concentration := float64(maxDaily) / float64(total)
	switch {
	case activeDays >= 5 && concentration <= 0.5:
		return 2
	case activeDays >= 3 && concentration <= 0.75:
		return 1
	default:
		return 0
	}
}

func flakeRecentPoints(references []RunReference, trendRange string) int {
	latest, ok := latestReferenceTimestamp(references)
	if !ok {
		return 0
	}
	anchor, ok := trendRangeEndAnchor(trendRange)
	if !ok {
		return 0
	}
	if latest.After(anchor) {
		latest = anchor
	}
	age := anchor.Sub(latest)
	if age < 0 {
		return 0
	}
	switch {
	case age <= 24*time.Hour:
		return 2
	case age <= 48*time.Hour:
		return 1
	default:
		return 0
	}
}

func flakeHistoryWeeksPoints(priorWeeksPresent int) int {
	if priorWeeksPresent <= 0 {
		return 0
	}
	if priorWeeksPresent > 4 {
		return 4
	}
	return priorWeeksPresent
}

func latestReferenceTimestamp(references []RunReference) (time.Time, bool) {
	var latest time.Time
	for _, reference := range references {
		ts, ok := ParseReferenceTimestamp(reference.OccurredAt)
		if !ok {
			continue
		}
		if latest.IsZero() || ts.After(latest) {
			latest = ts
		}
	}
	if latest.IsZero() {
		return time.Time{}, false
	}
	return latest, true
}

func trendRangeEndAnchor(trendRange string) (time.Time, bool) {
	parts := strings.Split(strings.TrimSpace(trendRange), "..")
	if len(parts) != 2 {
		return time.Time{}, false
	}
	endLabel := strings.TrimSuffix(strings.TrimSpace(parts[1]), " UTC")
	endDay, err := time.Parse("2006-01-02", strings.TrimSpace(endLabel))
	if err != nil {
		return time.Time{}, false
	}
	return endDay.UTC().Add(24 * time.Hour), true
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

func SortRowsByDefaultPriority(rows []SignatureRow) {
	sortRowsByDefaultPriorityWithImpact(rows, totalAffectedJobs(rows))
}

func sortRowsByDefaultPriorityWithImpact(rows []SignatureRow, impactTotalJobs int) {
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
		if rows[i].SupportShare != rows[j].SupportShare {
			return rows[i].SupportShare > rows[j].SupportShare
		}
		if rows[i].SupportCount != rows[j].SupportCount {
			return rows[i].SupportCount > rows[j].SupportCount
		}
		postGoodI := rowPostGoodCount(rows[i])
		postGoodJ := rowPostGoodCount(rows[j])
		if postGoodI != postGoodJ {
			return postGoodI > postGoodJ
		}
		if strings.TrimSpace(rows[i].Environment) != strings.TrimSpace(rows[j].Environment) {
			return strings.TrimSpace(rows[i].Environment) < strings.TrimSpace(rows[j].Environment)
		}
		if strings.TrimSpace(rows[i].Phrase) != strings.TrimSpace(rows[j].Phrase) {
			return strings.TrimSpace(rows[i].Phrase) < strings.TrimSpace(rows[j].Phrase)
		}
		return strings.TrimSpace(rows[i].ClusterID) < strings.TrimSpace(rows[j].ClusterID)
	})
}

func SortRowsByBadPRScore(rows []SignatureRow) {
	sort.Slice(rows, func(i, j int) bool {
		scoreI, _ := BadPRScoreAndReasons(rows[i])
		scoreJ, _ := BadPRScoreAndReasons(rows[j])
		if scoreI != scoreJ {
			return scoreI < scoreJ
		}
		if rows[i].SupportCount != rows[j].SupportCount {
			return rows[i].SupportCount > rows[j].SupportCount
		}
		postGoodI := rowPostGoodCount(rows[i])
		postGoodJ := rowPostGoodCount(rows[j])
		if postGoodI != postGoodJ {
			return postGoodI > postGoodJ
		}
		if rows[i].SupportShare != rows[j].SupportShare {
			return rows[i].SupportShare > rows[j].SupportShare
		}
		if strings.TrimSpace(rows[i].Environment) != strings.TrimSpace(rows[j].Environment) {
			return strings.TrimSpace(rows[i].Environment) < strings.TrimSpace(rows[j].Environment)
		}
		if strings.TrimSpace(rows[i].Phrase) != strings.TrimSpace(rows[j].Phrase) {
			return strings.TrimSpace(rows[i].Phrase) < strings.TrimSpace(rows[j].Phrase)
		}
		return strings.TrimSpace(rows[i].ClusterID) < strings.TrimSpace(rows[j].ClusterID)
	})
}

func isOnlySeenInDev(row SignatureRow) bool {
	if strings.ToLower(strings.TrimSpace(row.Environment)) != "dev" {
		return false
	}
	for _, value := range row.AlsoSeenIn {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}
	return true
}

func isSingleKnownPR(row SignatureRow) bool {
	references := OrderedUniqueReferences(rowScoreReferences(row))
	if len(references) == 0 {
		return false
	}
	uniquePRs := map[int]struct{}{}
	for _, reference := range references {
		if reference.PRNumber <= 0 {
			return false
		}
		uniquePRs[reference.PRNumber] = struct{}{}
	}
	return len(uniquePRs) == 1
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

func renderMainRow(row SignatureRow, rowID string, opts TableOptions) string {
	var b strings.Builder
	phrase := strings.TrimSpace(row.Phrase)
	if phrase == "" {
		phrase = "(unknown evidence)"
	}
	laneValue := rowLaneForDisplay(row)
	otherEnvironments := "none"
	if len(row.AlsoSeenIn) > 0 {
		otherEnvironments = strings.Join(row.AlsoSeenIn, ", ")
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
		tooltip := fmt.Sprintf("Likely bad PR signal (score %d/3): %s", badPRScore, strings.Join(badPRReasons, "; "))
		summaryText = fmt.Sprintf(
			"<span class=\"bad-pr-flag\" title=\"%s\" aria-label=\"%s\">⚠</span>%s",
			html.EscapeString(tooltip),
			html.EscapeString(tooltip),
			summaryText,
		)
	}
	badPRDetails := fmt.Sprintf("bad PR score: %d/3", badPRScore)
	if len(badPRReasons) > 0 {
		badPRDetails = fmt.Sprintf("%s (%s)", badPRDetails, strings.Join(badPRReasons, "; "))
	}
	jobsAffected := rowJobsAffected(row)
	postGoodCount := rowPostGoodCount(row)
	impactPercent := impactShare(jobsAffected, opts.ImpactTotalJobs)
	impactLabel := fmt.Sprintf("%.2f%%", impactPercent)
	impactTitle := fmt.Sprintf(
		"Impact from jobs affected / overall job count from metrics: %d/%d jobs",
		jobsAffected,
		maxInt(opts.ImpactTotalJobs, 0),
	)
	flakeScore, flakeReasons := FlakeScoreAndReasons(row)
	flakeDetails := fmt.Sprintf("flake score: %d/14", flakeScore)
	if len(flakeReasons) > 0 {
		flakeDetails = fmt.Sprintf("%s (%s)", flakeDetails, strings.Join(flakeReasons, "; "))
	}
	flakeCellTitle := flakeDetails
	if strings.TrimSpace(flakeCellTitle) == "" {
		flakeCellTitle = "flake score"
	}
	flakeClass := flakeScoreClass(flakeScore)
	manualSortValue := strings.TrimSpace(row.ManualIssueID)
	if manualSortValue == "" {
		manualSortValue = "~" + strings.ToLower(strings.TrimSpace(row.Environment)) + "|" + strings.TrimSpace(row.ClusterID)
	}
	b.WriteString(fmt.Sprintf(
		"        <tr class=\"triage-row\" data-row-id=\"%s\" data-sort-count=\"%d\" data-sort-post-good=\"%d\" data-sort-jobs=\"%d\" data-sort-impact=\"%.6f\" data-sort-flake=\"%d\" data-sort-share=\"%.6f\" data-sort-environment=\"%s\" data-sort-phrase=\"%s\" data-sort-cluster=\"%s\" data-sort-manual=\"%s\" data-filter-env=\"%s\" data-filter-lane=\"%s\" data-filter-search=\"%s\" data-filter-flagged=\"%t\" data-filter-review=\"%t\">",
		html.EscapeString(strings.TrimSpace(rowID)),
		row.SupportCount,
		postGoodCount,
		jobsAffected,
		impactPercent,
		flakeScore,
		row.SupportShare,
		html.EscapeString(strings.ToLower(strings.TrimSpace(row.Environment))),
		html.EscapeString(strings.TrimSpace(row.Phrase)),
		html.EscapeString(strings.TrimSpace(row.ClusterID)),
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
			selectionValue = strings.TrimSpace(row.ClusterID)
		}
		if selectionValue == "" {
			selectionValue = strings.TrimSpace(rowID)
		}
		b.WriteString(fmt.Sprintf(
			"<td class=\"triage-select-col\"><input class=\"triage-row-select\" type=\"checkbox\" name=\"%s\" value=\"%s\" /></td>",
			html.EscapeString(strings.TrimSpace(opts.SelectionInputName)),
			html.EscapeString(selectionValue),
		))
	}
	var signatureDetails strings.Builder
	signatureDetails.WriteString("<td><details class=\"signature-toggle\">")
	signatureDetails.WriteString(fmt.Sprintf("<summary>%s</summary>", summaryText))
	signatureDetails.WriteString("<div class=\"muted\">full signature:</div>")
	signatureDetails.WriteString(fmt.Sprintf("<pre>%s</pre>", html.EscapeString(phrase)))
	if successDetails := successDetailsFromSearchQuery(row.SearchQuery); successDetails != "" {
		signatureDetails.WriteString(fmt.Sprintf("<div class=\"muted\">%s</div>", html.EscapeString(successDetails)))
	}
	signatureDetails.WriteString(fmt.Sprintf("<div class=\"muted\">%s</div>", html.EscapeString(badPRDetails)))
	signatureDetails.WriteString(fmt.Sprintf("<div class=\"muted\">%s</div>", html.EscapeString(flakeDetails)))
	signatureDetails.WriteString("</details></td>")
	b.WriteString(signatureDetails.String())
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(laneValue)))
	b.WriteString(fmt.Sprintf("<td>%d</td>", jobsAffected))
	b.WriteString(fmt.Sprintf("<td title=\"%s\"><span class=\"impact-score %s\">%s</span></td>", html.EscapeString(impactTitle), impactScoreClass(impactPercent), html.EscapeString(impactLabel)))
	b.WriteString(fmt.Sprintf("<td title=\"%s\"><span class=\"flake-score %s\">%d</span></td>", html.EscapeString(flakeCellTitle), flakeClass, flakeScore))
	if opts.ShowCount {
		b.WriteString(fmt.Sprintf("<td>%d</td>", row.SupportCount))
	}
	if opts.ShowAfterLastPush {
		b.WriteString(fmt.Sprintf("<td>%d</td>", postGoodCount))
	}
	if opts.ShowShare {
		b.WriteString(fmt.Sprintf("<td>%.2f%%</td>", row.SupportShare))
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

func defaultSearchIndex(row SignatureRow) string {
	laneValue := rowLaneForDisplay(row)
	parts := []string{
		strings.TrimSpace(row.Environment),
		laneValue,
		strings.TrimSpace(row.JobName),
		strings.TrimSpace(row.TestName),
		strings.TrimSpace(row.TestSuite),
		strings.TrimSpace(row.Phrase),
		strings.TrimSpace(row.ClusterID),
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

func rowLaneForDisplay(row SignatureRow) string {
	lane := strings.TrimSpace(row.Lane)
	if lane != "" {
		return lane
	}
	ordered := OrderedContributingTests(row.ContributingTests)
	uniqueLanes := map[string]struct{}{}
	for _, contributing := range ordered {
		trimmedLane := strings.TrimSpace(contributing.Lane)
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

func renderTrendCell(row SignatureRow) string {
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

func trendTooltip(counts []int, dateRange string) string {
	countLabel := strings.TrimSpace(FormatCounts(counts))
	rangeLabel := strings.TrimSpace(dateRange)
	switch {
	case countLabel != "" && rangeLabel != "":
		return fmt.Sprintf("%s (%s)", countLabel, rangeLabel)
	case countLabel != "":
		return countLabel
	case rangeLabel != "":
		return rangeLabel
	default:
		return "n/a"
	}
}

func renderTrendBarsSVG(counts []int, ariaLabel string) string {
	if len(counts) == 0 {
		return "<span class=\"muted\">n/a</span>"
	}
	const (
		barWidth    = 4
		barGap      = 2
		chartHeight = 16
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

func renderDetailRow(row SignatureRow, rowID string, colSpan int, opts TableOptions) string {
	if colSpan <= 0 {
		colSpan = 1
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf(
		"        <tr class=\"triage-errors-row\" data-parent-row-id=\"%s\"><td colspan=\"%d\">",
		html.EscapeString(strings.TrimSpace(rowID)),
		colSpan,
	))
	if len(row.LinkedChildren) > 0 {
		b.WriteString(renderLinkedChildrenDetails(row.LinkedChildren, opts))
		b.WriteString("</td></tr>\n")
		return b.String()
	}
	b.WriteString("<div class=\"triage-detail-actions\">")
	b.WriteString(renderFullErrorDetails(row.FullErrorSamples, opts.FullErrorsSummaryLabel))
	b.WriteString(renderContributingTestsDetails(row.ContributingTests, opts.ContributingSummaryLabel))
	b.WriteString(renderAffectedRunsDetails(row.References, opts))
	b.WriteString("</div>")
	b.WriteString("</td></tr>\n")
	return b.String()
}

func renderLinkedChildrenDetails(children []SignatureRow, opts TableOptions) string {
	if len(children) == 0 {
		return "<span class=\"muted\">No linked signatures.</span>"
	}
	ordered := append([]SignatureRow(nil), children...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].SupportCount != ordered[j].SupportCount {
			return ordered[i].SupportCount > ordered[j].SupportCount
		}
		if ordered[i].PostGoodCount != ordered[j].PostGoodCount {
			return ordered[i].PostGoodCount > ordered[j].PostGoodCount
		}
		if strings.TrimSpace(ordered[i].Phrase) != strings.TrimSpace(ordered[j].Phrase) {
			return strings.TrimSpace(ordered[i].Phrase) < strings.TrimSpace(ordered[j].Phrase)
		}
		return strings.TrimSpace(ordered[i].ClusterID) < strings.TrimSpace(ordered[j].ClusterID)
	})
	var b strings.Builder
	b.WriteString(fmt.Sprintf("<details class=\"linked-signatures-toggle\"><summary>Linked signatures (%d)</summary>", len(ordered)))
	b.WriteString("<div class=\"triage-linked-list\">")
	for index, child := range ordered {
		phrase := strings.TrimSpace(child.Phrase)
		if phrase == "" {
			phrase = "(unknown evidence)"
		}
		jobsAffected := affectedJobCount(child)
		badPRScore, badPRReasons := BadPRScoreAndReasons(child)
		badPRDetails := fmt.Sprintf("bad PR score: %d/3", badPRScore)
		if len(badPRReasons) > 0 {
			badPRDetails = fmt.Sprintf("%s (%s)", badPRDetails, strings.Join(badPRReasons, "; "))
		}
		flakeScore, flakeReasons := FlakeScoreAndReasons(child)
		flakeDetails := fmt.Sprintf("flake score: %d/14", flakeScore)
		if len(flakeReasons) > 0 {
			flakeDetails = fmt.Sprintf("%s (%s)", flakeDetails, strings.Join(flakeReasons, "; "))
		}
		b.WriteString("<details class=\"linked-child-toggle triage-linked-item\">")
		b.WriteString("<summary>")
		if opts.ShowLinkedChildRemove {
			selectionValue := strings.TrimSpace(child.SelectionValue)
			if selectionValue == "" {
				selectionValue = strings.TrimSpace(child.ClusterID)
			}
			if selectionValue != "" {
				b.WriteString(fmt.Sprintf(
					"<button class=\"triage-linked-item-remove\" type=\"submit\" name=\"unlink_child\" value=\"%s\" title=\"Remove this signature from the linked cluster\" aria-label=\"Remove this signature from the linked cluster\" onclick=\"event.stopPropagation();\">-</button>",
					html.EscapeString(selectionValue),
				))
			}
		}
		b.WriteString(fmt.Sprintf(
			"<span class=\"triage-linked-item-summary\"><strong>%d.</strong> %s</span><span class=\"triage-linked-item-meta\">jobs affected: %d</span>",
			index+1,
			html.EscapeString(cleanInline(phrase, 220)),
			jobsAffected,
		))
		b.WriteString("</summary>")
		b.WriteString(fmt.Sprintf("<div class=\"muted\">%s</div>", html.EscapeString(badPRDetails)))
		b.WriteString(fmt.Sprintf("<div class=\"muted\">%s</div>", html.EscapeString(flakeDetails)))
		if opts.ShowLinkedChildQuality || opts.ShowLinkedChildReview {
			b.WriteString("<div class=\"triage-linked-item-flags\">")
			if opts.ShowLinkedChildQuality {
				b.WriteString(renderQualityBadges(child.QualityNoteLabels))
			}
			if opts.ShowLinkedChildReview {
				b.WriteString(renderReviewBadges(child.ReviewNoteLabels))
			}
			b.WriteString("</div>")
		}
		b.WriteString("<div class=\"triage-detail-actions\">")
		b.WriteString(renderFullErrorDetails(child.FullErrorSamples, opts.FullErrorsSummaryLabel))
		b.WriteString(renderContributingTestsDetails(child.ContributingTests, opts.ContributingSummaryLabel))
		b.WriteString(renderAffectedRunsDetails(child.References, opts))
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
		lane := strings.TrimSpace(item.Lane)
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
		b.WriteString(fmt.Sprintf("<td>%d</td>", item.SupportCount))
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
	default:
		return 1
	}
}

func phraseLooksLikeStructFragment(input string) bool {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return false
	}
	if strings.Contains(trimmed, "{") && strings.Contains(trimmed, "}") && strings.Contains(trimmed, ":") {
		return true
	}
	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		return true
	}
	if strings.Contains(trimmed, "map[") && strings.Contains(trimmed, "]") {
		return true
	}
	return false
}

func phraseMostlyPunctuation(input string) bool {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return false
	}
	total := 0
	punctuation := 0
	for _, r := range trimmed {
		if unicode.IsSpace(r) {
			continue
		}
		total++
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			punctuation++
		}
	}
	if total == 0 {
		return false
	}
	return float64(punctuation)/float64(total) >= 0.6
}

func isGenericFailurePhrase(input string) bool {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "failure", "failed", "error", "unknown error", "test failed":
		return true
	default:
		return false
	}
}

func containsDeserializationNoOutputSignal(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return false
	}
	return strings.Contains(normalized, "deserializaion error: no output from command") ||
		strings.Contains(normalized, "deserialization error: no output from command")
}

func referenceIsNewer(candidate RunReference, existing RunReference) bool {
	candidateTime, candidateHasTime := ParseReferenceTimestamp(candidate.OccurredAt)
	existingTime, existingHasTime := ParseReferenceTimestamp(existing.OccurredAt)
	switch {
	case candidateHasTime && existingHasTime && !candidateTime.Equal(existingTime):
		return candidateTime.After(existingTime)
	case candidateHasTime != existingHasTime:
		return candidateHasTime
	}
	return len(strings.TrimSpace(candidate.OccurredAt)) > len(strings.TrimSpace(existing.OccurredAt))
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
