package runlog

import (
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	frontservice "ci-failure-atlas/pkg/frontend/readmodel"
	frontui "ci-failure-atlas/pkg/frontend/ui"
	sourceoptions "ci-failure-atlas/pkg/source/options"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

type PageOptions struct {
	Chrome              frontui.ReportChromeOptions
	Query               frontservice.RunLogDayQuery
	FailurePatternsHref string
	APIHref             string
}

func RenderHTML(
	data frontservice.RunLogDayData,
	options PageOptions,
) string {
	totalRuns := 0
	totalFailedRuns := 0
	for _, environment := range data.Environments {
		totalRuns += environment.Summary.TotalRuns
		totalFailedRuns += environment.Summary.FailedRuns
	}

	var b strings.Builder
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Runs</title>\n")
	b.WriteString(frontui.ThemeInitScriptTag())
	b.WriteString("  <style>\n")
	b.WriteString("    body { font-family: Arial, sans-serif; margin: 20px; color: #1f2937; }\n")
	b.WriteString("    h1 { margin-bottom: 6px; }\n")
	b.WriteString("    h2 { margin-top: 22px; margin-bottom: 8px; }\n")
	b.WriteString("    .meta { color: #4b5563; margin-bottom: 8px; }\n")
	b.WriteString("    .cards { display: flex; flex-wrap: wrap; gap: 10px; margin: 14px 0 18px; }\n")
	b.WriteString("    .card { border: 1px solid #e5e7eb; border-radius: 8px; background: #f9fafb; padding: 10px 12px; min-width: 180px; }\n")
	b.WriteString("    .label { font-size: 12px; color: #6b7280; margin-bottom: 3px; }\n")
	b.WriteString("    .value { font-size: 20px; font-weight: 700; }\n")
	b.WriteString("    .section { border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; margin: 14px 0; }\n")
	b.WriteString("    .section-note { color: #4b5563; font-size: 12px; margin-top: -4px; margin-bottom: 8px; }\n")
	b.WriteString("    .muted { color: #6b7280; }\n")
	b.WriteString("    .page-actions { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin: 12px 0 18px; }\n")
	b.WriteString("    .action-btn { display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; padding: 8px 14px; font-size: 13px; font-weight: 600; text-decoration: none; }\n")
	b.WriteString("    .action-primary { border: 1px solid #111827; background: #111827; color: #ffffff; }\n")
	b.WriteString("    .action-primary:hover { background: #1f2937; }\n")
	b.WriteString("    .action-secondary { border: 1px solid #d1d5db; background: #ffffff; color: #1f2937; }\n")
	b.WriteString("    .action-secondary:hover { background: #f3f4f6; }\n")
	b.WriteString("    .runs-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0 12px; }\n")
	b.WriteString("    .runs-table th, .runs-table td { border: 1px solid #e5e7eb; padding: 8px 9px; text-align: left; vertical-align: top; }\n")
	b.WriteString("    .runs-table th { background: #f3f4f6; color: #374151; font-weight: 700; }\n")
	b.WriteString("    .runs-table td.result-col, .runs-table td.time-col, .runs-table td.pr-col, .runs-table td.failed-tests-col { white-space: nowrap; }\n")
	b.WriteString("    .status-badge { display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; padding: 2px 8px; font-size: 11px; font-weight: 700; border: 1px solid transparent; }\n")
	b.WriteString("    .status-failed { background: #fee2e2; border-color: #fecaca; color: #991b1b; }\n")
	b.WriteString("    .status-passed { background: #dcfce7; border-color: #bbf7d0; color: #166534; }\n")
	b.WriteString("    .job-submeta, .phrase-submeta, .detail-meta { color: #6b7280; font-size: 11px; margin-top: 4px; }\n")
	b.WriteString("    .run-flags, .failure-flags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px; }\n")
	b.WriteString("    .mini-badge { display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; padding: 2px 7px; font-size: 10px; font-weight: 700; background: #eff6ff; color: #1e40af; border: 1px solid #bfdbfe; }\n")
	b.WriteString("    .bad-pr-flag { display: inline-flex; align-items: center; justify-content: center; margin-right: 6px; color: #dc2626; font-weight: 700; }\n")
	b.WriteString("    .detail-list { display: flex; flex-direction: column; gap: 8px; margin-top: 8px; }\n")
	b.WriteString("    .detail-item { border: 1px solid #e5e7eb; border-radius: 8px; background: #f9fafb; padding: 8px 10px; }\n")
	b.WriteString("    .detail-title { font-weight: 700; }\n")
	b.WriteString("    .job-link { font-weight: 700; }\n")
	b.WriteString("    details { margin-top: 8px; }\n")
	b.WriteString("    details summary { cursor: pointer; color: #1d4ed8; font-weight: 600; }\n")
	b.WriteString("    .raw-failure-toggle > summary { display: inline-flex; align-items: center; justify-content: center; border: 1px solid #d1d5db; border-radius: 999px; padding: 4px 10px; font-size: 11px; font-weight: 600; color: #1f2937; background: #ffffff; }\n")
	b.WriteString("    .raw-failure-toggle[open] > summary { background: #f3f4f6; }\n")
	b.WriteString("    pre { white-space: pre-wrap; word-break: break-word; background: #111827; color: #f9fafb; padding: 8px; border-radius: 6px; font-size: 11px; margin: 8px 0 0; }\n")
	b.WriteString(frontui.ReportChromeCSS())
	b.WriteString(frontui.ThemeCSS())
	b.WriteString("    :root[data-theme=\"dark\"] .meta, :root[data-theme=\"dark\"] .muted, :root[data-theme=\"dark\"] .label, :root[data-theme=\"dark\"] .section-note, :root[data-theme=\"dark\"] .job-submeta, :root[data-theme=\"dark\"] .phrase-submeta, :root[data-theme=\"dark\"] .detail-meta { color: #94a3b8; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .card, :root[data-theme=\"dark\"] .section, :root[data-theme=\"dark\"] .detail-item { background: #111827; border-color: #334155; color: #e2e8f0; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .runs-table th { background: #1f2937; color: #e2e8f0; border-color: #334155; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .runs-table td { border-color: #334155; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .action-primary { background: #2563eb; border-color: #2563eb; color: #e2e8f0; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .action-primary:hover { background: #1d4ed8; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .action-secondary { background: #1f2937; border-color: #334155; color: #e2e8f0; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .action-secondary:hover { background: #0f172a; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] details summary { color: #93c5fd; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .mini-badge { background: #1e293b; border-color: #334155; color: #93c5fd; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .raw-failure-toggle > summary { background: #1f2937; border-color: #334155; color: #e2e8f0; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .raw-failure-toggle[open] > summary { background: #0f172a; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] pre { background: #020617; color: #e2e8f0; border: 1px solid #334155; }\n")
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString(frontui.ReportChromeHTML(options.Chrome))
	b.WriteString("  <h1>CI Runs</h1>\n")
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Date (UTC): <strong>%s</strong></p>\n",
		html.EscapeString(strings.TrimSpace(data.Meta.Date)),
	))
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Environments: <strong>%s</strong></p>\n",
		html.EscapeString(runLogDayEnvironmentList(data.Meta.Environments)),
	))
	b.WriteString("  <p class=\"meta\">Semantic matches and bad-PR signals use the latest contributing stored semantic snapshot for the matched signature so the score stays stable even on a single-day slice. <span class=\"failure-patterns-header-help\" title=\"The page shows one UTC day of runs, but semantic attachments and bad-PR scoring come from the latest contributing stored semantic snapshot for each matched signature rather than being recomputed from the day in isolation.\">?</span></p>\n")
	b.WriteString(runLogDayActionsHTML(options))
	b.WriteString("  <div class=\"cards\">\n")
	b.WriteString(runLogDayCardHTML("Environments in scope", fmt.Sprintf("%d", len(data.Environments))))
	b.WriteString(runLogDayCardHTML("Runs", fmt.Sprintf("%d", totalRuns)))
	b.WriteString(runLogDayCardHTML("Failed runs", fmt.Sprintf("%d", totalFailedRuns)))
	b.WriteString("  </div>\n")

	for _, environment := range data.Environments {
		b.WriteString(fmt.Sprintf("  <section id=\"runs-%s\" class=\"section\">\n", html.EscapeString(strings.TrimSpace(environment.Environment))))
		b.WriteString(fmt.Sprintf("    <h2>Environment: %s</h2>\n", html.EscapeString(strings.ToUpper(strings.TrimSpace(environment.Environment)))))
		if len(environment.Runs) == 0 {
			b.WriteString("    <p class=\"muted\">No runs were recorded for this environment on the selected day.</p>\n")
			b.WriteString("  </section>\n")
			continue
		}
		b.WriteString("    <table class=\"runs-table\">\n")
		b.WriteString("      <thead><tr><th>Time (UTC)</th><th>Job</th><th>Failed at</th><th>Result</th><th>PR</th><th>Failed tests</th><th>Details</th></tr></thead>\n")
		b.WriteString("      <tbody>\n")
		for _, row := range environment.Runs {
			b.WriteString(runLogDayRunRowHTML(row))
		}
		b.WriteString("      </tbody>\n")
		b.WriteString("    </table>\n")
		b.WriteString("  </section>\n")
	}

	b.WriteString(frontui.ThemeToggleScriptTag())
	b.WriteString("</body>\n")
	b.WriteString("</html>\n")
	return b.String()
}

func runLogDayActionsHTML(options PageOptions) string {
	var b strings.Builder
	b.WriteString("  <div class=\"page-actions\">\n")
	if href := strings.TrimSpace(options.FailurePatternsHref); href != "" {
		b.WriteString(fmt.Sprintf(
			"    <a class=\"action-btn action-primary\" href=\"%s\">Open failure patterns for this day</a>\n",
			html.EscapeString(href),
		))
	}
	if href := strings.TrimSpace(options.APIHref); href != "" {
		b.WriteString(fmt.Sprintf(
			"    <a class=\"action-btn action-secondary\" href=\"%s\">View JSON API</a>\n",
			html.EscapeString(href),
		))
	}
	b.WriteString("  </div>\n")
	return b.String()
}

func runLogDayCardHTML(label string, value string) string {
	return fmt.Sprintf(
		"    <div class=\"card\"><div class=\"label\">%s</div><div class=\"value\">%s</div></div>\n",
		html.EscapeString(strings.TrimSpace(label)),
		html.EscapeString(strings.TrimSpace(value)),
	)
}

func runLogDayRunRowHTML(row frontservice.JobHistoryRunRow) string {
	var b strings.Builder
	b.WriteString("        <tr>\n")
	b.WriteString(fmt.Sprintf("          <td class=\"time-col\">%s</td>\n", html.EscapeString(runLogDayRunTime(row.Run.OccurredAt))))
	b.WriteString("          <td>")
	b.WriteString(runLogDayJobHTML(row.Run))
	if flagsHTML := runLogDayRunFlagsHTML(row.Run); flagsHTML != "" {
		b.WriteString(flagsHTML)
	}
	b.WriteString(fmt.Sprintf("<div class=\"job-submeta\">%s</div>", html.EscapeString(runLogDayRunSubmeta(row.Run))))
	b.WriteString("</td>\n")
	b.WriteString(fmt.Sprintf("          <td>%s</td>\n", html.EscapeString(runLogDayLaneSummary(row))))
	b.WriteString(fmt.Sprintf("          <td class=\"result-col\">%s</td>\n", runLogDayResultBadgeHTML(row.Run)))
	b.WriteString(fmt.Sprintf("          <td class=\"pr-col\">%s</td>\n", runLogDayPRHTML(row)))
	b.WriteString(fmt.Sprintf("          <td class=\"failed-tests-col\">%s</td>\n", html.EscapeString(runLogDayFailedTestsLabel(row))))
	b.WriteString("          <td>")
	b.WriteString(html.EscapeString(runLogDayPrimaryPhrase(row)))
	if submeta := runLogDayPrimaryPhraseSubmeta(row); submeta != "" {
		b.WriteString(fmt.Sprintf("<div class=\"phrase-submeta\">%s</div>", html.EscapeString(submeta)))
	}
	if detailsHTML := runLogDayFailureDetailsHTML(row); detailsHTML != "" {
		b.WriteString(detailsHTML)
	}
	b.WriteString("</td>\n")
	b.WriteString("        </tr>\n")
	return b.String()
}

func runLogDayRunTime(occurredAt string) string {
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(occurredAt))
	if err != nil {
		return strings.TrimSpace(occurredAt)
	}
	return parsed.UTC().Format("15:04:05 UTC")
}

func runLogDayJobLabel(run storecontracts.RunRecord) string {
	label := strings.TrimSpace(run.JobName)
	if label != "" {
		return label
	}
	return "unknown job"
}

func runLogDayJobHTML(run storecontracts.RunRecord) string {
	label := runLogDayJobLabel(run)
	if href := strings.TrimSpace(run.RunURL); href != "" {
		return fmt.Sprintf("<a class=\"job-link\" href=\"%s\">%s</a>", html.EscapeString(href), html.EscapeString(label))
	}
	return fmt.Sprintf("<span class=\"job-link\">%s</span>", html.EscapeString(label))
}

func runLogDayRunFlagsHTML(run storecontracts.RunRecord) string {
	flags := make([]string, 0, 2)
	if run.PostGoodCommit {
		flags = append(flags, "post-good")
	}
	if run.MergedPR {
		flags = append(flags, "merged PR")
	}
	if len(flags) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("<div class=\"run-flags\">")
	for _, flag := range flags {
		b.WriteString(fmt.Sprintf("<span class=\"mini-badge\">%s</span>", html.EscapeString(flag)))
	}
	b.WriteString("</div>")
	return b.String()
}

func runLogDayRunSubmeta(run storecontracts.RunRecord) string {
	parts := make([]string, 0, 2)
	if short := runLogDayShortSHA(run.PRSHA); short != "" {
		parts = append(parts, "head "+short)
	}
	if short := runLogDayShortSHA(run.FinalMergedSHA); short != "" {
		parts = append(parts, "merge "+short)
	}
	if len(parts) == 0 {
		return "No additional run metadata captured"
	}
	return strings.Join(parts, " · ")
}

func runLogDayResultBadgeHTML(run storecontracts.RunRecord) string {
	label := "passed"
	className := "status-badge status-passed"
	if run.Failed {
		label = "failed"
		className = "status-badge status-failed"
	}
	return fmt.Sprintf("<span class=\"%s\">%s</span>", className, html.EscapeString(label))
}

func runLogDayPRHTML(row frontservice.JobHistoryRunRow) string {
	run := row.Run
	if run.PRNumber <= 0 {
		return "<span class=\"muted\">n/a</span>"
	}
	label := fmt.Sprintf("#%d", run.PRNumber)
	if state := runLogDayPRStateLabel(run); state != "" {
		label += " (" + state + ")"
	}
	content := html.EscapeString(label)
	if href := runLogDayGitHubPRURL(run.PRNumber); href != "" {
		content = fmt.Sprintf("<a href=\"%s\">%s</a>", html.EscapeString(href), content)
	}
	if badPRFlag := runLogDayBadPRFlagHTML(row); badPRFlag != "" {
		return badPRFlag + content
	}
	return content
}

func runLogDayPRStateLabel(run storecontracts.RunRecord) string {
	if run.PRNumber <= 0 {
		return ""
	}
	if run.MergedPR {
		return "merged"
	}
	state := strings.ToLower(strings.TrimSpace(run.PRState))
	switch state {
	case "open", "closed", "merged":
		return state
	default:
		return ""
	}
}

func runLogDayBadPRFlagHTML(row frontservice.JobHistoryRunRow) string {
	if !runLogDayShouldShowBadPRFlag(row) {
		return ""
	}
	score, reasons := runLogDayBadPRScoreAndReasons(row)
	if score != 3 {
		return ""
	}
	tooltip := frontui.FormatBadPRTooltip(reasons)
	_ = score
	return fmt.Sprintf(
		"<span class=\"bad-pr-flag\" title=\"%s\" aria-label=\"%s\">⚠</span>",
		html.EscapeString(tooltip),
		html.EscapeString(tooltip),
	)
}

func runLogDayShouldShowBadPRFlag(row frontservice.JobHistoryRunRow) bool {
	return row.Run.Failed && row.SemanticRollups.ClusteredRows > 0
}

func runLogDayBadPRScoreAndReasons(row frontservice.JobHistoryRunRow) (int, []string) {
	if row.BadPRScore <= 0 {
		return 0, nil
	}
	return row.BadPRScore, append([]string(nil), row.BadPRReasons...)
}

func runLogDayPrimaryPhrase(row frontservice.JobHistoryRunRow) string {
	if len(row.FailureRows) == 0 {
		if row.Run.Failed {
			return "Failure details unavailable"
		}
		return "n/a"
	}

	phrases := runLogDaySemanticPhrases(row)
	switch strings.TrimSpace(row.SemanticRollups.AttachmentSummary) {
	case "single_clustered":
		if len(phrases) > 0 {
			return phrases[0]
		}
	case "multiple_clustered", "mixed":
		return fmt.Sprintf("Multiple failures (%d)", row.FailedTestCount)
	case "unmatched_only":
		if len(row.FailureRows) == 1 {
			if text := strings.TrimSpace(row.FailureRows[0].FailureText); text != "" {
				return runLogDayPreviewText(text, 140)
			}
		}
		return fmt.Sprintf("Multiple failures (%d)", row.FailedTestCount)
	}
	if len(phrases) > 0 {
		return phrases[0]
	}
	return fmt.Sprintf("%d failure rows", len(row.FailureRows))
}

func runLogDayPrimaryPhraseSubmeta(row frontservice.JobHistoryRunRow) string {
	if len(row.FailureRows) == 0 {
		if row.Run.Failed {
			return "Failure details are not available for this run yet."
		}
		return ""
	}
	return ""
}

func runLogDayFailureDetailsHTML(row frontservice.JobHistoryRunRow) string {
	if len(row.FailureRows) == 0 {
		return ""
	}
	if runLogDayAllFailuresNonArtifactBacked(row.FailureRows) {
		return ""
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("<details><summary>Failure details (%d)</summary>", len(row.FailureRows)))
	b.WriteString("<div class=\"detail-list\">")
	for _, failure := range row.FailureRows {
		b.WriteString("<div class=\"detail-item\">")
		b.WriteString(fmt.Sprintf("<div class=\"detail-title\">%s</div>", html.EscapeString(runLogDayFailureTitle(failure))))
		b.WriteString(fmt.Sprintf("<div class=\"detail-meta\">%s</div>", html.EscapeString(runLogDayFailureMeta(failure))))
		if flags := runLogDayFailureFlagsHTML(failure); flags != "" {
			b.WriteString(flags)
		}
		b.WriteString(runLogDayRawFailureToggleHTML(failure))
		b.WriteString("</div>")
	}
	b.WriteString("</div></details>")
	return b.String()
}

func runLogDayAllFailuresNonArtifactBacked(rows []frontservice.JobHistoryFailureRow) bool {
	if len(rows) == 0 {
		return false
	}
	for _, row := range rows {
		if !row.NonArtifactBacked {
			return false
		}
	}
	return true
}

func runLogDayFailureTitle(row frontservice.JobHistoryFailureRow) string {
	if phrase := strings.TrimSpace(row.SemanticAttachment.CanonicalEvidencePhrase); phrase != "" {
		return phrase
	}
	if text := strings.TrimSpace(row.FailureText); text != "" {
		return runLogDayPreviewText(text, 140)
	}
	return "Failure detail"
}

func runLogDayFailureMeta(row frontservice.JobHistoryFailureRow) string {
	parts := make([]string, 0, 4)
	if occurredAt := runLogDayRunTime(row.OccurredAt); occurredAt != "" {
		parts = append(parts, occurredAt)
	}
	if lane := strings.TrimSpace(row.Lane); lane != "" {
		parts = append(parts, "failed at "+lane)
	}
	if testName := strings.TrimSpace(row.TestName); testName != "" {
		parts = append(parts, "test "+testName)
	}
	if testSuite := strings.TrimSpace(row.TestSuite); testSuite != "" {
		parts = append(parts, "suite "+testSuite)
	}
	return strings.Join(parts, " · ")
}

func runLogDayFailureFlagsHTML(row frontservice.JobHistoryFailureRow) string {
	flags := make([]string, 0, 2)
	if issueID := strings.TrimSpace(row.Phase3IssueID); issueID != "" {
		flags = append(flags, "phase3 "+issueID)
	}
	if row.NonArtifactBacked {
		flags = append(flags, "synthetic raw row")
	}
	if len(flags) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("<div class=\"failure-flags\">")
	for _, flag := range flags {
		b.WriteString(fmt.Sprintf("<span class=\"mini-badge\">%s</span>", html.EscapeString(flag)))
	}
	b.WriteString("</div>")
	return b.String()
}

func runLogDayRawFailureToggleHTML(row frontservice.JobHistoryFailureRow) string {
	text := strings.TrimSpace(row.FailureText)
	if text == "" {
		return ""
	}
	return fmt.Sprintf(
		"<details class=\"raw-failure-toggle\"><summary>Show raw failure</summary><pre>%s</pre></details>",
		html.EscapeString(text),
	)
}

func runLogDaySemanticPhrases(row frontservice.JobHistoryRunRow) []string {
	set := map[string]struct{}{}
	for _, failure := range row.FailureRows {
		phrase := strings.TrimSpace(failure.SemanticAttachment.CanonicalEvidencePhrase)
		if phrase == "" {
			continue
		}
		set[phrase] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for phrase := range set {
		out = append(out, phrase)
	}
	sort.Strings(out)
	return out
}

func runLogDayEnvironmentList(environments []string) string {
	normalized := normalizedQueryEnvironments(environments)
	if len(normalized) == 0 {
		return "none"
	}
	for i := range normalized {
		normalized[i] = strings.ToUpper(normalized[i])
	}
	return strings.Join(normalized, ", ")
}

func normalizedQueryEnvironments(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.ToLower(strings.TrimSpace(value))
		if trimmed == "" {
			continue
		}
		seen[trimmed] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func runLogDayLaneSummary(row frontservice.JobHistoryRunRow) string {
	if len(row.Lanes) == 0 {
		return "n/a"
	}
	return strings.Join(row.Lanes, ", ")
}

func runLogDayFailedTestsLabel(row frontservice.JobHistoryRunRow) string {
	if len(row.FailureRows) == 0 && row.Run.Failed {
		return "n/a"
	}
	return fmt.Sprintf("%d", row.FailedTestCount)
}

func runLogDayGitHubPRURL(prNumber int) string {
	if prNumber <= 0 {
		return ""
	}
	owner := strings.TrimSpace(sourceoptions.DefaultGitHubRepoOwner())
	repo := strings.TrimSpace(sourceoptions.DefaultGitHubRepoName())
	if owner == "" || repo == "" {
		return ""
	}
	return fmt.Sprintf("https://github.com/%s/%s/pull/%d", owner, repo, prNumber)
}

func runLogDayShortSHA(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if len(trimmed) <= 7 {
		return trimmed
	}
	return trimmed[:7]
}

func runLogDayPreviewText(value string, max int) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(value, "\n", " "), "\r", " "), "\t", " "))
	normalized = strings.Join(strings.Fields(normalized), " ")
	if max <= 0 || len([]rune(normalized)) <= max {
		return normalized
	}
	runes := []rune(normalized)
	return string(runes[:max-1]) + "..."
}
