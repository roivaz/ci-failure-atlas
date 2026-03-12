package triagehtml

import (
	"fmt"
	"html"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	DefaultGitHubRepoOwner = "Azure"
	DefaultGitHubRepoName  = "ARO-HCP"
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
	Phrase            string
	ClusterID         string
	SearchQuery       string
	SupportCount      int
	TrendSparkline    string
	TrendCounts       []int
	TrendRange        string
	SupportShare      float64
	PostGoodCount     int
	AlsoSeenIn        []string
	QualityScore      int
	QualityNoteLabels []string
	ContributingTests []ContributingTest
	FullErrorSamples  []string
	References        []RunReference
}

type TableOptions struct {
	IncludeQualityNotes      bool
	IncludeTrend             bool
	TrendHeaderLabel         string
	GitHubRepoOwner          string
	GitHubRepoName           string
	FullErrorsSummaryLabel   string
	ContributingSummaryLabel string
	AffectedRunsSummaryLabel string
}

func StylesCSS() string {
	return strings.Join([]string{
		"    .triage-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0 12px; }",
		"    .triage-table th, .triage-table td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; vertical-align: top; }",
		"    .triage-table th { background: #f3f4f6; color: #374151; font-weight: 700; }",
		"    .triage-table tr.triage-errors-row td { background: #eff6ff; }",
		"    .badge { display: inline-block; border-radius: 999px; padding: 2px 8px; font-size: 11px; margin: 1px 2px 1px 0; }",
		"    .badge-quality { background: #fee2e2; color: #991b1b; }",
		"    .badge-ok { background: #dcfce7; color: #166534; }",
		"    .quality-high { color: #991b1b; font-weight: 700; }",
		"    .quality-low { color: #374151; }",
		"    details { margin: 2px 0; }",
		"    details summary { cursor: pointer; color: #1d4ed8; }",
		"    details.signature-toggle > summary { font-size: 13px; font-weight: 700; color: #111827; }",
		"    .triage-errors-row .triage-detail-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-start; }",
		"    .triage-errors-row details.full-errors-toggle, .triage-errors-row details.affected-runs-toggle, .triage-errors-row details.contributing-tests-toggle { margin: 0; }",
		"    .triage-errors-row details.full-errors-toggle > summary, .triage-errors-row details.affected-runs-toggle > summary, .triage-errors-row details.contributing-tests-toggle > summary { display: inline-flex; align-items: center; gap: 6px; font-size: 9px; font-weight: 600; color: #1e3a8a; background: #dbeafe; border: 1px solid #93c5fd; border-radius: 999px; padding: 2px 10px; }",
		"    .triage-errors-row details.full-errors-toggle[open] > summary, .triage-errors-row details.affected-runs-toggle[open] > summary, .triage-errors-row details.contributing-tests-toggle[open] > summary { background: #bfdbfe; border-color: #60a5fa; color: #1e40af; }",
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

func RenderTable(rows []SignatureRow, options TableOptions) string {
	opts := normalizedOptions(options)
	headers := []string{"Signature", "Support"}
	if opts.IncludeTrend {
		headers = append(headers, opts.TrendHeaderLabel)
	}
	headers = append(headers, "Share", "Post-good", "Also seen in", "Quality score")
	if opts.IncludeQualityNotes {
		headers = append(headers, "Quality notes")
	}

	var b strings.Builder
	b.WriteString("    <table class=\"triage-table\">\n")
	b.WriteString("      <thead><tr>")
	for _, header := range headers {
		b.WriteString(fmt.Sprintf("<th>%s</th>", html.EscapeString(header)))
	}
	b.WriteString("</tr></thead>\n")
	b.WriteString("      <tbody>\n")
	colSpan := len(headers)
	for _, row := range rows {
		b.WriteString(renderMainRow(row, opts))
		b.WriteString(renderDetailRow(row, colSpan, opts))
	}
	b.WriteString("      </tbody>\n")
	b.WriteString("    </table>\n")
	return b.String()
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
		label = parsed.UTC().Format("2006-01-02 15:04Z")
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

	dateRange := fmt.Sprintf("%s..%s", startDay.Format("2006-01-02"), endDay.Format("2006-01-02"))
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

func normalizedOptions(options TableOptions) TableOptions {
	opts := options
	if strings.TrimSpace(opts.GitHubRepoOwner) == "" {
		opts.GitHubRepoOwner = DefaultGitHubRepoOwner
	}
	if strings.TrimSpace(opts.GitHubRepoName) == "" {
		opts.GitHubRepoName = DefaultGitHubRepoName
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
	return opts
}

func renderMainRow(row SignatureRow, opts TableOptions) string {
	var b strings.Builder
	phrase := strings.TrimSpace(row.Phrase)
	if phrase == "" {
		phrase = "(unknown evidence)"
	}
	otherEnvironments := "none"
	if len(row.AlsoSeenIn) > 0 {
		otherEnvironments = strings.Join(row.AlsoSeenIn, ", ")
	}
	clusterID := cleanInline(strings.TrimSpace(row.ClusterID), 180)
	if clusterID == "" {
		clusterID = "n/a"
	}
	searchQuery := cleanInline(strings.TrimSpace(row.SearchQuery), 180)
	if searchQuery == "" {
		searchQuery = "n/a"
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
	b.WriteString("        <tr>")
	b.WriteString(fmt.Sprintf("<td><details class=\"signature-toggle\"><summary>%s</summary><div class=\"muted\">full signature:</div><pre>%s</pre><div class=\"muted\">cluster: %s</div><div class=\"muted\">query: %s</div></details></td>",
		html.EscapeString(cleanInline(phrase, 180)),
		html.EscapeString(phrase),
		html.EscapeString(clusterID),
		html.EscapeString(searchQuery),
	))
	b.WriteString(fmt.Sprintf("<td>%d</td>", row.SupportCount))
	if opts.IncludeTrend {
		if row.TrendSparkline != "" {
			b.WriteString(fmt.Sprintf(
				"<td title=\"%s (%s)\">%s</td>",
				html.EscapeString(FormatCounts(row.TrendCounts)),
				html.EscapeString(row.TrendRange),
				html.EscapeString(row.TrendSparkline),
			))
		} else {
			b.WriteString("<td>n/a</td>")
		}
	}
	b.WriteString(fmt.Sprintf("<td>%.2f%%</td>", row.SupportShare))
	b.WriteString(fmt.Sprintf("<td>%d</td>", row.PostGoodCount))
	b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(otherEnvironments)))
	b.WriteString(fmt.Sprintf("<td><span class=\"%s\">%d</span></td>", qualityClass, row.QualityScore))
	if opts.IncludeQualityNotes {
		b.WriteString(fmt.Sprintf("<td>%s</td>", qualityNotes))
	}
	b.WriteString("</tr>\n")
	return b.String()
}

func renderDetailRow(row SignatureRow, colSpan int, opts TableOptions) string {
	if colSpan <= 0 {
		colSpan = 1
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("        <tr class=\"triage-errors-row\"><td colspan=\"%d\">", colSpan))
	b.WriteString("<div class=\"triage-detail-actions\">")
	b.WriteString(renderFullErrorDetails(row.FullErrorSamples, opts.FullErrorsSummaryLabel))
	b.WriteString(renderContributingTestsDetails(row.ContributingTests, opts.ContributingSummaryLabel))
	b.WriteString(renderAffectedRunsDetails(row.References, opts))
	b.WriteString("</div>")
	b.WriteString("</td></tr>\n")
	return b.String()
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
	b.WriteString("<div class=\"runs-scroll\"><table class=\"runs-table\"><thead><tr><th>Date</th><th>Associated PR</th><th>Prow job</th></tr></thead><tbody>")
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
