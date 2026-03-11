package summary

import (
	"fmt"
	"html"
	"sort"
	"strings"
	"time"
	"unicode"
)

var triageEnvironmentOrder = []string{"dev", "int", "stg", "prod"}

func buildGlobalTriageHTML(
	globalClusters []globalCluster,
	top int,
	minPercent float64,
	generatedAt time.Time,
	targetEnvironments []string,
	configuredWindowStart string,
	configuredWindowEnd string,
) string {
	byEnvironment := map[string][]globalCluster{}
	totalSupportByEnvironment := map[string]int{}
	phraseEnvironments := map[string]map[string]struct{}{}
	totalSupport := 0
	windowStart, windowEnd, hasWindow := resolvedGlobalWindow(globalClusters, configuredWindowStart, configuredWindowEnd)

	for _, row := range globalClusters {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			environment = "unknown"
		}
		row.Environment = environment
		byEnvironment[environment] = append(byEnvironment[environment], row)

		supportCount := row.SupportCount
		if supportCount < 0 {
			supportCount = 0
		}
		totalSupportByEnvironment[environment] += supportCount
		totalSupport += supportCount

		phrase := strings.TrimSpace(row.CanonicalEvidencePhrase)
		if phrase == "" {
			phrase = "(unknown evidence)"
		}
		if _, ok := phraseEnvironments[phrase]; !ok {
			phraseEnvironments[phrase] = map[string]struct{}{}
		}
		phraseEnvironments[phrase][environment] = struct{}{}
	}

	environments := orderedTriageEnvironments(byEnvironment, targetEnvironments)
	var b strings.Builder
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Global Signature Triage Report</title>\n")
	b.WriteString("  <style>\n")
	b.WriteString("    body { font-family: Arial, sans-serif; margin: 20px; color: #1f2937; }\n")
	b.WriteString("    h1 { margin-bottom: 6px; }\n")
	b.WriteString("    h2 { margin-top: 22px; }\n")
	b.WriteString("    .meta { color: #4b5563; margin-bottom: 8px; }\n")
	b.WriteString("    .cards { display: flex; flex-wrap: wrap; gap: 10px; margin: 12px 0 18px; }\n")
	b.WriteString("    .card { border: 1px solid #e5e7eb; border-radius: 8px; background: #f9fafb; padding: 10px 12px; min-width: 180px; }\n")
	b.WriteString("    .label { font-size: 12px; color: #6b7280; margin-bottom: 3px; }\n")
	b.WriteString("    .value { font-size: 20px; font-weight: 700; }\n")
	b.WriteString("    .section { border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; margin: 14px 0; }\n")
	b.WriteString("    .section-note { color: #4b5563; font-size: 12px; margin-top: -4px; margin-bottom: 8px; }\n")
	b.WriteString("    .triage-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0 12px; }\n")
	b.WriteString("    .triage-table th, .triage-table td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; vertical-align: top; }\n")
	b.WriteString("    .triage-table th { background: #f3f4f6; color: #374151; font-weight: 700; }\n")
	b.WriteString("    .muted { color: #6b7280; }\n")
	b.WriteString("    .badge { display: inline-block; border-radius: 999px; padding: 2px 8px; font-size: 11px; margin: 1px 2px 1px 0; }\n")
	b.WriteString("    .badge-quality { background: #fee2e2; color: #991b1b; }\n")
	b.WriteString("    .badge-ok { background: #dcfce7; color: #166534; }\n")
	b.WriteString("    .quality-high { color: #991b1b; font-weight: 700; }\n")
	b.WriteString("    .quality-low { color: #374151; }\n")
	b.WriteString("    details { margin: 2px 0; }\n")
	b.WriteString("    details summary { cursor: pointer; color: #1d4ed8; }\n")
	b.WriteString("    pre { white-space: pre-wrap; word-break: break-word; background: #111827; color: #f9fafb; padding: 8px; border-radius: 6px; font-size: 11px; margin: 6px 0 0; }\n")
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString("  <h1>CI Global Signature Triage Report</h1>\n")
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
	b.WriteString("  <p class=\"meta\">Global signatures grouped by environment for engineering triage. Includes lightweight quality scoring and cross-environment overlap hints.</p>\n")
	b.WriteString("  <div class=\"cards\">\n")
	b.WriteString(triageCardHTML("Environments in scope", fmt.Sprintf("%d", len(environments))))
	b.WriteString(triageCardHTML("Global signatures", fmt.Sprintf("%d", len(globalClusters))))
	b.WriteString(triageCardHTML("Total signature support", fmt.Sprintf("%d", totalSupport)))
	b.WriteString(triageCardHTML("Triage threshold", fmt.Sprintf("top %d, min %.2f%%", top, minPercent)))
	b.WriteString("  </div>\n")

	for _, environment := range environments {
		clusters := append([]globalCluster(nil), byEnvironment[environment]...)
		sort.Slice(clusters, func(i, j int) bool {
			if clusters[i].SupportCount != clusters[j].SupportCount {
				return clusters[i].SupportCount > clusters[j].SupportCount
			}
			if clusters[i].PostGoodCommitCount != clusters[j].PostGoodCommitCount {
				return clusters[i].PostGoodCommitCount > clusters[j].PostGoodCommitCount
			}
			return clusters[i].Phase2ClusterID < clusters[j].Phase2ClusterID
		})

		totalEnvironmentSupport := totalSupportByEnvironment[environment]
		filtered := make([]globalCluster, 0, len(clusters))
		for _, row := range clusters {
			share := pct(row.SupportCount, totalEnvironmentSupport)
			if minPercent > 0 && share < minPercent {
				continue
			}
			filtered = append(filtered, row)
		}
		if top > 0 && len(filtered) > top {
			filtered = filtered[:top]
		}

		b.WriteString("  <section class=\"section\">\n")
		b.WriteString(fmt.Sprintf("    <h2>Environment: %s</h2>\n", html.EscapeString(strings.ToUpper(environment))))
		b.WriteString(fmt.Sprintf("    <p class=\"section-note\">Rows shown: %d / %d signatures &middot; support sum: %d</p>\n", len(filtered), len(clusters), totalEnvironmentSupport))
		if len(filtered) == 0 {
			b.WriteString("    <p class=\"muted\">No signatures matched the configured threshold in this environment.</p>\n")
			b.WriteString("  </section>\n")
			continue
		}
		b.WriteString("    <table class=\"triage-table\">\n")
		b.WriteString("      <thead><tr><th>Signature</th><th>Support</th><th>Share</th><th>Post-good</th><th>Also seen in</th><th>Quality score</th><th>Quality notes</th><th>Contributing tests</th><th>Full failure examples</th><th>Latest runs</th></tr></thead>\n")
		b.WriteString("      <tbody>\n")
		for _, row := range filtered {
			phrase := strings.TrimSpace(row.CanonicalEvidencePhrase)
			if phrase == "" {
				phrase = "(unknown evidence)"
			}
			otherEnvironments := alsoSeenInOtherEnvironments(phraseEnvironments[phrase], environment)
			alsoSeenCell := "none"
			if len(otherEnvironments) > 0 {
				alsoSeenCell = strings.Join(otherEnvironments, ", ")
			}

			qualityCodes := globalQualityIssueCodes(phrase)
			qualityScore := globalQualityScore(qualityCodes)
			qualityClass := "quality-low"
			if qualityScore >= 8 {
				qualityClass = "quality-high"
			}

			qualityNotes := "<span class=\"badge badge-ok\">ok</span>"
			if len(qualityCodes) > 0 {
				parts := make([]string, 0, len(qualityCodes))
				for _, code := range qualityCodes {
					parts = append(parts, fmt.Sprintf("<span class=\"badge badge-quality\">%s</span>", html.EscapeString(globalQualityIssueLabel(code))))
				}
				qualityNotes = strings.Join(parts, "")
			}

			contributingTests := sampleContributingTests(row.ContributingTests, 3)
			if contributingTests == "" {
				contributingTests = "n/a"
			}
			fullErrorExamples := renderGlobalFullErrorSamples(row.FullErrorSamples)
			latestRuns := renderLatestRunLinks(row.References, 3)

			b.WriteString("        <tr>")
			b.WriteString(fmt.Sprintf("<td><details><summary>%s</summary><div class=\"muted\">full signature:</div><pre>%s</pre><div class=\"muted\">cluster: %s</div><div class=\"muted\">query: %s</div></details></td>",
				html.EscapeString(cleanInline(phrase, 180)),
				html.EscapeString(phrase),
				html.EscapeString(strings.TrimSpace(row.Phase2ClusterID)),
				html.EscapeString(cleanInline(row.SearchQueryPhrase, 180)),
			))
			b.WriteString(fmt.Sprintf("<td>%d</td>", row.SupportCount))
			b.WriteString(fmt.Sprintf("<td>%.2f%%</td>", pct(row.SupportCount, totalEnvironmentSupport)))
			b.WriteString(fmt.Sprintf("<td>%d</td>", row.PostGoodCommitCount))
			b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(alsoSeenCell)))
			b.WriteString(fmt.Sprintf("<td><span class=\"%s\">%d</span></td>", qualityClass, qualityScore))
			b.WriteString(fmt.Sprintf("<td>%s</td>", qualityNotes))
			b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(contributingTests)))
			b.WriteString(fmt.Sprintf("<td>%s</td>", fullErrorExamples))
			b.WriteString(fmt.Sprintf("<td>%s</td>", latestRuns))
			b.WriteString("</tr>\n")
		}
		b.WriteString("      </tbody>\n")
		b.WriteString("    </table>\n")
		b.WriteString("  </section>\n")
	}

	b.WriteString("</body>\n")
	b.WriteString("</html>\n")
	return b.String()
}

func triageCardHTML(label string, value string) string {
	return fmt.Sprintf(
		"    <div class=\"card\"><div class=\"label\">%s</div><div class=\"value\">%s</div></div>\n",
		html.EscapeString(strings.TrimSpace(label)),
		html.EscapeString(strings.TrimSpace(value)),
	)
}

func orderedTriageEnvironments(byEnvironment map[string][]globalCluster, targetEnvironments []string) []string {
	present := map[string]struct{}{}
	for env := range byEnvironment {
		present[normalizeReportEnvironment(env)] = struct{}{}
	}

	normalizedTargets := normalizeReportEnvironments(targetEnvironments)
	if len(normalizedTargets) > 0 {
		return normalizedTargets
	}

	out := append([]string(nil), triageEnvironmentOrder...)
	extras := make([]string, 0, len(present))
	for env := range present {
		found := false
		for _, fixed := range triageEnvironmentOrder {
			if env == fixed {
				found = true
				break
			}
		}
		if !found {
			extras = append(extras, env)
		}
	}
	sort.Strings(extras)
	out = append(out, extras...)
	return out
}

func alsoSeenInOtherEnvironments(seenByEnvironment map[string]struct{}, currentEnvironment string) []string {
	if len(seenByEnvironment) == 0 {
		return nil
	}
	out := make([]string, 0, len(seenByEnvironment))
	for environment := range seenByEnvironment {
		normalized := normalizeReportEnvironment(environment)
		if normalized == "" || normalized == normalizeReportEnvironment(currentEnvironment) {
			continue
		}
		out = append(out, strings.ToUpper(normalized))
	}
	sort.Strings(out)
	return out
}

func renderGlobalFullErrorSamples(samples []string) string {
	if len(samples) == 0 {
		return "n/a"
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("<details><summary>Show %d full failures</summary>", len(samples)))
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

func resolvedGlobalWindow(rows []globalCluster, configuredStart string, configuredEnd string) (time.Time, time.Time, bool) {
	if strings.TrimSpace(configuredStart) != "" && strings.TrimSpace(configuredEnd) != "" {
		start, end, ok := configuredReportWindowDisplayBounds(configuredStart, configuredEnd)
		if ok {
			return start, end, true
		}
	}
	return observedGlobalWindow(rows)
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

func observedGlobalWindow(rows []globalCluster) (time.Time, time.Time, bool) {
	var minTS time.Time
	var maxTS time.Time
	for _, row := range rows {
		for _, ref := range row.References {
			ts, ok := parseReferenceTimestamp(ref.OccurredAt)
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

func renderLatestRunLinks(rows []reference, limit int) string {
	if len(rows) == 0 || limit <= 0 {
		return "n/a"
	}
	byRunURL := map[string]reference{}
	for _, row := range rows {
		runURL := strings.TrimSpace(row.RunURL)
		if runURL == "" {
			continue
		}
		existing, ok := byRunURL[runURL]
		if !ok || referenceIsNewer(row, existing) {
			byRunURL[runURL] = row
		}
	}
	deduped := make([]reference, 0, len(byRunURL))
	for _, row := range byRunURL {
		deduped = append(deduped, row)
	}
	sort.Slice(deduped, func(i, j int) bool {
		ti, okI := parseReferenceTimestamp(deduped[i].OccurredAt)
		tj, okJ := parseReferenceTimestamp(deduped[j].OccurredAt)
		switch {
		case okI && okJ && !ti.Equal(tj):
			return ti.After(tj)
		case okI != okJ:
			return okI
		}
		return strings.TrimSpace(deduped[i].RunURL) < strings.TrimSpace(deduped[j].RunURL)
	})
	if len(deduped) > limit {
		deduped = deduped[:limit]
	}
	parts := make([]string, 0, len(deduped))
	for _, row := range deduped {
		label := strings.TrimSpace(row.OccurredAt)
		if parsed, ok := parseReferenceTimestamp(row.OccurredAt); ok {
			label = parsed.UTC().Format("2006-01-02 15:04Z")
		}
		if label == "" {
			label = "run"
		}
		parts = append(parts, fmt.Sprintf(
			"<a href=\"%s\" target=\"_blank\" rel=\"noopener noreferrer\">%s</a>",
			html.EscapeString(strings.TrimSpace(row.RunURL)),
			html.EscapeString(label),
		))
	}
	if len(parts) == 0 {
		return "n/a"
	}
	return strings.Join(parts, " &middot; ")
}

func referenceIsNewer(candidate reference, existing reference) bool {
	candidateTime, candidateHasTime := parseReferenceTimestamp(candidate.OccurredAt)
	existingTime, existingHasTime := parseReferenceTimestamp(existing.OccurredAt)
	switch {
	case candidateHasTime && existingHasTime && !candidateTime.Equal(existingTime):
		return candidateTime.After(existingTime)
	case candidateHasTime != existingHasTime:
		return candidateHasTime
	}
	return len(strings.TrimSpace(candidate.OccurredAt)) > len(strings.TrimSpace(existing.OccurredAt))
}

func parseReferenceTimestamp(value string) (time.Time, bool) {
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

func globalQualityIssueCodes(phrase string) []string {
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
	if globalIsGenericFailurePhrase(trimmed) {
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
	if globalPhraseLooksLikeStructFragment(trimmed) {
		add("struct_fragment")
	}
	if globalPhraseMostlyPunctuation(trimmed) {
		add("mostly_punctuation")
	}
	if globalContainsDeserializationNoOutputSignal(trimmed) {
		add("source_deserialization_no_output")
	}
	out := make([]string, 0, len(set))
	for code := range set {
		out = append(out, code)
	}
	sort.Slice(out, func(i, j int) bool {
		if globalQualityIssueWeight(out[i]) != globalQualityIssueWeight(out[j]) {
			return globalQualityIssueWeight(out[i]) > globalQualityIssueWeight(out[j])
		}
		return out[i] < out[j]
	})
	return out
}

func globalQualityScore(issueCodes []string) int {
	score := 0
	for _, issue := range issueCodes {
		score += globalQualityIssueWeight(issue)
	}
	return score
}

func globalQualityIssueWeight(code string) int {
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

func globalQualityIssueLabel(code string) string {
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

func globalPhraseLooksLikeStructFragment(input string) bool {
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

func globalPhraseMostlyPunctuation(input string) bool {
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

func globalIsGenericFailurePhrase(input string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(input)), " "))
	switch normalized {
	case "failure", "failure occurred", "unknown failure":
		return true
	default:
		return false
	}
}

func globalContainsDeserializationNoOutputSignal(value string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
	if normalized == "" {
		return false
	}
	hasDeserialization := strings.Contains(normalized, "deserializaion error") || strings.Contains(normalized, "deserialization error")
	return hasDeserialization && strings.Contains(normalized, "no output from command")
}
