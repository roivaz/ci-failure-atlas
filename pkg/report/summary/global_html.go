package summary

import (
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	"ci-failure-atlas/pkg/report/triagehtml"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	sourceoptions "ci-failure-atlas/pkg/source/options"
)

var triageEnvironmentOrder = sourceoptions.SupportedEnvironments()

const (
	triageTrendWindowDays = 7
	triageLoadedRowsLimit = 50
)

func buildTriageReportHTML(
	triageClusters []triageCluster,
	top int,
	minPercent float64,
	generatedAt time.Time,
	targetEnvironments []string,
	overallJobsByEnvironment map[string]int,
	configuredWindowStart string,
	configuredWindowEnd string,
	historyResolver semhistory.GlobalSignatureResolver,
	chrome triagehtml.ReportChromeOptions,
) string {
	byEnvironment := map[string][]triageCluster{}
	totalSupportByEnvironment := map[string]int{}
	phraseEnvironments := map[string]map[string]struct{}{}
	totalSupport := 0
	windowStart, windowEnd, hasWindow := resolvedTriageWindow(triageClusters, configuredWindowStart, configuredWindowEnd)
	trendAnchor := generatedAt.UTC()
	if trendAnchor.IsZero() {
		trendAnchor = time.Now().UTC()
	}
	if hasWindow {
		trendAnchor = windowEnd
	}

	for _, row := range triageClusters {
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
	b.WriteString("  <title>CI Signature Triage Report</title>\n")
	b.WriteString(triagehtml.ThemeInitScriptTag())
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
	b.WriteString("    .muted { color: #6b7280; }\n")
	b.WriteString(triagehtml.ReportChromeCSS())
	b.WriteString(triagehtml.StylesCSS())
	b.WriteString(triagehtml.ThemeCSS())
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString(triagehtml.ReportChromeHTML(chrome))
	b.WriteString("  <h1>CI Signature Triage Report</h1>\n")
	if hasWindow {
		windowDays := inclusiveWindowDays(windowStart, windowEnd)
		b.WriteString(fmt.Sprintf(
			"  <p class=\"meta\">Window (UTC): <strong>%s</strong> to <strong>%s</strong> (%d days)</p>\n",
			html.EscapeString(windowStart.Format("2006-01-02")),
			html.EscapeString(windowEnd.Format("2006-01-02")),
			windowDays,
		))
	}
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Generated (UTC): <strong>%s</strong></p>\n", html.EscapeString(generatedAt.Format(time.RFC3339))))
	b.WriteString("  <p class=\"meta\">Failure signatures grouped by environment for engineering triage. Includes lightweight quality scoring and cross-environment overlap hints.</p>\n")
	b.WriteString("  <div class=\"cards\">\n")
	b.WriteString(triageCardHTML("Environments in scope", fmt.Sprintf("%d", len(environments))))
	b.WriteString(triageCardHTML("Signatures in triage", fmt.Sprintf("%d", len(triageClusters))))
	b.WriteString(triageCardHTML("Total signature support", fmt.Sprintf("%d", totalSupport)))
	b.WriteString(triageCardHTML("Triage threshold", fmt.Sprintf("visible %d, loaded %d, min %.2f%%", top, triageLoadedRowsLimit, minPercent)))
	b.WriteString("  </div>\n")

	for _, environment := range environments {
		clusters := append([]triageCluster(nil), byEnvironment[environment]...)
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
		filtered := make([]triageCluster, 0, len(clusters))
		for _, row := range clusters {
			share := pct(row.SupportCount, totalEnvironmentSupport)
			if minPercent > 0 && share < minPercent {
				continue
			}
			filtered = append(filtered, row)
		}
		b.WriteString(fmt.Sprintf("  <section id=\"%s\" class=\"section\">\n", html.EscapeString(triageEnvironmentSectionID(environment))))
		b.WriteString(fmt.Sprintf("    <h2>Environment: %s</h2>\n", html.EscapeString(strings.ToUpper(environment))))
		if len(filtered) == 0 {
			b.WriteString(fmt.Sprintf("    <p class=\"section-note\">Rows shown: 0 / %d signatures &middot; support sum: %d</p>\n", len(clusters), totalEnvironmentSupport))
			b.WriteString("    <p class=\"muted\">No signatures matched the configured threshold in this environment.</p>\n")
			b.WriteString("  </section>\n")
			continue
		}
		triageRows := make([]triagehtml.SignatureRow, 0, len(filtered))
		for _, row := range filtered {
			phrase := strings.TrimSpace(row.CanonicalEvidencePhrase)
			if phrase == "" {
				phrase = "(unknown evidence)"
			}
			otherEnvironments := alsoSeenInOtherEnvironments(phraseEnvironments[phrase], environment)
			qualityCodes := triageQualityIssueCodes(phrase)
			qualityLabels := make([]string, 0, len(qualityCodes))
			for _, code := range qualityCodes {
				qualityLabels = append(qualityLabels, triageQualityIssueLabel(code))
			}
			runReferences := toTriageRunReferences(row.References)
			triageRow := triagehtml.SignatureRow{
				Environment:       environment,
				Phrase:            phrase,
				ClusterID:         strings.TrimSpace(row.Phase2ClusterID),
				SearchQuery:       strings.TrimSpace(row.SearchQueryPhrase),
				SupportCount:      row.SupportCount,
				SupportShare:      pct(row.SupportCount, totalEnvironmentSupport),
				PostGoodCount:     row.PostGoodCommitCount,
				AlsoSeenIn:        append([]string(nil), otherEnvironments...),
				QualityScore:      triageQualityScore(qualityCodes),
				QualityNoteLabels: qualityLabels,
				ContributingTests: toTriageContributingTests(row.ContributingTests),
				FullErrorSamples:  append([]string(nil), row.FullErrorSamples...),
				References:        runReferences,
			}
			triageRow.LinkedChildren = toLinkedChildSignatureRows(
				row.LinkedChildren,
				totalEnvironmentSupport,
				phraseEnvironments,
				environment,
			)
			if historyResolver != nil {
				presence := semhistory.SignaturePresence{}
				if len(triageRow.LinkedChildren) > 0 {
					presence = historyResolver.PresenceForPhase3Cluster(environment, strings.TrimSpace(row.Phase2ClusterID))
				} else {
					presence = historyResolver.PresenceFor(semhistory.SignatureKey{
						Environment: environment,
						Phrase:      phrase,
						SearchQuery: row.SearchQueryPhrase,
					})
				}
				triageRow.PriorWeeksPresent = presence.PriorWeeksPresent
				triageRow.PriorWeekStarts = append([]string(nil), presence.PriorWeekStarts...)
				triageRow.PriorJobsAffected = presence.PriorJobsAffected
				if !presence.PriorLastSeenAt.IsZero() {
					triageRow.PriorLastSeenAt = presence.PriorLastSeenAt.UTC().Format(time.RFC3339)
				}
			}
			if sparkline, counts, sparkRange, ok := triagehtml.DailyDensitySparkline(runReferences, triageTrendWindowDays, trendAnchor); ok {
				triageRow.TrendSparkline = sparkline
				triageRow.TrendCounts = append([]int(nil), counts...)
				triageRow.TrendRange = sparkRange
			}
			triageRows = append(triageRows, triageRow)
		}
		loadedRows := len(triageRows)
		if loadedRows > triageLoadedRowsLimit {
			loadedRows = triageLoadedRowsLimit
		}
		initialVisible := top
		if initialVisible < 0 {
			initialVisible = 0
		}
		if initialVisible > loadedRows {
			initialVisible = loadedRows
		}
		b.WriteString(fmt.Sprintf("    <p class=\"section-note\">Rows loaded: %d / %d signatures &middot; initially visible: %d &middot; support sum: %d</p>\n", loadedRows, len(clusters), initialVisible, totalEnvironmentSupport))
		b.WriteString(triagehtml.RenderTable(triageRows, triagehtml.TableOptions{
			IncludeTrend:       true,
			GitHubRepoOwner:    sourceoptions.DefaultGitHubRepoOwner(),
			GitHubRepoName:     sourceoptions.DefaultGitHubRepoName(),
			ImpactTotalJobs:    overallJobsByEnvironment[environment],
			LoadedRowsLimit:    triageLoadedRowsLimit,
			InitialVisibleRows: top,
		}))
		b.WriteString("  </section>\n")
	}

	b.WriteString(triagehtml.ThemeToggleScriptTag())
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

func orderedTriageEnvironments(byEnvironment map[string][]triageCluster, targetEnvironments []string) []string {
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

func toTriageContributingTests(items []triageContributingTest) []triagehtml.ContributingTest {
	out := make([]triagehtml.ContributingTest, 0, len(items))
	for _, item := range items {
		out = append(out, triagehtml.ContributingTest{
			Lane:         strings.TrimSpace(item.Lane),
			JobName:      strings.TrimSpace(item.JobName),
			TestName:     strings.TrimSpace(item.TestName),
			SupportCount: item.SupportCount,
		})
	}
	return out
}

func toTriageRunReferences(rows []triageReference) []triagehtml.RunReference {
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

func toLinkedChildSignatureRows(
	children []triageCluster,
	totalEnvironmentSupport int,
	phraseEnvironments map[string]map[string]struct{},
	environment string,
) []triagehtml.SignatureRow {
	if len(children) == 0 {
		return nil
	}
	out := make([]triagehtml.SignatureRow, 0, len(children))
	for _, child := range children {
		phrase := strings.TrimSpace(child.CanonicalEvidencePhrase)
		if phrase == "" {
			phrase = "(unknown evidence)"
		}
		qualityCodes := triageQualityIssueCodes(phrase)
		qualityLabels := make([]string, 0, len(qualityCodes))
		for _, code := range qualityCodes {
			qualityLabels = append(qualityLabels, triageQualityIssueLabel(code))
		}
		out = append(out, triagehtml.SignatureRow{
			Environment:       normalizeReportEnvironment(child.Environment),
			Phrase:            phrase,
			ClusterID:         strings.TrimSpace(child.Phase2ClusterID),
			SearchQuery:       strings.TrimSpace(child.SearchQueryPhrase),
			SupportCount:      child.SupportCount,
			SupportShare:      pct(child.SupportCount, totalEnvironmentSupport),
			PostGoodCount:     child.PostGoodCommitCount,
			AlsoSeenIn:        alsoSeenInOtherEnvironments(phraseEnvironments[phrase], environment),
			QualityScore:      triageQualityScore(qualityCodes),
			QualityNoteLabels: qualityLabels,
			ContributingTests: toTriageContributingTests(child.ContributingTests),
			FullErrorSamples:  append([]string(nil), child.FullErrorSamples...),
			References:        toTriageRunReferences(child.References),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SupportCount != out[j].SupportCount {
			return out[i].SupportCount > out[j].SupportCount
		}
		if out[i].PostGoodCount != out[j].PostGoodCount {
			return out[i].PostGoodCount > out[j].PostGoodCount
		}
		if strings.TrimSpace(out[i].Phrase) != strings.TrimSpace(out[j].Phrase) {
			return strings.TrimSpace(out[i].Phrase) < strings.TrimSpace(out[j].Phrase)
		}
		return strings.TrimSpace(out[i].ClusterID) < strings.TrimSpace(out[j].ClusterID)
	})
	return out
}

func resolvedTriageWindow(rows []triageCluster, configuredStart string, configuredEnd string) (time.Time, time.Time, bool) {
	if strings.TrimSpace(configuredStart) != "" && strings.TrimSpace(configuredEnd) != "" {
		start, end, ok := configuredReportWindowDisplayBounds(configuredStart, configuredEnd)
		if ok {
			return start, end, true
		}
	}
	return observedTriageWindow(rows)
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

func observedTriageWindow(rows []triageCluster) (time.Time, time.Time, bool) {
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

func resolveGitHubPRURLFromProwRun(runURL string, prNumber int) (string, bool) {
	return triagehtml.ResolveGitHubPRURLFromProwRun(
		runURL,
		prNumber,
		sourceoptions.DefaultGitHubRepoOwner(),
		sourceoptions.DefaultGitHubRepoName(),
	)
}

func parseReferenceTimestamp(value string) (time.Time, bool) {
	return triagehtml.ParseReferenceTimestamp(value)
}

func triageQualityIssueCodes(phrase string) []string {
	return triagehtml.QualityIssueCodes(phrase)
}

func triageQualityScore(issueCodes []string) int {
	return triagehtml.QualityScore(issueCodes)
}

func triageQualityIssueLabel(code string) string {
	return triagehtml.QualityIssueLabel(code)
}

func triageEnvironmentSectionID(environment string) string {
	normalized := normalizeReportEnvironment(environment)
	if normalized == "" {
		return "env-unknown"
	}
	return "env-" + normalized
}
