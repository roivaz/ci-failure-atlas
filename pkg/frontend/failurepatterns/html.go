package failurepatterns

import (
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	frontservice "ci-failure-atlas/pkg/frontend/readmodel"
	frontui "ci-failure-atlas/pkg/frontend/ui"
	sourceoptions "ci-failure-atlas/pkg/source/options"
)

const (
	failurePatternsLoadedRowsLimit    = 50
	failurePatternsInitialVisibleRows = 25
)

type PageOptions struct {
	Chrome frontui.ReportChromeOptions
	Query  frontservice.FailurePatternsQuery
}

func RenderHTML(
	data frontservice.FailurePatternsData,
	options PageOptions,
) string {
	totalRows := 0
	totalMatchedFailures := 0
	for _, environment := range data.Environments {
		totalRows += len(environment.Rows)
		totalMatchedFailures += environment.Summary.MatchedFailureCount
	}

	startDate, endDate, hasWindow := parseFailurePatternsDates(data.Meta.StartDate, data.Meta.EndDate)
	var b strings.Builder
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Failure Patterns</title>\n")
	b.WriteString(frontui.ThemeInitScriptTag())
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
	b.WriteString(frontui.ReportChromeCSS())
	b.WriteString(frontui.StylesCSS())
	b.WriteString(frontui.ThemeCSS())
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString(frontui.ReportChromeHTML(options.Chrome))
	b.WriteString("  <p class=\"meta\">Runs affected, run impact, and seen-in are recomputed across the selected window. <span class=\"failure-patterns-header-help\" title=\"Flake signal and trend stay anchored to the most recent weekly data for each failure pattern, keeping the signal stable across longer date ranges.\">?</span></p>\n")
	b.WriteString("  <div class=\"cards\">\n")
	b.WriteString(failurePatternsCardHTML("Environments", fmt.Sprintf("%d", len(data.Environments))))
	b.WriteString(failurePatternsCardHTML("Failure patterns", fmt.Sprintf("%d", totalRows)))
	b.WriteString(failurePatternsCardHTML("Failures matched", fmt.Sprintf("%d", totalMatchedFailures)))
	if hasWindow {
		b.WriteString(failurePatternsCardHTML("Window", fmt.Sprintf("%d days", failurePatternsInclusiveDays(startDate, endDate))))
	}
	b.WriteString("  </div>\n")

	for _, environment := range data.Environments {
		failurePatternRows := failurePatternsFailurePatternRows(environment.Rows, environment.Summary.MatchedFailureCount)
		b.WriteString(fmt.Sprintf("  <section id=\"window-%s\" class=\"section\">\n", html.EscapeString(strings.TrimSpace(environment.Environment))))
		b.WriteString(fmt.Sprintf("    <h2>Environment: %s</h2>\n", html.EscapeString(strings.ToUpper(strings.TrimSpace(environment.Environment)))))
		if len(failurePatternRows) == 0 {
			b.WriteString(fmt.Sprintf(
				"    <p class=\"section-note\">Failure patterns: 0 &middot; Failures matched: %d &middot; Total runs: %d</p>\n",
				environment.Summary.MatchedFailureCount,
				environment.Summary.TotalRuns,
			))
			b.WriteString("    <p class=\"muted\">No semantic signatures had matching failures in this window.</p>\n")
			b.WriteString("  </section>\n")
			continue
		}
		b.WriteString(fmt.Sprintf(
			"    <p class=\"section-note\">Failure patterns: %d &middot; Failures matched: %d &middot; Total runs: %d &middot; Runs affected: %d</p>\n",
			len(failurePatternRows),
			environment.Summary.MatchedFailureCount,
			environment.Summary.TotalRuns,
			environment.Summary.JobsAffected,
		))
		b.WriteString(frontui.RenderTable(failurePatternRows, frontui.TableOptions{
			IncludeTrend:       true,
			GitHubRepoOwner:    sourceoptions.DefaultGitHubRepoOwner(),
			GitHubRepoName:     sourceoptions.DefaultGitHubRepoName(),
			ImpactTotalJobs:    environment.Summary.TotalRuns,
			LoadedRowsLimit:    failurePatternsLoadedRowsLimit,
			InitialVisibleRows: failurePatternsInitialVisibleRows,
		}))
		b.WriteString("  </section>\n")
	}

	b.WriteString(frontui.ThemeToggleScriptTag())
	b.WriteString("</body>\n")
	b.WriteString("</html>\n")
	return b.String()
}

func failurePatternsFailurePatternRows(
	rows []frontservice.FailurePatternsRow,
	totalEnvironmentFailures int,
) []frontservice.FailurePatternRow {
	out := make([]frontservice.FailurePatternRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, frontservice.FailurePatternRow{
			Environment:        strings.TrimSpace(row.Environment),
			FailedAt:           strings.TrimSpace(row.Lane),
			JobName:            strings.TrimSpace(row.JobName),
			TestName:           strings.TrimSpace(row.TestName),
			TestSuite:          strings.TrimSpace(row.TestSuite),
			FailurePattern:     strings.TrimSpace(row.CanonicalEvidencePhrase),
			FailurePatternID:   strings.TrimSpace(row.ClusterID),
			SearchQuery:        strings.TrimSpace(row.SearchQueryPhrase),
			Occurrences:        row.WindowFailureCount,
			TrendCounts:        append([]int(nil), row.TrendCounts...),
			TrendRange:         strings.TrimSpace(row.TrendRange),
			OccurrenceShare:    failurePatternsPercent(row.WindowFailureCount, totalEnvironmentFailures),
			AfterLastPushCount: row.WeeklyPostGoodCount,
			AlsoIn:             append([]string(nil), row.SeenIn...),
			ContributingTests:  failurePatternsContributingTests(row.ContributingTests),
			FullErrorSamples:   append([]string(nil), row.FullErrorSamples...),
			AffectedRuns:       failurePatternsRunReferences(row.References),
			ScoringReferences:  failurePatternsRunReferences(row.ScoringReferences),
			PriorWeeksPresent:  row.PriorWeeksPresent,
			PriorWeekStarts:    append([]string(nil), row.PriorWeekStarts...),
			PriorRunsAffected:  row.PriorJobsAffected,
			PriorLastSeenAt:    strings.TrimSpace(row.PriorLastSeenAt),
			LinkedPatterns:     failurePatternsFailurePatternRows(row.LinkedChildren, totalEnvironmentFailures),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Occurrences != out[j].Occurrences {
			return out[i].Occurrences > out[j].Occurrences
		}
		if out[i].FailurePatternID != out[j].FailurePatternID {
			return out[i].FailurePatternID < out[j].FailurePatternID
		}
		return out[i].FailurePattern < out[j].FailurePattern
	})
	return out
}

func failurePatternsRunReferences(rows []frontservice.FailurePatternReportReference) []frontservice.RunReference {
	out := make([]frontservice.RunReference, 0, len(rows))
	for _, row := range rows {
		out = append(out, frontservice.RunReference{
			RunURL:      strings.TrimSpace(row.RunURL),
			OccurredAt:  strings.TrimSpace(row.OccurredAt),
			SignatureID: strings.TrimSpace(row.SignatureID),
			PRNumber:    row.PRNumber,
		})
	}
	return out
}

func failurePatternsContributingTests(rows []frontservice.FailurePatternReportContributingTest) []frontservice.ContributingTest {
	out := make([]frontservice.ContributingTest, 0, len(rows))
	for _, row := range rows {
		out = append(out, frontservice.ContributingTest{
			FailedAt:    strings.TrimSpace(row.Lane),
			JobName:     strings.TrimSpace(row.JobName),
			TestName:    strings.TrimSpace(row.TestName),
			Occurrences: row.SupportCount,
		})
	}
	return out
}

func failurePatternsCardHTML(label string, value string) string {
	return fmt.Sprintf(
		"    <div class=\"card\"><div class=\"label\">%s</div><div class=\"value\">%s</div></div>\n",
		html.EscapeString(strings.TrimSpace(label)),
		html.EscapeString(strings.TrimSpace(value)),
	)
}

func failurePatternsPercent(value int, total int) float64 {
	if total <= 0 {
		return 0
	}
	return (float64(value) * 100.0) / float64(total)
}

func parseFailurePatternsDates(startDate string, endDate string) (time.Time, time.Time, bool) {
	start, err := time.Parse("2006-01-02", strings.TrimSpace(startDate))
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	end, err := time.Parse("2006-01-02", strings.TrimSpace(endDate))
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	return start.UTC(), end.UTC(), true
}

func failurePatternsInclusiveDays(startDate time.Time, endDate time.Time) int {
	startDay := startDate.UTC().Truncate(24 * time.Hour)
	endDay := endDate.UTC().Truncate(24 * time.Hour)
	if endDay.Before(startDay) {
		return 0
	}
	return int(endDay.Sub(startDay)/(24*time.Hour)) + 1
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
