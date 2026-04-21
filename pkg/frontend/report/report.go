package report

import (
	"context"
	"errors"
	"fmt"
	"html"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	frontservice "ci-failure-atlas/pkg/frontend/readmodel"
	frontui "ci-failure-atlas/pkg/frontend/ui"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
)

const (
	windowDays = 7

	weeklySignatureLoadedRowsLimit = 50
	weeklySignatureVisibleRows     = 10
	weeklySignatureMinImpactPct    = 1.0
	weeklyTestSuccessTarget        = 95.0
	weeklyTestSuccessMinRuns       = 10
)

type Options struct {
	OutputPath          string
	StartDate           string
	TargetRate          float64
	Week                string
	HistoryHorizonWeeks int
	HistoryResolver     semhistory.FailurePatternHistoryResolver
	RunLogDayBasePath   string
	Chrome              frontui.ReportChromeOptions
}

type validatedOptions struct {
	OutputPath          string
	StartDate           time.Time
	TargetRate          float64
	Week                string
	HistoryHorizonWeeks int
	HistoryResolver     semhistory.FailurePatternHistoryResolver
	RunLogDayBasePath   string
	Chrome              frontui.ReportChromeOptions
}

type counts = frontservice.WeeklyCounts
type runOutcomes = frontservice.WeeklyRunOutcomes
type dayReport = frontservice.WeeklyDayReport
type envReport = frontservice.WeeklyEnvReport
type semanticEnvSummary = frontservice.WeeklySemanticEnvSummary
type semanticSnapshot = frontservice.WeeklySemanticSnapshot
type belowTargetTest = frontservice.WeeklyBelowTargetTest
type topSignature = frontservice.WeeklyTopSignature
type reportData = frontservice.ReportData

func DefaultOptions() Options {
	return Options{
		OutputPath:          "data/reports/weekly-metrics.html",
		StartDate:           "",
		TargetRate:          95.0,
		HistoryHorizonWeeks: 4,
	}
}

func Generate(ctx context.Context, store storecontracts.Store, opts Options) error {
	return GenerateWithComparison(ctx, store, nil, opts)
}

func GenerateHTML(ctx context.Context, store storecontracts.Store, opts Options) (string, error) {
	return GenerateHTMLWithComparison(ctx, store, nil, opts)
}

func RenderHTML(data reportData, opts Options) string {
	return buildHTML(
		data.StartDate,
		data.EndDate,
		data.CurrentReports,
		data.PreviousReports,
		data.TargetRate,
		data.TestsBelowTargetByEnv,
		reportWindowedFailurePatternRowsByEnv(data.TopSignaturesByEnv),
		strings.TrimSpace(opts.RunLogDayBasePath),
		opts.Chrome,
	)
}

func GenerateWithComparison(
	ctx context.Context,
	store storecontracts.Store,
	previousSemanticStore storecontracts.Store,
	opts Options,
) error {
	validated, err := validateOptions(opts)
	if err != nil {
		return err
	}
	if store == nil {
		return fmt.Errorf("store is required")
	}

	data, err := frontservice.BuildWeeklyReportData(ctx, store, previousSemanticStore, frontservice.WeeklyReportBuildOptions{
		StartDate:           validated.StartDate,
		TargetRate:          validated.TargetRate,
		Week:                validated.Week,
		HistoryHorizonWeeks: validated.HistoryHorizonWeeks,
		HistoryResolver:     validated.HistoryResolver,
	})
	if err != nil {
		return err
	}
	rendered := buildHTML(
		data.StartDate,
		data.EndDate,
		data.CurrentReports,
		data.PreviousReports,
		data.TargetRate,
		data.TestsBelowTargetByEnv,
		legacyWeeklyFailurePatternRowsByEnv(data.TopSignaturesByEnv, data.HistoryResolver, data.EndDate),
		validated.RunLogDayBasePath,
		validated.Chrome,
	)

	if err := os.MkdirAll(filepath.Dir(validated.OutputPath), 0o755); err != nil {
		return fmt.Errorf("create weekly report output directory: %w", err)
	}
	if err := os.WriteFile(validated.OutputPath, []byte(rendered), 0o644); err != nil {
		return fmt.Errorf("write weekly report: %w", err)
	}
	return nil
}

func GenerateHTMLWithComparison(
	ctx context.Context,
	store storecontracts.Store,
	previousSemanticStore storecontracts.Store,
	opts Options,
) (string, error) {
	tmp, err := os.CreateTemp("", "cfa-weekly-*.html")
	if err != nil {
		return "", fmt.Errorf("create temp weekly output: %w", err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	opts.OutputPath = tmpPath
	if err := GenerateWithComparison(ctx, store, previousSemanticStore, opts); err != nil {
		return "", err
	}
	content, err := os.ReadFile(tmpPath)
	if err != nil {
		return "", fmt.Errorf("read generated weekly output: %w", err)
	}
	return string(content), nil
}

func validateOptions(opts Options) (validatedOptions, error) {
	outputPath := strings.TrimSpace(opts.OutputPath)
	if outputPath == "" {
		return validatedOptions{}, errors.New("missing --output path")
	}
	startDateRaw := strings.TrimSpace(opts.StartDate)
	if startDateRaw == "" {
		return validatedOptions{}, errors.New("missing --start-date (expected YYYY-MM-DD)")
	}
	startDate, err := time.Parse("2006-01-02", startDateRaw)
	if err != nil {
		return validatedOptions{}, fmt.Errorf("invalid --start-date %q (expected YYYY-MM-DD): %w", startDateRaw, err)
	}
	if opts.TargetRate <= 0 || opts.TargetRate > 100 {
		return validatedOptions{}, fmt.Errorf("invalid --target-rate %.2f (expected range: 0 < target <= 100)", opts.TargetRate)
	}
	startWeek, err := postgresstore.NormalizeWeek(startDateRaw)
	if err != nil {
		return validatedOptions{}, fmt.Errorf("invalid --start-date %q (expected YYYY-MM-DD Monday start): %w", startDateRaw, err)
	}
	week := strings.TrimSpace(opts.Week)
	if week == "" {
		week = startWeek
	}
	normalizedWeek, err := postgresstore.NormalizeWeek(week)
	if err != nil {
		return validatedOptions{}, fmt.Errorf("invalid week %q: %w", week, err)
	}
	if normalizedWeek != startWeek {
		return validatedOptions{}, fmt.Errorf("week %q must match --start-date %q", normalizedWeek, startWeek)
	}
	if opts.HistoryHorizonWeeks <= 0 {
		opts.HistoryHorizonWeeks = 4
	}
	return validatedOptions{
		OutputPath:          outputPath,
		StartDate:           startDate.UTC(),
		TargetRate:          opts.TargetRate,
		Week:                normalizedWeek,
		HistoryHorizonWeeks: opts.HistoryHorizonWeeks,
		HistoryResolver:     opts.HistoryResolver,
		RunLogDayBasePath:   strings.TrimSpace(opts.RunLogDayBasePath),
		Chrome:              opts.Chrome,
	}, nil
}

func buildHTML(
	startDate time.Time,
	endDate time.Time,
	reports []envReport,
	previousReports []envReport,
	targetRate float64,
	testsBelowTargetByEnv map[string][]belowTargetTest,
	topSignaturesByEnv map[string][]frontservice.FailurePatternRow,
	runLogDayBasePath string,
	chrome frontui.ReportChromeOptions,
) string {
	var b strings.Builder
	failurePatternsBaseHref := strings.TrimSpace(chrome.ContextFailurePatternsHref)
	if failurePatternsBaseHref == "" {
		failurePatternsBaseHref = "failure-patterns-report.html"
	}
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CIHealth Overview</title>\n")
	b.WriteString(frontui.ThemeInitScriptTag())
	b.WriteString("  <style>\n")
	b.WriteString("    body { font-family: Arial, sans-serif; margin: 0; color: #1f2937; }\n")
	b.WriteString("    h1 { margin-bottom: 4px; }\n")
	b.WriteString("    .meta { color: #4b5563; margin-bottom: 16px; }\n")
	b.WriteString("    .env { border: 1px solid #e5e7eb; border-radius: 8px; margin: 14px 0; padding: 12px; }\n")
	b.WriteString("    .overview-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 10px 0 16px; }\n")
	b.WriteString("    .overview-table th, .overview-table td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; vertical-align: top; }\n")
	b.WriteString("    .overview-table th { background: #f3f4f6; color: #374151; font-weight: 700; }\n")
	b.WriteString("    .exec-heading-wrap { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: start; gap: 6px; }\n")
	b.WriteString("    .exec-heading-wrap .exec-heading-label { min-width: 0; }\n")
	b.WriteString("    .status-on-track { color: #166534; font-weight: 700; }\n")
	b.WriteString("    .status-off-track { color: #991b1b; font-weight: 700; }\n")
	b.WriteString("    .status-near-track { color: #92400e; font-weight: 700; }\n")
	b.WriteString("    .status-na { color: #6b7280; font-weight: 700; }\n")
	b.WriteString("    .pp-positive { color: #166534; font-weight: 700; }\n")
	b.WriteString("    .pp-negative { color: #991b1b; font-weight: 700; }\n")
	b.WriteString("    .pp-neutral { color: #6b7280; font-weight: 700; }\n")
	b.WriteString("    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px; margin: 8px 0 12px; }\n")
	b.WriteString("    .cards.cards-post-good { margin-top: 0; }\n")
	b.WriteString("    .cards.cards-dev { grid-template-columns: repeat(4, minmax(0, 1fr)); }\n")
	b.WriteString("    .card { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 6px; padding: 8px 10px; min-width: 0; }\n")
	b.WriteString("    .card-label-row { display: flex; align-items: flex-start; justify-content: space-between; gap: 6px; }\n")
	b.WriteString("    .card-label-row .label { margin: 0; min-width: 0; }\n")
	b.WriteString("    .label { font-size: 12px; color: #6b7280; }\n")
	b.WriteString("    .value { font-size: 18px; font-weight: 700; }\n")
	b.WriteString("    .chart-title { margin: 4px 0 6px; font-size: 14px; }\n")
	b.WriteString("    .legend { display: flex; gap: 12px; flex-wrap: wrap; font-size: 12px; color: #4b5563; margin: 4px 0 8px; }\n")
	b.WriteString("    .legend-item { display: inline-flex; align-items: center; gap: 6px; }\n")
	b.WriteString("    .legend-swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }\n")
	b.WriteString("    .outcomes { display: flex; flex-direction: column; gap: 12px; margin-bottom: 12px; }\n")
	b.WriteString("    .outcome-row { display: grid; grid-template-columns: 95px minmax(0, 1fr); align-items: start; gap: 12px; font-size: 12px; }\n")
	b.WriteString("    .outcome-date { color: #374151; font-weight: 600; padding-top: 6px; }\n")
	b.WriteString("    .outcome-main { display: flex; align-items: center; gap: 12px; min-width: 0; }\n")
	b.WriteString("    .outcome-meta { flex: 0 0 auto; display: flex; justify-content: flex-end; }\n")
	b.WriteString("    .outcome-total { display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; padding: 4px 10px; font-size: 11px; font-weight: 700; background: #eef2ff; color: #312e81; border: 1px solid #c7d2fe; }\n")
	b.WriteString("    .outcome-bar-shell { flex: 1 1 auto; min-width: 0; display: flex; align-items: stretch; }\n")
	b.WriteString("    .outcome-bar { position: relative; flex: 0 0 auto; max-width: 100%; min-width: 1px; min-height: 24px; background: #f8fafc; border: 1px solid #d1d5db; border-radius: 999px; overflow: visible; }\n")
	b.WriteString("    .outcome-bar-empty { width: 100%; display: flex; justify-content: center; align-items: center; color: #6b7280; font-size: 11px; }\n")
	b.WriteString("    .outcome-segment-wrap { position: absolute; top: 0; bottom: 0; min-width: 1px; overflow: visible; }\n")
	b.WriteString("    .outcome-segment-wrap:first-child { border-top-left-radius: 999px; border-bottom-left-radius: 999px; }\n")
	b.WriteString("    .outcome-segment-wrap:last-child { border-top-right-radius: 999px; border-bottom-right-radius: 999px; }\n")
	b.WriteString("    .outcome-segment { display: flex; align-items: center; justify-content: center; width: 100%; height: 100%; min-width: 1px; border: 0; padding: 0; background: transparent; color: #ffffff; cursor: pointer; overflow: hidden; }\n")
	b.WriteString("    .outcome-segment-label { display: inline-flex; align-items: center; justify-content: center; padding: 0 4px; font-size: 10px; font-weight: 700; line-height: 1; white-space: nowrap; text-shadow: 0 1px 1px rgba(15, 23, 42, 0.35); pointer-events: none; }\n")
	b.WriteString("    .outcome-segment-label.is-dark { color: #0f172a; text-shadow: none; }\n")
	b.WriteString("    .outcome-segment:hover { filter: brightness(0.96); }\n")
	b.WriteString("    .outcome-segment:focus-visible { position: relative; z-index: 1; outline: 2px solid #1d4ed8; outline-offset: -2px; }\n")
	b.WriteString("    .seg-success { background: #5f8a69; }\n")
	b.WriteString("    .seg-provision { background: #ad8651; }\n")
	b.WriteString("    .seg-e2e { background: #9f676d; }\n")
	b.WriteString("    .seg-ciinfra { background: #708092; }\n")
	b.WriteString("    .drill-tabs { display: flex; gap: 8px; flex-wrap: wrap; margin: 8px 0 12px; border-bottom: 1px solid #e5e7eb; padding-bottom: 8px; }\n")
	b.WriteString("    .drill-tab { border: 1px solid #d1d5db; border-radius: 999px; padding: 4px 10px; background: #f9fafb; color: #374151; font-size: 12px; cursor: pointer; }\n")
	b.WriteString("    .drill-tab.active { background: #111827; border-color: #111827; color: #ffffff; font-weight: 700; }\n")
	b.WriteString("    .drill-panel[hidden] { display: none; }\n")
	b.WriteString("    .panel-note { margin: 4px 0 10px; color: #4b5563; font-size: 12px; }\n")
	b.WriteString("    .panel-empty { margin: 6px 0 12px; color: #6b7280; font-size: 12px; }\n")
	b.WriteString("    .detail-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0 12px; }\n")
	b.WriteString("    .detail-table th, .detail-table td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; vertical-align: top; }\n")
	b.WriteString("    .detail-table th { background: #f9fafb; color: #374151; font-weight: 700; }\n")
	b.WriteString("    .detail-table details { margin: 0; }\n")
	b.WriteString("    .detail-table summary { cursor: pointer; color: #1d4ed8; }\n")
	b.WriteString("    .detail-table pre { white-space: pre-wrap; word-break: break-word; background: #111827; color: #f9fafb; padding: 8px; border-radius: 6px; font-size: 11px; margin: 6px 0 0; }\n")
	b.WriteString(frontui.ReportChromeCSS())
	b.WriteString(frontui.StylesCSS())
	b.WriteString(frontui.ThemeCSS())
	b.WriteString("    .outcome-bar > .outcome-segment-wrap.inline-tooltip { display: block; position: absolute; top: 0; bottom: 0; min-width: 1px; vertical-align: initial; overflow: visible; }\n")
	b.WriteString("    .outcome-bar > .outcome-segment-wrap.inline-tooltip:hover, .outcome-bar > .outcome-segment-wrap.inline-tooltip[data-open=\"true\"], .outcome-bar > .outcome-segment-wrap.inline-tooltip:focus-within { z-index: 3; }\n")
	b.WriteString("    .outcome-bar > .outcome-segment-wrap.inline-tooltip .outcome-segment.inline-tooltip-trigger { display: flex; align-items: center; justify-content: center; width: 100%; height: 100%; min-width: 1px; background: transparent; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .outcome-date { color: #e2e8f0; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .outcome-total { background: #0f172a; border-color: #334155; color: #dbeafe; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .outcome-bar { background: #0f172a; border-color: #334155; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .outcome-bar-empty { color: #94a3b8; }\n")
	b.WriteString("    :root[data-theme=\"dark\"] .outcome-segment:focus-visible { outline-color: #93c5fd; }\n")
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString(frontui.ReportChromeHTML(chrome))
	b.WriteString("<main class=\"page-content\">\n")

	previousByEnvironment := map[string]envReport{}
	for _, report := range previousReports {
		previousByEnvironment[normalizeReportEnvironment(report.Environment)] = report
	}

	b.WriteString("  <section class=\"env\">\n")
	b.WriteString("    <h2>Executive Status</h2>\n")
	b.WriteString("    <table class=\"overview-table\">\n")
	b.WriteString("      <thead><tr>")
	executiveHeaders := []struct {
		label   string
		tooltip string
	}{
		{label: "Env", tooltip: "Environment partition: dev, int, stg, or prod."},
		{label: "Goal", tooltip: goalBasisTooltip()},
		{label: "Runs", tooltip: "Number of job runs in the selected goal definition for this environment."},
		{label: "Success", tooltip: "Success rate on the selected goal definition: (runs - failed runs) / runs * 100."},
		{label: "Gap vs target", tooltip: "Difference in percentage points between current success and the configured target rate."},
		{label: "Change vs prev", tooltip: "How much the success rate changed compared with the immediately preceding equal-length window, using the same goal definition as this row."},
		{label: "Provision success", tooltip: executiveProvisionSuccessTooltip()},
		{label: "Provision change vs prev", tooltip: executiveProvisionChangeTooltip()},
		{label: "E2E success", tooltip: executiveE2ESuccessTooltip()},
		{label: "E2E success vs prev", tooltip: executiveE2EChangeTooltip()},
	}
	for i, header := range executiveHeaders {
		b.WriteString(executiveHeaderHTML(header.label, header.tooltip, tooltipPlacementForOrderedItems(i, len(executiveHeaders))))
	}
	b.WriteString("</tr></thead>\n")
	b.WriteString("      <tbody>\n")
	for _, report := range reports {
		environment := normalizeReportEnvironment(report.Environment)
		goalBasis, goalRuns, currentSuccess, goalAvailable := goalBasisKPI(report)
		statusClass := "status-na"
		statusLabel := "insufficient data"
		gapCell := "n/a"
		if goalAvailable {
			statusClass, statusLabel = targetStatus(currentSuccess, targetRate)
			gapCell = formatSignedPercentPointCell(currentSuccess - targetRate)
		}
		prevCell := "n/a"
		if prev, ok := previousByEnvironment[environment]; ok {
			_, _, prevSuccess, prevAvailable := goalBasisKPI(prev)
			if goalAvailable && prevAvailable {
				prevCell = formatSignedPercentPointCell(currentSuccess - prevSuccess)
			}
		}

		successCell := fmt.Sprintf("<span class=\"%s\">n/a (%s)</span>", statusClass, html.EscapeString(statusLabel))
		if goalAvailable {
			successCell = fmt.Sprintf("<span class=\"%s\">%.2f%% (%s)</span>", statusClass, currentSuccess, html.EscapeString(statusLabel))
		}

		provisionSuccessCell := "n/a"
		provisionWoWCell := "n/a"
		currentProvision, hasProvision := provisionStepKPI(report)
		if hasProvision {
			currentProvisionSuccess := successPct(currentProvision.TotalAttempted, currentProvision.Failed)
			provisionSuccessCell = fmt.Sprintf("%.2f%% (%d/%d)", currentProvisionSuccess, currentProvision.Successful, currentProvision.TotalAttempted)
			if prev, ok := previousByEnvironment[environment]; ok {
				previousProvision, hadPreviousProvision := provisionStepKPI(prev)
				if hadPreviousProvision {
					previousProvisionSuccess := successPct(previousProvision.TotalAttempted, previousProvision.Failed)
					provisionWoWCell = formatSignedPercentPointCell(currentProvisionSuccess - previousProvisionSuccess)
				}
			}
		}

		currentE2E := summarizeE2EStepOutcomesForGoalBasis(report)
		e2eSuccessCell := "n/a"
		e2eWoWCell := "n/a"
		if currentE2E.TotalAttempted > 0 {
			currentE2ESuccess := successPct(currentE2E.TotalAttempted, currentE2E.Failed)
			e2eSuccessCell = fmt.Sprintf("%.2f%% (%d/%d)", currentE2ESuccess, currentE2E.Successful, currentE2E.TotalAttempted)
			if prev, ok := previousByEnvironment[environment]; ok {
				previousE2E := summarizeE2EStepOutcomesForGoalBasis(prev)
				if previousE2E.TotalAttempted > 0 {
					previousE2ESuccess := successPct(previousE2E.TotalAttempted, previousE2E.Failed)
					e2eWoWCell = formatSignedPercentPointCell(currentE2ESuccess - previousE2ESuccess)
				}
			}
		}

		b.WriteString("        <tr>")
		b.WriteString(fmt.Sprintf("<td><strong>%s</strong></td>", html.EscapeString(strings.ToUpper(environment))))
		b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(formatGoalDefinition(targetRate, goalBasis))))
		b.WriteString(fmt.Sprintf("<td>%d</td>", goalRuns))
		b.WriteString(fmt.Sprintf("<td>%s</td>", successCell))
		b.WriteString(fmt.Sprintf("<td>%s</td>", gapCell))
		b.WriteString(fmt.Sprintf("<td>%s</td>", prevCell))
		b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(provisionSuccessCell)))
		b.WriteString(fmt.Sprintf("<td>%s</td>", provisionWoWCell))
		b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(e2eSuccessCell)))
		b.WriteString(fmt.Sprintf("<td>%s</td>", e2eWoWCell))
		b.WriteString("</tr>\n")
	}
	b.WriteString("      </tbody>\n")
	b.WriteString("    </table>\n")
	b.WriteString("  </section>\n")

	for _, report := range reports {
		environment := normalizeReportEnvironment(report.Environment)
		envLabel := strings.ToUpper(strings.TrimSpace(report.Environment))
		lanePanelID := fmt.Sprintf("drill-%s-lane", environment)
		testsPanelID := fmt.Sprintf("drill-%s-tests", environment)
		signaturesPanelID := fmt.Sprintf("drill-%s-signatures", environment)
		b.WriteString(fmt.Sprintf("  <section class=\"env\">\n    <h2>Environment: %s</h2>\n", html.EscapeString(envLabel)))
		b.WriteString("    <div class=\"drill-tabs\" role=\"tablist\" aria-label=\"Drill-down views\"")
		b.WriteString(fmt.Sprintf(" data-env=\"%s\">\n", html.EscapeString(environment)))
		b.WriteString(fmt.Sprintf("      <button type=\"button\" class=\"drill-tab active\" role=\"tab\" aria-selected=\"true\" data-target=\"%s\">Run outcomes</button>\n", html.EscapeString(lanePanelID)))
		b.WriteString(fmt.Sprintf("      <button type=\"button\" class=\"drill-tab\" role=\"tab\" aria-selected=\"false\" data-target=\"%s\">Tests below %.0f%%</button>\n", html.EscapeString(testsPanelID), weeklyTestSuccessTarget))
		b.WriteString(fmt.Sprintf("      <button type=\"button\" class=\"drill-tab\" role=\"tab\" aria-selected=\"false\" data-target=\"%s\">Top failure patterns</button>\n", html.EscapeString(signaturesPanelID)))
		b.WriteString("    </div>\n")

		b.WriteString(fmt.Sprintf("    <div id=\"%s\" class=\"drill-panel\" data-env=\"%s\" role=\"tabpanel\">\n", html.EscapeString(lanePanelID), html.EscapeString(environment)))
		cardsClass := "cards"
		if report.Environment == "dev" {
			cardsClass = "cards cards-dev"
		}
		b.WriteString(fmt.Sprintf("    <div class=\"%s\">\n", html.EscapeString(cardsClass)))
		b.WriteString(cardHTML("E2E Jobs", report.Totals.RunCount, "", tooltipPlacementForOrderedItems(0, 4)))
		b.WriteString(cardHTML("Success Rate", fmt.Sprintf("%.2f%%", successPct(report.Totals.RunCount, report.Totals.FailureCount)), "", tooltipPlacementForOrderedItems(1, 4)))
		allRunsE2E := summarizeE2EStepOutcomes(report.Days)
		if report.Environment == "dev" {
			provisionStep := summarizeProvisionStepOutcomes(report.Days)
			provisionStepValue := formatStepSuccessCardValue(provisionStep.TotalAttempted, provisionStep.Successful, provisionStep.Failed)
			b.WriteString(cardHTML("Provision success", provisionStepValue, provisionSuccessTooltip(), tooltipPlacementForOrderedItems(2, 4)))
			b.WriteString(cardHTML("E2E success", formatStepSuccessCardValue(allRunsE2E.TotalAttempted, allRunsE2E.Successful, allRunsE2E.Failed), e2eSuccessTooltip(), tooltipPlacementForOrderedItems(3, 4)))
		} else {
			b.WriteString(cardHTML("E2E success", formatStepSuccessCardValue(allRunsE2E.TotalAttempted, allRunsE2E.Successful, allRunsE2E.Failed), e2eSuccessTooltip(), tooltipPlacementForOrderedItems(2, 3)))
		}
		b.WriteString("    </div>\n")
		if report.Environment == "dev" {
			postGoodTotals := summarizePostGoodRunOutcomes(report.Days)
			postGoodProvision := summarizeProvisionStepOutcomesForGoalBasis(report)
			postGoodE2E := summarizeE2EStepOutcomesForGoalBasis(report)
			b.WriteString("    <div class=\"cards cards-post-good cards-dev\">\n")
			b.WriteString(cardHTML("E2E Jobs (after last push of merged PR)", postGoodTotals.TotalRuns, afterLastPushE2EJobsTooltip(), tooltipPlacementForOrderedItems(0, 4)))
			b.WriteString(cardHTML("Success Rate (after last push of merged PR)", fmt.Sprintf("%.2f%%", successPct(postGoodTotals.TotalRuns, postGoodTotals.FailedRuns)), afterLastPushSuccessRateTooltip(), tooltipPlacementForOrderedItems(1, 4)))
			b.WriteString(cardHTML(
				"Provision success (after last push of merged PR)",
				formatStepSuccessCardValue(postGoodProvision.TotalAttempted, postGoodProvision.Successful, postGoodProvision.Failed),
				afterLastPushProvisionSuccessTooltip(),
				tooltipPlacementForOrderedItems(2, 4),
			))
			b.WriteString(cardHTML(
				"E2E success (after last push of merged PR)",
				formatStepSuccessCardValue(postGoodE2E.TotalAttempted, postGoodE2E.Successful, postGoodE2E.Failed),
				afterLastPushE2ESuccessTooltip(),
				tooltipPlacementForOrderedItems(3, 4),
			))
			b.WriteString("    </div>\n")
		}
		b.WriteString(renderOutcomeChart("Daily Run Outcomes", outcomeChartDaysFromCounts(report.Days), report.Environment, runLogDayBasePath, "Successful runs"))
		if report.Environment == "dev" {
			b.WriteString(renderOutcomeChart(
				"Daily Run Outcomes for DEV Goal Basis (after last push of merged PR)",
				outcomeChartDaysFromPostGood(report.Days),
				report.Environment,
				runLogDayBasePath,
				"Successful runs (after last push of merged PR)",
			))
		}
		b.WriteString("    </div>\n")

		b.WriteString(fmt.Sprintf("    <div id=\"%s\" class=\"drill-panel\" data-env=\"%s\" role=\"tabpanel\" hidden>\n", html.EscapeString(testsPanelID), html.EscapeString(environment)))
		tests := testsBelowTargetByEnv[environment]
		if len(tests) == 0 {
			b.WriteString(fmt.Sprintf("      <p class=\"panel-empty\">No tests below %.2f%% in this window with at least %d runs.</p>\n", weeklyTestSuccessTarget, weeklyTestSuccessMinRuns))
		} else {
			b.WriteString("      <table class=\"detail-table\">\n")
			b.WriteString("        <thead><tr><th>Pass rate</th><th>Runs</th><th>Date (UTC)</th><th>Suite</th><th>Test</th></tr></thead>\n")
			b.WriteString("        <tbody>\n")
			for _, item := range tests {
				suite := cleanInline(item.TestSuite, 80)
				if suite == "" {
					suite = "n/a"
				}
				b.WriteString("          <tr>")
				b.WriteString(fmt.Sprintf("<td>%.2f%%</td>", item.PassRate))
				b.WriteString(fmt.Sprintf("<td>%d</td>", item.Runs))
				b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(item.Date+" UTC")))
				b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(suite)))
				b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(cleanInline(item.TestName, 160))))
				b.WriteString("</tr>\n")
			}
			b.WriteString("        </tbody>\n")
			b.WriteString("      </table>\n")
		}
		b.WriteString("    </div>\n")

		b.WriteString(fmt.Sprintf("    <div id=\"%s\" class=\"drill-panel\" data-env=\"%s\" role=\"tabpanel\" hidden>\n", html.EscapeString(signaturesPanelID), html.EscapeString(environment)))
		b.WriteString(fmt.Sprintf(
			"      <p class=\"panel-note\">Up to %d failure patterns are loaded (minimum %.2f%% impact), with %d shown by default. Click column headers to re-sort.</p>\n",
			weeklySignatureLoadedRowsLimit,
			weeklySignatureMinImpactPct,
			weeklySignatureVisibleRows,
		))
		if failurePatternReportHref := failurePatternReportEnvironmentHref(failurePatternsBaseHref, environment); failurePatternReportHref != "" {
			b.WriteString(fmt.Sprintf(
				"      <p class=\"panel-note\"><a href=\"%s\">Jump to the full Failure Patterns view for this window</a></p>\n",
				html.EscapeString(failurePatternReportHref),
			))
		}
		failurePatternRows := make([]frontservice.FailurePatternRow, 0, len(topSignaturesByEnv[environment]))
		for _, row := range topSignaturesByEnv[environment] {
			if weeklyFailurePatternRowImpactPercent(row, report.Totals.RunCount) < weeklySignatureMinImpactPct {
				continue
			}
			failurePatternRows = append(failurePatternRows, row)
		}
		if len(failurePatternRows) == 0 {
			b.WriteString("      <p class=\"panel-empty\">No failure patterns available for this environment in the selected window.</p>\n")
		} else {
			b.WriteString(frontui.RenderTable(failurePatternRows, frontui.TableOptions{
				IncludeTrend:       true,
				ImpactTotalJobs:    report.Totals.RunCount,
				LoadedRowsLimit:    weeklySignatureLoadedRowsLimit,
				InitialVisibleRows: weeklySignatureVisibleRows,
			}))
		}
		b.WriteString("    </div>\n")
		b.WriteString("  </section>\n")
	}

	b.WriteString("</main>\n")
	b.WriteString(frontui.TooltipScriptTag())
	b.WriteString(frontui.ThemeToggleScriptTag())
	b.WriteString("<script>\n")
	b.WriteString("(function(){\n")
	b.WriteString("  function activateDrillTab(button) {\n")
	b.WriteString("    if (!button) { return; }\n")
	b.WriteString("    var group = button.closest('.drill-tabs');\n")
	b.WriteString("    if (!group) { return; }\n")
	b.WriteString("    var env = group.getAttribute('data-env') || '';\n")
	b.WriteString("    var target = button.getAttribute('data-target') || '';\n")
	b.WriteString("    var buttons = group.querySelectorAll('.drill-tab');\n")
	b.WriteString("    for (var i = 0; i < buttons.length; i++) {\n")
	b.WriteString("      var current = buttons[i];\n")
	b.WriteString("      var active = current === button;\n")
	b.WriteString("      current.classList.toggle('active', active);\n")
	b.WriteString("      current.setAttribute('aria-selected', active ? 'true' : 'false');\n")
	b.WriteString("    }\n")
	b.WriteString("    var panels = document.querySelectorAll('.drill-panel[data-env=\"' + env + '\"]');\n")
	b.WriteString("    for (var j = 0; j < panels.length; j++) {\n")
	b.WriteString("      var panel = panels[j];\n")
	b.WriteString("      panel.hidden = panel.id !== target;\n")
	b.WriteString("    }\n")
	b.WriteString("  }\n")
	b.WriteString("  var tabs = document.querySelectorAll('.drill-tab');\n")
	b.WriteString("  for (var k = 0; k < tabs.length; k++) {\n")
	b.WriteString("    tabs[k].addEventListener('click', function(e) {\n")
	b.WriteString("      activateDrillTab(e.currentTarget);\n")
	b.WriteString("    });\n")
	b.WriteString("  }\n")
	b.WriteString("  var groups = document.querySelectorAll('.drill-tabs');\n")
	b.WriteString("  for (var g = 0; g < groups.length; g++) {\n")
	b.WriteString("    var firstTab = groups[g].querySelector('.drill-tab');\n")
	b.WriteString("    if (firstTab) {\n")
	b.WriteString("      activateDrillTab(firstTab);\n")
	b.WriteString("    }\n")
	b.WriteString("  }\n")
	b.WriteString("})();\n")
	b.WriteString("</script>\n")
	b.WriteString("</body>\n</html>\n")
	return b.String()
}

func weeklyTopSignatureImpactPercent(item topSignature, overallJobs int) float64 {
	if overallJobs <= 0 {
		return 0
	}
	jobsAffected := weeklyTopSignatureJobsAffected(item)
	if jobsAffected <= 0 {
		return 0
	}
	return (float64(jobsAffected) * 100.0) / float64(overallJobs)
}

func weeklyTopSignatureJobsAffected(item topSignature) int {
	if len(item.LinkedChildren) == 0 {
		return len(frontservice.OrderedUniqueReferences(item.References))
	}
	total := 0
	for _, child := range item.LinkedChildren {
		total += len(frontservice.OrderedUniqueReferences(child.References))
	}
	if total > 0 {
		return total
	}
	return len(frontservice.OrderedUniqueReferences(item.References))
}

func topSignatureToFailurePatternRow(item topSignature) frontservice.FailurePatternRow {
	row := frontservice.FailurePatternRow{
		Environment:        item.Environment,
		FailurePattern:     item.Phrase,
		FailurePatternID:   item.ClusterID,
		SearchQuery:        item.SearchQuery,
		Occurrences:        item.SupportCount,
		OccurrenceShare:    item.SupportShare,
		AfterLastPushCount: item.PostGoodCount,
		AlsoIn:             append([]string(nil), item.SeenInOtherEnvs...),
		QualityScore:       item.QualityScore,
		QualityNoteLabels:  append([]string(nil), item.QualityNoteLabels...),
		ContributingTests:  append([]frontservice.ContributingTest(nil), item.ContributingTests...),
		FullErrorSamples:   append([]string(nil), item.FullErrorSamples...),
		AffectedRuns:       append([]frontservice.RunReference(nil), item.References...),
	}
	if len(item.LinkedChildren) == 0 {
		return row
	}
	row.LinkedPatterns = make([]frontservice.FailurePatternRow, 0, len(item.LinkedChildren))
	for _, child := range item.LinkedChildren {
		childRow := topSignatureToFailurePatternRow(child)
		childRow.LinkedPatterns = nil
		row.LinkedPatterns = append(row.LinkedPatterns, childRow)
	}
	return row
}

func legacyWeeklyFailurePatternRowsByEnv(
	source map[string][]topSignature,
	historyResolver semhistory.FailurePatternHistoryResolver,
	endDate time.Time,
) map[string][]frontservice.FailurePatternRow {
	out := make(map[string][]frontservice.FailurePatternRow, len(source))
	for environment, items := range source {
		rows := make([]frontservice.FailurePatternRow, 0, len(items))
		for _, item := range items {
			failurePatternRow := topSignatureToFailurePatternRow(item)
			if historyResolver != nil {
				presence := historyResolver.PresenceFor(semhistory.FailurePatternKey{
					Environment: item.Environment,
					Phrase:      item.Phrase,
					SearchQuery: item.SearchQuery,
				})
				failurePatternRow.PriorWeeksPresent = presence.PriorWeeksPresent
				failurePatternRow.PriorWeekStarts = append([]string(nil), presence.PriorWeekStarts...)
				failurePatternRow.PriorRunsAffected = presence.PriorJobsAffected
				if !presence.PriorLastSeenAt.IsZero() {
					failurePatternRow.PriorLastSeenAt = presence.PriorLastSeenAt.UTC().Format(time.RFC3339)
				}
			}
			if sparkline, counts, sparkRange, ok := frontservice.DailyDensitySparkline(
				failurePatternRow.AffectedRuns,
				windowDays,
				endDate,
			); ok {
				failurePatternRow.TrendSparkline = sparkline
				failurePatternRow.TrendCounts = append([]int(nil), counts...)
				failurePatternRow.TrendRange = sparkRange
			}
			rows = append(rows, failurePatternRow)
		}
		out[environment] = rows
	}
	return out
}

func reportWindowedFailurePatternRowsByEnv(
	source map[string][]frontservice.FailurePatternsRow,
) map[string][]frontservice.FailurePatternRow {
	out := make(map[string][]frontservice.FailurePatternRow, len(source))
	for environment, rows := range source {
		out[environment] = reportWindowedFailurePatternRows(rows, reportWindowedFailureTotal(rows))
	}
	return out
}

func reportWindowedFailurePatternRows(
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
			OccurrenceShare:    reportWindowedPercent(row.WindowFailureCount, totalEnvironmentFailures),
			AfterLastPushCount: row.WeeklyPostGoodCount,
			AlsoIn:             append([]string(nil), row.SeenIn...),
			ContributingTests:  reportWindowedContributingTests(row.ContributingTests),
			FullErrorSamples:   append([]string(nil), row.FullErrorSamples...),
			AffectedRuns:       reportWindowedRunReferences(row.References),
			ScoringReferences:  reportWindowedRunReferences(row.ScoringReferences),
			PriorWeeksPresent:  row.PriorWeeksPresent,
			PriorWeekStarts:    append([]string(nil), row.PriorWeekStarts...),
			PriorRunsAffected:  row.PriorJobsAffected,
			PriorLastSeenAt:    strings.TrimSpace(row.PriorLastSeenAt),
			LinkedPatterns:     reportWindowedFailurePatternRows(row.LinkedChildren, totalEnvironmentFailures),
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

func reportWindowedRunReferences(rows []frontservice.FailurePatternReportReference) []frontservice.RunReference {
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

func reportWindowedContributingTests(rows []frontservice.FailurePatternReportContributingTest) []frontservice.ContributingTest {
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

func reportWindowedFailureTotal(rows []frontservice.FailurePatternsRow) int {
	total := 0
	for _, row := range rows {
		total += row.WindowFailureCount
	}
	return total
}

func reportWindowedPercent(value int, total int) float64 {
	if total <= 0 {
		return 0
	}
	return (float64(value) * 100.0) / float64(total)
}

func weeklyFailurePatternRowImpactPercent(item frontservice.FailurePatternRow, overallJobs int) float64 {
	if overallJobs <= 0 {
		return 0
	}
	jobsAffected := weeklyFailurePatternRowJobsAffected(item)
	if jobsAffected <= 0 {
		return 0
	}
	return (float64(jobsAffected) * 100.0) / float64(overallJobs)
}

func weeklyFailurePatternRowJobsAffected(item frontservice.FailurePatternRow) int {
	if len(item.LinkedPatterns) == 0 {
		return len(frontservice.OrderedUniqueReferences(item.AffectedRuns))
	}
	total := 0
	for _, child := range item.LinkedPatterns {
		total += len(frontservice.OrderedUniqueReferences(child.AffectedRuns))
	}
	if total > 0 {
		return total
	}
	return len(frontservice.OrderedUniqueReferences(item.AffectedRuns))
}

func dailyRunOutcomeCounts(day counts) (successfulRuns int, ciInfraFailedRuns int, provisionFailedRuns int, e2eFailedRuns int) {
	ciInfraFailedRuns = day.FailedCIInfraRunCount
	provisionFailedRuns = day.FailedProvisionRunCount
	e2eFailedRuns = day.FailedE2ERunCount

	// Keep the chart partition complete even when lane-level failed-run counts are
	// partially missing in older data snapshots.
	unclassifiedFailedRuns := day.FailureCount - (ciInfraFailedRuns + provisionFailedRuns + e2eFailedRuns)
	if unclassifiedFailedRuns > 0 {
		ciInfraFailedRuns += unclassifiedFailedRuns
	}

	totalFailedRuns := ciInfraFailedRuns + provisionFailedRuns + e2eFailedRuns
	successfulRuns = day.RunCount - totalFailedRuns
	if successfulRuns < 0 {
		successfulRuns = 0
	}
	return successfulRuns, ciInfraFailedRuns, provisionFailedRuns, e2eFailedRuns
}

type runOutcomesTotals struct {
	TotalRuns      int
	FailedRuns     int
	SuccessfulRuns int
}

type provisionStepTotals struct {
	TotalAttempted int
	Successful     int
	Failed         int
}

type e2eStepTotals struct {
	TotalAttempted int
	Successful     int
	Failed         int
}

func summarizePostGoodRunOutcomes(days []dayReport) runOutcomesTotals {
	out := runOutcomesTotals{}
	for _, day := range days {
		out.TotalRuns += day.PostGoodRunOutcomes.TotalRuns
		out.SuccessfulRuns += day.PostGoodRunOutcomes.SuccessfulRuns
		out.FailedRuns += day.PostGoodRunOutcomes.CIInfraFailedRuns +
			day.PostGoodRunOutcomes.ProvisionFailedRuns +
			day.PostGoodRunOutcomes.E2EFailedRuns
	}
	return out
}

func summarizeProvisionStepOutcomes(days []dayReport) provisionStepTotals {
	out := provisionStepTotals{}
	for _, day := range days {
		successfulRuns, _, provisionFailedRuns, e2eFailedRuns := dailyRunOutcomeCounts(day.Counts)
		attempted := successfulRuns + provisionFailedRuns + e2eFailedRuns
		successfulProvision := successfulRuns + e2eFailedRuns
		if attempted < 0 {
			attempted = 0
		}
		if successfulProvision < 0 {
			successfulProvision = 0
		}
		failedProvision := provisionFailedRuns
		if failedProvision < 0 {
			failedProvision = 0
		}
		out.TotalAttempted += attempted
		out.Successful += successfulProvision
		out.Failed += failedProvision
	}
	return out
}

func summarizeProvisionStepOutcomesForGoalBasis(report envReport) provisionStepTotals {
	if normalizeReportEnvironment(report.Environment) == "dev" {
		return summarizeProvisionStepOutcomesFromRunOutcomes(postGoodRunOutcomesByDay(report.Days))
	}
	return summarizeProvisionStepOutcomes(report.Days)
}

func provisionStepKPI(report envReport) (provisionStepTotals, bool) {
	if normalizeReportEnvironment(report.Environment) != "dev" {
		return provisionStepTotals{}, false
	}
	outcomes := summarizeProvisionStepOutcomesForGoalBasis(report)
	if outcomes.TotalAttempted <= 0 {
		return outcomes, false
	}
	return outcomes, true
}

func summarizeProvisionStepOutcomesFromRunOutcomes(outcomes []runOutcomes) provisionStepTotals {
	out := provisionStepTotals{}
	for _, outcome := range outcomes {
		attempted := outcome.SuccessfulRuns + outcome.ProvisionFailedRuns + outcome.E2EFailedRuns
		successfulProvision := outcome.SuccessfulRuns + outcome.E2EFailedRuns
		if attempted < 0 {
			attempted = 0
		}
		if successfulProvision < 0 {
			successfulProvision = 0
		}
		failedProvision := outcome.ProvisionFailedRuns
		if failedProvision < 0 {
			failedProvision = 0
		}
		out.TotalAttempted += attempted
		out.Successful += successfulProvision
		out.Failed += failedProvision
	}
	return out
}

func summarizeE2EStepOutcomes(days []dayReport) e2eStepTotals {
	out := e2eStepTotals{}
	for _, day := range days {
		successfulRuns, _, _, e2eFailedRuns := dailyRunOutcomeCounts(day.Counts)
		attempted := successfulRuns + e2eFailedRuns
		successfulE2E := successfulRuns
		failedE2E := e2eFailedRuns
		if attempted < 0 {
			attempted = 0
		}
		if successfulE2E < 0 {
			successfulE2E = 0
		}
		if failedE2E < 0 {
			failedE2E = 0
		}
		out.TotalAttempted += attempted
		out.Successful += successfulE2E
		out.Failed += failedE2E
	}
	return out
}

func summarizeE2EStepOutcomesForGoalBasis(report envReport) e2eStepTotals {
	if normalizeReportEnvironment(report.Environment) == "dev" {
		return summarizeE2EStepOutcomesFromRunOutcomes(postGoodRunOutcomesByDay(report.Days))
	}
	return summarizeE2EStepOutcomes(report.Days)
}

func summarizeE2EStepOutcomesFromRunOutcomes(outcomes []runOutcomes) e2eStepTotals {
	out := e2eStepTotals{}
	for _, outcome := range outcomes {
		attempted := outcome.SuccessfulRuns + outcome.E2EFailedRuns
		successfulE2E := outcome.SuccessfulRuns
		failedE2E := outcome.E2EFailedRuns
		if attempted < 0 {
			attempted = 0
		}
		if successfulE2E < 0 {
			successfulE2E = 0
		}
		if failedE2E < 0 {
			failedE2E = 0
		}
		out.TotalAttempted += attempted
		out.Successful += successfulE2E
		out.Failed += failedE2E
	}
	return out
}

func postGoodRunOutcomesByDay(days []dayReport) []runOutcomes {
	out := make([]runOutcomes, 0, len(days))
	for _, day := range days {
		out = append(out, day.PostGoodRunOutcomes)
	}
	return out
}

type outcomeChartDay struct {
	Date                string
	TotalRuns           int
	SuccessfulRuns      int
	ProvisionFailedRuns int
	E2EFailedRuns       int
	CIInfraFailedRuns   int
}

func outcomeChartDaysFromCounts(days []dayReport) []outcomeChartDay {
	out := make([]outcomeChartDay, 0, len(days))
	for _, day := range days {
		successfulRuns, ciInfraFailedRuns, provisionFailedRuns, e2eFailedRuns := dailyRunOutcomeCounts(day.Counts)
		out = append(out, outcomeChartDay{
			Date:                day.Date,
			TotalRuns:           day.Counts.RunCount,
			SuccessfulRuns:      successfulRuns,
			ProvisionFailedRuns: provisionFailedRuns,
			E2EFailedRuns:       e2eFailedRuns,
			CIInfraFailedRuns:   ciInfraFailedRuns,
		})
	}
	return out
}

func outcomeChartDaysFromPostGood(days []dayReport) []outcomeChartDay {
	out := make([]outcomeChartDay, 0, len(days))
	for _, day := range days {
		out = append(out, outcomeChartDay{
			Date:                day.Date,
			TotalRuns:           day.PostGoodRunOutcomes.TotalRuns,
			SuccessfulRuns:      day.PostGoodRunOutcomes.SuccessfulRuns,
			ProvisionFailedRuns: day.PostGoodRunOutcomes.ProvisionFailedRuns,
			E2EFailedRuns:       day.PostGoodRunOutcomes.E2EFailedRuns,
			CIInfraFailedRuns:   day.PostGoodRunOutcomes.CIInfraFailedRuns,
		})
	}
	return out
}

func outcomeSegmentTooltip(label string, value int, total int) string {
	if total <= 0 {
		return fmt.Sprintf("%s: %d runs", strings.TrimSpace(label), value)
	}
	return fmt.Sprintf(
		"%s: %d of %d runs (%.1f%%)",
		strings.TrimSpace(label),
		value,
		total,
		outcomePct(value, total),
	)
}

func outcomeSegmentHTML(leftPercent float64, widthPercent float64, visibleWidthPercent float64, className string, label string, tooltip string, placement string) string {
	if widthPercent <= 0 {
		return ""
	}
	segmentColor := outcomeSegmentColor(className)
	labelHTML := outcomeSegmentLabelHTML(className, widthPercent, visibleWidthPercent)
	return fmt.Sprintf(
		"<div class=\"inline-tooltip align-%s outcome-segment-wrap\" data-inline-tooltip style=\"left: %.6f%%; width: %.6f%%; background: %s;\"><span tabindex=\"0\" role=\"button\" class=\"inline-tooltip-trigger outcome-segment %s\" data-tooltip-trigger aria-label=\"%s\">%s</span><span class=\"inline-tooltip-panel\" role=\"tooltip\">%s</span></div>",
		html.EscapeString(placement),
		leftPercent,
		widthPercent,
		html.EscapeString(segmentColor),
		html.EscapeString(className),
		html.EscapeString(tooltip),
		labelHTML,
		html.EscapeString(tooltip),
	)
}

func renderOutcomeSegment(value int, total int, leftPercent float64, barWidthPercent float64, className string, label string, placement string) (string, float64) {
	if total <= 0 || value <= 0 {
		return "", leftPercent
	}
	widthPercent := outcomePct(value, total)
	visibleWidthPercent := widthPercent * barWidthPercent / 100.0
	tooltip := outcomeSegmentTooltip(label, value, total)
	segmentHTML := outcomeSegmentHTML(leftPercent, widthPercent, visibleWidthPercent, className, label, tooltip, placement)
	return segmentHTML, leftPercent + widthPercent
}

func outcomeSegmentColor(className string) string {
	switch strings.TrimSpace(className) {
	case "seg-success":
		return "#5f8a69"
	case "seg-provision":
		return "#ad8651"
	case "seg-e2e":
		return "#9f676d"
	case "seg-ciinfra":
		return "#708092"
	default:
		return "transparent"
	}
}

func outcomeSegmentLabelHTML(className string, labelPercent float64, visibleWidthPercent float64) string {
	label := outcomeSegmentVisibleLabel(labelPercent, visibleWidthPercent)
	if label == "" {
		return ""
	}
	labelClass := "outcome-segment-label"
	if outcomeSegmentUsesDarkLabel(className) {
		labelClass += " is-dark"
	}
	return fmt.Sprintf("<span class=\"%s\">%s</span>", html.EscapeString(labelClass), html.EscapeString(label))
}

func outcomeSegmentVisibleLabel(labelPercent float64, visibleWidthPercent float64) string {
	if visibleWidthPercent <= 10 {
		return ""
	}
	return fmt.Sprintf("%d%%", int(math.Round(labelPercent)))
}

func outcomeSegmentUsesDarkLabel(className string) bool {
	switch strings.TrimSpace(className) {
	case "seg-provision":
		return true
	default:
		return false
	}
}

func outcomePct(value int, total int) float64 {
	if total <= 0 || value <= 0 {
		return 0
	}
	return float64(value) * 100.0 / float64(total)
}

func executiveHeaderHTML(label string, tooltip string, placement string) string {
	return fmt.Sprintf(
		"<th><div class=\"exec-heading-wrap\"><span class=\"exec-heading-label\">%s</span>%s</div></th>",
		html.EscapeString(strings.TrimSpace(label)),
		frontui.HelpTooltipHTMLWithPlacement(strings.TrimSpace(tooltip), "exec-heading-help", placement),
	)
}

func cardHTML(label string, value any, tooltip string, placement string) string {
	var b strings.Builder
	b.WriteString("      <div class=\"card\">")
	if strings.TrimSpace(tooltip) != "" {
		b.WriteString("<div class=\"card-label-row\">")
		b.WriteString(fmt.Sprintf("<div class=\"label\">%s</div>", html.EscapeString(label)))
		b.WriteString(frontui.HelpTooltipHTMLWithPlacement(strings.TrimSpace(tooltip), "card-help", placement))
		b.WriteString("</div>")
	} else {
		b.WriteString(fmt.Sprintf("<div class=\"label\">%s</div>", html.EscapeString(label)))
	}
	b.WriteString(fmt.Sprintf("<div class=\"value\">%v</div>", value))
	b.WriteString("</div>\n")
	return b.String()
}

func tooltipPlacementForOrderedItems(index int, total int) string {
	if total <= 1 {
		return "center"
	}
	if total >= 6 {
		if index <= 1 {
			return "start"
		}
		if index >= total-2 {
			return "end"
		}
		return "center"
	}
	if index <= 0 {
		return "start"
	}
	if index >= total-1 {
		return "end"
	}
	return "center"
}

func outcomeChartMaxTotalRuns(days []outcomeChartDay) int {
	maxTotalRuns := 0
	for _, day := range days {
		if day.TotalRuns > maxTotalRuns {
			maxTotalRuns = day.TotalRuns
		}
	}
	return maxTotalRuns
}

func outcomeRelativeBarPct(totalRuns int, maxTotalRuns int) float64 {
	if totalRuns <= 0 || maxTotalRuns <= 0 {
		return 0
	}
	return float64(totalRuns) * 100.0 / float64(maxTotalRuns)
}

func renderOutcomeChart(
	title string,
	days []outcomeChartDay,
	environment string,
	runLogDayBasePath string,
	successLabel string,
) string {
	var b strings.Builder
	maxTotalRuns := outcomeChartMaxTotalRuns(days)
	b.WriteString(fmt.Sprintf("    <h3 class=\"chart-title\">%s</h3>\n", html.EscapeString(strings.TrimSpace(title))))
	b.WriteString("    <div class=\"legend\">\n")
	b.WriteString(fmt.Sprintf("      <span class=\"legend-item\"><span class=\"legend-swatch seg-success\"></span>%s</span>\n", html.EscapeString(strings.TrimSpace(successLabel))))
	b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-provision\"></span>Provision failures</span>\n")
	b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-e2e\"></span>E2E failures</span>\n")
	b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-ciinfra\"></span>Other failures</span>\n")
	b.WriteString("    </div>\n")
	b.WriteString("    <div class=\"outcomes\">\n")
	for _, day := range days {
		b.WriteString("      <div class=\"outcome-row\">")
		b.WriteString(fmt.Sprintf(
			"<div class=\"outcome-date\">%s</div>",
			weeklyOutcomeDateHTML(day.Date, environment, runLogDayBasePath),
		))
		b.WriteString("<div class=\"outcome-main\">")
		b.WriteString(fmt.Sprintf("<div class=\"outcome-meta\"><span class=\"outcome-total\">%s</span></div>", html.EscapeString(outcomeTotalLabel(day.TotalRuns))))
		b.WriteString("<div class=\"outcome-bar-shell\">")
		if day.TotalRuns <= 0 {
			b.WriteString("<div class=\"outcome-bar outcome-bar-empty\">No runs</div>")
		} else {
			barWidthPercent := outcomeRelativeBarPct(day.TotalRuns, maxTotalRuns)
			currentLeft := 0.0
			b.WriteString(fmt.Sprintf("<div class=\"outcome-bar\" style=\"width: %.6f%%;\">", barWidthPercent))
			var segmentHTML string
			segmentHTML, currentLeft = renderOutcomeSegment(day.SuccessfulRuns, day.TotalRuns, currentLeft, barWidthPercent, "seg-success", successLabel, "center")
			b.WriteString(segmentHTML)
			segmentHTML, currentLeft = renderOutcomeSegment(day.ProvisionFailedRuns, day.TotalRuns, currentLeft, barWidthPercent, "seg-provision", "Provision failures", "center")
			b.WriteString(segmentHTML)
			segmentHTML, currentLeft = renderOutcomeSegment(day.E2EFailedRuns, day.TotalRuns, currentLeft, barWidthPercent, "seg-e2e", "E2E failures", "center")
			b.WriteString(segmentHTML)
			segmentHTML, currentLeft = renderOutcomeSegment(day.CIInfraFailedRuns, day.TotalRuns, currentLeft, barWidthPercent, "seg-ciinfra", "Other failures", "center")
			b.WriteString(segmentHTML)
			b.WriteString("</div>")
		}
		b.WriteString("</div>")
		b.WriteString("</div>")
		b.WriteString("</div>\n")
	}
	b.WriteString("    </div>\n")
	return b.String()
}

func outcomeTotalLabel(totalRuns int) string {
	if totalRuns == 1 {
		return "1 run"
	}
	return fmt.Sprintf("%d runs", totalRuns)
}

func formatStepSuccessCardValue(totalAttempted int, successful int, failed int) string {
	if totalAttempted <= 0 {
		return "n/a"
	}
	if successful < 0 {
		successful = 0
	}
	if failed < 0 {
		failed = 0
	}
	return fmt.Sprintf("%.2f%% (%d/%d)", successPct(totalAttempted, failed), successful, totalAttempted)
}

func joinTooltipParts(parts ...string) string {
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

func goalBasisTooltip() string {
	return "Configured success target and run scope for this environment. INT/STG/PROD use all E2E job runs. DEV uses only runs that happened after the final push to a PR that later merged."
}

func afterLastPushMergedPRTooltip() string {
	return "These DEV-only metrics include only runs that happened after the final push to a PR that later merged. This gives a view of the signal closest to what actually landed, rather than earlier trial runs from the same PR."
}

func provisionSuccessTooltip() string {
	return "Provisioning here means infrastructure setup before E2E tests start. A run counts as provision-successful if it got past provisioning, even if it later failed during E2E. Runs that failed for 'Other' CI-infrastructure reasons are excluded because those runs never reached the provision step."
}

func e2eSuccessTooltip() string {
	return "Among runs that reached E2E execution, this shows how many completed successfully. Provision failures and 'Other' CI-infrastructure failures are excluded because those runs never reached the E2E step."
}

func afterLastPushE2EJobsTooltip() string {
	return joinTooltipParts(afterLastPushMergedPRTooltip(), "This card shows the number of runs in that DEV-only goal basis.")
}

func afterLastPushSuccessRateTooltip() string {
	return joinTooltipParts(afterLastPushMergedPRTooltip(), "This card shows the overall success rate for that DEV-only goal basis.")
}

func afterLastPushProvisionSuccessTooltip() string {
	return joinTooltipParts(afterLastPushMergedPRTooltip(), provisionSuccessTooltip())
}

func afterLastPushE2ESuccessTooltip() string {
	return joinTooltipParts(afterLastPushMergedPRTooltip(), e2eSuccessTooltip())
}

func executiveProvisionSuccessTooltip() string {
	return joinTooltipParts(provisionSuccessTooltip(), "DEV rows use only runs after the final push to a PR that later merged. INT/STG/PROD show n/a because provisioning is not part of those environments.")
}

func executiveProvisionChangeTooltip() string {
	return "Change in provision success, in percentage points, compared with the previous equal-length window. Uses the same provision-success definition as the Provision success column. DEV only; INT/STG/PROD show n/a."
}

func executiveE2ESuccessTooltip() string {
	return joinTooltipParts(e2eSuccessTooltip(), "DEV rows use only runs after the final push to a PR that later merged. INT/STG/PROD use all E2E job runs.")
}

func executiveE2EChangeTooltip() string {
	return "Change in E2E success, in percentage points, compared with the previous equal-length window. Uses the same E2E-success definition as the E2E success column."
}

func formatGoalDefinition(targetRate float64, goalBasis string) string {
	return fmt.Sprintf("%s%% - %s", formatGoalTargetRate(targetRate), strings.TrimSpace(goalBasis))
}

func formatGoalTargetRate(targetRate float64) string {
	trimmed := strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", targetRate), "0"), ".")
	if trimmed == "" {
		return "0"
	}
	return trimmed
}

func successPct(total int, failed int) float64 {
	if total <= 0 {
		return 0
	}
	successful := total - failed
	if successful < 0 {
		successful = 0
	}
	return float64(successful) * 100.0 / float64(total)
}

func formatSignedPercentPointCell(value float64) string {
	className := "pp-neutral"
	if value > 0 {
		className = "pp-positive"
	} else if value < 0 {
		className = "pp-negative"
	}
	return fmt.Sprintf("<span class=\"%s\">%+.2fpp</span>", className, value)
}

func normalizeReportEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func weeklyEnvironmentDateKey(environment string, date string) string {
	normalizedEnvironment := normalizeReportEnvironment(environment)
	trimmedDate := strings.TrimSpace(date)
	if normalizedEnvironment == "" || trimmedDate == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedDate
}

func dateFromTimestamp(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", false
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts.UTC().Format("2006-01-02"), true
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts.UTC().Format("2006-01-02"), true
	}
	return "", false
}

func formatSignedInt(value int) string {
	if value > 0 {
		return fmt.Sprintf("+%d", value)
	}
	return fmt.Sprintf("%d", value)
}

func goalBasisKPI(report envReport) (string, int, float64, bool) {
	environment := normalizeReportEnvironment(report.Environment)
	if environment == "dev" {
		postMergeTotals := summarizePostGoodRunOutcomes(report.Days)
		if postMergeTotals.TotalRuns <= 0 {
			return "After last push of a PR that merges", 0, 0, false
		}
		return "After last push of a PR that merges", postMergeTotals.TotalRuns, successPct(postMergeTotals.TotalRuns, postMergeTotals.FailedRuns), true
	}
	if report.Totals.RunCount <= 0 {
		return "All E2E job runs", 0, 0, false
	}
	return "All E2E job runs", report.Totals.RunCount, successPct(report.Totals.RunCount, report.Totals.FailureCount), true
}

func topFailedLaneForGoalBasis(report envReport) (string, int) {
	environment := normalizeReportEnvironment(report.Environment)
	if environment == "dev" {
		return topFailedLaneFromCounts(
			report.Totals.PostGoodFailedCIInfra,
			report.Totals.PostGoodFailedProvision,
			report.Totals.PostGoodFailedE2EJobs,
		)
	}
	return topFailedLane(report.Totals)
}

func topFailedLane(total counts) (string, int) {
	return topFailedLaneFromCounts(total.FailedCIInfraRunCount, total.FailedProvisionRunCount, total.FailedE2ERunCount)
}

func topFailedLaneFromCounts(ciInfraCount, provisionCount, e2eCount int) (string, int) {
	bestLane := "other"
	bestCount := ciInfraCount
	if provisionCount > bestCount {
		bestLane = "provision"
		bestCount = provisionCount
	}
	if e2eCount > bestCount {
		bestLane = "e2e"
		bestCount = e2eCount
	}
	return bestLane, bestCount
}

func targetStatus(successRate float64, targetRate float64) (string, string) {
	if successRate >= targetRate {
		return "status-on-track", "on track"
	}
	if successRate >= targetRate-5.0 {
		return "status-near-track", "near target"
	}
	return "status-off-track", "off track"
}

func cleanInline(input string, max int) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(input, "\n", " "), "\r", " "), "\t", " "))
	normalized = strings.Join(strings.Fields(normalized), " ")
	if max <= 0 {
		return normalized
	}
	runes := []rune(normalized)
	if len(runes) <= max {
		return normalized
	}
	return string(runes[:max-1]) + "..."
}

func failurePatternReportEnvironmentHref(baseHref string, environment string) string {
	trimmedBase := strings.TrimSpace(baseHref)
	if trimmedBase == "" {
		return ""
	}
	normalizedEnvironment := normalizeReportEnvironment(environment)
	if normalizedEnvironment == "" || strings.Contains(trimmedBase, "#") {
		return trimmedBase
	}
	return trimmedBase + "#env-" + normalizedEnvironment
}

func weeklyOutcomeDateHTML(date string, environment string, basePath string) string {
	trimmedDate := strings.TrimSpace(date)
	if trimmedDate == "" {
		return ""
	}
	label := trimmedDate + " UTC"
	href := weeklyRunsDayHref(basePath, trimmedDate, environment)
	if strings.TrimSpace(href) == "" {
		return html.EscapeString(label)
	}
	return fmt.Sprintf("<a href=\"%s\">%s</a>", html.EscapeString(href), html.EscapeString(label))
}

func weeklyRunsDayHref(basePath string, date string, environment string) string {
	trimmedBasePath := strings.TrimSpace(basePath)
	if trimmedBasePath == "" {
		return ""
	}
	if !strings.HasPrefix(trimmedBasePath, "/") && !strings.Contains(trimmedBasePath, "://") && !strings.HasPrefix(trimmedBasePath, ".") {
		trimmedBasePath = "/" + trimmedBasePath
	}
	values := url.Values{}
	if trimmedDate := strings.TrimSpace(date); trimmedDate != "" {
		values.Set("date", trimmedDate)
	}
	if normalizedEnvironment := normalizeReportEnvironment(environment); normalizedEnvironment != "" {
		values.Add("env", normalizedEnvironment)
	}
	if encoded := values.Encode(); encoded != "" {
		return trimmedBasePath + "?" + encoded
	}
	return trimmedBasePath
}
