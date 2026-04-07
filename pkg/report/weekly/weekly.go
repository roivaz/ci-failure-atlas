package weekly

import (
	"context"
	"errors"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"strings"
	"time"

	frontservice "ci-failure-atlas/pkg/frontend/service"
	"ci-failure-atlas/pkg/report/triagehtml"
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
	HistoryResolver     semhistory.GlobalSignatureResolver
	Chrome              triagehtml.ReportChromeOptions
}

type validatedOptions struct {
	OutputPath          string
	StartDate           time.Time
	TargetRate          float64
	Week                string
	HistoryHorizonWeeks int
	HistoryResolver     semhistory.GlobalSignatureResolver
	Chrome              triagehtml.ReportChromeOptions
}

type counts = frontservice.WeeklyCounts
type runOutcomes = frontservice.WeeklyRunOutcomes
type dayReport = frontservice.WeeklyDayReport
type envReport = frontservice.WeeklyEnvReport
type semanticEnvSummary = frontservice.WeeklySemanticEnvSummary
type semanticSnapshot = frontservice.WeeklySemanticSnapshot
type belowTargetTest = frontservice.WeeklyBelowTargetTest
type topSignature = frontservice.WeeklyTopSignature

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
		data.CurrentSemantic,
		data.PreviousSemantic,
		data.TestsBelowTargetByEnv,
		data.TopSignaturesByEnv,
		data.HistoryResolver,
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
		return validatedOptions{}, fmt.Errorf("invalid --start-date %q (expected YYYY-MM-DD Sunday start): %w", startDateRaw, err)
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
		Chrome:              opts.Chrome,
	}, nil
}

func buildHTML(
	startDate time.Time,
	endDate time.Time,
	reports []envReport,
	previousReports []envReport,
	targetRate float64,
	currentSemantic semanticSnapshot,
	previousSemantic semanticSnapshot,
	testsBelowTargetByEnv map[string][]belowTargetTest,
	topSignaturesByEnv map[string][]topSignature,
	historyResolver semhistory.GlobalSignatureResolver,
	chrome triagehtml.ReportChromeOptions,
) string {
	var b strings.Builder
	globalTriageBaseHref := strings.TrimSpace(chrome.GlobalHref)
	if globalTriageBaseHref == "" {
		globalTriageBaseHref = "global-signature-triage.html"
	}
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Weekly Report</title>\n")
	b.WriteString(triagehtml.ThemeInitScriptTag())
	b.WriteString("  <style>\n")
	b.WriteString("    body { font-family: Arial, sans-serif; margin: 20px; color: #1f2937; }\n")
	b.WriteString("    h1 { margin-bottom: 4px; }\n")
	b.WriteString("    .meta { color: #4b5563; margin-bottom: 16px; }\n")
	b.WriteString("    .chart-controls { margin: 0 0 16px; font-size: 13px; color: #374151; display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }\n")
	b.WriteString("    .chart-controls label { display: inline-flex; align-items: center; gap: 6px; }\n")
	b.WriteString("    .env { border: 1px solid #e5e7eb; border-radius: 8px; margin: 14px 0; padding: 12px; }\n")
	b.WriteString("    .overview-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 10px 0 16px; }\n")
	b.WriteString("    .overview-table th, .overview-table td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; vertical-align: top; }\n")
	b.WriteString("    .overview-table th { background: #f3f4f6; color: #374151; font-weight: 700; }\n")
	b.WriteString("    .exec-heading-help { border-bottom: 1px dotted #9ca3af; cursor: help; }\n")
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
	b.WriteString("    .label { font-size: 12px; color: #6b7280; }\n")
	b.WriteString("    .value { font-size: 18px; font-weight: 700; }\n")
	b.WriteString("    .chart-title { margin: 4px 0 6px; font-size: 14px; }\n")
	b.WriteString("    .legend { display: flex; gap: 12px; flex-wrap: wrap; font-size: 12px; color: #4b5563; margin: 4px 0 8px; }\n")
	b.WriteString("    .legend-item { display: inline-flex; align-items: center; gap: 6px; }\n")
	b.WriteString("    .legend-swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }\n")
	b.WriteString("    .outcomes { display: flex; flex-direction: column; gap: 6px; margin-bottom: 12px; }\n")
	b.WriteString("    .outcome-row { display: grid; grid-template-columns: 95px 1fr 275px; align-items: center; gap: 8px; font-size: 12px; }\n")
	b.WriteString("    .outcome-date { color: #374151; font-weight: 600; }\n")
	b.WriteString("    .outcome-bar { height: 14px; background: #f3f4f6; border: 1px solid #e5e7eb; border-radius: 999px; overflow: hidden; display: flex; }\n")
	b.WriteString("    .outcome-bar-empty { display: flex; justify-content: center; align-items: center; color: #9ca3af; font-size: 11px; }\n")
	b.WriteString("    .outcome-segment { display: flex; align-items: center; justify-content: center; height: 100%; min-width: 0; overflow: hidden; }\n")
	b.WriteString("    .segment-label { font-size: 10px; font-weight: 700; color: #ffffff; text-shadow: 0 1px 1px rgba(0,0,0,0.45); white-space: nowrap; padding: 0 2px; }\n")
	b.WriteString("    .outcome-segment.label-hidden .segment-label { display: none; }\n")
	b.WriteString("    .seg-success { background: #22c55e; }\n")
	b.WriteString("    .seg-provision { background: #f97316; }\n")
	b.WriteString("    .seg-e2e { background: #ef4444; }\n")
	b.WriteString("    .seg-ciinfra { background: #eab308; }\n")
	b.WriteString("    .seg-ciinfra .segment-label { color: #1f2937; text-shadow: none; }\n")
	b.WriteString("    .outcome-values { color: #4b5563; font-size: 11px; text-align: right; white-space: nowrap; }\n")
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
	b.WriteString(triagehtml.ReportChromeCSS())
	b.WriteString(triagehtml.StylesCSS())
	b.WriteString(triagehtml.ThemeCSS())
	b.WriteString("    body[data-chart-mode=\"count\"] .mode-percent { display: none; }\n")
	b.WriteString("    body[data-chart-mode=\"percent\"] .mode-count { display: none; }\n")
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body data-chart-mode=\"count\">\n")
	b.WriteString(triagehtml.ReportChromeHTML(chrome))
	b.WriteString("  <h1>CI Weekly Report</h1>\n")
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Window: <strong>%s</strong> to <strong>%s</strong> (7 days)</p>\n",
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"),
	))
	b.WriteString("  <div class=\"meta\">Goals:<br/>- e2e-integration, e2e-stage, e2e-prod job runs should each succeed 95% of the time<br/>- e2e-dev job runs should succeed 95% of the time after the last push of a PR that merges</div>\n")

	previousByEnvironment := map[string]envReport{}
	for _, report := range previousReports {
		previousByEnvironment[normalizeReportEnvironment(report.Environment)] = report
	}

	b.WriteString("  <section class=\"env\">\n")
	b.WriteString("    <h2>Executive Status (Week-over-Week)</h2>\n")
	b.WriteString("    <table class=\"overview-table\">\n")
	b.WriteString("      <thead><tr>")
	b.WriteString(executiveHeaderHTML("Env", "Environment partition: dev, int, stg, or prod."))
	b.WriteString(executiveHeaderHTML("Goal basis", "INT/STG/PROD use all E2E job runs. DEV uses runs after the last push of a PR that merges."))
	b.WriteString(executiveHeaderHTML("Runs", "Number of job runs in the selected goal basis for this environment."))
	b.WriteString(executiveHeaderHTML("Success", "Success rate on the goal basis: (runs - failed runs) / runs * 100."))
	b.WriteString(executiveHeaderHTML("Gap vs target", "Difference in percentage points between current success and the configured target rate."))
	b.WriteString(executiveHeaderHTML("Change WoW", "How much the success rate changed compared with last week, using the same run scope as this row."))
	b.WriteString(executiveHeaderHTML("Provision success", "DEV-only provision-step estimate on runs after last push of a merged PR. INT/STG/PROD show n/a because provisioning is not part of those environments. Formula: (successful + e2e_failed) / (successful + provision_failed + e2e_failed)."))
	b.WriteString(executiveHeaderHTML("Provision change WoW", "DEV-only week-over-week change in provision-step success, in percentage points. INT/STG/PROD show n/a."))
	b.WriteString(executiveHeaderHTML("E2E success", "E2E-step success on the same goal basis used in this row (DEV: runs after last push of a merged PR; INT/STG/PROD: all runs). Formula: successful / (successful + e2e_failed)."))
	b.WriteString(executiveHeaderHTML("E2E success WoW", "Week-over-week change in E2E-step success, in percentage points, using the same goal basis as this row."))
	b.WriteString("</tr></thead>\n")
	b.WriteString("      <tbody>\n")
	_ = currentSemantic
	_ = previousSemantic
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
		b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(goalBasis)))
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

	b.WriteString("  <div class=\"chart-controls\">\n")
	b.WriteString("    <strong>Chart mode:</strong>\n")
	b.WriteString("    <label><input type=\"radio\" name=\"chart-mode\" value=\"count\" checked> Absolute counts</label>\n")
	b.WriteString("    <label><input type=\"radio\" name=\"chart-mode\" value=\"percent\"> 100% stacked percentages</label>\n")
	b.WriteString("  </div>\n")

	for _, report := range reports {
		environment := normalizeReportEnvironment(report.Environment)
		envLabel := strings.ToUpper(strings.TrimSpace(report.Environment))
		envMaxRuns := maxRunCount(report.Days)
		lanePanelID := fmt.Sprintf("drill-%s-lane", environment)
		testsPanelID := fmt.Sprintf("drill-%s-tests", environment)
		signaturesPanelID := fmt.Sprintf("drill-%s-signatures", environment)
		b.WriteString(fmt.Sprintf("  <section class=\"env\">\n    <h2>Environment: %s</h2>\n", html.EscapeString(envLabel)))
		b.WriteString("    <div class=\"drill-tabs\" role=\"tablist\" aria-label=\"Drill-down views\"")
		b.WriteString(fmt.Sprintf(" data-env=\"%s\">\n", html.EscapeString(environment)))
		b.WriteString(fmt.Sprintf("      <button type=\"button\" class=\"drill-tab active\" role=\"tab\" aria-selected=\"true\" data-target=\"%s\">Lane outcomes</button>\n", html.EscapeString(lanePanelID)))
		b.WriteString(fmt.Sprintf("      <button type=\"button\" class=\"drill-tab\" role=\"tab\" aria-selected=\"false\" data-target=\"%s\">Tests below %.0f%%</button>\n", html.EscapeString(testsPanelID), weeklyTestSuccessTarget))
		b.WriteString(fmt.Sprintf("      <button type=\"button\" class=\"drill-tab\" role=\"tab\" aria-selected=\"false\" data-target=\"%s\">Top failure signatures</button>\n", html.EscapeString(signaturesPanelID)))
		b.WriteString("    </div>\n")

		b.WriteString(fmt.Sprintf("    <div id=\"%s\" class=\"drill-panel\" data-env=\"%s\" role=\"tabpanel\">\n", html.EscapeString(lanePanelID), html.EscapeString(environment)))
		cardsClass := "cards"
		if report.Environment == "dev" {
			cardsClass = "cards cards-dev"
		}
		b.WriteString(fmt.Sprintf("    <div class=\"%s\">\n", html.EscapeString(cardsClass)))
		b.WriteString(cardHTML("E2E Jobs", report.Totals.RunCount))
		b.WriteString(cardHTML("Success Rate", fmt.Sprintf("%.2f%%", successPct(report.Totals.RunCount, report.Totals.FailureCount))))
		allRunsE2E := summarizeE2EStepOutcomes(report.Days)
		if report.Environment == "dev" {
			provisionStep := summarizeProvisionStepOutcomes(report.Days)
			provisionStepValue := formatStepSuccessCardValue(provisionStep.TotalAttempted, provisionStep.Successful, provisionStep.Failed)
			b.WriteString(cardHTML("Provision step success rate (Other excluded)", provisionStepValue))
			b.WriteString(cardHTML("E2E success (runs reaching E2E)", formatStepSuccessCardValue(allRunsE2E.TotalAttempted, allRunsE2E.Successful, allRunsE2E.Failed)))
		} else {
			b.WriteString(cardHTML("E2E success (runs reaching E2E)", formatStepSuccessCardValue(allRunsE2E.TotalAttempted, allRunsE2E.Successful, allRunsE2E.Failed)))
		}
		b.WriteString("    </div>\n")
		if report.Environment == "dev" {
			postGoodTotals := summarizePostGoodRunOutcomes(report.Days)
			postGoodProvision := summarizeProvisionStepOutcomesForGoalBasis(report)
			postGoodE2E := summarizeE2EStepOutcomesForGoalBasis(report)
			b.WriteString("    <div class=\"cards cards-post-good cards-dev\">\n")
			b.WriteString(cardHTML("E2E Jobs (after last push of merged PR)", postGoodTotals.TotalRuns))
			b.WriteString(cardHTML("Success Rate (after last push of merged PR)", fmt.Sprintf("%.2f%%", successPct(postGoodTotals.TotalRuns, postGoodTotals.FailedRuns))))
			b.WriteString(cardHTML(
				"Provision success (after last push of merged PR)",
				formatStepSuccessCardValue(postGoodProvision.TotalAttempted, postGoodProvision.Successful, postGoodProvision.Failed),
			))
			b.WriteString(cardHTML(
				"E2E success (after last push of merged PR)",
				formatStepSuccessCardValue(postGoodE2E.TotalAttempted, postGoodE2E.Successful, postGoodE2E.Failed),
			))
			b.WriteString("    </div>\n")
		}
		b.WriteString("    <h3 class=\"chart-title\">Daily Run Outcomes (stacked by run-level lane)</h3>\n")
		b.WriteString("    <div class=\"legend\">\n")
		b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-success\"></span>Successful runs</span>\n")
		b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-provision\"></span>Provision failures</span>\n")
		b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-e2e\"></span>E2E failures</span>\n")
		b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-ciinfra\"></span>Other failures</span>\n")
		b.WriteString("    </div>\n")
		b.WriteString("    <div class=\"outcomes\">\n")
		for _, day := range report.Days {
			successfulRuns, ciInfraFailedRuns, provisionFailedRuns, e2eFailedRuns := dailyRunOutcomeCounts(day.Counts)
			totalRuns := day.Counts.RunCount
			b.WriteString("      <div class=\"outcome-row\">")
			b.WriteString(fmt.Sprintf("<div class=\"outcome-date\">%s</div>", html.EscapeString(day.Date)))
			if totalRuns <= 0 {
				b.WriteString("<div class=\"outcome-bar outcome-bar-empty\">No runs</div>")
			} else {
				b.WriteString("<div class=\"outcome-bar\">")
				b.WriteString(outcomeSegmentHTML("seg-success", successfulRuns, totalRuns, envMaxRuns, "Successful runs"))
				b.WriteString(outcomeSegmentHTML("seg-provision", provisionFailedRuns, totalRuns, envMaxRuns, "Provision failures"))
				b.WriteString(outcomeSegmentHTML("seg-e2e", e2eFailedRuns, totalRuns, envMaxRuns, "E2E failures"))
				b.WriteString(outcomeSegmentHTML("seg-ciinfra", ciInfraFailedRuns, totalRuns, envMaxRuns, "Other failures"))
				b.WriteString("</div>")
			}
			b.WriteString("<div class=\"outcome-values\">")
			b.WriteString(fmt.Sprintf(
				"<span class=\"mode-count\">S:%d &nbsp; P:%d &nbsp; E2E:%d &nbsp; Other:%d</span>",
				successfulRuns,
				provisionFailedRuns,
				e2eFailedRuns,
				ciInfraFailedRuns,
			))
			b.WriteString(fmt.Sprintf(
				"<span class=\"mode-percent\">S:%.2f%% &nbsp; P:%.2f%% &nbsp; E2E:%.2f%% &nbsp; Other:%.2f%%</span>",
				outcomePct(successfulRuns, totalRuns),
				outcomePct(provisionFailedRuns, totalRuns),
				outcomePct(e2eFailedRuns, totalRuns),
				outcomePct(ciInfraFailedRuns, totalRuns),
			))
			b.WriteString("</div>")
			b.WriteString("</div>\n")
		}
		b.WriteString("    </div>\n")
		if report.Environment == "dev" {
			b.WriteString("    <h3 class=\"chart-title\">Daily Run Outcomes for DEV Goal Basis (after last push of merged PR)</h3>\n")
			b.WriteString("    <div class=\"legend\">\n")
			b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-success\"></span>Successful runs (after last push of merged PR)</span>\n")
			b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-provision\"></span>Provision failures</span>\n")
			b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-e2e\"></span>E2E failures</span>\n")
			b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-ciinfra\"></span>Other failures</span>\n")
			b.WriteString("    </div>\n")
			b.WriteString("    <div class=\"outcomes\">\n")
			for _, day := range report.Days {
				successfulRuns := day.PostGoodRunOutcomes.SuccessfulRuns
				ciInfraFailedRuns := day.PostGoodRunOutcomes.CIInfraFailedRuns
				provisionFailedRuns := day.PostGoodRunOutcomes.ProvisionFailedRuns
				e2eFailedRuns := day.PostGoodRunOutcomes.E2EFailedRuns
				totalRuns := day.PostGoodRunOutcomes.TotalRuns
				b.WriteString("      <div class=\"outcome-row\">")
				b.WriteString(fmt.Sprintf("<div class=\"outcome-date\">%s</div>", html.EscapeString(day.Date)))
				if totalRuns <= 0 {
					b.WriteString("<div class=\"outcome-bar outcome-bar-empty\">No runs</div>")
				} else {
					b.WriteString("<div class=\"outcome-bar\">")
					b.WriteString(outcomeSegmentHTML("seg-success", successfulRuns, totalRuns, envMaxRuns, "Successful runs (after last push of merged PR)"))
					b.WriteString(outcomeSegmentHTML("seg-provision", provisionFailedRuns, totalRuns, envMaxRuns, "Provision failures"))
					b.WriteString(outcomeSegmentHTML("seg-e2e", e2eFailedRuns, totalRuns, envMaxRuns, "E2E failures"))
					b.WriteString(outcomeSegmentHTML("seg-ciinfra", ciInfraFailedRuns, totalRuns, envMaxRuns, "Other failures"))
					b.WriteString("</div>")
				}
				b.WriteString("<div class=\"outcome-values\">")
				b.WriteString(fmt.Sprintf(
					"<span class=\"mode-count\">S:%d &nbsp; P:%d &nbsp; E2E:%d &nbsp; Other:%d</span>",
					successfulRuns,
					provisionFailedRuns,
					e2eFailedRuns,
					ciInfraFailedRuns,
				))
				b.WriteString(fmt.Sprintf(
					"<span class=\"mode-percent\">S:%.2f%% &nbsp; P:%.2f%% &nbsp; E2E:%.2f%% &nbsp; Other:%.2f%%</span>",
					outcomePct(successfulRuns, totalRuns),
					outcomePct(provisionFailedRuns, totalRuns),
					outcomePct(e2eFailedRuns, totalRuns),
					outcomePct(ciInfraFailedRuns, totalRuns),
				))
				b.WriteString("</div>")
				b.WriteString("</div>\n")
			}
			b.WriteString("    </div>\n")
		}
		b.WriteString("    </div>\n")

		b.WriteString(fmt.Sprintf("    <div id=\"%s\" class=\"drill-panel\" data-env=\"%s\" role=\"tabpanel\" hidden>\n", html.EscapeString(testsPanelID), html.EscapeString(environment)))
		tests := testsBelowTargetByEnv[environment]
		if len(tests) == 0 {
			b.WriteString(fmt.Sprintf("      <p class=\"panel-empty\">No tests below %.2f%% in this window with at least %d runs.</p>\n", weeklyTestSuccessTarget, weeklyTestSuccessMinRuns))
		} else {
			b.WriteString("      <table class=\"detail-table\">\n")
			b.WriteString("        <thead><tr><th>Pass rate</th><th>Runs</th><th>Date</th><th>Suite</th><th>Test</th></tr></thead>\n")
			b.WriteString("        <tbody>\n")
			for _, item := range tests {
				suite := cleanInline(item.TestSuite, 80)
				if suite == "" {
					suite = "n/a"
				}
				b.WriteString("          <tr>")
				b.WriteString(fmt.Sprintf("<td>%.2f%%</td>", item.PassRate))
				b.WriteString(fmt.Sprintf("<td>%d</td>", item.Runs))
				b.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(item.Date)))
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
			"      <p class=\"panel-note\">Up to %d failures are loaded in this window (minimum %.2f%% impact), with %d shown by default. Default sorting is impact desc, jobs affected desc, flake score desc; click headers to re-sort.</p>\n",
			weeklySignatureLoadedRowsLimit,
			weeklySignatureMinImpactPct,
			weeklySignatureVisibleRows,
		))
		if triageReportHref := globalTriageEnvironmentHref(globalTriageBaseHref, environment); triageReportHref != "" {
			b.WriteString(fmt.Sprintf(
				"      <p class=\"panel-note\"><a href=\"%s\">Jump to Global signature triage for this week</a></p>\n",
				html.EscapeString(triageReportHref),
			))
		}
		signatures := topSignaturesByEnv[environment]
		if len(signatures) == 0 {
			b.WriteString("      <p class=\"panel-empty\">No semantic signatures available for this environment in the selected semantic week.</p>\n")
		} else {
			triageRows := make([]triagehtml.SignatureRow, 0, len(signatures))
			for _, item := range signatures {
				if weeklyTopSignatureImpactPercent(item, report.Totals.RunCount) < weeklySignatureMinImpactPct {
					continue
				}
				triageRow := topSignatureToTriageRow(item)
				if historyResolver != nil {
					presence := semhistory.SignaturePresence{}
					if len(triageRow.LinkedChildren) > 0 && strings.TrimSpace(item.ClusterID) != "" {
						presence = historyResolver.PresenceForPhase3Cluster(item.Environment, item.ClusterID)
					} else {
						presence = historyResolver.PresenceFor(semhistory.SignatureKey{
							Environment: item.Environment,
							Phrase:      item.Phrase,
							SearchQuery: item.SearchQuery,
						})
					}
					triageRow.PriorWeeksPresent = presence.PriorWeeksPresent
					triageRow.PriorWeekStarts = append([]string(nil), presence.PriorWeekStarts...)
					triageRow.PriorJobsAffected = presence.PriorJobsAffected
					if !presence.PriorLastSeenAt.IsZero() {
						triageRow.PriorLastSeenAt = presence.PriorLastSeenAt.UTC().Format(time.RFC3339)
					}
				}
				if sparkline, counts, sparkRange, ok := triagehtml.DailyDensitySparkline(
					triageRow.References,
					windowDays,
					endDate,
				); ok {
					triageRow.TrendSparkline = sparkline
					triageRow.TrendCounts = append([]int(nil), counts...)
					triageRow.TrendRange = sparkRange
				}
				triageRows = append(triageRows, triageRow)
			}
			if len(triageRows) == 0 {
				b.WriteString(fmt.Sprintf("      <p class=\"panel-empty\">No failures meet the minimum %.2f%% impact threshold in this environment.</p>\n", weeklySignatureMinImpactPct))
			} else {
				b.WriteString(triagehtml.RenderTable(triageRows, triagehtml.TableOptions{
					IncludeTrend:       true,
					ImpactTotalJobs:    report.Totals.RunCount,
					LoadedRowsLimit:    weeklySignatureLoadedRowsLimit,
					InitialVisibleRows: weeklySignatureVisibleRows,
				}))
			}
		}
		b.WriteString("    </div>\n")
		b.WriteString("  </section>\n")
	}

	b.WriteString(triagehtml.ThemeToggleScriptTag())
	b.WriteString("<script>\n")
	b.WriteString("(function(){\n")
	b.WriteString("  function applyChartMode(mode) {\n")
	b.WriteString("    document.body.setAttribute('data-chart-mode', mode);\n")
	b.WriteString("    var attr = mode === 'percent' ? 'data-width-percent' : 'data-width-count';\n")
	b.WriteString("    var segments = document.querySelectorAll('.outcome-segment');\n")
	b.WriteString("    for (var i = 0; i < segments.length; i++) {\n")
	b.WriteString("      var segment = segments[i];\n")
	b.WriteString("      var width = segment.getAttribute(attr) || '0';\n")
	b.WriteString("      segment.style.width = width + '%';\n")
	b.WriteString("      var widthValue = parseFloat(width);\n")
	b.WriteString("      if (!isFinite(widthValue)) { widthValue = 0; }\n")
	b.WriteString("      if (widthValue < 12) {\n")
	b.WriteString("        segment.classList.add('label-hidden');\n")
	b.WriteString("      } else {\n")
	b.WriteString("        segment.classList.remove('label-hidden');\n")
	b.WriteString("      }\n")
	b.WriteString("    }\n")
	b.WriteString("  }\n")
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
	b.WriteString("  var radios = document.querySelectorAll('input[name=\"chart-mode\"]');\n")
	b.WriteString("  for (var i = 0; i < radios.length; i++) {\n")
	b.WriteString("    radios[i].addEventListener('change', function(e) {\n")
	b.WriteString("      if (e.target && e.target.checked) {\n")
	b.WriteString("        applyChartMode(e.target.value);\n")
	b.WriteString("      }\n")
	b.WriteString("    });\n")
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
	b.WriteString("  applyChartMode('count');\n")
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
		return len(triagehtml.OrderedUniqueReferences(item.References))
	}
	total := 0
	for _, child := range item.LinkedChildren {
		total += len(triagehtml.OrderedUniqueReferences(child.References))
	}
	if total > 0 {
		return total
	}
	return len(triagehtml.OrderedUniqueReferences(item.References))
}

func topSignatureToTriageRow(item topSignature) triagehtml.SignatureRow {
	row := triagehtml.SignatureRow{
		Environment:       item.Environment,
		Phrase:            item.Phrase,
		ClusterID:         item.ClusterID,
		SearchQuery:       item.SearchQuery,
		SupportCount:      item.SupportCount,
		SupportShare:      item.SupportShare,
		PostGoodCount:     item.PostGoodCount,
		AlsoSeenIn:        append([]string(nil), item.SeenInOtherEnvs...),
		QualityScore:      item.QualityScore,
		QualityNoteLabels: append([]string(nil), item.QualityNoteLabels...),
		ContributingTests: append([]triagehtml.ContributingTest(nil), item.ContributingTests...),
		FullErrorSamples:  append([]string(nil), item.FullErrorSamples...),
		References:        append([]triagehtml.RunReference(nil), item.References...),
	}
	if len(item.LinkedChildren) == 0 {
		return row
	}
	row.LinkedChildren = make([]triagehtml.SignatureRow, 0, len(item.LinkedChildren))
	for _, child := range item.LinkedChildren {
		childRow := topSignatureToTriageRow(child)
		childRow.LinkedChildren = nil
		row.LinkedChildren = append(row.LinkedChildren, childRow)
	}
	return row
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

func maxRunCount(days []dayReport) int {
	max := 0
	for _, day := range days {
		if day.Counts.RunCount > max {
			max = day.Counts.RunCount
		}
	}
	return max
}

func outcomeSegmentHTML(className string, value int, total int, max int, label string) string {
	if total <= 0 || value <= 0 || max <= 0 {
		return ""
	}
	widthCount := float64(value) * 100.0 / float64(max)
	widthPercent := float64(value) * 100.0 / float64(total)
	return fmt.Sprintf(
		"<span class=\"outcome-segment %s\" style=\"width: %.6f%%\" data-width-count=\"%.6f\" data-width-percent=\"%.6f\" title=\"%s: %d (%.2f%%)\"><span class=\"segment-label\">%.1f%%</span></span>",
		className,
		widthCount,
		widthCount,
		widthPercent,
		html.EscapeString(label),
		value,
		widthPercent,
		widthPercent,
	)
}

func outcomePct(value int, total int) float64 {
	if total <= 0 || value <= 0 {
		return 0
	}
	return float64(value) * 100.0 / float64(total)
}

func executiveHeaderHTML(label string, tooltip string) string {
	return fmt.Sprintf(
		"<th><span class=\"exec-heading-help\" title=\"%s\">%s</span></th>",
		html.EscapeString(strings.TrimSpace(tooltip)),
		html.EscapeString(strings.TrimSpace(label)),
	)
}

func cardHTML(label string, value any) string {
	return fmt.Sprintf("      <div class=\"card\"><div class=\"label\">%s</div><div class=\"value\">%v</div></div>\n", html.EscapeString(label), value)
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

func globalTriageEnvironmentHref(baseHref string, environment string) string {
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
