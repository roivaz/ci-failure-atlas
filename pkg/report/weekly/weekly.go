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

	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

const (
	windowDays = 7

	metricRunCount                      = "run_count"
	metricFailureCount                  = "failure_count"
	metricFailureRowCount               = "failure_row_count"
	metricFailedCIInfraRunCount         = "failed_ci_infra_run_count"
	metricFailedProvisionRunCount       = "failed_provision_run_count"
	metricFailedE2ERunCount             = "failed_e2e_run_count"
	metricCIInfraFailureCount           = "ci_infra_failure_count"
	metricProvisionFailureCount         = "provision_failure_count"
	metricE2EFailureCount               = "e2e_failure_count"
	metricPostGoodRunCount              = "post_good_run_count"
	metricPostGoodFailureCount          = "post_good_failure_count"
	metricPostGoodFailedE2EJobs         = "post_good_failed_e2e_jobs"
	metricPostGoodCIInfraFailureCount   = "post_good_ci_infra_failure_count"
	metricPostGoodProvisionFailureCount = "post_good_provision_failure_count"
	metricPostGoodE2EFailureCount       = "post_good_e2e_failure_count"
)

const (
	runLaneCIInfra   = "ci_infra"
	runLaneProvision = "provision"
	runLaneE2E       = "e2e"
)

var reportEnvironments = []string{"dev", "int", "stg", "prod"}

type Options struct {
	OutputPath string
	StartDate  string
}

type validatedOptions struct {
	OutputPath string
	StartDate  time.Time
}

type counts struct {
	RunCount                      int
	FailureCount                  int
	FailureRowCount               int
	FailedCIInfraRunCount         int
	FailedProvisionRunCount       int
	FailedE2ERunCount             int
	CIInfraFailureCount           int
	ProvisionFailureCount         int
	E2EFailureCount               int
	PostGoodRunCount              int
	PostGoodFailureCount          int
	PostGoodFailedE2EJobs         int
	PostGoodCIInfraFailureCount   int
	PostGoodProvisionFailureCount int
	PostGoodE2EFailureCount       int
}

type runOutcomes struct {
	TotalRuns           int
	SuccessfulRuns      int
	CIInfraFailedRuns   int
	ProvisionFailedRuns int
	E2EFailedRuns       int
}

type dayReport struct {
	Date                string
	Counts              counts
	PostGoodRunOutcomes runOutcomes
}

type envReport struct {
	Environment string
	Days        []dayReport
	Totals      counts
}

func DefaultOptions() Options {
	return Options{
		OutputPath: "data/reports/weekly-metrics.html",
		StartDate:  "",
	}
}

func Generate(ctx context.Context, store storecontracts.Store, opts Options) error {
	validated, err := validateOptions(opts)
	if err != nil {
		return err
	}
	if store == nil {
		return fmt.Errorf("store is required")
	}

	dates := dateWindow(validated.StartDate, windowDays)
	reports := make([]envReport, 0, len(reportEnvironments))
	for _, env := range reportEnvironments {
		report := envReport{
			Environment: env,
			Days:        make([]dayReport, 0, len(dates)),
		}
		for _, date := range dates {
			rows, err := store.ListMetricsDailyByDate(ctx, env, date)
			if err != nil {
				return fmt.Errorf("list metrics for env=%q date=%q: %w", env, date, err)
			}
			dayCounts := collectCounts(rows)
			day := dayReport{
				Date:   date,
				Counts: dayCounts,
			}
			if env == "dev" {
				postGoodOutcomes, err := collectPostGoodRunOutcomes(ctx, store, env, date, dayCounts)
				if err != nil {
					return fmt.Errorf("collect post-good run outcomes for env=%q date=%q: %w", env, date, err)
				}
				day.PostGoodRunOutcomes = postGoodOutcomes
			}
			report.Days = append(report.Days, day)
			report.Totals = addCounts(report.Totals, dayCounts)
		}
		reports = append(reports, report)
	}

	startDate := validated.StartDate.UTC()
	endDate := startDate.AddDate(0, 0, windowDays-1)
	rendered := buildHTML(startDate, endDate, reports, time.Now().UTC())

	if err := os.MkdirAll(filepath.Dir(validated.OutputPath), 0o755); err != nil {
		return fmt.Errorf("create weekly report output directory: %w", err)
	}
	if err := os.WriteFile(validated.OutputPath, []byte(rendered), 0o644); err != nil {
		return fmt.Errorf("write weekly report: %w", err)
	}
	return nil
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
	return validatedOptions{
		OutputPath: outputPath,
		StartDate:  startDate.UTC(),
	}, nil
}

func dateWindow(startDate time.Time, days int) []string {
	if days <= 0 {
		return nil
	}
	out := make([]string, 0, days)
	for i := 0; i < days; i++ {
		out = append(out, startDate.AddDate(0, 0, i).Format("2006-01-02"))
	}
	return out
}

func collectCounts(rows []storecontracts.MetricDailyRecord) counts {
	out := counts{}
	for _, row := range rows {
		value := int(row.Value)
		switch strings.TrimSpace(row.Metric) {
		case metricRunCount:
			out.RunCount = value
		case metricFailureCount:
			out.FailureCount = value
		case metricFailureRowCount:
			out.FailureRowCount = value
		case metricFailedCIInfraRunCount:
			out.FailedCIInfraRunCount = value
		case metricFailedProvisionRunCount:
			out.FailedProvisionRunCount = value
		case metricFailedE2ERunCount:
			out.FailedE2ERunCount = value
		case metricCIInfraFailureCount:
			out.CIInfraFailureCount = value
		case metricProvisionFailureCount:
			out.ProvisionFailureCount = value
		case metricE2EFailureCount:
			out.E2EFailureCount = value
		case metricPostGoodRunCount:
			out.PostGoodRunCount = value
		case metricPostGoodFailureCount:
			out.PostGoodFailureCount = value
		case metricPostGoodFailedE2EJobs:
			out.PostGoodFailedE2EJobs = value
		case metricPostGoodCIInfraFailureCount:
			out.PostGoodCIInfraFailureCount = value
		case metricPostGoodProvisionFailureCount:
			out.PostGoodProvisionFailureCount = value
		case metricPostGoodE2EFailureCount:
			out.PostGoodE2EFailureCount = value
		}
	}
	return out
}

func addCounts(a counts, b counts) counts {
	return counts{
		RunCount:                      a.RunCount + b.RunCount,
		FailureCount:                  a.FailureCount + b.FailureCount,
		FailureRowCount:               a.FailureRowCount + b.FailureRowCount,
		FailedCIInfraRunCount:         a.FailedCIInfraRunCount + b.FailedCIInfraRunCount,
		FailedProvisionRunCount:       a.FailedProvisionRunCount + b.FailedProvisionRunCount,
		FailedE2ERunCount:             a.FailedE2ERunCount + b.FailedE2ERunCount,
		CIInfraFailureCount:           a.CIInfraFailureCount + b.CIInfraFailureCount,
		ProvisionFailureCount:         a.ProvisionFailureCount + b.ProvisionFailureCount,
		E2EFailureCount:               a.E2EFailureCount + b.E2EFailureCount,
		PostGoodRunCount:              a.PostGoodRunCount + b.PostGoodRunCount,
		PostGoodFailureCount:          a.PostGoodFailureCount + b.PostGoodFailureCount,
		PostGoodFailedE2EJobs:         a.PostGoodFailedE2EJobs + b.PostGoodFailedE2EJobs,
		PostGoodCIInfraFailureCount:   a.PostGoodCIInfraFailureCount + b.PostGoodCIInfraFailureCount,
		PostGoodProvisionFailureCount: a.PostGoodProvisionFailureCount + b.PostGoodProvisionFailureCount,
		PostGoodE2EFailureCount:       a.PostGoodE2EFailureCount + b.PostGoodE2EFailureCount,
	}
}

func buildHTML(startDate time.Time, endDate time.Time, reports []envReport, generatedAt time.Time) string {
	var b strings.Builder
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Weekly Metrics Report</title>\n")
	b.WriteString("  <style>\n")
	b.WriteString("    body { font-family: Arial, sans-serif; margin: 20px; color: #1f2937; }\n")
	b.WriteString("    h1 { margin-bottom: 4px; }\n")
	b.WriteString("    .meta { color: #4b5563; margin-bottom: 16px; }\n")
	b.WriteString("    .chart-controls { margin: 0 0 16px; font-size: 13px; color: #374151; display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }\n")
	b.WriteString("    .chart-controls label { display: inline-flex; align-items: center; gap: 6px; }\n")
	b.WriteString("    .env { border: 1px solid #e5e7eb; border-radius: 8px; margin: 14px 0; padding: 12px; }\n")
	b.WriteString("    .cards { display: flex; flex-wrap: wrap; gap: 8px; margin: 8px 0 12px; }\n")
	b.WriteString("    .cards.cards-post-good { margin-top: 0; }\n")
	b.WriteString("    .card { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 6px; padding: 8px 10px; min-width: 160px; }\n")
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
	b.WriteString("    body[data-chart-mode=\"count\"] .mode-percent { display: none; }\n")
	b.WriteString("    body[data-chart-mode=\"percent\"] .mode-count { display: none; }\n")
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body data-chart-mode=\"count\">\n")
	b.WriteString("  <h1>CI Weekly Metrics Report</h1>\n")
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Window: <strong>%s</strong> to <strong>%s</strong> (7 days) &middot; Generated: %s</p>\n",
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"),
		html.EscapeString(generatedAt.Format(time.RFC3339)),
	))
	b.WriteString("  <div class=\"chart-controls\">\n")
	b.WriteString("    <strong>Chart mode:</strong>\n")
	b.WriteString("    <label><input type=\"radio\" name=\"chart-mode\" value=\"count\" checked> Absolute counts</label>\n")
	b.WriteString("    <label><input type=\"radio\" name=\"chart-mode\" value=\"percent\"> 100% stacked percentages</label>\n")
	b.WriteString("  </div>\n")

	for _, report := range reports {
		envLabel := strings.ToUpper(strings.TrimSpace(report.Environment))
		envMaxRuns := maxRunCount(report.Days)
		b.WriteString(fmt.Sprintf("  <section class=\"env\">\n    <h2>Environment: %s</h2>\n", html.EscapeString(envLabel)))
		b.WriteString("    <div class=\"cards\">\n")
		b.WriteString(cardHTML("E2E Jobs", report.Totals.RunCount))
		b.WriteString(cardHTML("Failed E2E Jobs", report.Totals.FailureCount))
		b.WriteString(cardHTML("Failed Tests", report.Totals.FailureRowCount))
		b.WriteString(cardHTML("Success Rate", fmt.Sprintf("%.2f%%", successPct(report.Totals.RunCount, report.Totals.FailureCount))))
		b.WriteString("    </div>\n")
		if report.Environment == "dev" {
			postGoodTotals := summarizePostGoodRunOutcomes(report.Days)
			b.WriteString("    <div class=\"cards cards-post-good\">\n")
			b.WriteString(cardHTML("E2E Jobs (good commits)", postGoodTotals.TotalRuns))
			b.WriteString(cardHTML("Failed E2E Jobs (good commits)", postGoodTotals.FailedRuns))
			b.WriteString(cardHTML("Failed Tests (good commits)", report.Totals.PostGoodFailureCount))
			b.WriteString(cardHTML("Success Rate (good commits)", fmt.Sprintf("%.2f%%", successPct(postGoodTotals.TotalRuns, postGoodTotals.FailedRuns))))
			b.WriteString("    </div>\n")
		}
		b.WriteString("    <h3 class=\"chart-title\">Daily Run Outcomes (stacked by run-level lane)</h3>\n")
		b.WriteString("    <div class=\"legend\">\n")
		b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-success\"></span>Successful runs</span>\n")
		b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-provision\"></span>Provision failures</span>\n")
		b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-e2e\"></span>E2E failures</span>\n")
		b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-ciinfra\"></span>CI/Infra failures</span>\n")
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
				b.WriteString(outcomeSegmentHTML("seg-ciinfra", ciInfraFailedRuns, totalRuns, envMaxRuns, "CI/Infra failures"))
				b.WriteString("</div>")
			}
			b.WriteString("<div class=\"outcome-values\">")
			b.WriteString(fmt.Sprintf(
				"<span class=\"mode-count\">S:%d &nbsp; P:%d &nbsp; E2E:%d &nbsp; CI:%d</span>",
				successfulRuns,
				provisionFailedRuns,
				e2eFailedRuns,
				ciInfraFailedRuns,
			))
			b.WriteString(fmt.Sprintf(
				"<span class=\"mode-percent\">S:%.2f%% &nbsp; P:%.2f%% &nbsp; E2E:%.2f%% &nbsp; CI:%.2f%%</span>",
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
			b.WriteString("    <h3 class=\"chart-title\">Daily Run Outcomes for Good PRs (stacked by run-level lane)</h3>\n")
			b.WriteString("    <div class=\"legend\">\n")
			b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-success\"></span>Successful runs (good PR semantics)</span>\n")
			b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-provision\"></span>Provision failures</span>\n")
			b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-e2e\"></span>E2E failures</span>\n")
			b.WriteString("      <span class=\"legend-item\"><span class=\"legend-swatch seg-ciinfra\"></span>CI/Infra failures</span>\n")
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
					b.WriteString(outcomeSegmentHTML("seg-success", successfulRuns, totalRuns, envMaxRuns, "Successful runs (good PR semantics)"))
					b.WriteString(outcomeSegmentHTML("seg-provision", provisionFailedRuns, totalRuns, envMaxRuns, "Provision failures"))
					b.WriteString(outcomeSegmentHTML("seg-e2e", e2eFailedRuns, totalRuns, envMaxRuns, "E2E failures"))
					b.WriteString(outcomeSegmentHTML("seg-ciinfra", ciInfraFailedRuns, totalRuns, envMaxRuns, "CI/Infra failures"))
					b.WriteString("</div>")
				}
				b.WriteString("<div class=\"outcome-values\">")
				b.WriteString(fmt.Sprintf(
					"<span class=\"mode-count\">S:%d &nbsp; P:%d &nbsp; E2E:%d &nbsp; CI:%d</span>",
					successfulRuns,
					provisionFailedRuns,
					e2eFailedRuns,
					ciInfraFailedRuns,
				))
				b.WriteString(fmt.Sprintf(
					"<span class=\"mode-percent\">S:%.2f%% &nbsp; P:%.2f%% &nbsp; E2E:%.2f%% &nbsp; CI:%.2f%%</span>",
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
		b.WriteString("  </section>\n")
	}

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
	b.WriteString("  var radios = document.querySelectorAll('input[name=\"chart-mode\"]');\n")
	b.WriteString("  for (var i = 0; i < radios.length; i++) {\n")
	b.WriteString("    radios[i].addEventListener('change', function(e) {\n")
	b.WriteString("      if (e.target && e.target.checked) {\n")
	b.WriteString("        applyChartMode(e.target.value);\n")
	b.WriteString("      }\n")
	b.WriteString("    });\n")
	b.WriteString("  }\n")
	b.WriteString("  applyChartMode('count');\n")
	b.WriteString("})();\n")
	b.WriteString("</script>\n")
	b.WriteString("</body>\n</html>\n")
	return b.String()
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

func collectPostGoodRunOutcomes(
	ctx context.Context,
	store storecontracts.Store,
	environment string,
	date string,
	day counts,
) (runOutcomes, error) {
	out := runOutcomes{}

	rawRows, err := store.ListRawFailuresByDate(ctx, environment, date)
	if err != nil {
		return runOutcomes{}, err
	}

	failedLaneByRunURL := map[string]string{}
	runCache := map[string]storecontracts.RunRecord{}
	runFoundCache := map[string]bool{}
	for _, row := range rawRows {
		runURL := strings.TrimSpace(row.RunURL)
		if runURL == "" {
			continue
		}
		postGoodRun, err := isPostGoodRun(ctx, store, environment, runURL, runCache, runFoundCache)
		if err != nil {
			return runOutcomes{}, err
		}
		if !postGoodRun {
			continue
		}
		lane, err := classifyPostGoodRunLane(ctx, store, environment, row, runCache, runFoundCache)
		if err != nil {
			return runOutcomes{}, err
		}
		failedLaneByRunURL[runURL] = mergePostGoodFailedRunLane(failedLaneByRunURL[runURL], lane)
	}

	ciInfraFailedRuns := 0
	provisionFailedRuns := 0
	e2eFailedRuns := 0
	for _, lane := range failedLaneByRunURL {
		switch lane {
		case runLaneProvision:
			provisionFailedRuns++
		case runLaneE2E:
			e2eFailedRuns++
		default:
			ciInfraFailedRuns++
		}
	}

	// post_good_failed_e2e_jobs is the post-good failed-run denominator.
	postGoodFailedRuns := day.PostGoodFailedE2EJobs
	accountedFailedRuns := ciInfraFailedRuns + provisionFailedRuns + e2eFailedRuns
	if postGoodFailedRuns < accountedFailedRuns {
		postGoodFailedRuns = accountedFailedRuns
	}
	if postGoodFailedRuns > accountedFailedRuns {
		ciInfraFailedRuns += postGoodFailedRuns - accountedFailedRuns
	}
	totalRuns := day.PostGoodRunCount
	if totalRuns < postGoodFailedRuns {
		totalRuns = postGoodFailedRuns
	}
	successfulRuns := totalRuns - postGoodFailedRuns
	if successfulRuns < 0 {
		successfulRuns = 0
	}

	out.TotalRuns = totalRuns
	out.SuccessfulRuns = successfulRuns
	out.CIInfraFailedRuns = ciInfraFailedRuns
	out.ProvisionFailedRuns = provisionFailedRuns
	out.E2EFailedRuns = e2eFailedRuns
	return out, nil
}

func isPostGoodRun(
	ctx context.Context,
	store storecontracts.Store,
	environment string,
	runURL string,
	runCache map[string]storecontracts.RunRecord,
	runFoundCache map[string]bool,
) (bool, error) {
	normalizedRunURL := strings.TrimSpace(runURL)
	if normalizedRunURL == "" {
		return false, nil
	}
	if cachedFound, ok := runFoundCache[normalizedRunURL]; ok {
		if !cachedFound {
			return false, nil
		}
		return runCache[normalizedRunURL].PostGoodCommit, nil
	}
	run, found, err := store.GetRun(ctx, environment, normalizedRunURL)
	if err != nil {
		return false, err
	}
	runFoundCache[normalizedRunURL] = found
	if !found {
		return false, nil
	}
	runCache[normalizedRunURL] = run
	return run.PostGoodCommit, nil
}

type runOutcomesTotals struct {
	TotalRuns      int
	FailedRuns     int
	SuccessfulRuns int
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

func classifyPostGoodRunLane(
	ctx context.Context,
	store storecontracts.Store,
	environment string,
	row storecontracts.RawFailureRecord,
	runCache map[string]storecontracts.RunRecord,
	runFoundCache map[string]bool,
) (string, error) {
	if row.NonArtifactBacked {
		return runLaneCIInfra, nil
	}

	runURL := strings.TrimSpace(row.RunURL)
	jobName := ""
	if runURL != "" {
		if cachedFound, ok := runFoundCache[runURL]; ok {
			if cachedFound {
				jobName = strings.TrimSpace(runCache[runURL].JobName)
			}
		} else {
			run, found, err := store.GetRun(ctx, environment, runURL)
			if err != nil {
				return "", err
			}
			runFoundCache[runURL] = found
			if found {
				runCache[runURL] = run
				jobName = strings.TrimSpace(run.JobName)
			}
		}
	}

	switch deriveRunLane(jobName, row.TestName, row.TestSuite) {
	case runLaneProvision:
		return runLaneProvision, nil
	case runLaneE2E:
		return runLaneE2E, nil
	default:
		return runLaneCIInfra, nil
	}
}

func mergePostGoodFailedRunLane(current string, next string) string {
	currentRank := postGoodFailedRunLaneRank(current)
	nextRank := postGoodFailedRunLaneRank(next)
	if nextRank > currentRank {
		return normalizePostGoodFailedRunLane(next)
	}
	return normalizePostGoodFailedRunLane(current)
}

func postGoodFailedRunLaneRank(lane string) int {
	switch normalizePostGoodFailedRunLane(lane) {
	case runLaneProvision:
		return 3
	case runLaneE2E:
		return 2
	default:
		return 1
	}
}

func normalizePostGoodFailedRunLane(lane string) string {
	switch strings.TrimSpace(lane) {
	case runLaneProvision:
		return runLaneProvision
	case runLaneE2E:
		return runLaneE2E
	default:
		return runLaneCIInfra
	}
}

func deriveRunLane(jobName string, testName string, testSuite string) string {
	normalizedJob := strings.ToLower(strings.TrimSpace(jobName))
	normalizedName := strings.ToLower(strings.TrimSpace(testName))
	normalizedSuite := strings.ToLower(strings.TrimSpace(testSuite))

	switch {
	case strings.Contains(normalizedSuite, "step graph"):
		return runLaneProvision
	case strings.HasPrefix(normalizedName, "run pipeline step "):
		return runLaneProvision
	case strings.Contains(normalizedJob, "provision"):
		return runLaneProvision
	case strings.Contains(normalizedJob, "e2e"):
		return runLaneE2E
	default:
		return runLaneCIInfra
	}
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

func cardHTML(label string, value any) string {
	return fmt.Sprintf("      <div class=\"card\"><div class=\"label\">%s</div><div class=\"value\">%v</div></div>\n", html.EscapeString(label), value)
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
