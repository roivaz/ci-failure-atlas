package weekly

import (
	"context"
	"errors"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"ci-failure-atlas/pkg/report/triagehtml"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	phase3engine "ci-failure-atlas/pkg/semantic/engine/phase3"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

const (
	windowDays = 7

	metricRunCount                = "run_count"
	metricFailureCount            = "failure_count"
	metricFailedCIInfraRunCount   = "failed_ci_infra_run_count"
	metricFailedProvisionRunCount = "failed_provision_run_count"
	metricFailedE2ERunCount       = "failed_e2e_run_count"
	metricPostGoodRunCount        = "post_good_run_count"
	metricPostGoodFailedE2EJobs   = "post_good_failed_e2e_jobs"
	metricPostGoodFailedCIInfra   = "post_good_failed_ci_infra_run_count"
	metricPostGoodFailedProvision = "post_good_failed_provision_run_count"

	weeklyTestsBelowTargetTopLimit   = 5
	weeklySignatureLoadedRowsLimit   = 50
	weeklySignatureVisibleRows       = 10
	weeklySignatureMinImpactPct      = 1.0
	weeklySippyDefaultPeriod         = "default"
	weeklyTestSuccessTarget          = 95.0
	weeklyTestSuccessMinRuns         = 10
	weeklySignatureFullErrorExamples = 3
)

var reportEnvironments = []string{"dev", "int", "stg", "prod"}

type Options struct {
	OutputPath           string
	StartDate            string
	TargetRate           float64
	DataDirectory        string
	SemanticSubdirectory string
	HistoryHorizonWeeks  int
	HistoryResolver      semhistory.GlobalSignatureResolver
	Chrome               triagehtml.ReportChromeOptions
}

type validatedOptions struct {
	OutputPath           string
	StartDate            time.Time
	TargetRate           float64
	DataDirectory        string
	SemanticSubdirectory string
	HistoryHorizonWeeks  int
	HistoryResolver      semhistory.GlobalSignatureResolver
	Chrome               triagehtml.ReportChromeOptions
}

type counts struct {
	RunCount                int
	FailureCount            int
	FailedCIInfraRunCount   int
	FailedProvisionRunCount int
	FailedE2ERunCount       int
	PostGoodRunCount        int
	PostGoodFailedE2EJobs   int
	PostGoodFailedCIInfra   int
	PostGoodFailedProvision int
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

type semanticEnvSummary struct {
	GlobalClusters int
	TestClusters   int
	ReviewItems    int
	TopPhrase      string
	TopSupport     int
	TopPostGood    int
}

type semanticSnapshot struct {
	ByEnvironment                    map[string]semanticEnvSummary
	ClusterSignaturesByEnv           map[string][]topSignature
	PhraseSupportByEnv               map[string]map[string]int
	PhrasePostGoodByEnv              map[string]map[string]int
	PhraseReferencesByEnv            map[string]map[string][]triagehtml.RunReference
	PhraseContributingTestsByEnv     map[string]map[string][]triagehtml.ContributingTest
	PhraseClusterIDByEnv             map[string]map[string]string
	PhraseSearchQueryByEnv           map[string]map[string]string
	PhraseRepresentativeSupportByEnv map[string]map[string]int
	PhraseSignatureIDs               map[string]map[string]map[string]struct{}
	PhraseFullErrorsByEnv            map[string]map[string][]string
}

type belowTargetTest struct {
	TestName  string
	TestSuite string
	Date      string
	PassRate  float64
	Runs      int
}

type topSignature struct {
	Environment       string
	Phrase            string
	ClusterID         string
	SearchQuery       string
	SupportCount      int
	SupportShare      float64
	PostGoodCount     int
	BadPRScore        int
	SeenInOtherEnvs   []string
	QualityScore      int
	QualityNoteLabels []string
	ContributingTests []triagehtml.ContributingTest
	References        []triagehtml.RunReference
	FullErrorSamples  []string
	LinkedChildren    []topSignature
}

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

	currentDates := dateWindow(validated.StartDate, windowDays)
	currentReports, err := buildEnvReports(ctx, store, currentDates)
	if err != nil {
		return err
	}

	previousStart := validated.StartDate.AddDate(0, 0, -windowDays)
	previousDates := dateWindow(previousStart, windowDays)
	previousReports, err := buildEnvReports(ctx, store, previousDates)
	if err != nil {
		return err
	}

	currentSemantic, err := loadSemanticSnapshot(ctx, store)
	if err != nil {
		return fmt.Errorf("load current semantic snapshot: %w", err)
	}
	rawFailureRows, err := store.ListRawFailures(ctx)
	if err != nil {
		return fmt.Errorf("list raw failures: %w", err)
	}
	loadSignatureFullErrorSamplesByEnvironment(
		currentDates,
		rawFailureRows,
		&currentSemantic,
		weeklySignatureFullErrorExamples,
	)
	testsBelowTargetByEnv, err := loadBelowTargetTestsByEnvironment(
		ctx,
		store,
		currentDates,
		weeklySippyDefaultPeriod,
		weeklyTestSuccessTarget,
		weeklyTestSuccessMinRuns,
		weeklyTestsBelowTargetTopLimit,
	)
	if err != nil {
		return fmt.Errorf("load weekly tests below target: %w", err)
	}
	topSignaturesByEnv := rankTopSignaturesByEnvironment(currentSemantic, 0, 0)
	var previousSemantic semanticSnapshot
	if previousSemanticStore != nil {
		previousSemantic, err = loadSemanticSnapshot(ctx, previousSemanticStore)
		if err != nil {
			return fmt.Errorf("load previous semantic snapshot: %w", err)
		}
	}
	historyResolver := validated.HistoryResolver
	if historyResolver == nil {
		historyResolver, err = semhistory.BuildGlobalSignatureResolver(ctx, semhistory.BuildOptions{
			DataDirectory:                validated.DataDirectory,
			CurrentSemanticSubdir:        validated.SemanticSubdirectory,
			GlobalSignatureLookbackWeeks: validated.HistoryHorizonWeeks,
		})
		if err != nil {
			return fmt.Errorf("build global signature history resolver: %w", err)
		}
	}

	startDate := validated.StartDate.UTC()
	endDate := startDate.AddDate(0, 0, windowDays-1)
	rendered := buildHTML(
		startDate,
		endDate,
		currentReports,
		previousReports,
		validated.TargetRate,
		currentSemantic,
		previousSemantic,
		testsBelowTargetByEnv,
		topSignaturesByEnv,
		historyResolver,
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

func buildEnvReports(ctx context.Context, store storecontracts.Store, dates []string) ([]envReport, error) {
	metricsByEnvironmentDate, err := loadMetricsDailyByEnvironmentDate(ctx, store, reportEnvironments, dates)
	if err != nil {
		return nil, err
	}
	reports := make([]envReport, 0, len(reportEnvironments))
	for _, env := range reportEnvironments {
		report := envReport{
			Environment: env,
			Days:        make([]dayReport, 0, len(dates)),
		}
		for _, date := range dates {
			rows := metricsByEnvironmentDate[weeklyEnvironmentDateKey(env, date)]
			dayCounts := collectCounts(rows)
			day := dayReport{
				Date:   date,
				Counts: dayCounts,
			}
			if env == "dev" {
				day.PostGoodRunOutcomes = collectPostGoodRunOutcomes(dayCounts)
			}
			report.Days = append(report.Days, day)
			report.Totals = addCounts(report.Totals, dayCounts)
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func loadMetricsDailyByEnvironmentDate(
	ctx context.Context,
	store storecontracts.Store,
	environments []string,
	dates []string,
) (map[string][]storecontracts.MetricDailyRecord, error) {
	rows, err := store.ListMetricsDaily(ctx)
	if err != nil {
		return nil, fmt.Errorf("list metrics daily: %w", err)
	}
	environmentSet := map[string]struct{}{}
	for _, environment := range environments {
		normalized := normalizeReportEnvironment(environment)
		if normalized == "" {
			continue
		}
		environmentSet[normalized] = struct{}{}
	}
	dateSet := map[string]struct{}{}
	for _, date := range dates {
		trimmed := strings.TrimSpace(date)
		if trimmed == "" {
			continue
		}
		dateSet[trimmed] = struct{}{}
	}
	out := make(map[string][]storecontracts.MetricDailyRecord, len(environmentSet)*len(dateSet))
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		if _, ok := environmentSet[environment]; !ok {
			continue
		}
		date := strings.TrimSpace(row.Date)
		if _, ok := dateSet[date]; !ok {
			continue
		}
		key := weeklyEnvironmentDateKey(environment, date)
		out[key] = append(out[key], row)
	}
	for key := range out {
		metricRows := out[key]
		sort.Slice(metricRows, func(i, j int) bool {
			return strings.TrimSpace(metricRows[i].Metric) < strings.TrimSpace(metricRows[j].Metric)
		})
		out[key] = metricRows
	}
	return out, nil
}

func loadSemanticSnapshot(ctx context.Context, store storecontracts.Store) (semanticSnapshot, error) {
	out := semanticSnapshot{
		ByEnvironment:                    map[string]semanticEnvSummary{},
		ClusterSignaturesByEnv:           map[string][]topSignature{},
		PhraseSupportByEnv:               map[string]map[string]int{},
		PhrasePostGoodByEnv:              map[string]map[string]int{},
		PhraseReferencesByEnv:            map[string]map[string][]triagehtml.RunReference{},
		PhraseContributingTestsByEnv:     map[string]map[string][]triagehtml.ContributingTest{},
		PhraseClusterIDByEnv:             map[string]map[string]string{},
		PhraseSearchQueryByEnv:           map[string]map[string]string{},
		PhraseRepresentativeSupportByEnv: map[string]map[string]int{},
		PhraseSignatureIDs:               map[string]map[string]map[string]struct{}{},
		PhraseFullErrorsByEnv:            map[string]map[string][]string{},
	}

	sourceGlobalClusters, err := store.ListGlobalClusters(ctx)
	if err != nil {
		return out, err
	}
	phase3Links, err := store.ListPhase3Links(ctx)
	if err != nil {
		return out, err
	}
	linkedChildrenByMergedClusterKey, err := weeklyLinkedChildrenByMergedClusterKey(sourceGlobalClusters, phase3Links)
	if err != nil {
		return out, err
	}
	globalClusters, err := phase3engine.Merge(sourceGlobalClusters, phase3Links)
	if err != nil {
		return out, fmt.Errorf("merge phase3 linked global clusters: %w", err)
	}
	for _, row := range globalClusters {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		summary := out.ByEnvironment[environment]
		summary.GlobalClusters++

		phrase := strings.TrimSpace(row.CanonicalEvidencePhrase)
		if phrase == "" {
			phrase = "(unknown evidence)"
		}
		support := row.SupportCount
		if support < 0 {
			support = 0
		}
		postGood := row.PostGoodCommitCount
		if postGood < 0 {
			postGood = 0
		}

		if support > summary.TopSupport || (support == summary.TopSupport && (summary.TopPhrase == "" || phrase < summary.TopPhrase)) {
			summary.TopPhrase = phrase
			summary.TopSupport = support
			summary.TopPostGood = postGood
		}
		out.ByEnvironment[environment] = summary

		if _, ok := out.PhraseSupportByEnv[environment]; !ok {
			out.PhraseSupportByEnv[environment] = map[string]int{}
		}
		out.PhraseSupportByEnv[environment][phrase] += support

		if _, ok := out.PhrasePostGoodByEnv[environment]; !ok {
			out.PhrasePostGoodByEnv[environment] = map[string]int{}
		}
		out.PhrasePostGoodByEnv[environment][phrase] += postGood

		if _, ok := out.PhraseReferencesByEnv[environment]; !ok {
			out.PhraseReferencesByEnv[environment] = map[string][]triagehtml.RunReference{}
		}
		out.PhraseReferencesByEnv[environment][phrase] = append(
			out.PhraseReferencesByEnv[environment][phrase],
			toTriageRunReferences(row.References)...,
		)
		if sourceRunURL := strings.TrimSpace(row.SearchQuerySourceRunURL); sourceRunURL != "" {
			out.PhraseReferencesByEnv[environment][phrase] = append(
				out.PhraseReferencesByEnv[environment][phrase],
				triagehtml.RunReference{
					RunURL:      sourceRunURL,
					SignatureID: strings.TrimSpace(row.SearchQuerySourceSignatureID),
				},
			)
		}

		if _, ok := out.PhraseContributingTestsByEnv[environment]; !ok {
			out.PhraseContributingTestsByEnv[environment] = map[string][]triagehtml.ContributingTest{}
		}
		out.PhraseContributingTestsByEnv[environment][phrase] = mergeTriageContributingTests(
			out.PhraseContributingTestsByEnv[environment][phrase],
			toTriageContributingTests(row.ContributingTests),
		)

		if _, ok := out.PhraseRepresentativeSupportByEnv[environment]; !ok {
			out.PhraseRepresentativeSupportByEnv[environment] = map[string]int{}
		}
		if _, ok := out.PhraseClusterIDByEnv[environment]; !ok {
			out.PhraseClusterIDByEnv[environment] = map[string]string{}
		}
		if _, ok := out.PhraseSearchQueryByEnv[environment]; !ok {
			out.PhraseSearchQueryByEnv[environment] = map[string]string{}
		}
		repSupport := out.PhraseRepresentativeSupportByEnv[environment][phrase]
		if support > repSupport || strings.TrimSpace(out.PhraseClusterIDByEnv[environment][phrase]) == "" {
			out.PhraseRepresentativeSupportByEnv[environment][phrase] = support
			out.PhraseClusterIDByEnv[environment][phrase] = strings.TrimSpace(row.Phase2ClusterID)
			out.PhraseSearchQueryByEnv[environment][phrase] = strings.TrimSpace(row.SearchQueryPhrase)
		}

		if _, ok := out.PhraseSignatureIDs[environment]; !ok {
			out.PhraseSignatureIDs[environment] = map[string]map[string]struct{}{}
		}
		if _, ok := out.PhraseSignatureIDs[environment][phrase]; !ok {
			out.PhraseSignatureIDs[environment][phrase] = map[string]struct{}{}
		}
		signatureIDs := out.PhraseSignatureIDs[environment][phrase]
		for _, signatureID := range row.MemberSignatureIDs {
			trimmedSignatureID := strings.TrimSpace(signatureID)
			if trimmedSignatureID == "" {
				continue
			}
			signatureIDs[trimmedSignatureID] = struct{}{}
		}
		if sourceSignatureID := strings.TrimSpace(row.SearchQuerySourceSignatureID); sourceSignatureID != "" {
			signatureIDs[sourceSignatureID] = struct{}{}
		}
		for _, ref := range row.References {
			signatureID := strings.TrimSpace(ref.SignatureID)
			if signatureID == "" {
				continue
			}
			signatureIDs[signatureID] = struct{}{}
		}

		qualityCodes := triagehtml.QualityIssueCodes(strings.TrimSpace(phrase))
		qualityLabels := make([]string, 0, len(qualityCodes))
		for _, code := range qualityCodes {
			qualityLabels = append(qualityLabels, triagehtml.QualityIssueLabel(code))
		}
		linkedChildren := []topSignature(nil)
		linkedChildrenRaw := linkedChildrenByMergedClusterKey[weeklyGlobalClusterKey(environment, row.Phase2ClusterID)]
		if len(linkedChildrenRaw) > 0 {
			linkedChildren = topSignaturesFromGlobalClusters(linkedChildrenRaw)
		}
		rowReferences := toTriageRunReferences(row.References)
		if sourceRunURL := strings.TrimSpace(row.SearchQuerySourceRunURL); sourceRunURL != "" {
			rowReferences = append(rowReferences, triagehtml.RunReference{
				RunURL:      sourceRunURL,
				SignatureID: strings.TrimSpace(row.SearchQuerySourceSignatureID),
			})
		}
		out.ClusterSignaturesByEnv[environment] = append(out.ClusterSignaturesByEnv[environment], topSignature{
			Environment:       environment,
			Phrase:            strings.TrimSpace(phrase),
			ClusterID:         strings.TrimSpace(row.Phase2ClusterID),
			SearchQuery:       strings.TrimSpace(row.SearchQueryPhrase),
			SupportCount:      support,
			PostGoodCount:     postGood,
			QualityScore:      triagehtml.QualityScore(qualityCodes),
			QualityNoteLabels: qualityLabels,
			ContributingTests: triagehtml.OrderedContributingTests(toTriageContributingTests(row.ContributingTests)),
			References:        rowReferences,
			LinkedChildren:    linkedChildren,
		})
	}

	// Keep phrase sample lookup aware of child phrases after phase3 merge.
	for _, row := range sourceGlobalClusters {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		phrase := strings.TrimSpace(row.CanonicalEvidencePhrase)
		if phrase == "" {
			phrase = "(unknown evidence)"
		}
		if _, ok := out.PhraseSignatureIDs[environment]; !ok {
			out.PhraseSignatureIDs[environment] = map[string]map[string]struct{}{}
		}
		if _, ok := out.PhraseSignatureIDs[environment][phrase]; !ok {
			out.PhraseSignatureIDs[environment][phrase] = map[string]struct{}{}
		}
		signatureIDs := out.PhraseSignatureIDs[environment][phrase]
		for _, signatureID := range row.MemberSignatureIDs {
			trimmedSignatureID := strings.TrimSpace(signatureID)
			if trimmedSignatureID == "" {
				continue
			}
			signatureIDs[trimmedSignatureID] = struct{}{}
		}
		if sourceSignatureID := strings.TrimSpace(row.SearchQuerySourceSignatureID); sourceSignatureID != "" {
			signatureIDs[sourceSignatureID] = struct{}{}
		}
		for _, ref := range row.References {
			signatureID := strings.TrimSpace(ref.SignatureID)
			if signatureID == "" {
				continue
			}
			signatureIDs[signatureID] = struct{}{}
		}
	}

	testClusters, err := store.ListTestClusters(ctx)
	if err != nil {
		return out, err
	}
	for _, row := range testClusters {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		summary := out.ByEnvironment[environment]
		summary.TestClusters++
		out.ByEnvironment[environment] = summary
	}

	reviewItems, err := store.ListReviewQueue(ctx)
	if err != nil {
		return out, err
	}
	for _, row := range reviewItems {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		summary := out.ByEnvironment[environment]
		summary.ReviewItems++
		out.ByEnvironment[environment] = summary
	}

	return out, nil
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
	if opts.HistoryHorizonWeeks <= 0 {
		opts.HistoryHorizonWeeks = 4
	}
	return validatedOptions{
		OutputPath:           outputPath,
		StartDate:            startDate.UTC(),
		TargetRate:           opts.TargetRate,
		DataDirectory:        strings.TrimSpace(opts.DataDirectory),
		SemanticSubdirectory: strings.TrimSpace(opts.SemanticSubdirectory),
		HistoryHorizonWeeks:  opts.HistoryHorizonWeeks,
		HistoryResolver:      opts.HistoryResolver,
		Chrome:               opts.Chrome,
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
		case metricFailedCIInfraRunCount:
			out.FailedCIInfraRunCount = value
		case metricFailedProvisionRunCount:
			out.FailedProvisionRunCount = value
		case metricFailedE2ERunCount:
			out.FailedE2ERunCount = value
		case metricPostGoodRunCount:
			out.PostGoodRunCount = value
		case metricPostGoodFailedE2EJobs:
			out.PostGoodFailedE2EJobs = value
		case metricPostGoodFailedCIInfra:
			out.PostGoodFailedCIInfra = value
		case metricPostGoodFailedProvision:
			out.PostGoodFailedProvision = value
		}
	}
	return out
}

func addCounts(a counts, b counts) counts {
	return counts{
		RunCount:                a.RunCount + b.RunCount,
		FailureCount:            a.FailureCount + b.FailureCount,
		FailedCIInfraRunCount:   a.FailedCIInfraRunCount + b.FailedCIInfraRunCount,
		FailedProvisionRunCount: a.FailedProvisionRunCount + b.FailedProvisionRunCount,
		FailedE2ERunCount:       a.FailedE2ERunCount + b.FailedE2ERunCount,
		PostGoodRunCount:        a.PostGoodRunCount + b.PostGoodRunCount,
		PostGoodFailedE2EJobs:   a.PostGoodFailedE2EJobs + b.PostGoodFailedE2EJobs,
		PostGoodFailedCIInfra:   a.PostGoodFailedCIInfra + b.PostGoodFailedCIInfra,
		PostGoodFailedProvision: a.PostGoodFailedProvision + b.PostGoodFailedProvision,
	}
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
			b.WriteString("      <p class=\"panel-empty\">No semantic signatures available for this environment in the selected semantic snapshot.</p>\n")
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
					GitHubRepoOwner:    triagehtml.DefaultGitHubRepoOwner,
					GitHubRepoName:     triagehtml.DefaultGitHubRepoName,
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

func loadBelowTargetTestsByEnvironment(
	ctx context.Context,
	store storecontracts.Store,
	dates []string,
	period string,
	targetPassRate float64,
	minRuns int,
	limit int,
) (map[string][]belowTargetTest, error) {
	out := make(map[string][]belowTargetTest, len(reportEnvironments))
	trimmedPeriod := strings.TrimSpace(period)
	windowEndDate := ""
	for i := len(dates) - 1; i >= 0; i-- {
		candidate := strings.TrimSpace(dates[i])
		if candidate == "" {
			continue
		}
		windowEndDate = candidate
		break
	}
	if windowEndDate == "" {
		return out, nil
	}

	metricDates, err := store.ListMetricDates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list metric dates for test metadata date selection: %w", err)
	}
	candidateDatesAfter := metadataDatesAfter(metricDates, windowEndDate)
	candidateDatesBefore := metadataDatesBefore(metricDates, windowEndDate)

	for _, environment := range reportEnvironments {
		selectedDate, selectErr := firstMetadataDateForEnvironment(
			ctx,
			store,
			environment,
			trimmedPeriod,
			candidateDatesAfter,
		)
		if selectErr != nil {
			return nil, selectErr
		}
		if selectedDate == "" {
			selectedDate, selectErr = firstMetadataDateForEnvironment(
				ctx,
				store,
				environment,
				trimmedPeriod,
				candidateDatesBefore,
			)
			if selectErr != nil {
				return nil, selectErr
			}
		}
		if selectedDate == "" {
			out[environment] = nil
			continue
		}

		filtered, _, collectErr := collectBelowTargetTestsForDates(
			ctx,
			store,
			environment,
			[]string{selectedDate},
			trimmedPeriod,
			targetPassRate,
			minRuns,
			limit,
		)
		if collectErr != nil {
			return nil, collectErr
		}
		out[environment] = filtered
	}
	return out, nil
}

func collectBelowTargetTestsForDates(
	ctx context.Context,
	store storecontracts.Store,
	environment string,
	dates []string,
	period string,
	targetPassRate float64,
	minRuns int,
	limit int,
) ([]belowTargetTest, bool, error) {
	bestByTestKey := map[string]belowTargetTest{}
	hadRows := false
	for _, date := range dates {
		rows, err := store.ListTestMetadataDailyByDate(ctx, environment, date)
		if err != nil {
			return nil, hadRows, fmt.Errorf("list test metadata daily for env=%q date=%q: %w", environment, date, err)
		}
		for _, row := range rows {
			if period != "" && strings.TrimSpace(row.Period) != period {
				continue
			}
			testName := strings.TrimSpace(row.TestName)
			if testName == "" {
				continue
			}
			hadRows = true
			testSuite := strings.TrimSpace(row.TestSuite)
			candidate := belowTargetTest{
				TestName:  testName,
				TestSuite: testSuite,
				Date:      strings.TrimSpace(row.Date),
				PassRate:  row.CurrentPassPercentage,
				Runs:      row.CurrentRuns,
			}
			key := strings.ToLower(testSuite) + "|" + strings.ToLower(testName)
			existing, exists := bestByTestKey[key]
			if !exists || preferBelowTargetTest(candidate, existing) {
				bestByTestKey[key] = candidate
			}
		}
	}

	filtered := make([]belowTargetTest, 0, len(bestByTestKey))
	for _, candidate := range bestByTestKey {
		if candidate.Runs < minRuns || candidate.PassRate >= targetPassRate {
			continue
		}
		filtered = append(filtered, candidate)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].PassRate != filtered[j].PassRate {
			return filtered[i].PassRate < filtered[j].PassRate
		}
		if filtered[i].Runs != filtered[j].Runs {
			return filtered[i].Runs > filtered[j].Runs
		}
		if filtered[i].TestSuite != filtered[j].TestSuite {
			return filtered[i].TestSuite < filtered[j].TestSuite
		}
		return filtered[i].TestName < filtered[j].TestName
	})
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered, hadRows, nil
}

func firstMetadataDateForEnvironment(
	ctx context.Context,
	store storecontracts.Store,
	environment string,
	period string,
	candidateDates []string,
) (string, error) {
	for _, date := range candidateDates {
		rows, err := store.ListTestMetadataDailyByDate(ctx, environment, date)
		if err != nil {
			return "", fmt.Errorf("list test metadata daily for env=%q date=%q: %w", environment, date, err)
		}
		for _, row := range rows {
			if period != "" && strings.TrimSpace(row.Period) != period {
				continue
			}
			if strings.TrimSpace(row.TestName) == "" {
				continue
			}
			return date, nil
		}
	}
	return "", nil
}

func metadataDatesAfter(metricDates []string, threshold string) []string {
	trimmedThreshold := strings.TrimSpace(threshold)
	unique := map[string]struct{}{}
	for _, date := range metricDates {
		trimmed := strings.TrimSpace(date)
		if trimmed == "" {
			continue
		}
		if trimmedThreshold != "" && trimmed <= trimmedThreshold {
			continue
		}
		unique[trimmed] = struct{}{}
	}
	out := make([]string, 0, len(unique))
	for value := range unique {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func metadataDatesBefore(metricDates []string, threshold string) []string {
	trimmedThreshold := strings.TrimSpace(threshold)
	unique := map[string]struct{}{}
	for _, date := range metricDates {
		trimmed := strings.TrimSpace(date)
		if trimmed == "" {
			continue
		}
		if trimmedThreshold != "" && trimmed >= trimmedThreshold {
			continue
		}
		unique[trimmed] = struct{}{}
	}
	out := make([]string, 0, len(unique))
	for value := range unique {
		out = append(out, value)
	}
	sort.Strings(out)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func preferBelowTargetTest(candidate belowTargetTest, existing belowTargetTest) bool {
	if candidate.Date != existing.Date {
		return candidate.Date > existing.Date
	}
	if candidate.Runs != existing.Runs {
		return candidate.Runs > existing.Runs
	}
	if candidate.PassRate != existing.PassRate {
		return candidate.PassRate < existing.PassRate
	}
	if candidate.TestSuite != existing.TestSuite {
		return candidate.TestSuite < existing.TestSuite
	}
	return candidate.TestName < existing.TestName
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

func rankTopSignaturesByEnvironment(snapshot semanticSnapshot, limit int, minShare float64) map[string][]topSignature {
	if len(snapshot.ClusterSignaturesByEnv) > 0 {
		return rankTopSignaturesByEnvironmentFromClusters(snapshot, limit, minShare)
	}
	return rankTopSignaturesByEnvironmentFromPhrases(snapshot, limit, minShare)
}

func rankTopSignaturesByEnvironmentFromClusters(snapshot semanticSnapshot, limit int, minShare float64) map[string][]topSignature {
	out := make(map[string][]topSignature, len(reportEnvironments))
	for _, environment := range reportEnvironments {
		totalSupport := 0
		clusterRows := snapshot.ClusterSignaturesByEnv[environment]
		for _, item := range clusterRows {
			support := item.SupportCount
			if support > 0 {
				totalSupport += support
			}
		}

		rows := make([]topSignature, 0, len(clusterRows))
		for _, source := range clusterRows {
			phrase := strings.TrimSpace(source.Phrase)
			if phrase == "" {
				phrase = "(unknown evidence)"
			}
			support := source.SupportCount
			if support <= 0 {
				continue
			}
			otherEnvironments := make([]string, 0, len(reportEnvironments)-1)
			for _, candidateEnvironment := range reportEnvironments {
				if candidateEnvironment == environment {
					continue
				}
				if snapshot.PhraseSupportByEnv[candidateEnvironment][phrase] <= 0 {
					continue
				}
				otherEnvironments = append(otherEnvironments, strings.ToUpper(candidateEnvironment))
			}
			share := 0.0
			if totalSupport > 0 {
				share = float64(support) * 100.0 / float64(totalSupport)
			}
			if minShare > 0 && share < minShare {
				continue
			}
			references := append([]triagehtml.RunReference(nil), source.References...)
			badPRScore, _ := triagehtml.BadPRScoreAndReasons(triagehtml.SignatureRow{
				Environment:   environment,
				PostGoodCount: source.PostGoodCount,
				AlsoSeenIn:    otherEnvironments,
				References:    references,
			})
			linkedChildren := make([]topSignature, 0, len(source.LinkedChildren))
			for _, child := range source.LinkedChildren {
				childEnvironment := normalizeReportEnvironment(child.Environment)
				if childEnvironment == "" {
					childEnvironment = environment
				}
				childPhrase := strings.TrimSpace(child.Phrase)
				if childPhrase == "" {
					childPhrase = "(unknown evidence)"
				}
				childSupport := child.SupportCount
				childShare := 0.0
				if totalSupport > 0 && childSupport > 0 {
					childShare = float64(childSupport) * 100.0 / float64(totalSupport)
				}
				linkedChildren = append(linkedChildren, topSignature{
					Environment:       childEnvironment,
					Phrase:            childPhrase,
					ClusterID:         strings.TrimSpace(child.ClusterID),
					SearchQuery:       strings.TrimSpace(child.SearchQuery),
					SupportCount:      childSupport,
					SupportShare:      childShare,
					PostGoodCount:     child.PostGoodCount,
					QualityScore:      child.QualityScore,
					QualityNoteLabels: append([]string(nil), child.QualityNoteLabels...),
					ContributingTests: append([]triagehtml.ContributingTest(nil), child.ContributingTests...),
					References:        append([]triagehtml.RunReference(nil), child.References...),
					FullErrorSamples:  append([]string(nil), snapshot.PhraseFullErrorsByEnv[childEnvironment][childPhrase]...),
				})
			}
			rows = append(rows, topSignature{
				Environment:       environment,
				Phrase:            phrase,
				ClusterID:         strings.TrimSpace(source.ClusterID),
				SearchQuery:       strings.TrimSpace(source.SearchQuery),
				SupportCount:      support,
				SupportShare:      share,
				PostGoodCount:     source.PostGoodCount,
				BadPRScore:        badPRScore,
				SeenInOtherEnvs:   otherEnvironments,
				QualityScore:      source.QualityScore,
				QualityNoteLabels: append([]string(nil), source.QualityNoteLabels...),
				ContributingTests: append([]triagehtml.ContributingTest(nil), source.ContributingTests...),
				References:        references,
				FullErrorSamples:  append([]string(nil), snapshot.PhraseFullErrorsByEnv[environment][phrase]...),
				LinkedChildren:    linkedChildren,
			})
		}
		sortTopSignatures(rows)
		if limit > 0 && len(rows) > limit {
			rows = rows[:limit]
		}
		out[environment] = rows
	}
	return out
}

func rankTopSignaturesByEnvironmentFromPhrases(snapshot semanticSnapshot, limit int, minShare float64) map[string][]topSignature {
	out := make(map[string][]topSignature, len(reportEnvironments))
	for _, environment := range reportEnvironments {
		supportByPhrase := snapshot.PhraseSupportByEnv[environment]
		postGoodByPhrase := snapshot.PhrasePostGoodByEnv[environment]
		totalSupport := 0
		for _, support := range supportByPhrase {
			if support > 0 {
				totalSupport += support
			}
		}

		rows := make([]topSignature, 0, len(supportByPhrase))
		for phrase, support := range supportByPhrase {
			if support <= 0 {
				continue
			}
			otherEnvironments := make([]string, 0, len(reportEnvironments)-1)
			for _, candidateEnvironment := range reportEnvironments {
				if candidateEnvironment == environment {
					continue
				}
				if snapshot.PhraseSupportByEnv[candidateEnvironment][phrase] <= 0 {
					continue
				}
				otherEnvironments = append(otherEnvironments, strings.ToUpper(candidateEnvironment))
			}
			share := 0.0
			if totalSupport > 0 {
				share = float64(support) * 100.0 / float64(totalSupport)
			}
			if minShare > 0 && share < minShare {
				continue
			}
			qualityCodes := triagehtml.QualityIssueCodes(strings.TrimSpace(phrase))
			qualityLabels := make([]string, 0, len(qualityCodes))
			for _, code := range qualityCodes {
				qualityLabels = append(qualityLabels, triagehtml.QualityIssueLabel(code))
			}
			references := append([]triagehtml.RunReference(nil), snapshot.PhraseReferencesByEnv[environment][phrase]...)
			badPRScore, _ := triagehtml.BadPRScoreAndReasons(triagehtml.SignatureRow{
				Environment:   environment,
				PostGoodCount: postGoodByPhrase[phrase],
				AlsoSeenIn:    otherEnvironments,
				References:    references,
			})
			rows = append(rows, topSignature{
				Environment:       environment,
				Phrase:            strings.TrimSpace(phrase),
				ClusterID:         strings.TrimSpace(snapshot.PhraseClusterIDByEnv[environment][phrase]),
				SearchQuery:       strings.TrimSpace(snapshot.PhraseSearchQueryByEnv[environment][phrase]),
				SupportCount:      support,
				SupportShare:      share,
				PostGoodCount:     postGoodByPhrase[phrase],
				BadPRScore:        badPRScore,
				SeenInOtherEnvs:   otherEnvironments,
				QualityScore:      triagehtml.QualityScore(qualityCodes),
				QualityNoteLabels: qualityLabels,
				ContributingTests: append([]triagehtml.ContributingTest(nil), snapshot.PhraseContributingTestsByEnv[environment][phrase]...),
				References:        references,
				FullErrorSamples:  append([]string(nil), snapshot.PhraseFullErrorsByEnv[environment][phrase]...),
			})
		}
		sortTopSignatures(rows)
		if limit > 0 && len(rows) > limit {
			rows = rows[:limit]
		}
		out[environment] = rows
	}
	return out
}

func sortTopSignatures(rows []topSignature) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].BadPRScore != rows[j].BadPRScore {
			return rows[i].BadPRScore < rows[j].BadPRScore
		}
		if rows[i].SupportCount != rows[j].SupportCount {
			return rows[i].SupportCount > rows[j].SupportCount
		}
		if rows[i].PostGoodCount != rows[j].PostGoodCount {
			return rows[i].PostGoodCount > rows[j].PostGoodCount
		}
		return rows[i].Phrase < rows[j].Phrase
	})
}

func topSignaturesFromGlobalClusters(rows []semanticcontracts.GlobalClusterRecord) []topSignature {
	out := make([]topSignature, 0, len(rows))
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		phrase := strings.TrimSpace(row.CanonicalEvidencePhrase)
		if phrase == "" {
			phrase = "(unknown evidence)"
		}
		support := row.SupportCount
		if support < 0 {
			support = 0
		}
		postGood := row.PostGoodCommitCount
		if postGood < 0 {
			postGood = 0
		}
		qualityCodes := triagehtml.QualityIssueCodes(phrase)
		qualityLabels := make([]string, 0, len(qualityCodes))
		for _, code := range qualityCodes {
			qualityLabels = append(qualityLabels, triagehtml.QualityIssueLabel(code))
		}
		references := toTriageRunReferences(row.References)
		if sourceRunURL := strings.TrimSpace(row.SearchQuerySourceRunURL); sourceRunURL != "" {
			references = append(references, triagehtml.RunReference{
				RunURL:      sourceRunURL,
				SignatureID: strings.TrimSpace(row.SearchQuerySourceSignatureID),
			})
		}
		out = append(out, topSignature{
			Environment:       environment,
			Phrase:            phrase,
			ClusterID:         strings.TrimSpace(row.Phase2ClusterID),
			SearchQuery:       strings.TrimSpace(row.SearchQueryPhrase),
			SupportCount:      support,
			PostGoodCount:     postGood,
			QualityScore:      triagehtml.QualityScore(qualityCodes),
			QualityNoteLabels: qualityLabels,
			ContributingTests: triagehtml.OrderedContributingTests(toTriageContributingTests(row.ContributingTests)),
			References:        references,
		})
	}
	return out
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

func weeklyLinkedChildrenByMergedClusterKey(
	globalClusters []semanticcontracts.GlobalClusterRecord,
	phase3Links []semanticcontracts.Phase3LinkRecord,
) (map[string][]semanticcontracts.GlobalClusterRecord, error) {
	if len(globalClusters) == 0 || len(phase3Links) == 0 {
		return map[string][]semanticcontracts.GlobalClusterRecord{}, nil
	}
	phase3ClusterByAnchor := make(map[string]string, len(phase3Links))
	for _, link := range phase3Links {
		anchor := weeklyPhase3AnchorKey(link.Environment, link.RunURL, link.RowID)
		clusterID := strings.TrimSpace(link.IssueID)
		if anchor == "" || clusterID == "" {
			continue
		}
		phase3ClusterByAnchor[anchor] = clusterID
	}

	grouped := map[string][]semanticcontracts.GlobalClusterRecord{}
	for _, cluster := range globalClusters {
		clusterIDs := weeklyPhase3ClusterIDsForGlobalCluster(cluster, phase3ClusterByAnchor)
		if len(clusterIDs) == 0 {
			continue
		}
		if len(clusterIDs) > 1 {
			return nil, fmt.Errorf("global cluster %q/%q maps to multiple phase3 IDs: %v", cluster.Environment, cluster.Phase2ClusterID, clusterIDs)
		}
		key := weeklyGlobalClusterKey(cluster.Environment, clusterIDs[0])
		grouped[key] = append(grouped[key], cluster)
	}
	for key := range grouped {
		rows := grouped[key]
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].SupportCount != rows[j].SupportCount {
				return rows[i].SupportCount > rows[j].SupportCount
			}
			if strings.TrimSpace(rows[i].CanonicalEvidencePhrase) != strings.TrimSpace(rows[j].CanonicalEvidencePhrase) {
				return strings.TrimSpace(rows[i].CanonicalEvidencePhrase) < strings.TrimSpace(rows[j].CanonicalEvidencePhrase)
			}
			return strings.TrimSpace(rows[i].Phase2ClusterID) < strings.TrimSpace(rows[j].Phase2ClusterID)
		})
		grouped[key] = rows
	}
	return grouped, nil
}

func weeklyPhase3ClusterIDsForGlobalCluster(
	cluster semanticcontracts.GlobalClusterRecord,
	phase3ClusterByAnchor map[string]string,
) []string {
	seen := map[string]struct{}{}
	for _, reference := range cluster.References {
		clusterID := strings.TrimSpace(phase3ClusterByAnchor[weeklyPhase3AnchorKey(
			cluster.Environment,
			reference.RunURL,
			reference.RowID,
		)])
		if clusterID == "" {
			continue
		}
		seen[clusterID] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for clusterID := range seen {
		out = append(out, clusterID)
	}
	sort.Strings(out)
	return out
}

func weeklyPhase3AnchorKey(environment string, runURL string, rowID string) string {
	env := normalizeReportEnvironment(environment)
	run := strings.TrimSpace(runURL)
	row := strings.TrimSpace(rowID)
	if env == "" || run == "" || row == "" {
		return ""
	}
	return env + "|" + run + "|" + row
}

func weeklyGlobalClusterKey(environment string, clusterID string) string {
	env := normalizeReportEnvironment(environment)
	cluster := strings.TrimSpace(clusterID)
	if env == "" || cluster == "" {
		return ""
	}
	return env + "|" + cluster
}

func toTriageRunReferences(rows []semanticcontracts.ReferenceRecord) []triagehtml.RunReference {
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

func toTriageContributingTests(rows []semanticcontracts.ContributingTestRecord) []triagehtml.ContributingTest {
	out := make([]triagehtml.ContributingTest, 0, len(rows))
	for _, row := range rows {
		out = append(out, triagehtml.ContributingTest{
			Lane:         strings.TrimSpace(row.Lane),
			JobName:      strings.TrimSpace(row.JobName),
			TestName:     strings.TrimSpace(row.TestName),
			SupportCount: row.SupportCount,
		})
	}
	return out
}

func mergeTriageContributingTests(existing []triagehtml.ContributingTest, incoming []triagehtml.ContributingTest) []triagehtml.ContributingTest {
	if len(incoming) == 0 {
		return existing
	}
	type key struct {
		lane string
		job  string
		test string
	}
	merged := make(map[key]triagehtml.ContributingTest, len(existing)+len(incoming))
	for _, item := range existing {
		merged[key{
			lane: strings.TrimSpace(item.Lane),
			job:  strings.TrimSpace(item.JobName),
			test: strings.TrimSpace(item.TestName),
		}] = item
	}
	for _, item := range incoming {
		k := key{
			lane: strings.TrimSpace(item.Lane),
			job:  strings.TrimSpace(item.JobName),
			test: strings.TrimSpace(item.TestName),
		}
		existingItem, ok := merged[k]
		if !ok {
			merged[k] = item
			continue
		}
		existingItem.SupportCount += item.SupportCount
		merged[k] = existingItem
	}
	out := make([]triagehtml.ContributingTest, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	return triagehtml.OrderedContributingTests(out)
}

func loadSignatureFullErrorSamplesByEnvironment(
	dates []string,
	rawRows []storecontracts.RawFailureRecord,
	snapshot *semanticSnapshot,
	limit int,
) {
	if snapshot == nil || limit <= 0 || len(dates) == 0 {
		return
	}
	if snapshot.PhraseFullErrorsByEnv == nil {
		snapshot.PhraseFullErrorsByEnv = map[string]map[string][]string{}
	}
	rawByEnvironmentDate := indexRawFailuresByEnvironmentDate(rawRows)
	for environment, signatureIDsByPhrase := range snapshot.PhraseSignatureIDs {
		if len(signatureIDsByPhrase) == 0 {
			continue
		}
		signatureToPhrases := map[string][]string{}
		for phrase, signatureIDs := range signatureIDsByPhrase {
			for signatureID := range signatureIDs {
				trimmedSignatureID := strings.TrimSpace(signatureID)
				if trimmedSignatureID == "" {
					continue
				}
				signatureToPhrases[trimmedSignatureID] = append(signatureToPhrases[trimmedSignatureID], phrase)
			}
		}
		if len(signatureToPhrases) == 0 {
			continue
		}
		if _, ok := snapshot.PhraseFullErrorsByEnv[environment]; !ok {
			snapshot.PhraseFullErrorsByEnv[environment] = map[string][]string{}
		}
		for dateIndex := len(dates) - 1; dateIndex >= 0; dateIndex-- {
			date := strings.TrimSpace(dates[dateIndex])
			if date == "" {
				continue
			}
			for _, row := range rawByEnvironmentDate[weeklyEnvironmentDateKey(environment, date)] {
				signatureID := strings.TrimSpace(row.SignatureID)
				if signatureID == "" {
					continue
				}
				phrases := signatureToPhrases[signatureID]
				if len(phrases) == 0 {
					continue
				}
				sample := strings.TrimSpace(row.RawText)
				if sample == "" {
					sample = strings.TrimSpace(row.NormalizedText)
				}
				if sample == "" {
					continue
				}
				for _, phrase := range phrases {
					existing := snapshot.PhraseFullErrorsByEnv[environment][phrase]
					snapshot.PhraseFullErrorsByEnv[environment][phrase] = appendUniqueLimitedSample(existing, sample, limit)
				}
			}
		}
	}
}

func indexRawFailuresByEnvironmentDate(rows []storecontracts.RawFailureRecord) map[string][]storecontracts.RawFailureRecord {
	out := map[string][]storecontracts.RawFailureRecord{}
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		date, ok := dateFromTimestamp(row.OccurredAt)
		if !ok {
			continue
		}
		key := weeklyEnvironmentDateKey(environment, date)
		if key == "" {
			continue
		}
		out[key] = append(out[key], row)
	}
	for key := range out {
		rawRows := out[key]
		sort.Slice(rawRows, func(i, j int) bool {
			if rawRows[i].OccurredAt != rawRows[j].OccurredAt {
				return rawRows[i].OccurredAt < rawRows[j].OccurredAt
			}
			if rawRows[i].RunURL != rawRows[j].RunURL {
				return rawRows[i].RunURL < rawRows[j].RunURL
			}
			if rawRows[i].RowID != rawRows[j].RowID {
				return rawRows[i].RowID < rawRows[j].RowID
			}
			return rawRows[i].SignatureID < rawRows[j].SignatureID
		})
		out[key] = rawRows
	}
	return out
}

func appendUniqueLimitedSample(existing []string, candidate string, limit int) []string {
	trimmedCandidate := strings.TrimSpace(candidate)
	if trimmedCandidate == "" {
		return existing
	}
	for _, value := range existing {
		if value == trimmedCandidate {
			return existing
		}
	}
	if limit > 0 && len(existing) >= limit {
		return existing
	}
	return append(existing, trimmedCandidate)
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

func collectPostGoodRunOutcomes(day counts) runOutcomes {
	out := runOutcomes{}

	ciInfraFailedRuns := day.PostGoodFailedCIInfra
	provisionFailedRuns := day.PostGoodFailedProvision
	e2eFailedRuns := day.PostGoodFailedE2EJobs
	totalFailedRuns := ciInfraFailedRuns + provisionFailedRuns + e2eFailedRuns

	totalRuns := day.PostGoodRunCount
	if totalRuns < totalFailedRuns {
		totalRuns = totalFailedRuns
	}
	successfulRuns := totalRuns - totalFailedRuns
	if successfulRuns < 0 {
		successfulRuns = 0
	}

	out.TotalRuns = totalRuns
	out.SuccessfulRuns = successfulRuns
	out.CIInfraFailedRuns = ciInfraFailedRuns
	out.ProvisionFailedRuns = provisionFailedRuns
	out.E2EFailedRuns = e2eFailedRuns
	return out
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
