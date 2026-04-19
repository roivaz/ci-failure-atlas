package readmodel

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	semanticquery "ci-failure-atlas/pkg/semantic/query"
	sourceoptions "ci-failure-atlas/pkg/source/options"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

const (
	weeklyWindowDays               = 7
	weeklyMetricRunCount           = "run_count"
	weeklyMetricFailureCount       = "failure_count"
	weeklyMetricFailedCIInfraRuns  = "failed_ci_infra_run_count"
	weeklyMetricFailedProvisionRun = "failed_provision_run_count"
	weeklyMetricFailedE2ERun       = "failed_e2e_run_count"
	weeklyMetricPostGoodRunCount   = "post_good_run_count"
	weeklyMetricPostGoodFailedE2E  = "post_good_failed_e2e_jobs"
	weeklyMetricPostGoodFailedCI   = "post_good_failed_ci_infra_run_count"
	weeklyMetricPostGoodFailedProv = "post_good_failed_provision_run_count"

	weeklyDefaultPeriod            = "default"
	weeklyTestSuccessTarget        = 95.0
	weeklyTestSuccessMinRuns       = 10
	weeklyTestsBelowTargetTopLimit = 5
	weeklyFullErrorExamples        = 3
)

var weeklyReportEnvironments = sourceoptions.SupportedEnvironments()

type WeeklyReportBuildOptions struct {
	StartDate           time.Time
	TargetRate          float64
	Week                string
	HistoryHorizonWeeks int
	HistoryResolver     semhistory.FailurePatternHistoryResolver
}

type WeeklyCounts struct {
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

type counts = WeeklyCounts

type WeeklyRunOutcomes struct {
	TotalRuns           int
	SuccessfulRuns      int
	CIInfraFailedRuns   int
	ProvisionFailedRuns int
	E2EFailedRuns       int
}

type runOutcomes = WeeklyRunOutcomes

type WeeklyDayReport struct {
	Date                string
	Counts              WeeklyCounts
	PostGoodRunOutcomes WeeklyRunOutcomes
}

type dayReport = WeeklyDayReport

type WeeklyEnvReport struct {
	Environment string
	Days        []WeeklyDayReport
	Totals      WeeklyCounts
}

type envReport = WeeklyEnvReport

type WeeklySemanticEnvSummary struct {
	FailurePatternClusters int
	TestClusters           int
	ReviewItems            int
	TopPhrase              string
	TopSupport             int
	TopPostGood            int
}

type semanticEnvSummary = WeeklySemanticEnvSummary

type WeeklyTopSignature struct {
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
	ContributingTests []ContributingTest
	References        []RunReference
	FullErrorSamples  []string
	LinkedChildren    []WeeklyTopSignature
}

type topSignature = WeeklyTopSignature

type WeeklySemanticSnapshot struct {
	ByEnvironment                    map[string]WeeklySemanticEnvSummary
	ClusterSignaturesByEnv           map[string][]WeeklyTopSignature
	PhraseSupportByEnv               map[string]map[string]int
	PhrasePostGoodByEnv              map[string]map[string]int
	PhraseReferencesByEnv            map[string]map[string][]RunReference
	PhraseContributingTestsByEnv     map[string]map[string][]ContributingTest
	PhraseClusterIDByEnv             map[string]map[string]string
	PhraseSearchQueryByEnv           map[string]map[string]string
	PhraseRepresentativeSupportByEnv map[string]map[string]int
	PhraseReferenceKeysByEnv         map[string]map[string]map[string]struct{}
	PhraseFullErrorsByEnv            map[string]map[string][]string
}

type semanticSnapshot = WeeklySemanticSnapshot

type WeeklyBelowTargetTest struct {
	TestName  string
	TestSuite string
	Date      string
	PassRate  float64
	Runs      int
}

type belowTargetTest = WeeklyBelowTargetTest

type WeeklyReportData struct {
	StartDate             time.Time
	EndDate               time.Time
	CurrentReports        []WeeklyEnvReport
	PreviousReports       []WeeklyEnvReport
	TargetRate            float64
	CurrentSemantic       WeeklySemanticSnapshot
	PreviousSemantic      WeeklySemanticSnapshot
	TestsBelowTargetByEnv map[string][]WeeklyBelowTargetTest
	TopSignaturesByEnv    map[string][]WeeklyTopSignature
	HistoryResolver       semhistory.FailurePatternHistoryResolver
}

func BuildWeeklyReportData(
	ctx context.Context,
	store storecontracts.Store,
	previousSemanticStore storecontracts.Store,
	opts WeeklyReportBuildOptions,
) (WeeklyReportData, error) {
	if store == nil {
		return WeeklyReportData{}, fmt.Errorf("store is required")
	}
	if opts.StartDate.IsZero() {
		return WeeklyReportData{}, fmt.Errorf("start date is required")
	}

	currentDates := dateWindow(opts.StartDate, weeklyWindowDays)
	currentReports, err := buildEnvReports(ctx, store, currentDates)
	if err != nil {
		return WeeklyReportData{}, err
	}

	previousStart := opts.StartDate.AddDate(0, 0, -weeklyWindowDays)
	previousDates := dateWindow(previousStart, weeklyWindowDays)
	previousReports, err := buildEnvReports(ctx, store, previousDates)
	if err != nil {
		return WeeklyReportData{}, err
	}

	currentWeekData, err := semanticquery.LoadWeekData(ctx, store, semanticquery.LoadWeekDataOptions{
		IncludeRawFailures: true,
	})
	if err != nil {
		return WeeklyReportData{}, fmt.Errorf("load current semantic inputs: %w", err)
	}
	currentSemantic, err := loadSemanticSnapshot(currentWeekData)
	if err != nil {
		return WeeklyReportData{}, fmt.Errorf("load current semantic week: %w", err)
	}
	loadSignatureFullErrorSamplesByEnvironment(
		currentDates,
		currentWeekData.RawFailures,
		&currentSemantic,
		weeklyFullErrorExamples,
	)
	testsBelowTargetByEnv, err := loadBelowTargetTestsByEnvironment(
		ctx,
		store,
		currentDates,
		weeklyDefaultPeriod,
		weeklyTestSuccessTarget,
		weeklyTestSuccessMinRuns,
		weeklyTestsBelowTargetTopLimit,
	)
	if err != nil {
		return WeeklyReportData{}, fmt.Errorf("load weekly tests below target: %w", err)
	}
	topSignaturesByEnv := rankTopSignaturesByEnvironment(currentSemantic, 0, 0)

	var previousSemantic semanticSnapshot
	if previousSemanticStore != nil {
		previousWeekData, loadErr := semanticquery.LoadWeekData(ctx, previousSemanticStore, semanticquery.LoadWeekDataOptions{})
		if loadErr != nil {
			return WeeklyReportData{}, fmt.Errorf("load previous semantic inputs: %w", loadErr)
		}
		if err := semanticcontracts.RequireCompatibleWeekSchemas(
			currentWeekData.WeekSchemaVersion,
			previousWeekData.WeekSchemaVersion,
			"weekly report comparison",
		); err != nil {
			return WeeklyReportData{}, err
		}
		previousSemantic, err = loadSemanticSnapshot(previousWeekData)
		if err != nil {
			return WeeklyReportData{}, fmt.Errorf("load previous semantic week: %w", err)
		}
	}

	historyResolver := opts.HistoryResolver
	if historyResolver == nil {
		lookbackWeeks := opts.HistoryHorizonWeeks
		if lookbackWeeks <= 0 {
			lookbackWeeks = DefaultHistoryWeeks
		}
		historyResolver, err = semhistory.BuildFailurePatternHistoryResolver(ctx, semhistory.BuildOptions{
			CurrentWeek:                        strings.TrimSpace(opts.Week),
			CurrentSchemaVersion:               currentWeekData.WeekSchemaVersion,
			FailurePatternHistoryLookbackWeeks: lookbackWeeks,
		})
		if err != nil {
			return WeeklyReportData{}, fmt.Errorf("build signature history resolver: %w", err)
		}
	}

	startDate := opts.StartDate.UTC()
	return WeeklyReportData{
		StartDate:             startDate,
		EndDate:               startDate.AddDate(0, 0, weeklyWindowDays-1),
		CurrentReports:        currentReports,
		PreviousReports:       previousReports,
		TargetRate:            opts.TargetRate,
		CurrentSemantic:       currentSemantic,
		PreviousSemantic:      previousSemantic,
		TestsBelowTargetByEnv: testsBelowTargetByEnv,
		TopSignaturesByEnv:    topSignaturesByEnv,
		HistoryResolver:       historyResolver,
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

func buildEnvReports(ctx context.Context, store storecontracts.Store, dates []string) ([]envReport, error) {
	metricsByEnvironmentDate, err := loadMetricsDailyByEnvironmentDate(ctx, store, weeklyReportEnvironments, dates)
	if err != nil {
		return nil, err
	}
	reports := make([]envReport, 0, len(weeklyReportEnvironments))
	for _, env := range weeklyReportEnvironments {
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
	rows, err := loadMetricsDailyForDates(ctx, store, environments, dates)
	if err != nil {
		return nil, fmt.Errorf("list metrics daily for dates: %w", err)
	}
	out := make(map[string][]storecontracts.MetricDailyRecord)
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		date := strings.TrimSpace(row.Date)
		if environment == "" || date == "" {
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

func loadSemanticSnapshot(weekData semanticquery.WeekData) (semanticSnapshot, error) {
	out := semanticSnapshot{
		ByEnvironment:                    map[string]semanticEnvSummary{},
		ClusterSignaturesByEnv:           map[string][]topSignature{},
		PhraseSupportByEnv:               map[string]map[string]int{},
		PhrasePostGoodByEnv:              map[string]map[string]int{},
		PhraseReferencesByEnv:            map[string]map[string][]RunReference{},
		PhraseContributingTestsByEnv:     map[string]map[string][]ContributingTest{},
		PhraseClusterIDByEnv:             map[string]map[string]string{},
		PhraseSearchQueryByEnv:           map[string]map[string]string{},
		PhraseRepresentativeSupportByEnv: map[string]map[string]int{},
		PhraseReferenceKeysByEnv:         map[string]map[string]map[string]struct{}{},
		PhraseFullErrorsByEnv:            map[string]map[string][]string{},
	}

	for _, row := range weekData.FailurePatterns {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		summary := out.ByEnvironment[environment]
		summary.FailurePatternClusters++

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
			out.PhraseReferencesByEnv[environment] = map[string][]RunReference{}
		}
		out.PhraseReferencesByEnv[environment][phrase] = append(
			out.PhraseReferencesByEnv[environment][phrase],
			toFailurePatternRunReferences(row.References)...,
		)
		if sourceRunURL := strings.TrimSpace(row.SearchQuerySourceRunURL); sourceRunURL != "" {
			out.PhraseReferencesByEnv[environment][phrase] = append(
				out.PhraseReferencesByEnv[environment][phrase],
				RunReference{
					RunURL:      sourceRunURL,
					SignatureID: strings.TrimSpace(row.SearchQuerySourceSignatureID),
				},
			)
		}

		if _, ok := out.PhraseContributingTestsByEnv[environment]; !ok {
			out.PhraseContributingTestsByEnv[environment] = map[string][]ContributingTest{}
		}
		out.PhraseContributingTestsByEnv[environment][phrase] = mergeFailurePatternContributingTests(
			out.PhraseContributingTestsByEnv[environment][phrase],
			toFailurePatternContributingTests(row.ContributingTests),
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

		mergePhraseReferenceKeys(out.PhraseReferenceKeysByEnv, environment, phrase, row.References)

		qualityCodes := QualityIssueCodes(strings.TrimSpace(phrase))
		qualityLabels := make([]string, 0, len(qualityCodes))
		for _, code := range qualityCodes {
			qualityLabels = append(qualityLabels, QualityIssueLabel(code))
		}
		rowReferences := toFailurePatternRunReferences(row.References)
		if sourceRunURL := strings.TrimSpace(row.SearchQuerySourceRunURL); sourceRunURL != "" {
			rowReferences = append(rowReferences, RunReference{
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
			QualityScore:      QualityScore(qualityCodes),
			QualityNoteLabels: qualityLabels,
			ContributingTests: OrderedContributingTests(toFailurePatternContributingTests(row.ContributingTests)),
			References:        rowReferences,
		})
	}

	for _, row := range weekData.SourceFailurePatterns {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		phrase := strings.TrimSpace(row.CanonicalEvidencePhrase)
		if phrase == "" {
			phrase = "(unknown evidence)"
		}
		mergePhraseReferenceKeys(out.PhraseReferenceKeysByEnv, environment, phrase, row.References)
	}

	for environment, testClusterCount := range weekData.TestClusterCountsByEnv {
		summary := out.ByEnvironment[environment]
		summary.TestClusters = testClusterCount
		out.ByEnvironment[environment] = summary
	}

	for _, row := range weekData.ReviewQueue {
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

func collectCounts(rows []storecontracts.MetricDailyRecord) counts {
	out := counts{}
	for _, row := range rows {
		value := int(row.Value)
		switch strings.TrimSpace(row.Metric) {
		case weeklyMetricRunCount:
			out.RunCount = value
		case weeklyMetricFailureCount:
			out.FailureCount = value
		case weeklyMetricFailedCIInfraRuns:
			out.FailedCIInfraRunCount = value
		case weeklyMetricFailedProvisionRun:
			out.FailedProvisionRunCount = value
		case weeklyMetricFailedE2ERun:
			out.FailedE2ERunCount = value
		case weeklyMetricPostGoodRunCount:
			out.PostGoodRunCount = value
		case weeklyMetricPostGoodFailedE2E:
			out.PostGoodFailedE2EJobs = value
		case weeklyMetricPostGoodFailedCI:
			out.PostGoodFailedCIInfra = value
		case weeklyMetricPostGoodFailedProv:
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

func loadBelowTargetTestsByEnvironment(
	ctx context.Context,
	store storecontracts.Store,
	dates []string,
	period string,
	targetPassRate float64,
	minRuns int,
	limit int,
) (map[string][]belowTargetTest, error) {
	out := make(map[string][]belowTargetTest, len(weeklyReportEnvironments))
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

	for _, environment := range weeklyReportEnvironments {
		availableDates, err := store.ListTestMetadataDatesByEnvironment(ctx, environment, trimmedPeriod)
		if err != nil {
			return nil, fmt.Errorf("list test metadata dates for env=%q period=%q: %w", environment, trimmedPeriod, err)
		}
		selectedDate := preferredMetadataDateForWindow(windowEndDate, availableDates)
		if selectedDate == "" {
			out[environment] = nil
			continue
		}
		rows, err := store.ListBelowTargetTestMetadataByDate(
			ctx,
			environment,
			selectedDate,
			trimmedPeriod,
			targetPassRate,
			minRuns,
			limit,
		)
		if err != nil {
			return nil, fmt.Errorf("list below-target test metadata for env=%q date=%q: %w", environment, selectedDate, err)
		}
		out[environment] = belowTargetTestsFromMetadataRows(rows)
	}
	return out, nil
}

func belowTargetTestsFromMetadataRows(rows []storecontracts.TestMetadataDailyRecord) []belowTargetTest {
	if len(rows) == 0 {
		return nil
	}
	out := make([]belowTargetTest, 0, len(rows))
	for _, row := range rows {
		testName := strings.TrimSpace(row.TestName)
		if testName == "" {
			continue
		}
		out = append(out, belowTargetTest{
			TestName:  testName,
			TestSuite: strings.TrimSpace(row.TestSuite),
			Date:      strings.TrimSpace(row.Date),
			PassRate:  row.CurrentPassPercentage,
			Runs:      row.CurrentRuns,
		})
	}
	return out
}

func preferredMetadataDateForWindow(windowEndDate string, availableDates []string) string {
	candidateDatesAfter := metadataDatesAfter(availableDates, windowEndDate)
	if len(candidateDatesAfter) > 0 {
		return candidateDatesAfter[0]
	}
	candidateDatesBefore := metadataDatesBefore(availableDates, windowEndDate)
	if len(candidateDatesBefore) > 0 {
		return candidateDatesBefore[0]
	}
	return ""
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
	return sortedStringSet(unique)
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
	out := sortedStringSet(unique)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func rankTopSignaturesByEnvironment(snapshot semanticSnapshot, limit int, minShare float64) map[string][]topSignature {
	if len(snapshot.ClusterSignaturesByEnv) > 0 {
		return rankTopSignaturesByEnvironmentFromClusters(snapshot, limit, minShare)
	}
	return rankTopSignaturesByEnvironmentFromPhrases(snapshot, limit, minShare)
}

func rankTopSignaturesByEnvironmentFromClusters(snapshot semanticSnapshot, limit int, minShare float64) map[string][]topSignature {
	out := make(map[string][]topSignature, len(weeklyReportEnvironments))
	for _, environment := range weeklyReportEnvironments {
		totalSupport := 0
		clusterRows := snapshot.ClusterSignaturesByEnv[environment]
		for _, item := range clusterRows {
			if item.SupportCount > 0 {
				totalSupport += item.SupportCount
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
			otherEnvironments := make([]string, 0, len(weeklyReportEnvironments)-1)
			for _, candidateEnvironment := range weeklyReportEnvironments {
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
			references := append([]RunReference(nil), source.References...)
			badPRScore, _ := BadPRScoreAndReasons(FailurePatternRow{
				Environment:        environment,
				AfterLastPushCount: source.PostGoodCount,
				AlsoIn:             otherEnvironments,
				AffectedRuns:       references,
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
					ContributingTests: append([]ContributingTest(nil), child.ContributingTests...),
					References:        append([]RunReference(nil), child.References...),
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
				ContributingTests: append([]ContributingTest(nil), source.ContributingTests...),
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
	out := make(map[string][]topSignature, len(weeklyReportEnvironments))
	for _, environment := range weeklyReportEnvironments {
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
			otherEnvironments := make([]string, 0, len(weeklyReportEnvironments)-1)
			for _, candidateEnvironment := range weeklyReportEnvironments {
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
			qualityCodes := QualityIssueCodes(strings.TrimSpace(phrase))
			qualityLabels := make([]string, 0, len(qualityCodes))
			for _, code := range qualityCodes {
				qualityLabels = append(qualityLabels, QualityIssueLabel(code))
			}
			references := append([]RunReference(nil), snapshot.PhraseReferencesByEnv[environment][phrase]...)
			badPRScore, _ := BadPRScoreAndReasons(FailurePatternRow{
				Environment:        environment,
				AfterLastPushCount: postGoodByPhrase[phrase],
				AlsoIn:             otherEnvironments,
				AffectedRuns:       references,
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
				QualityScore:      QualityScore(qualityCodes),
				QualityNoteLabels: qualityLabels,
				ContributingTests: append([]ContributingTest(nil), snapshot.PhraseContributingTestsByEnv[environment][phrase]...),
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

func topSignaturesFromFailurePatternClusters(rows []semanticcontracts.FailurePatternRecord) []topSignature {
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
		qualityCodes := QualityIssueCodes(phrase)
		qualityLabels := make([]string, 0, len(qualityCodes))
		for _, code := range qualityCodes {
			qualityLabels = append(qualityLabels, QualityIssueLabel(code))
		}
		references := toFailurePatternRunReferences(row.References)
		if sourceRunURL := strings.TrimSpace(row.SearchQuerySourceRunURL); sourceRunURL != "" {
			references = append(references, RunReference{
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
			QualityScore:      QualityScore(qualityCodes),
			QualityNoteLabels: qualityLabels,
			ContributingTests: OrderedContributingTests(toFailurePatternContributingTests(row.ContributingTests)),
			References:        references,
		})
	}
	return out
}

func toFailurePatternRunReferences(rows []semanticcontracts.ReferenceRecord) []RunReference {
	out := make([]RunReference, 0, len(rows))
	for _, row := range rows {
		out = append(out, RunReference{
			RunURL:      strings.TrimSpace(row.RunURL),
			OccurredAt:  strings.TrimSpace(row.OccurredAt),
			SignatureID: strings.TrimSpace(row.SignatureID),
			PRNumber:    row.PRNumber,
		})
	}
	return out
}

func toFailurePatternContributingTests(rows []semanticcontracts.ContributingTestRecord) []ContributingTest {
	out := make([]ContributingTest, 0, len(rows))
	for _, row := range rows {
		out = append(out, ContributingTest{
			FailedAt:    strings.TrimSpace(row.Lane),
			JobName:     strings.TrimSpace(row.JobName),
			TestName:    strings.TrimSpace(row.TestName),
			Occurrences: row.SupportCount,
		})
	}
	return out
}

func mergeFailurePatternContributingTests(existing []ContributingTest, incoming []ContributingTest) []ContributingTest {
	if len(incoming) == 0 {
		return existing
	}
	type mergeKey struct {
		lane string
		job  string
		test string
	}
	merged := make(map[mergeKey]ContributingTest, len(existing)+len(incoming))
	for _, item := range existing {
		merged[mergeKey{
			lane: strings.TrimSpace(item.FailedAt),
			job:  strings.TrimSpace(item.JobName),
			test: strings.TrimSpace(item.TestName),
		}] = item
	}
	for _, item := range incoming {
		key := mergeKey{
			lane: strings.TrimSpace(item.FailedAt),
			job:  strings.TrimSpace(item.JobName),
			test: strings.TrimSpace(item.TestName),
		}
		existingItem, ok := merged[key]
		if !ok {
			merged[key] = item
			continue
		}
		existingItem.Occurrences += item.Occurrences
		merged[key] = existingItem
	}
	out := make([]ContributingTest, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	return OrderedContributingTests(out)
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
	for environment, referenceKeysByPhrase := range snapshot.PhraseReferenceKeysByEnv {
		if len(referenceKeysByPhrase) == 0 {
			continue
		}
		matchKeyToPhrases := map[string]map[string]struct{}{}
		for phrase, keySet := range referenceKeysByPhrase {
			for key := range keySet {
				trimmedKey := strings.TrimSpace(key)
				if trimmedKey == "" {
					continue
				}
				if _, ok := matchKeyToPhrases[trimmedKey]; !ok {
					matchKeyToPhrases[trimmedKey] = map[string]struct{}{}
				}
				matchKeyToPhrases[trimmedKey][phrase] = struct{}{}
			}
		}
		if len(matchKeyToPhrases) == 0 {
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
				phraseSet := map[string]struct{}{}
				for _, key := range failurePatternsRawFailureMatchKeys(row) {
					for phrase := range matchKeyToPhrases[key] {
						phraseSet[phrase] = struct{}{}
					}
				}
				if len(phraseSet) == 0 {
					continue
				}
				sample := strings.TrimSpace(row.RawText)
				if sample == "" {
					sample = strings.TrimSpace(row.NormalizedText)
				}
				if sample == "" {
					continue
				}
				for phrase := range phraseSet {
					existing := snapshot.PhraseFullErrorsByEnv[environment][phrase]
					snapshot.PhraseFullErrorsByEnv[environment][phrase] = appendUniqueLimitedSample(existing, sample, limit)
				}
			}
		}
	}
}

func mergePhraseReferenceKeys(
	byEnvironment map[string]map[string]map[string]struct{},
	environment string,
	phrase string,
	references []semanticcontracts.ReferenceRecord,
) {
	if byEnvironment == nil {
		return
	}
	normalizedEnvironment := normalizeReportEnvironment(environment)
	trimmedPhrase := strings.TrimSpace(phrase)
	if normalizedEnvironment == "" || trimmedPhrase == "" {
		return
	}
	if _, ok := byEnvironment[normalizedEnvironment]; !ok {
		byEnvironment[normalizedEnvironment] = map[string]map[string]struct{}{}
	}
	if _, ok := byEnvironment[normalizedEnvironment][trimmedPhrase]; !ok {
		byEnvironment[normalizedEnvironment][trimmedPhrase] = map[string]struct{}{}
	}
	keySet := byEnvironment[normalizedEnvironment][trimmedPhrase]
	for _, key := range failurePatternReportReferenceKeys(toFailurePatternReportReferences(references)) {
		keySet[key] = struct{}{}
	}
}

func failurePatternReportReferenceKeys(rows []FailurePatternReportReference) []string {
	keys := make([]string, 0, len(rows)*2)
	seen := map[string]struct{}{}
	for _, row := range rows {
		for _, key := range failurePatternsReferenceMatchKeys(row) {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	return keys
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
