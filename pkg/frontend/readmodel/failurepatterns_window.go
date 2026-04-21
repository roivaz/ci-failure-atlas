package readmodel

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

type FailurePatternsQuery struct {
	StartDate    string
	EndDate      string
	Week         string
	Mode         string
	Environments []string
	GeneratedAt  time.Time
}

type FailurePatternsData struct {
	Meta         FailurePatternsMeta          `json:"meta"`
	Environments []FailurePatternsEnvironment `json:"environments"`
}

type FailurePatternsMeta struct {
	StartDate    string   `json:"start_date"`
	EndDate      string   `json:"end_date"`
	AnchorWeek   string   `json:"-"`
	Timezone     string   `json:"timezone"`
	GeneratedAt  string   `json:"generated_at"`
	Environments []string `json:"environments"`
}

type FailurePatternsEnvironment struct {
	Environment string                 `json:"environment"`
	Summary     FailurePatternsSummary `json:"summary"`
	Rows        []FailurePatternsRow   `json:"rows"`
}

type FailurePatternsSummary struct {
	TotalRuns           int `json:"total_runs"`
	FailedRuns          int `json:"failed_runs"`
	RawFailureCount     int `json:"raw_occurrences"`
	MatchedFailureCount int `json:"matched_occurrences"`
	JobsAffected        int `json:"runs_affected"`
}

type FailurePatternsRow struct {
	Environment             string                                 `json:"environment"`
	ClusterID               string                                 `json:"failure_pattern_id"`
	CanonicalEvidencePhrase string                                 `json:"failure_pattern"`
	SearchQueryPhrase       string                                 `json:"search_query,omitempty"`
	Lane                    string                                 `json:"failed_at,omitempty"`
	JobName                 string                                 `json:"job_name,omitempty"`
	TestName                string                                 `json:"test_name,omitempty"`
	TestSuite               string                                 `json:"test_suite,omitempty"`
	WindowFailureCount      int                                    `json:"occurrences"`
	JobsAffected            int                                    `json:"runs_affected"`
	FailedRuns              int                                    `json:"failed_runs"`
	ImpactPercent           float64                                `json:"run_impact_percent"`
	WeeklySupportCount      int                                    `json:"anchor_occurrences"`
	WeeklyPostGoodCount     int                                    `json:"after_last_push_count"`
	SeenIn                  []string                               `json:"also_in,omitempty"`
	TrendCounts             []int                                  `json:"trend_counts,omitempty"`
	TrendRange              string                                 `json:"trend_range,omitempty"`
	PriorWeeksPresent       int                                    `json:"prior_windows_present"`
	PriorWeekStarts         []string                               `json:"prior_window_starts,omitempty"`
	PriorJobsAffected       int                                    `json:"prior_runs_affected"`
	PriorLastSeenAt         string                                 `json:"prior_last_seen_at,omitempty"`
	ContributingTests       []FailurePatternReportContributingTest `json:"contributing_tests,omitempty"`
	FullErrorSamples        []string                               `json:"full_error_samples,omitempty"`
	References              []FailurePatternReportReference        `json:"affected_runs,omitempty"`
	ScoringReferences       []FailurePatternReportReference        `json:"-"`
	LinkedChildren          []FailurePatternsRow                   `json:"linked_failure_patterns,omitempty"`
	AnchorWeek              string                                 `json:"-"`
	MergeKey                string                                 `json:"-"`
}

type failurePatternsScope struct {
	StartDate         string
	EndDate           string
	StartTime         time.Time
	EndTime           time.Time
	ResolvedWeek      string
	SemanticWeekStart time.Time
	SemanticWeekEnd   time.Time
	DateLabels        []string
}

type failurePatternsEnvironmentFacts struct {
	RawFailures []storecontracts.RawFailureRecord
	RunsByURL   map[string]storecontracts.RunRecord
	FailedRuns  int
}

type failurePatternsMatch struct {
	FailureCount int
	References   []FailurePatternReportReference
	RawFailures  []storecontracts.RawFailureRecord
	FailedRuns   int
}

func (s *Service) BuildFailurePatterns(ctx context.Context, query FailurePatternsQuery) (FailurePatternsData, error) {
	if s == nil {
		return FailurePatternsData{}, fmt.Errorf("service is required")
	}

	scope, err := s.resolvePresentationWindow(ctx, presentationWindowRequest{
		StartDate:   query.StartDate,
		EndDate:     query.EndDate,
		Week:        query.Week,
		DefaultMode: presentationWindowDefaultLatestWeek,
	})
	if err != nil {
		return FailurePatternsData{}, err
	}
	requestedEnvironments := normalizeStringSlice(query.Environments)
	weeklyDataByWeek := make(map[string]FailurePatternReportData, len(scope.SemanticWeeks))
	targetEnvironmentSet := map[string]struct{}{}
	windowSchemaVersion := ""
	for _, week := range scope.SemanticWeeks {
		store, err := s.OpenStoreForWeek(week)
		if err != nil {
			return FailurePatternsData{}, err
		}
		weeklyData, err := BuildFailurePatternReportData(ctx, store, FailurePatternReportBuildOptions{
			Week:                week,
			Environments:        requestedEnvironments,
			HistoryHorizonWeeks: s.historyWeeks,
		})
		if err == nil {
			err = semanticcontracts.RequireCompatibleWeekSchemas(
				windowSchemaVersion,
				weeklyData.WeekSchemaVersion,
				fmt.Sprintf("failure-pattern window %s..%s", scope.StartDate, scope.EndDate),
			)
		}
		if err == nil && strings.TrimSpace(windowSchemaVersion) == "" {
			windowSchemaVersion = weeklyData.WeekSchemaVersion
		}
		if err == nil {
			weeklyData.HistoryResolver, err = s.BuildHistoryResolverForWeek(ctx, week, weeklyData.WeekSchemaVersion)
			if err != nil {
				err = fmt.Errorf("build failure-pattern history resolver for %s: %w", week, err)
			}
		}
		_ = store.Close()
		if err != nil {
			return FailurePatternsData{}, fmt.Errorf("build weekly failure-pattern data for window week %s: %w", week, err)
		}
		weeklyDataByWeek[week] = weeklyData
		if len(requestedEnvironments) > 0 {
			for _, environment := range requestedEnvironments {
				targetEnvironmentSet[environment] = struct{}{}
			}
			continue
		}
		for _, environment := range weeklyData.TargetEnvironments {
			targetEnvironmentSet[normalizeEnvironment(environment)] = struct{}{}
		}
	}

	targetEnvironments := sortedStringSet(targetEnvironmentSet)
	if len(targetEnvironments) == 0 {
		targetEnvironments = append([]string(nil), requestedEnvironments...)
	}

	factsStore, err := s.OpenStoreForWeek(scope.AnchorWeek)
	if err != nil {
		return FailurePatternsData{}, err
	}
	defer func() {
		_ = factsStore.Close()
	}()

	factsByEnvironment, err := loadFailurePatternsFacts(ctx, factsStore, targetEnvironments, scope)
	if err != nil {
		return FailurePatternsData{}, err
	}
	metricRunTotals, err := failurePatternReportMetricRunTotalsByEnvironment(
		ctx,
		factsStore,
		targetEnvironments,
		scope.StartTime,
		scope.EndTime,
	)
	if err != nil {
		return FailurePatternsData{}, fmt.Errorf("load failure-pattern metric run totals: %w", err)
	}

	rowsByEnvironment := make(map[string]map[string]FailurePatternsRow, len(targetEnvironments))
	for _, week := range scope.SemanticWeeks {
		weeklyData := weeklyDataByWeek[week]
		trendAnchor := failurePatternsTrendAnchor(week)
		for _, cluster := range weeklyData.FailurePatternClusters {
			environment := normalizeEnvironment(cluster.Environment)
			if environment == "" {
				continue
			}
			facts := failurePatternsFactsForWeek(factsByEnvironment[environment], week)
			row, ok := buildFailurePatternsRow(cluster, facts, weeklyData.HistoryResolver, trendAnchor, week)
			if !ok {
				continue
			}
			if rowsByEnvironment[environment] == nil {
				rowsByEnvironment[environment] = map[string]FailurePatternsRow{}
			}
			existing, exists := rowsByEnvironment[environment][row.MergeKey]
			if !exists {
				rowsByEnvironment[environment][row.MergeKey] = cloneFailurePatternsRow(row)
				continue
			}
			rowsByEnvironment[environment][row.MergeKey] = mergeFailurePatternsRows(existing, row, facts.RunsByURL)
		}
	}

	finalRowsByEnvironment := make(map[string][]FailurePatternsRow, len(rowsByEnvironment))
	phraseEnvironments := map[string]map[string]struct{}{}
	for _, environment := range targetEnvironments {
		rowMap := rowsByEnvironment[environment]
		rows := make([]FailurePatternsRow, 0, len(rowMap))
		for _, row := range rowMap {
			rows = append(rows, row)
			collectWindowedPhraseEnvironments(row, phraseEnvironments)
		}
		finalRowsByEnvironment[environment] = rows
	}

	generatedAt := query.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	environments := make([]FailurePatternsEnvironment, 0, len(targetEnvironments))
	for _, environment := range targetEnvironments {
		rows := applyWindowedSeenIn(finalRowsByEnvironment[environment], phraseEnvironments, environment)
		totalRuns := metricRunTotals[environment]
		if totalRuns <= 0 {
			totalRuns = len(factsByEnvironment[environment].RunsByURL)
		}
		rows = applyWindowedImpact(rows, totalRuns)
		sortFailurePatternsRows(rows)
		environments = append(environments, FailurePatternsEnvironment{
			Environment: environment,
			Summary:     buildFailurePatternsSummary(factsByEnvironment[environment], rows, totalRuns),
			Rows:        rows,
		})
	}

	return FailurePatternsData{
		Meta: FailurePatternsMeta{
			StartDate:    scope.StartDate,
			EndDate:      scope.EndDate,
			AnchorWeek:   scope.AnchorWeek,
			Timezone:     "UTC",
			GeneratedAt:  generatedAt.UTC().Format(time.RFC3339),
			Environments: append([]string(nil), targetEnvironments...),
		},
		Environments: environments,
	}, nil
}

func resolveFailurePatternsScope(query FailurePatternsQuery) (failurePatternsScope, error) {
	startLabel, startDate, err := normalizeDateLabel(query.StartDate)
	if err != nil {
		return failurePatternsScope{}, fmt.Errorf("invalid start_date: %w", err)
	}
	endLabel, endDate, err := normalizeDateLabel(query.EndDate)
	if err != nil {
		return failurePatternsScope{}, fmt.Errorf("invalid end_date: %w", err)
	}
	startTime := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.UTC)
	endInclusive := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, time.UTC)
	if endInclusive.Before(startTime) {
		return failurePatternsScope{}, fmt.Errorf("end_date %s must be on or after start_date %s", endLabel, startLabel)
	}
	endTime := endInclusive.AddDate(0, 0, 1).UTC()

	resolvedWeek, err := resolveFailurePatternsWeekLabel(startTime, endInclusive, query.Week)
	if err != nil {
		return failurePatternsScope{}, err
	}
	semanticWeekStart, err := time.Parse("2006-01-02", resolvedWeek)
	if err != nil {
		return failurePatternsScope{}, fmt.Errorf("parse resolved week %q: %w", resolvedWeek, err)
	}
	semanticWeekStart = semanticWeekStart.UTC()
	semanticWeekEnd := semanticWeekStart.AddDate(0, 0, 7).UTC()
	if startTime.Before(semanticWeekStart) || !endInclusive.Before(semanticWeekEnd) {
		return failurePatternsScope{}, fmt.Errorf(
			"window %s..%s must fall within one semantic week (%s..%s)",
			startLabel,
			endLabel,
			semanticWeekStart.Format("2006-01-02"),
			semanticWeekEnd.AddDate(0, 0, -1).Format("2006-01-02"),
		)
	}

	return failurePatternsScope{
		StartDate:         startLabel,
		EndDate:           endLabel,
		StartTime:         startTime,
		EndTime:           endTime,
		ResolvedWeek:      resolvedWeek,
		SemanticWeekStart: semanticWeekStart,
		SemanticWeekEnd:   semanticWeekEnd,
		DateLabels:        metricDateLabelsFromWindow(startTime, endTime),
	}, nil
}

func resolveFailurePatternsWeekLabel(startDate time.Time, endDate time.Time, override string) (string, error) {
	trimmedOverride := strings.TrimSpace(override)
	if trimmedOverride != "" {
		weekStart, err := normalizeWeekLabel(trimmedOverride)
		if err != nil {
			return "", err
		}
		weekDate, _ := time.Parse("2006-01-02", weekStart)
		weekStartTime := weekDate.UTC()
		weekEndTime := weekStartTime.AddDate(0, 0, 7).UTC()
		if startDate.Before(weekStartTime) || !endDate.Before(weekEndTime) {
			return "", fmt.Errorf("window %s..%s does not fall within semantic week %s", startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), weekStart)
		}
		return weekStart, nil
	}

	startWeek := startDate.AddDate(0, 0, -int((startDate.Weekday()+6)%7)).UTC().Format("2006-01-02")
	endWeek := endDate.AddDate(0, 0, -int((endDate.Weekday()+6)%7)).UTC().Format("2006-01-02")
	if startWeek != endWeek {
		return "", fmt.Errorf("window %s..%s crosses semantic week boundaries (%s vs %s)", startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), startWeek, endWeek)
	}
	return startWeek, nil
}

func loadFailurePatternsFacts(
	ctx context.Context,
	store storecontracts.Store,
	environments []string,
	scope presentationWindow,
) (map[string]failurePatternsEnvironmentFacts, error) {
	factsByEnvironment := make(map[string]failurePatternsEnvironmentFacts, len(environments))
	for _, environment := range environments {
		normalizedEnvironment := normalizeEnvironment(environment)
		if normalizedEnvironment == "" {
			continue
		}
		facts := failurePatternsEnvironmentFacts{
			RawFailures: []storecontracts.RawFailureRecord{},
			RunsByURL:   map[string]storecontracts.RunRecord{},
		}
		for _, dateLabel := range scope.DateLabels {
			rawFailures, err := store.ListRawFailuresByDate(ctx, normalizedEnvironment, dateLabel)
			if err != nil {
				return nil, fmt.Errorf("list raw failures for %s on %s: %w", normalizedEnvironment, dateLabel, err)
			}
			facts.RawFailures = append(facts.RawFailures, rawFailures...)
			runs, err := store.ListRunsByDate(ctx, normalizedEnvironment, dateLabel)
			if err != nil {
				return nil, fmt.Errorf("list runs for %s on %s: %w", normalizedEnvironment, dateLabel, err)
			}
			for _, row := range runs {
				runURL := strings.TrimSpace(row.RunURL)
				if runURL == "" {
					continue
				}
				facts.RunsByURL[runURL] = row
			}
		}
		if err := fillMissingRunsForWindowFacts(ctx, store, normalizedEnvironment, &facts); err != nil {
			return nil, err
		}
		facts.FailedRuns = 0
		for _, row := range facts.RunsByURL {
			if row.Failed {
				facts.FailedRuns++
			}
		}
		sortWindowedRawFailures(facts.RawFailures)
		factsByEnvironment[normalizedEnvironment] = facts
	}
	return factsByEnvironment, nil
}

func failurePatternsFactsForWeek(
	facts failurePatternsEnvironmentFacts,
	week string,
) failurePatternsEnvironmentFacts {
	startDate, endDate := semanticWeekDateRange(week)
	if startDate == "" || endDate == "" {
		return facts
	}
	startTime, errStart := time.Parse("2006-01-02", startDate)
	endInclusive, errEnd := time.Parse("2006-01-02", endDate)
	if errStart != nil || errEnd != nil {
		return facts
	}
	endTime := endInclusive.UTC().AddDate(0, 0, 1)
	filtered := failurePatternsEnvironmentFacts{
		RawFailures: make([]storecontracts.RawFailureRecord, 0, len(facts.RawFailures)),
		RunsByURL:   map[string]storecontracts.RunRecord{},
	}
	for _, row := range facts.RawFailures {
		occurredAt, err := time.Parse(time.RFC3339, strings.TrimSpace(row.OccurredAt))
		if err != nil {
			continue
		}
		occurredAt = occurredAt.UTC()
		if occurredAt.Before(startTime.UTC()) || !occurredAt.Before(endTime) {
			continue
		}
		filtered.RawFailures = append(filtered.RawFailures, row)
	}
	for runURL, run := range facts.RunsByURL {
		occurredAt, err := time.Parse(time.RFC3339, strings.TrimSpace(run.OccurredAt))
		if err != nil {
			continue
		}
		occurredAt = occurredAt.UTC()
		if occurredAt.Before(startTime.UTC()) || !occurredAt.Before(endTime) {
			continue
		}
		filtered.RunsByURL[runURL] = run
		if run.Failed {
			filtered.FailedRuns++
		}
	}
	return filtered
}

func fillMissingRunsForWindowFacts(
	ctx context.Context,
	store storecontracts.Store,
	environment string,
	facts *failurePatternsEnvironmentFacts,
) error {
	if facts == nil {
		return nil
	}
	for _, row := range facts.RawFailures {
		runURL := strings.TrimSpace(row.RunURL)
		if runURL == "" {
			continue
		}
		if _, exists := facts.RunsByURL[runURL]; exists {
			continue
		}
		run, found, err := store.GetRun(ctx, environment, runURL)
		if err != nil {
			return fmt.Errorf("get run %s for %s: %w", runURL, environment, err)
		}
		if found {
			facts.RunsByURL[runURL] = run
		}
	}
	return nil
}

func buildFailurePatternsRow(
	cluster FailurePatternReportCluster,
	facts failurePatternsEnvironmentFacts,
	historyResolver semhistory.FailurePatternHistoryResolver,
	trendAnchor time.Time,
	anchorWeek string,
) (FailurePatternsRow, bool) {
	children := make([]FailurePatternsRow, 0, len(cluster.LinkedChildren))
	for _, child := range cluster.LinkedChildren {
		childRow, ok := buildFailurePatternsRow(child, facts, nil, trendAnchor, anchorWeek)
		if !ok {
			continue
		}
		children = append(children, childRow)
	}

	match := matchFailurePatternsCluster(cluster, facts)
	if match.FailureCount == 0 && len(children) == 0 {
		return FailurePatternsRow{}, false
	}

	primary := primaryContributingTestForReport(cluster.ContributingTests)
	references := append([]FailurePatternReportReference(nil), match.References...)
	failedRuns := match.FailedRuns
	failureCount := match.FailureCount
	fullErrorSamples := windowedFullErrorSamples(match.RawFailures, failurePatternReportFullErrorExamplesLimit)
	if len(references) == 0 && len(children) > 0 {
		references = windowedReferencesFromChildren(children)
		failedRuns = windowedFailedRunsFromReferences(references, facts.RunsByURL)
		failureCount = 0
		for _, child := range children {
			failureCount += child.WindowFailureCount
		}
		fullErrorSamples = windowedFullErrorSamplesFromChildren(children, failurePatternReportFullErrorExamplesLimit)
	}

	anchorWeekReferences := append([]FailurePatternReportReference(nil), cluster.References...)
	row := FailurePatternsRow{
		Environment:             normalizeEnvironment(cluster.Environment),
		ClusterID:               strings.TrimSpace(cluster.Phase2ClusterID),
		CanonicalEvidencePhrase: strings.TrimSpace(cluster.CanonicalEvidencePhrase),
		SearchQueryPhrase:       strings.TrimSpace(cluster.SearchQueryPhrase),
		Lane:                    strings.TrimSpace(primary.Lane),
		JobName:                 strings.TrimSpace(primary.JobName),
		TestName:                strings.TrimSpace(primary.TestName),
		TestSuite:               "",
		WindowFailureCount:      failureCount,
		JobsAffected:            windowedDistinctRunCount(references),
		FailedRuns:              failedRuns,
		WeeklySupportCount:      cluster.SupportCount,
		WeeklyPostGoodCount:     windowedPostGoodCount(references),
		ContributingTests:       append([]FailurePatternReportContributingTest(nil), cluster.ContributingTests...),
		FullErrorSamples:        fullErrorSamples,
		References:              references,
		ScoringReferences:       append([]FailurePatternReportReference(nil), references...),
		LinkedChildren:          children,
		AnchorWeek:              strings.TrimSpace(anchorWeek),
		MergeKey:                failurePatternsMergeKeyForCluster(cluster),
	}

	if historyResolver != nil {
		presence := historyResolver.PresenceFor(semhistory.FailurePatternKey{
			Environment: row.Environment,
			Phrase:      row.CanonicalEvidencePhrase,
			SearchQuery: row.SearchQueryPhrase,
		})
		row.PriorWeeksPresent = presence.PriorWeeksPresent
		row.PriorWeekStarts = append([]string(nil), presence.PriorWeekStarts...)
		row.PriorJobsAffected = presence.PriorJobsAffected
		if !presence.PriorLastSeenAt.IsZero() {
			row.PriorLastSeenAt = presence.PriorLastSeenAt.UTC().Format(time.RFC3339)
		}
	}

	if counts, trendRange, ok := buildWindowedTrend(anchorWeekReferences, trendAnchor); ok {
		row.TrendCounts = counts
		row.TrendRange = trendRange
	}

	sortFailurePatternsRows(row.LinkedChildren)
	return row, true
}

func buildWindowedTrend(references []FailurePatternReportReference, trendAnchor time.Time) ([]int, string, bool) {
	if trendAnchor.IsZero() {
		return nil, "", false
	}
	if _, counts, trendRange, ok := DailyDensitySparkline(toWindowedHTMLRunReferences(references), 7, trendAnchor); ok {
		return append([]int(nil), counts...), trendRange, true
	}
	return nil, "", false
}

func failurePatternsTrendAnchor(week string) time.Time {
	weekStart, err := time.Parse("2006-01-02", strings.TrimSpace(week))
	if err != nil {
		return time.Now().UTC()
	}
	return weekStart.UTC().AddDate(0, 0, 7).Add(-time.Nanosecond)
}

func failurePatternsMergeKeyForCluster(cluster FailurePatternReportCluster) string {
	environment := normalizeEnvironment(cluster.Environment)
	if environment == "" {
		return ""
	}
	clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
	phraseKey := normalizePhrase(cluster.CanonicalEvidencePhrase)
	searchKey := normalizePhrase(cluster.SearchQueryPhrase)
	if phraseKey == "" && searchKey == "" {
		if clusterID == "" {
			return ""
		}
		return "cluster|" + environment + "|" + clusterID
	}
	return "fallback|" + environment + "|" + phraseKey + "|" + searchKey
}

func cloneFailurePatternsRow(row FailurePatternsRow) FailurePatternsRow {
	cloned := row
	cloned.SeenIn = append([]string(nil), row.SeenIn...)
	cloned.TrendCounts = append([]int(nil), row.TrendCounts...)
	cloned.PriorWeekStarts = append([]string(nil), row.PriorWeekStarts...)
	cloned.ContributingTests = append([]FailurePatternReportContributingTest(nil), row.ContributingTests...)
	cloned.FullErrorSamples = append([]string(nil), row.FullErrorSamples...)
	cloned.References = append([]FailurePatternReportReference(nil), row.References...)
	cloned.ScoringReferences = append([]FailurePatternReportReference(nil), row.ScoringReferences...)
	if len(row.LinkedChildren) == 0 {
		cloned.LinkedChildren = nil
		return cloned
	}
	cloned.LinkedChildren = make([]FailurePatternsRow, 0, len(row.LinkedChildren))
	for _, child := range row.LinkedChildren {
		cloned.LinkedChildren = append(cloned.LinkedChildren, cloneFailurePatternsRow(child))
	}
	return cloned
}

func mergeFailurePatternsRows(
	existing FailurePatternsRow,
	incoming FailurePatternsRow,
	runsByURL map[string]storecontracts.RunRecord,
) FailurePatternsRow {
	merged := cloneFailurePatternsRow(existing)
	merged.WindowFailureCount += incoming.WindowFailureCount
	merged.References = mergeFailurePatternsReferences(merged.References, incoming.References)
	merged.LinkedChildren = mergeFailurePatternsChildren(merged.LinkedChildren, incoming.LinkedChildren, runsByURL)
	merged.FullErrorSamples = mergeFailurePatternsSamples(merged.FullErrorSamples, incoming.FullErrorSamples, failurePatternReportFullErrorExamplesLimit)
	if strings.TrimSpace(incoming.AnchorWeek) >= strings.TrimSpace(merged.AnchorWeek) {
		merged.Environment = incoming.Environment
		merged.ClusterID = incoming.ClusterID
		merged.CanonicalEvidencePhrase = incoming.CanonicalEvidencePhrase
		merged.SearchQueryPhrase = incoming.SearchQueryPhrase
		merged.Lane = incoming.Lane
		merged.JobName = incoming.JobName
		merged.TestName = incoming.TestName
		merged.TestSuite = incoming.TestSuite
		merged.WeeklySupportCount = incoming.WeeklySupportCount
		merged.WeeklyPostGoodCount = incoming.WeeklyPostGoodCount
		merged.TrendCounts = append([]int(nil), incoming.TrendCounts...)
		merged.TrendRange = incoming.TrendRange
		merged.PriorWeeksPresent = incoming.PriorWeeksPresent
		merged.PriorWeekStarts = append([]string(nil), incoming.PriorWeekStarts...)
		merged.PriorJobsAffected = incoming.PriorJobsAffected
		merged.PriorLastSeenAt = incoming.PriorLastSeenAt
		merged.ContributingTests = append([]FailurePatternReportContributingTest(nil), incoming.ContributingTests...)
		merged.ScoringReferences = mergeFailurePatternsReferences(merged.ScoringReferences, incoming.ScoringReferences)
		merged.AnchorWeek = incoming.AnchorWeek
	}
	merged.JobsAffected = windowedDistinctRunCount(merged.References)
	merged.FailedRuns = windowedFailedRunsFromReferences(merged.References, runsByURL)
	merged.WeeklyPostGoodCount = windowedPostGoodCount(merged.ScoringReferences)
	if len(merged.References) == 0 && len(merged.LinkedChildren) > 0 {
		merged.References = windowedReferencesFromChildren(merged.LinkedChildren)
		merged.FullErrorSamples = windowedFullErrorSamplesFromChildren(merged.LinkedChildren, failurePatternReportFullErrorExamplesLimit)
		merged.JobsAffected = windowedDistinctRunCount(merged.References)
		merged.FailedRuns = windowedFailedRunsFromReferences(merged.References, runsByURL)
		if len(merged.ScoringReferences) == 0 {
			merged.ScoringReferences = append([]FailurePatternReportReference(nil), merged.References...)
		}
		merged.WeeklyPostGoodCount = windowedPostGoodCount(merged.ScoringReferences)
	}
	return merged
}

func mergeFailurePatternsChildren(
	existing []FailurePatternsRow,
	incoming []FailurePatternsRow,
	runsByURL map[string]storecontracts.RunRecord,
) []FailurePatternsRow {
	if len(existing) == 0 {
		out := make([]FailurePatternsRow, 0, len(incoming))
		for _, row := range incoming {
			out = append(out, cloneFailurePatternsRow(row))
		}
		return out
	}
	merged := make(map[string]FailurePatternsRow, len(existing)+len(incoming))
	order := make([]string, 0, len(existing)+len(incoming))
	for _, row := range existing {
		key := strings.TrimSpace(row.MergeKey)
		if key == "" {
			key = fmt.Sprintf("existing|%d", len(order))
		}
		if _, exists := merged[key]; !exists {
			order = append(order, key)
		}
		merged[key] = cloneFailurePatternsRow(row)
	}
	for _, row := range incoming {
		key := strings.TrimSpace(row.MergeKey)
		if key == "" {
			key = fmt.Sprintf("incoming|%d", len(order))
		}
		existingRow, exists := merged[key]
		if !exists {
			order = append(order, key)
			merged[key] = cloneFailurePatternsRow(row)
			continue
		}
		merged[key] = mergeFailurePatternsRows(existingRow, row, runsByURL)
	}
	out := make([]FailurePatternsRow, 0, len(order))
	for _, key := range order {
		out = append(out, merged[key])
	}
	sortFailurePatternsRows(out)
	return out
}

func mergeFailurePatternsReferences(
	existing []FailurePatternReportReference,
	incoming []FailurePatternReportReference,
) []FailurePatternReportReference {
	if len(existing) == 0 {
		return append([]FailurePatternReportReference(nil), incoming...)
	}
	seen := map[string]struct{}{}
	out := make([]FailurePatternReportReference, 0, len(existing)+len(incoming))
	appendUnique := func(rows []FailurePatternReportReference) {
		for _, row := range rows {
			key := failurePatternsReferenceDedupKey(row)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, row)
		}
	}
	appendUnique(existing)
	appendUnique(incoming)
	sortWindowedReferences(out)
	return out
}

func mergeFailurePatternsSamples(existing []string, incoming []string, limit int) []string {
	out := append([]string(nil), existing...)
	for _, sample := range incoming {
		out = failurePatternReportAppendUniqueLimitedSample(out, sample, limit)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func matchFailurePatternsCluster(cluster FailurePatternReportCluster, facts failurePatternsEnvironmentFacts) failurePatternsMatch {
	referencesByKey := failurePatternsReferenceMatchMap(cluster.References)
	if len(referencesByKey) == 0 {
		return failurePatternsMatch{}
	}

	match := failurePatternsMatch{
		References:  []FailurePatternReportReference{},
		RawFailures: []storecontracts.RawFailureRecord{},
	}
	failedRunURLs := map[string]struct{}{}
	for _, row := range facts.RawFailures {
		if _, ok := failurePatternsStoredReferenceForRawFailure(row, referencesByKey); !ok {
			continue
		}
		match.FailureCount++
		match.RawFailures = append(match.RawFailures, row)
		runURL := strings.TrimSpace(row.RunURL)
		run := facts.RunsByURL[runURL]
		reference := failurePatternsReferenceFromRawFailure(row, run)
		if storedReference, ok := failurePatternsStoredReferenceForRawFailure(row, referencesByKey); ok {
			reference = failurePatternsOverlayStoredReference(reference, storedReference, run)
		}
		match.References = append(match.References, reference)
		if run.Failed && runURL != "" {
			failedRunURLs[runURL] = struct{}{}
		}
	}
	sortWindowedReferences(match.References)
	match.FailedRuns = len(failedRunURLs)
	return match
}

func failurePatternsReferenceMatchMap(rows []FailurePatternReportReference) map[string]FailurePatternReportReference {
	if len(rows) == 0 {
		return nil
	}
	out := make(map[string]FailurePatternReportReference, len(rows)*2)
	for _, row := range rows {
		for _, key := range failurePatternsReferenceMatchKeys(row) {
			out[key] = row
		}
	}
	return out
}

func failurePatternsReferenceMatchKeys(row FailurePatternReportReference) []string {
	keys := make([]string, 0, 2)
	rowID := strings.TrimSpace(row.RowID)
	if rowID != "" {
		keys = append(keys, "row|"+rowID)
	}
	if key := failurePatternsReferenceTupleKey(row.RunURL, row.OccurredAt, row.SignatureID); key != "" {
		keys = append(keys, key)
	}
	return keys
}

func failurePatternsRawFailureMatchKeys(row storecontracts.RawFailureRecord) []string {
	keys := make([]string, 0, 2)
	rowID := strings.TrimSpace(row.RowID)
	if rowID != "" {
		keys = append(keys, "row|"+rowID)
	}
	if key := failurePatternsReferenceTupleKey(row.RunURL, row.OccurredAt, row.SignatureID); key != "" {
		keys = append(keys, key)
	}
	return keys
}

func failurePatternsReferenceTupleKey(runURL string, occurredAt string, signatureID string) string {
	trimmedRunURL := strings.TrimSpace(runURL)
	trimmedOccurredAt := strings.TrimSpace(occurredAt)
	trimmedSignatureID := strings.TrimSpace(signatureID)
	if trimmedRunURL == "" && trimmedOccurredAt == "" && trimmedSignatureID == "" {
		return ""
	}
	return "ref|" + trimmedRunURL + "|" + trimmedOccurredAt + "|" + trimmedSignatureID
}

func failurePatternsReferenceDedupKey(row FailurePatternReportReference) string {
	keys := failurePatternsReferenceMatchKeys(row)
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}

func failurePatternsStoredReferenceForRawFailure(
	row storecontracts.RawFailureRecord,
	referencesByKey map[string]FailurePatternReportReference,
) (FailurePatternReportReference, bool) {
	for _, key := range failurePatternsRawFailureMatchKeys(row) {
		reference, ok := referencesByKey[key]
		if ok {
			return reference, true
		}
	}
	return FailurePatternReportReference{}, false
}

func failurePatternsReferenceFromRawFailure(
	row storecontracts.RawFailureRecord,
	run storecontracts.RunRecord,
) FailurePatternReportReference {
	return FailurePatternReportReference{
		RowID:          strings.TrimSpace(row.RowID),
		RunURL:         strings.TrimSpace(row.RunURL),
		OccurredAt:     strings.TrimSpace(row.OccurredAt),
		SignatureID:    strings.TrimSpace(row.SignatureID),
		PRNumber:       run.PRNumber,
		PostGoodCommit: run.PostGoodCommit,
	}
}

func failurePatternsOverlayStoredReference(
	raw FailurePatternReportReference,
	stored FailurePatternReportReference,
	run storecontracts.RunRecord,
) FailurePatternReportReference {
	out := raw
	if trimmed := strings.TrimSpace(stored.RowID); trimmed != "" {
		out.RowID = trimmed
	}
	if trimmed := strings.TrimSpace(stored.RunURL); trimmed != "" {
		out.RunURL = trimmed
	}
	if trimmed := strings.TrimSpace(stored.OccurredAt); trimmed != "" {
		out.OccurredAt = trimmed
	}
	if trimmed := strings.TrimSpace(stored.SignatureID); trimmed != "" {
		out.SignatureID = trimmed
	}
	if stored.PRNumber != 0 {
		out.PRNumber = stored.PRNumber
	} else if out.PRNumber == 0 {
		out.PRNumber = run.PRNumber
	}
	if stored.PostGoodCommit || run.PostGoodCommit {
		out.PostGoodCommit = true
	}
	return out
}

func collectWindowedPhraseEnvironments(row FailurePatternsRow, phraseEnvironments map[string]map[string]struct{}) {
	phraseKey := normalizePhrase(row.CanonicalEvidencePhrase)
	if phraseKey != "" && row.WindowFailureCount > 0 {
		set := phraseEnvironments[phraseKey]
		if set == nil {
			set = map[string]struct{}{}
			phraseEnvironments[phraseKey] = set
		}
		set[row.Environment] = struct{}{}
	}
	for _, child := range row.LinkedChildren {
		collectWindowedPhraseEnvironments(child, phraseEnvironments)
	}
}

func applyWindowedSeenIn(
	rows []FailurePatternsRow,
	phraseEnvironments map[string]map[string]struct{},
	currentEnvironment string,
) []FailurePatternsRow {
	if len(rows) == 0 {
		return nil
	}
	out := append([]FailurePatternsRow(nil), rows...)
	for index := range out {
		phraseKey := normalizePhrase(out[index].CanonicalEvidencePhrase)
		if phraseKey != "" {
			out[index].SeenIn = windowedSeenInOtherEnvironments(phraseEnvironments[phraseKey], currentEnvironment)
		}
		out[index].LinkedChildren = applyWindowedSeenIn(out[index].LinkedChildren, phraseEnvironments, currentEnvironment)
	}
	return out
}

func applyWindowedImpact(rows []FailurePatternsRow, totalRuns int) []FailurePatternsRow {
	if len(rows) == 0 {
		return nil
	}
	out := append([]FailurePatternsRow(nil), rows...)
	for index := range out {
		out[index].ImpactPercent = windowedImpactShare(out[index].JobsAffected, totalRuns)
		out[index].LinkedChildren = applyWindowedImpact(out[index].LinkedChildren, totalRuns)
	}
	return out
}

func buildFailurePatternsSummary(
	facts failurePatternsEnvironmentFacts,
	rows []FailurePatternsRow,
	totalRuns int,
) FailurePatternsSummary {
	matchedFailureCount := 0
	affectedRuns := map[string]struct{}{}
	for _, row := range rows {
		matchedFailureCount += row.WindowFailureCount
		for _, ref := range windowedRowAllReferences(row) {
			runURL := strings.TrimSpace(ref.RunURL)
			if runURL == "" {
				continue
			}
			affectedRuns[runURL] = struct{}{}
		}
	}
	return FailurePatternsSummary{
		TotalRuns:           totalRuns,
		FailedRuns:          facts.FailedRuns,
		RawFailureCount:     len(facts.RawFailures),
		MatchedFailureCount: matchedFailureCount,
		JobsAffected:        len(affectedRuns),
	}
}

func windowedRowAllReferences(row FailurePatternsRow) []FailurePatternReportReference {
	combined := append([]FailurePatternReportReference(nil), row.References...)
	for _, child := range row.LinkedChildren {
		combined = append(combined, windowedRowAllReferences(child)...)
	}
	return combined
}

func windowedDistinctRunCount(references []FailurePatternReportReference) int {
	seen := map[string]struct{}{}
	for _, row := range references {
		runURL := strings.TrimSpace(row.RunURL)
		if runURL == "" {
			continue
		}
		seen[runURL] = struct{}{}
	}
	return len(seen)
}

func windowedPostGoodCount(references []FailurePatternReportReference) int {
	if len(references) == 0 {
		return 0
	}
	total := 0
	for _, reference := range references {
		if reference.PostGoodCommit {
			total++
		}
	}
	return total
}

func windowedFailedRunsFromReferences(
	references []FailurePatternReportReference,
	runsByURL map[string]storecontracts.RunRecord,
) int {
	seen := map[string]struct{}{}
	for _, row := range references {
		runURL := strings.TrimSpace(row.RunURL)
		if runURL == "" {
			continue
		}
		run := runsByURL[runURL]
		if !run.Failed {
			continue
		}
		seen[runURL] = struct{}{}
	}
	return len(seen)
}

func windowedReferencesFromChildren(children []FailurePatternsRow) []FailurePatternReportReference {
	combined := make([]FailurePatternReportReference, 0)
	for _, child := range children {
		combined = append(combined, child.References...)
	}
	sortWindowedReferences(combined)
	return combined
}

func windowedFullErrorSamples(rows []storecontracts.RawFailureRecord, limit int) []string {
	if len(rows) == 0 || limit <= 0 {
		return nil
	}
	ordered := append([]storecontracts.RawFailureRecord(nil), rows...)
	sortWindowedRawFailures(ordered)
	samples := make([]string, 0, limit)
	for _, row := range ordered {
		samples = failurePatternReportAppendUniqueLimitedSample(samples, sampleFailureText(row), limit)
		if len(samples) >= limit {
			break
		}
	}
	return samples
}

func windowedFullErrorSamplesFromChildren(children []FailurePatternsRow, limit int) []string {
	if len(children) == 0 || limit <= 0 {
		return nil
	}
	samples := make([]string, 0, limit)
	for _, child := range children {
		for _, sample := range child.FullErrorSamples {
			samples = failurePatternReportAppendUniqueLimitedSample(samples, sample, limit)
			if len(samples) >= limit {
				return samples
			}
		}
	}
	return samples
}

func windowedSeenInOtherEnvironments(seenByEnvironment map[string]struct{}, currentEnvironment string) []string {
	if len(seenByEnvironment) == 0 {
		return nil
	}
	out := make([]string, 0, len(seenByEnvironment))
	for environment := range seenByEnvironment {
		normalizedEnvironment := normalizeEnvironment(environment)
		if normalizedEnvironment == "" || normalizedEnvironment == normalizeEnvironment(currentEnvironment) {
			continue
		}
		out = append(out, strings.ToUpper(normalizedEnvironment))
	}
	sort.Strings(out)
	return out
}

func sortFailurePatternsRows(rows []FailurePatternsRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].WindowFailureCount != rows[j].WindowFailureCount {
			return rows[i].WindowFailureCount > rows[j].WindowFailureCount
		}
		if rows[i].JobsAffected != rows[j].JobsAffected {
			return rows[i].JobsAffected > rows[j].JobsAffected
		}
		if rows[i].WeeklySupportCount != rows[j].WeeklySupportCount {
			return rows[i].WeeklySupportCount > rows[j].WeeklySupportCount
		}
		left := strings.TrimSpace(rows[i].CanonicalEvidencePhrase)
		right := strings.TrimSpace(rows[j].CanonicalEvidencePhrase)
		if left != right {
			return left < right
		}
		return strings.TrimSpace(rows[i].ClusterID) < strings.TrimSpace(rows[j].ClusterID)
	})
}

func sortWindowedReferences(rows []FailurePatternReportReference) {
	sort.Slice(rows, func(i, j int) bool {
		ti, okI := ParseReferenceTimestamp(rows[i].OccurredAt)
		tj, okJ := ParseReferenceTimestamp(rows[j].OccurredAt)
		switch {
		case okI && okJ && !ti.Equal(tj):
			return ti.After(tj)
		case okI != okJ:
			return okI
		}
		if strings.TrimSpace(rows[i].RunURL) != strings.TrimSpace(rows[j].RunURL) {
			return strings.TrimSpace(rows[i].RunURL) < strings.TrimSpace(rows[j].RunURL)
		}
		if strings.TrimSpace(rows[i].SignatureID) != strings.TrimSpace(rows[j].SignatureID) {
			return strings.TrimSpace(rows[i].SignatureID) < strings.TrimSpace(rows[j].SignatureID)
		}
		return strings.TrimSpace(rows[i].RowID) < strings.TrimSpace(rows[j].RowID)
	})
}

func sortWindowedRawFailures(rows []storecontracts.RawFailureRecord) {
	sort.Slice(rows, func(i, j int) bool {
		ti, okI := ParseReferenceTimestamp(rows[i].OccurredAt)
		tj, okJ := ParseReferenceTimestamp(rows[j].OccurredAt)
		switch {
		case okI && okJ && !ti.Equal(tj):
			return ti.After(tj)
		case okI != okJ:
			return okI
		}
		if rows[i].RunURL != rows[j].RunURL {
			return rows[i].RunURL < rows[j].RunURL
		}
		if rows[i].SignatureID != rows[j].SignatureID {
			return rows[i].SignatureID < rows[j].SignatureID
		}
		return rows[i].RowID < rows[j].RowID
	})
}

func toWindowedHTMLRunReferences(rows []FailurePatternReportReference) []RunReference {
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

func primaryContributingTestForReport(rows []FailurePatternReportContributingTest) FailurePatternReportContributingTest {
	if len(rows) == 0 {
		return FailurePatternReportContributingTest{}
	}
	best := rows[0]
	for _, row := range rows[1:] {
		if row.SupportCount != best.SupportCount {
			if row.SupportCount > best.SupportCount {
				best = row
			}
			continue
		}
		currentKey := strings.TrimSpace(row.Lane) + "|" + strings.TrimSpace(row.JobName) + "|" + strings.TrimSpace(row.TestName)
		bestKey := strings.TrimSpace(best.Lane) + "|" + strings.TrimSpace(best.JobName) + "|" + strings.TrimSpace(best.TestName)
		if currentKey < bestKey {
			best = row
		}
	}
	return best
}

func windowedImpactShare(jobsAffected int, totalRuns int) float64 {
	if totalRuns <= 0 {
		return 0
	}
	return (float64(jobsAffected) * 100.0) / float64(totalRuns)
}
