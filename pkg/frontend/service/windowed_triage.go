package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"ci-failure-atlas/pkg/report/triagehtml"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

type WindowedTriageQuery struct {
	StartDate    string
	EndDate      string
	Week         string
	Environments []string
	GeneratedAt  time.Time
}

type WindowedTriageData struct {
	Meta         WindowedTriageMeta          `json:"meta"`
	Environments []WindowedTriageEnvironment `json:"environments"`
}

type WindowedTriageMeta struct {
	StartDate    string   `json:"start_date"`
	EndDate      string   `json:"end_date"`
	ResolvedWeek string   `json:"resolved_week"`
	Timezone     string   `json:"timezone"`
	GeneratedAt  string   `json:"generated_at"`
	Environments []string `json:"environments"`
}

type WindowedTriageEnvironment struct {
	Environment string                `json:"environment"`
	Summary     WindowedTriageSummary `json:"summary"`
	Rows        []WindowedTriageRow   `json:"rows"`
}

type WindowedTriageSummary struct {
	TotalRuns           int `json:"total_runs"`
	FailedRuns          int `json:"failed_runs"`
	RawFailureCount     int `json:"raw_failure_count"`
	MatchedFailureCount int `json:"matched_failure_count"`
	JobsAffected        int `json:"jobs_affected"`
}

type WindowedTriageRow struct {
	Environment             string                         `json:"environment"`
	ClusterID               string                         `json:"cluster_id"`
	CanonicalEvidencePhrase string                         `json:"canonical_evidence_phrase"`
	SearchQueryPhrase       string                         `json:"search_query_phrase,omitempty"`
	Lane                    string                         `json:"lane,omitempty"`
	JobName                 string                         `json:"job_name,omitempty"`
	TestName                string                         `json:"test_name,omitempty"`
	TestSuite               string                         `json:"test_suite,omitempty"`
	WindowFailureCount      int                            `json:"window_failure_count"`
	JobsAffected            int                            `json:"jobs_affected"`
	FailedRuns              int                            `json:"failed_runs"`
	ImpactPercent           float64                        `json:"impact_percent"`
	WeeklySupportCount      int                            `json:"weekly_support_count"`
	WeeklyPostGoodCount     int                            `json:"weekly_post_good_count"`
	SeenIn                  []string                       `json:"seen_in,omitempty"`
	TrendCounts             []int                          `json:"trend_counts,omitempty"`
	TrendRange              string                         `json:"trend_range,omitempty"`
	PriorWeeksPresent       int                            `json:"prior_weeks_present"`
	PriorWeekStarts         []string                       `json:"prior_week_starts,omitempty"`
	PriorJobsAffected       int                            `json:"prior_jobs_affected"`
	PriorLastSeenAt         string                         `json:"prior_last_seen_at,omitempty"`
	ContributingTests       []TriageReportContributingTest `json:"contributing_tests,omitempty"`
	FullErrorSamples        []string                       `json:"full_error_samples,omitempty"`
	References              []TriageReportReference        `json:"references,omitempty"`
	ScoringReferences       []TriageReportReference        `json:"-"`
	LinkedChildren          []WindowedTriageRow            `json:"linked_children,omitempty"`
}

type windowedTriageScope struct {
	StartDate         string
	EndDate           string
	StartTime         time.Time
	EndTime           time.Time
	ResolvedWeek      string
	SemanticWeekStart time.Time
	SemanticWeekEnd   time.Time
	DateLabels        []string
}

type windowedTriageEnvironmentFacts struct {
	RawFailures []storecontracts.RawFailureRecord
	RunsByURL   map[string]storecontracts.RunRecord
	FailedRuns  int
}

type windowedTriageMatch struct {
	FailureCount int
	References   []TriageReportReference
	RawFailures  []storecontracts.RawFailureRecord
	FailedRuns   int
}

func (s *Service) BuildWindowedTriage(ctx context.Context, query WindowedTriageQuery) (WindowedTriageData, error) {
	if s == nil {
		return WindowedTriageData{}, fmt.Errorf("service is required")
	}

	scope, err := resolveWindowedTriageScope(query)
	if err != nil {
		return WindowedTriageData{}, err
	}
	scope.ResolvedWeek, err = s.ensureWeekExists(ctx, scope.ResolvedWeek)
	if err != nil {
		return WindowedTriageData{}, err
	}

	store, err := s.OpenStoreForWeek(scope.ResolvedWeek)
	if err != nil {
		return WindowedTriageData{}, err
	}
	defer func() {
		_ = store.Close()
	}()

	historyResolver, err := s.BuildHistoryResolver(ctx, scope.ResolvedWeek)
	if err != nil {
		return WindowedTriageData{}, fmt.Errorf("build windowed triage history resolver: %w", err)
	}
	weeklyData, err := BuildTriageReportData(ctx, store, TriageReportBuildOptions{
		Week:                scope.ResolvedWeek,
		Environments:        query.Environments,
		HistoryHorizonWeeks: s.historyWeeks,
		HistoryResolver:     historyResolver,
	})
	if err != nil {
		return WindowedTriageData{}, fmt.Errorf("build weekly triage data for window: %w", err)
	}

	targetEnvironments := append([]string(nil), weeklyData.TargetEnvironments...)
	if len(targetEnvironments) == 0 {
		targetEnvironments = normalizeStringSlice(query.Environments)
	}
	factsByEnvironment, err := loadWindowedTriageFacts(ctx, store, targetEnvironments, scope)
	if err != nil {
		return WindowedTriageData{}, err
	}
	metricRunTotals, err := triageReportMetricRunTotalsByEnvironment(
		ctx,
		store,
		targetEnvironments,
		scope.StartTime,
		scope.EndTime,
	)
	if err != nil {
		return WindowedTriageData{}, fmt.Errorf("load windowed triage metric run totals: %w", err)
	}

	trendAnchor := scope.SemanticWeekEnd.Add(-time.Nanosecond).UTC()
	if trendAnchor.IsZero() {
		trendAnchor = time.Now().UTC()
	}

	rowsByEnvironment := map[string][]WindowedTriageRow{}
	phraseEnvironments := map[string]map[string]struct{}{}
	for _, cluster := range weeklyData.TriageClusters {
		environment := normalizeEnvironment(cluster.Environment)
		if environment == "" {
			continue
		}
		facts := factsByEnvironment[environment]
		row, ok := buildWindowedTriageRow(cluster, facts, weeklyData.HistoryResolver, trendAnchor)
		if !ok {
			continue
		}
		rowsByEnvironment[environment] = append(rowsByEnvironment[environment], row)
		collectWindowedPhraseEnvironments(row, phraseEnvironments)
	}

	generatedAt := query.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	environments := make([]WindowedTriageEnvironment, 0, len(targetEnvironments))
	for _, environment := range targetEnvironments {
		rows := applyWindowedSeenIn(rowsByEnvironment[environment], phraseEnvironments, environment)
		totalRuns := metricRunTotals[environment]
		if totalRuns <= 0 {
			totalRuns = len(factsByEnvironment[environment].RunsByURL)
		}
		rows = applyWindowedImpact(rows, totalRuns)
		sortWindowedTriageRows(rows)
		environments = append(environments, WindowedTriageEnvironment{
			Environment: environment,
			Summary:     buildWindowedTriageSummary(factsByEnvironment[environment], rows, totalRuns),
			Rows:        rows,
		})
	}

	return WindowedTriageData{
		Meta: WindowedTriageMeta{
			StartDate:    scope.StartDate,
			EndDate:      scope.EndDate,
			ResolvedWeek: scope.ResolvedWeek,
			Timezone:     "UTC",
			GeneratedAt:  generatedAt.UTC().Format(time.RFC3339),
			Environments: append([]string(nil), targetEnvironments...),
		},
		Environments: environments,
	}, nil
}

func resolveWindowedTriageScope(query WindowedTriageQuery) (windowedTriageScope, error) {
	startLabel, startDate, err := normalizeDateLabel(query.StartDate)
	if err != nil {
		return windowedTriageScope{}, fmt.Errorf("invalid start_date: %w", err)
	}
	endLabel, endDate, err := normalizeDateLabel(query.EndDate)
	if err != nil {
		return windowedTriageScope{}, fmt.Errorf("invalid end_date: %w", err)
	}
	startTime := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.UTC)
	endInclusive := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, time.UTC)
	if endInclusive.Before(startTime) {
		return windowedTriageScope{}, fmt.Errorf("end_date %s must be on or after start_date %s", endLabel, startLabel)
	}
	endTime := endInclusive.AddDate(0, 0, 1).UTC()

	resolvedWeek, err := resolveWindowedTriageWeekLabel(startTime, endInclusive, query.Week)
	if err != nil {
		return windowedTriageScope{}, err
	}
	semanticWeekStart, err := time.Parse("2006-01-02", resolvedWeek)
	if err != nil {
		return windowedTriageScope{}, fmt.Errorf("parse resolved week %q: %w", resolvedWeek, err)
	}
	semanticWeekStart = semanticWeekStart.UTC()
	semanticWeekEnd := semanticWeekStart.AddDate(0, 0, 7).UTC()
	if startTime.Before(semanticWeekStart) || !endInclusive.Before(semanticWeekEnd) {
		return windowedTriageScope{}, fmt.Errorf(
			"window %s..%s must fall within one semantic week (%s..%s)",
			startLabel,
			endLabel,
			semanticWeekStart.Format("2006-01-02"),
			semanticWeekEnd.AddDate(0, 0, -1).Format("2006-01-02"),
		)
	}

	return windowedTriageScope{
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

func resolveWindowedTriageWeekLabel(startDate time.Time, endDate time.Time, override string) (string, error) {
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

	startWeek := startDate.AddDate(0, 0, -int(startDate.Weekday())).UTC().Format("2006-01-02")
	endWeek := endDate.AddDate(0, 0, -int(endDate.Weekday())).UTC().Format("2006-01-02")
	if startWeek != endWeek {
		return "", fmt.Errorf("window %s..%s crosses semantic week boundaries (%s vs %s)", startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), startWeek, endWeek)
	}
	return startWeek, nil
}

func loadWindowedTriageFacts(
	ctx context.Context,
	store storecontracts.Store,
	environments []string,
	scope windowedTriageScope,
) (map[string]windowedTriageEnvironmentFacts, error) {
	factsByEnvironment := make(map[string]windowedTriageEnvironmentFacts, len(environments))
	for _, environment := range environments {
		normalizedEnvironment := normalizeEnvironment(environment)
		if normalizedEnvironment == "" {
			continue
		}
		facts := windowedTriageEnvironmentFacts{
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

func fillMissingRunsForWindowFacts(
	ctx context.Context,
	store storecontracts.Store,
	environment string,
	facts *windowedTriageEnvironmentFacts,
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

func buildWindowedTriageRow(
	cluster TriageReportCluster,
	facts windowedTriageEnvironmentFacts,
	historyResolver semhistory.GlobalSignatureResolver,
	trendAnchor time.Time,
) (WindowedTriageRow, bool) {
	children := make([]WindowedTriageRow, 0, len(cluster.LinkedChildren))
	for _, child := range cluster.LinkedChildren {
		childRow, ok := buildWindowedTriageRow(child, facts, nil, trendAnchor)
		if !ok {
			continue
		}
		children = append(children, childRow)
	}

	match := matchWindowedTriageCluster(cluster, facts)
	if match.FailureCount == 0 && len(children) == 0 {
		return WindowedTriageRow{}, false
	}

	primary := primaryContributingTestForReport(cluster.ContributingTests)
	references := append([]TriageReportReference(nil), match.References...)
	failedRuns := match.FailedRuns
	failureCount := match.FailureCount
	fullErrorSamples := windowedFullErrorSamples(match.RawFailures, triageReportFullErrorExamplesLimit)
	if len(references) == 0 && len(children) > 0 {
		references = windowedReferencesFromChildren(children)
		failedRuns = windowedFailedRunsFromReferences(references, facts.RunsByURL)
		failureCount = 0
		for _, child := range children {
			failureCount += child.WindowFailureCount
		}
		fullErrorSamples = windowedFullErrorSamplesFromChildren(children, triageReportFullErrorExamplesLimit)
	}

	weeklyReferences := append([]TriageReportReference(nil), cluster.References...)
	row := WindowedTriageRow{
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
		WeeklyPostGoodCount:     cluster.PostGoodCommitCount,
		ContributingTests:       append([]TriageReportContributingTest(nil), cluster.ContributingTests...),
		FullErrorSamples:        fullErrorSamples,
		References:              references,
		ScoringReferences:       weeklyReferences,
		LinkedChildren:          children,
	}

	if historyResolver != nil {
		presence := semhistory.SignaturePresence{}
		if len(children) > 0 {
			presence = historyResolver.PresenceForPhase3Cluster(row.Environment, row.ClusterID)
		} else {
			presence = historyResolver.PresenceFor(semhistory.SignatureKey{
				Environment: row.Environment,
				Phrase:      row.CanonicalEvidencePhrase,
				SearchQuery: row.SearchQueryPhrase,
			})
		}
		row.PriorWeeksPresent = presence.PriorWeeksPresent
		row.PriorWeekStarts = append([]string(nil), presence.PriorWeekStarts...)
		row.PriorJobsAffected = presence.PriorJobsAffected
		if !presence.PriorLastSeenAt.IsZero() {
			row.PriorLastSeenAt = presence.PriorLastSeenAt.UTC().Format(time.RFC3339)
		}
	}

	if counts, trendRange, ok := buildWindowedTrend(weeklyReferences, trendAnchor); ok {
		row.TrendCounts = counts
		row.TrendRange = trendRange
	}

	sortWindowedTriageRows(row.LinkedChildren)
	return row, true
}

func buildWindowedTrend(references []TriageReportReference, trendAnchor time.Time) ([]int, string, bool) {
	if trendAnchor.IsZero() {
		return nil, "", false
	}
	if _, counts, trendRange, ok := triagehtml.DailyDensitySparkline(toWindowedHTMLRunReferences(references), 7, trendAnchor); ok {
		return append([]int(nil), counts...), trendRange, true
	}
	return nil, "", false
}

func matchWindowedTriageCluster(cluster TriageReportCluster, facts windowedTriageEnvironmentFacts) windowedTriageMatch {
	signatureSet := map[string]struct{}{}
	for _, signatureID := range cluster.MemberSignatureIDs {
		trimmed := strings.TrimSpace(signatureID)
		if trimmed == "" {
			continue
		}
		signatureSet[trimmed] = struct{}{}
	}
	if len(signatureSet) == 0 {
		return windowedTriageMatch{}
	}

	match := windowedTriageMatch{
		References:  []TriageReportReference{},
		RawFailures: []storecontracts.RawFailureRecord{},
	}
	failedRunURLs := map[string]struct{}{}
	for _, row := range facts.RawFailures {
		signatureID := strings.TrimSpace(row.SignatureID)
		if _, ok := signatureSet[signatureID]; !ok {
			continue
		}
		match.FailureCount++
		match.RawFailures = append(match.RawFailures, row)
		runURL := strings.TrimSpace(row.RunURL)
		run := facts.RunsByURL[runURL]
		match.References = append(match.References, TriageReportReference{
			RunURL:         runURL,
			OccurredAt:     strings.TrimSpace(row.OccurredAt),
			SignatureID:    signatureID,
			PRNumber:       run.PRNumber,
			PostGoodCommit: run.PostGoodCommit,
		})
		if run.Failed && runURL != "" {
			failedRunURLs[runURL] = struct{}{}
		}
	}
	sortWindowedReferences(match.References)
	match.FailedRuns = len(failedRunURLs)
	return match
}

func collectWindowedPhraseEnvironments(row WindowedTriageRow, phraseEnvironments map[string]map[string]struct{}) {
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
	rows []WindowedTriageRow,
	phraseEnvironments map[string]map[string]struct{},
	currentEnvironment string,
) []WindowedTriageRow {
	if len(rows) == 0 {
		return nil
	}
	out := append([]WindowedTriageRow(nil), rows...)
	for index := range out {
		phraseKey := normalizePhrase(out[index].CanonicalEvidencePhrase)
		if phraseKey != "" {
			out[index].SeenIn = windowedSeenInOtherEnvironments(phraseEnvironments[phraseKey], currentEnvironment)
		}
		out[index].LinkedChildren = applyWindowedSeenIn(out[index].LinkedChildren, phraseEnvironments, currentEnvironment)
	}
	return out
}

func applyWindowedImpact(rows []WindowedTriageRow, totalRuns int) []WindowedTriageRow {
	if len(rows) == 0 {
		return nil
	}
	out := append([]WindowedTriageRow(nil), rows...)
	for index := range out {
		out[index].ImpactPercent = windowedImpactShare(out[index].JobsAffected, totalRuns)
		out[index].LinkedChildren = applyWindowedImpact(out[index].LinkedChildren, totalRuns)
	}
	return out
}

func buildWindowedTriageSummary(
	facts windowedTriageEnvironmentFacts,
	rows []WindowedTriageRow,
	totalRuns int,
) WindowedTriageSummary {
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
	return WindowedTriageSummary{
		TotalRuns:           totalRuns,
		FailedRuns:          facts.FailedRuns,
		RawFailureCount:     len(facts.RawFailures),
		MatchedFailureCount: matchedFailureCount,
		JobsAffected:        len(affectedRuns),
	}
}

func windowedRowAllReferences(row WindowedTriageRow) []TriageReportReference {
	combined := append([]TriageReportReference(nil), row.References...)
	for _, child := range row.LinkedChildren {
		combined = append(combined, windowedRowAllReferences(child)...)
	}
	return combined
}

func windowedDistinctRunCount(references []TriageReportReference) int {
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

func windowedFailedRunsFromReferences(
	references []TriageReportReference,
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

func windowedReferencesFromChildren(children []WindowedTriageRow) []TriageReportReference {
	combined := make([]TriageReportReference, 0)
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
		samples = triageReportAppendUniqueLimitedSample(samples, sampleFailureText(row), limit)
		if len(samples) >= limit {
			break
		}
	}
	return samples
}

func windowedFullErrorSamplesFromChildren(children []WindowedTriageRow, limit int) []string {
	if len(children) == 0 || limit <= 0 {
		return nil
	}
	samples := make([]string, 0, limit)
	for _, child := range children {
		for _, sample := range child.FullErrorSamples {
			samples = triageReportAppendUniqueLimitedSample(samples, sample, limit)
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

func sortWindowedTriageRows(rows []WindowedTriageRow) {
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

func sortWindowedReferences(rows []TriageReportReference) {
	sort.Slice(rows, func(i, j int) bool {
		ti, okI := triagehtml.ParseReferenceTimestamp(rows[i].OccurredAt)
		tj, okJ := triagehtml.ParseReferenceTimestamp(rows[j].OccurredAt)
		switch {
		case okI && okJ && !ti.Equal(tj):
			return ti.After(tj)
		case okI != okJ:
			return okI
		}
		if strings.TrimSpace(rows[i].RunURL) != strings.TrimSpace(rows[j].RunURL) {
			return strings.TrimSpace(rows[i].RunURL) < strings.TrimSpace(rows[j].RunURL)
		}
		return strings.TrimSpace(rows[i].SignatureID) < strings.TrimSpace(rows[j].SignatureID)
	})
}

func sortWindowedRawFailures(rows []storecontracts.RawFailureRecord) {
	sort.Slice(rows, func(i, j int) bool {
		ti, okI := triagehtml.ParseReferenceTimestamp(rows[i].OccurredAt)
		tj, okJ := triagehtml.ParseReferenceTimestamp(rows[j].OccurredAt)
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

func toWindowedHTMLRunReferences(rows []TriageReportReference) []triagehtml.RunReference {
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

func primaryContributingTestForReport(rows []TriageReportContributingTest) TriageReportContributingTest {
	if len(rows) == 0 {
		return TriageReportContributingTest{}
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
