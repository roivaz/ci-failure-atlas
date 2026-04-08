package service

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

type DailyTriageQuery struct {
	Date         string
	Week         string
	Environments []string
	GeneratedAt  time.Time
}

type DailyTriageResponse struct {
	Meta         DailyTriageMeta          `json:"meta"`
	Environments []DailyTriageEnvironment `json:"environments"`
}

type DailyTriageMeta struct {
	Date         string   `json:"date"`
	ResolvedWeek string   `json:"resolved_week"`
	GeneratedAt  string   `json:"generated_at"`
	Environments []string `json:"environments"`
}

type DailyTriageEnvironment struct {
	Environment string             `json:"environment"`
	Summary     DailyTriageSummary `json:"summary"`
	Items       []DailyTriageItem  `json:"items"`
}

type DailyTriageSummary struct {
	RawFailureCount         int `json:"raw_failure_count"`
	DistinctRuns            int `json:"distinct_runs"`
	FailedRuns              int `json:"failed_runs"`
	PostGoodRawFailureCount int `json:"post_good_raw_failure_count"`
	PostGoodRunCount        int `json:"post_good_run_count"`
	MatchedClusterCount     int `json:"matched_cluster_count"`
	UnmatchedSignatureCount int `json:"unmatched_signature_count"`
}

type DailyTriageItem struct {
	Environment             string                   `json:"environment"`
	SignatureID             string                   `json:"signature_id,omitempty"`
	ClusterID               string                   `json:"cluster_id,omitempty"`
	CanonicalEvidencePhrase string                   `json:"canonical_evidence_phrase,omitempty"`
	SearchQueryPhrase       string                   `json:"search_query_phrase,omitempty"`
	Lane                    string                   `json:"lane,omitempty"`
	JobName                 string                   `json:"job_name,omitempty"`
	TestName                string                   `json:"test_name,omitempty"`
	TestSuite               string                   `json:"test_suite,omitempty"`
	DailyCount              int                      `json:"daily_count"`
	DistinctRuns            int                      `json:"distinct_runs"`
	FailedRuns              int                      `json:"failed_runs"`
	PostGoodDailyCount      int                      `json:"post_good_daily_count"`
	PostGoodRunCount        int                      `json:"post_good_run_count"`
	WeeklySupportCount      int                      `json:"weekly_support_count"`
	WeeklyPostGoodCount     int                      `json:"weekly_post_good_count"`
	AffectedEnvironments    []string                 `json:"affected_environments,omitempty"`
	PriorWeeksPresent       int                      `json:"prior_weeks_present"`
	PriorWeekStarts         []string                 `json:"prior_week_starts,omitempty"`
	PriorLastSeenAt         string                   `json:"prior_last_seen_at,omitempty"`
	Phase3Issues            []DailyTriagePhase3Issue `json:"phase3_issues,omitempty"`
	DailyRunURLs            []string                 `json:"daily_run_urls,omitempty"`
	SampleRawText           string                   `json:"sample_raw_text,omitempty"`
}

type DailyTriagePhase3Issue struct {
	IssueID string `json:"issue_id"`
	Title   string `json:"title,omitempty"`
}

type dailyWeekContext struct {
	availableEnvironments []string
	clusterBySignature    map[string]semanticcontracts.GlobalClusterRecord
	phraseEnvironments    map[string][]string
	phase3IssueByAnchor   map[string]string
	phase3IssuesByID      map[string]semanticcontracts.Phase3IssueRecord
}

type dailyItemAggregate struct {
	item            DailyTriageItem
	cluster         semanticcontracts.GlobalClusterRecord
	hasCluster      bool
	runURLs         map[string]struct{}
	failedRunURLs   map[string]struct{}
	postGoodRunURLs map[string]struct{}
	issueIDs        map[string]struct{}
}

func (s *Service) BuildDailyTriage(ctx context.Context, query DailyTriageQuery) (DailyTriageResponse, error) {
	dateLabel, dateValue, err := normalizeDateLabel(query.Date)
	if err != nil {
		return DailyTriageResponse{}, err
	}
	resolvedWeek, err := resolveDailyWeekLabel(dateValue, query.Week)
	if err != nil {
		return DailyTriageResponse{}, err
	}
	resolvedWeek, err = s.ensureWeekExists(ctx, resolvedWeek)
	if err != nil {
		return DailyTriageResponse{}, err
	}

	store, err := s.OpenStoreForWeek(resolvedWeek)
	if err != nil {
		return DailyTriageResponse{}, err
	}
	defer func() {
		_ = store.Close()
	}()

	weekData, err := semanticquery.LoadWeekData(ctx, store, semanticquery.LoadWeekDataOptions{})
	if err != nil {
		return DailyTriageResponse{}, fmt.Errorf("load semantic week data: %w", err)
	}
	phase3Issues, err := store.ListPhase3Issues(ctx)
	if err != nil {
		return DailyTriageResponse{}, fmt.Errorf("list phase3 issues: %w", err)
	}
	historyResolver, err := s.BuildHistoryResolver(ctx, resolvedWeek)
	if err != nil {
		return DailyTriageResponse{}, fmt.Errorf("build signature history resolver: %w", err)
	}

	weekContext := buildDailyWeekContext(weekData, phase3Issues)
	targetEnvironments, err := resolveDailyEnvironments(query.Environments, weekContext.availableEnvironments)
	if err != nil {
		return DailyTriageResponse{}, err
	}

	environments := make([]DailyTriageEnvironment, 0, len(targetEnvironments))
	for _, environment := range targetEnvironments {
		projected, err := buildDailyTriageEnvironment(ctx, store, environment, dateLabel, weekContext, historyResolver)
		if err != nil {
			return DailyTriageResponse{}, err
		}
		environments = append(environments, projected)
	}

	generatedAt := query.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	return DailyTriageResponse{
		Meta: DailyTriageMeta{
			Date:         dateLabel,
			ResolvedWeek: resolvedWeek,
			GeneratedAt:  generatedAt.UTC().Format(time.RFC3339),
			Environments: append([]string(nil), targetEnvironments...),
		},
		Environments: environments,
	}, nil
}

func buildDailyTriageEnvironment(
	ctx context.Context,
	store storecontracts.Store,
	environment string,
	dateLabel string,
	weekContext dailyWeekContext,
	historyResolver semhistory.GlobalSignatureResolver,
) (DailyTriageEnvironment, error) {
	rawFailures, err := store.ListRawFailuresByDate(ctx, environment, dateLabel)
	if err != nil {
		return DailyTriageEnvironment{}, fmt.Errorf("list raw failures for %s on %s: %w", environment, dateLabel, err)
	}
	runs, err := store.ListRunsByDate(ctx, environment, dateLabel)
	if err != nil {
		return DailyTriageEnvironment{}, fmt.Errorf("list runs for %s on %s: %w", environment, dateLabel, err)
	}

	runsByURL := map[string]storecontracts.RunRecord{}
	for _, row := range runs {
		runURL := strings.TrimSpace(row.RunURL)
		if runURL == "" {
			continue
		}
		runsByURL[runURL] = row
	}

	summary := DailyTriageSummary{}
	itemsByKey := map[string]*dailyItemAggregate{}
	for _, row := range rawFailures {
		runURL := strings.TrimSpace(row.RunURL)
		rowID := strings.TrimSpace(row.RowID)
		signatureID := strings.TrimSpace(row.SignatureID)
		itemKey := signatureID
		if itemKey == "" {
			itemKey = "row:" + rowID
		}
		aggregate := itemsByKey[itemKey]
		if aggregate == nil {
			aggregate = &dailyItemAggregate{
				item: DailyTriageItem{
					Environment:   environment,
					SignatureID:   signatureID,
					TestName:      strings.TrimSpace(row.TestName),
					TestSuite:     strings.TrimSpace(row.TestSuite),
					SampleRawText: sampleFailureText(row),
				},
				runURLs:         map[string]struct{}{},
				failedRunURLs:   map[string]struct{}{},
				postGoodRunURLs: map[string]struct{}{},
				issueIDs:        map[string]struct{}{},
			}
			if cluster, ok := weekContext.clusterBySignature[signatureLookupKey(environment, signatureID)]; ok {
				aggregate.cluster = cluster
				aggregate.hasCluster = true
				applyClusterMetadata(&aggregate.item, cluster, weekContext.phraseEnvironments)
			}
			itemsByKey[itemKey] = aggregate
		}

		summary.RawFailureCount++
		aggregate.item.DailyCount++
		if runURL != "" {
			aggregate.runURLs[runURL] = struct{}{}
			if _, ok := runsByURL[runURL]; !ok {
				run, found, err := store.GetRun(ctx, environment, runURL)
				if err != nil {
					return DailyTriageEnvironment{}, fmt.Errorf("get run %s for %s: %w", runURL, environment, err)
				}
				if found {
					runsByURL[runURL] = run
				}
			}
		}

		if runURL != "" {
			if run, ok := runsByURL[runURL]; ok {
				if run.Failed {
					aggregate.failedRunURLs[runURL] = struct{}{}
				}
				if run.PostGoodCommit {
					aggregate.postGoodRunURLs[runURL] = struct{}{}
					aggregate.item.PostGoodDailyCount++
					summary.PostGoodRawFailureCount++
				}
			}
		}

		if issueID := weekContext.phase3IssueByAnchor[phase3AnchorKey(environment, runURL, rowID)]; issueID != "" {
			aggregate.issueIDs[issueID] = struct{}{}
		}
	}

	summary.DistinctRuns = len(runsByURL)
	for _, row := range runsByURL {
		if row.Failed {
			summary.FailedRuns++
		}
		if row.PostGoodCommit {
			summary.PostGoodRunCount++
		}
	}

	items := make([]DailyTriageItem, 0, len(itemsByKey))
	for _, aggregate := range itemsByKey {
		if aggregate.hasCluster {
			if issue := strings.TrimSpace(aggregate.cluster.Phase2ClusterID); issue != "" {
				if _, ok := weekContext.phase3IssuesByID[issue]; ok {
					aggregate.issueIDs[issue] = struct{}{}
				}
			}
		}
		aggregate.item.DistinctRuns = len(aggregate.runURLs)
		aggregate.item.FailedRuns = len(aggregate.failedRunURLs)
		aggregate.item.PostGoodRunCount = len(aggregate.postGoodRunURLs)
		aggregate.item.DailyRunURLs = sortedStringSet(aggregate.runURLs)
		aggregate.item.Phase3Issues = collectDailyPhase3Issues(aggregate.issueIDs, weekContext.phase3IssuesByID)

		historyPresence := semhistory.SignaturePresence{}
		if len(aggregate.item.Phase3Issues) == 1 {
			historyPresence = historyResolver.PresenceForPhase3Cluster(environment, aggregate.item.Phase3Issues[0].IssueID)
		} else if aggregate.hasCluster {
			historyPresence = historyResolver.PresenceFor(semhistory.SignatureKey{
				Environment: environment,
				Phrase:      aggregate.cluster.CanonicalEvidencePhrase,
				SearchQuery: aggregate.cluster.SearchQueryPhrase,
			})
		}
		aggregate.item.PriorWeeksPresent = historyPresence.PriorWeeksPresent
		aggregate.item.PriorWeekStarts = append([]string(nil), historyPresence.PriorWeekStarts...)
		if !historyPresence.PriorLastSeenAt.IsZero() {
			aggregate.item.PriorLastSeenAt = historyPresence.PriorLastSeenAt.UTC().Format(time.RFC3339)
		}

		if aggregate.hasCluster {
			summary.MatchedClusterCount++
		} else {
			summary.UnmatchedSignatureCount++
		}
		items = append(items, aggregate.item)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].DailyCount != items[j].DailyCount {
			return items[i].DailyCount > items[j].DailyCount
		}
		if items[i].PostGoodDailyCount != items[j].PostGoodDailyCount {
			return items[i].PostGoodDailyCount > items[j].PostGoodDailyCount
		}
		if items[i].WeeklySupportCount != items[j].WeeklySupportCount {
			return items[i].WeeklySupportCount > items[j].WeeklySupportCount
		}
		leftLabel := strings.TrimSpace(items[i].CanonicalEvidencePhrase)
		if leftLabel == "" {
			leftLabel = strings.TrimSpace(items[i].SampleRawText)
		}
		rightLabel := strings.TrimSpace(items[j].CanonicalEvidencePhrase)
		if rightLabel == "" {
			rightLabel = strings.TrimSpace(items[j].SampleRawText)
		}
		if leftLabel != rightLabel {
			return leftLabel < rightLabel
		}
		return strings.TrimSpace(items[i].SignatureID) < strings.TrimSpace(items[j].SignatureID)
	})

	return DailyTriageEnvironment{
		Environment: environment,
		Summary:     summary,
		Items:       items,
	}, nil
}

func buildDailyWeekContext(
	weekData semanticquery.WeekData,
	phase3Issues []semanticcontracts.Phase3IssueRecord,
) dailyWeekContext {
	phraseEnvironmentSet := map[string]map[string]struct{}{}
	clusterBySignature := map[string]semanticcontracts.GlobalClusterRecord{}
	for _, cluster := range weekData.GlobalClusters {
		environment := normalizeEnvironment(cluster.Environment)
		if environment == "" {
			continue
		}
		phraseKey := normalizePhrase(cluster.CanonicalEvidencePhrase)
		if phraseKey != "" {
			set := phraseEnvironmentSet[phraseKey]
			if set == nil {
				set = map[string]struct{}{}
				phraseEnvironmentSet[phraseKey] = set
			}
			set[environment] = struct{}{}
		}
		for _, signatureID := range cluster.MemberSignatureIDs {
			key := signatureLookupKey(environment, signatureID)
			if key == "" {
				continue
			}
			current, exists := clusterBySignature[key]
			if !exists || dailyClusterLess(current, cluster) {
				clusterBySignature[key] = cluster
			}
		}
	}

	phraseEnvironments := map[string][]string{}
	for phraseKey, set := range phraseEnvironmentSet {
		phraseEnvironments[phraseKey] = sortedStringSet(set)
	}

	phase3IssueByAnchor := map[string]string{}
	for _, row := range weekData.Phase3Links {
		issueID := strings.TrimSpace(row.IssueID)
		if issueID == "" {
			continue
		}
		key := phase3AnchorKey(row.Environment, row.RunURL, row.RowID)
		if key == "" {
			continue
		}
		phase3IssueByAnchor[key] = issueID
	}

	phase3IssuesByID := map[string]semanticcontracts.Phase3IssueRecord{}
	for _, row := range phase3Issues {
		issueID := strings.TrimSpace(row.IssueID)
		if issueID == "" {
			continue
		}
		phase3IssuesByID[issueID] = row
	}

	return dailyWeekContext{
		availableEnvironments: append([]string(nil), weekData.AvailableEnvironments...),
		clusterBySignature:    clusterBySignature,
		phraseEnvironments:    phraseEnvironments,
		phase3IssueByAnchor:   phase3IssueByAnchor,
		phase3IssuesByID:      phase3IssuesByID,
	}
}

func resolveDailyEnvironments(requested []string, available []string) ([]string, error) {
	supportedSet := map[string]struct{}{}
	for _, environment := range sourceoptions.SupportedEnvironments() {
		supportedSet[normalizeEnvironment(environment)] = struct{}{}
	}
	if len(requested) == 0 {
		out := normalizeStringSlice(available)
		if len(out) != 0 {
			return out, nil
		}
		return normalizeStringSlice(sourceoptions.SupportedEnvironments()), nil
	}
	out := normalizeStringSlice(requested)
	for _, environment := range out {
		if _, ok := supportedSet[environment]; ok {
			continue
		}
		return nil, fmt.Errorf("unsupported environment %q", environment)
	}
	return out, nil
}

func resolveDailyWeekLabel(date time.Time, override string) (string, error) {
	trimmedOverride := strings.TrimSpace(override)
	if trimmedOverride != "" {
		weekStart, err := normalizeWeekLabel(trimmedOverride)
		if err != nil {
			return "", err
		}
		weekDate, _ := time.Parse("2006-01-02", weekStart)
		day := date.UTC()
		if day.Before(weekDate.UTC()) || !day.Before(weekDate.AddDate(0, 0, 7).UTC()) {
			return "", fmt.Errorf("date %s does not fall within semantic week %s", day.Format("2006-01-02"), weekStart)
		}
		return weekStart, nil
	}
	day := date.UTC()
	dayStart := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	return dayStart.AddDate(0, 0, -int(dayStart.Weekday())).Format("2006-01-02"), nil
}

func normalizeDateLabel(value string) (string, time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", time.Time{}, fmt.Errorf("date query parameter is required (YYYY-MM-DD)")
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil || parsed.Format("2006-01-02") != trimmed {
		return "", time.Time{}, fmt.Errorf("date must use YYYY-MM-DD format")
	}
	return parsed.UTC().Format("2006-01-02"), parsed.UTC(), nil
}

func normalizeWeekLabel(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("week is required")
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil || parsed.Format("2006-01-02") != trimmed {
		return "", fmt.Errorf("week must use YYYY-MM-DD format")
	}
	if parsed.Weekday() != time.Sunday {
		return "", fmt.Errorf("week must start on Sunday")
	}
	return parsed.UTC().Format("2006-01-02"), nil
}

func applyClusterMetadata(item *DailyTriageItem, cluster semanticcontracts.GlobalClusterRecord, phraseEnvironments map[string][]string) {
	if item == nil {
		return
	}
	primary := primaryContributingTest(cluster.ContributingTests)
	item.ClusterID = strings.TrimSpace(cluster.Phase2ClusterID)
	item.CanonicalEvidencePhrase = strings.TrimSpace(cluster.CanonicalEvidencePhrase)
	item.SearchQueryPhrase = strings.TrimSpace(cluster.SearchQueryPhrase)
	item.Lane = strings.TrimSpace(primary.Lane)
	item.JobName = strings.TrimSpace(primary.JobName)
	item.TestName = strings.TrimSpace(primary.TestName)
	item.WeeklySupportCount = cluster.SupportCount
	item.WeeklyPostGoodCount = cluster.PostGoodCommitCount
	phraseKey := normalizePhrase(cluster.CanonicalEvidencePhrase)
	if phraseKey == "" {
		return
	}
	item.AffectedEnvironments = append([]string(nil), phraseEnvironments[phraseKey]...)
}

func collectDailyPhase3Issues(
	issueIDs map[string]struct{},
	issuesByID map[string]semanticcontracts.Phase3IssueRecord,
) []DailyTriagePhase3Issue {
	if len(issueIDs) == 0 {
		return nil
	}
	orderedIDs := sortedStringSet(issueIDs)
	out := make([]DailyTriagePhase3Issue, 0, len(orderedIDs))
	for _, issueID := range orderedIDs {
		row := issuesByID[issueID]
		out = append(out, DailyTriagePhase3Issue{
			IssueID: issueID,
			Title:   strings.TrimSpace(row.Title),
		})
	}
	return out
}

func dailyClusterLess(left semanticcontracts.GlobalClusterRecord, right semanticcontracts.GlobalClusterRecord) bool {
	if right.SupportCount != left.SupportCount {
		return right.SupportCount > left.SupportCount
	}
	leftID := strings.TrimSpace(left.Phase2ClusterID)
	rightID := strings.TrimSpace(right.Phase2ClusterID)
	if leftID != rightID {
		return rightID < leftID
	}
	return strings.TrimSpace(right.CanonicalEvidencePhrase) < strings.TrimSpace(left.CanonicalEvidencePhrase)
}

func primaryContributingTest(rows []semanticcontracts.ContributingTestRecord) semanticcontracts.ContributingTestRecord {
	if len(rows) == 0 {
		return semanticcontracts.ContributingTestRecord{}
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

func sampleFailureText(row storecontracts.RawFailureRecord) string {
	text := strings.TrimSpace(row.RawText)
	if text == "" {
		text = strings.TrimSpace(row.NormalizedText)
	}
	if len(text) > 240 {
		return text[:240]
	}
	return text
}

func phase3AnchorKey(environment string, runURL string, rowID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedRunURL := strings.TrimSpace(runURL)
	trimmedRowID := strings.TrimSpace(rowID)
	if normalizedEnvironment == "" || trimmedRunURL == "" || trimmedRowID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedRunURL + "|" + trimmedRowID
}

func signatureLookupKey(environment string, signatureID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedSignatureID := strings.TrimSpace(signatureID)
	if normalizedEnvironment == "" || trimmedSignatureID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedSignatureID
}

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizePhrase(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(strings.Join(strings.Fields(trimmed), " "))
}

func normalizeStringSlice(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		normalized := normalizeEnvironment(value)
		if normalized == "" {
			continue
		}
		set[normalized] = struct{}{}
	}
	return sortedStringSet(set)
}

func sortedStringSet(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
