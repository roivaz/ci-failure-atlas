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
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

const (
	reviewMetricRunCount      = "run_count"
	reviewTrendWindowDays     = 7
	reviewInformationalReason = "phase1_cluster_id_collision"
)

type ReviewPhase3Anchor struct {
	Environment string
	RunURL      string
	RowID       string
}

func (a ReviewPhase3Anchor) Key() string {
	return phase3AnchorKey(a.Environment, a.RunURL, a.RowID)
}

type ReviewWeekSnapshot struct {
	Weeks                []string
	Week                 string
	PreviousWeek         string
	NextWeek             string
	Rows                 []FailurePatternRow
	OverallJobsByEnv     map[string]int
	AnchorsByClusterID   map[string][]ReviewPhase3Anchor
	LaneKeysByClusterID  map[string][]string
	AggregatedSelection  map[string]struct{}
	UnassignedCount      int
	MissingAnchorCount   int
	TotalClusters        int
	AnchoredClusterCount int
}

type reviewSignalIndex struct {
	ByPhase1ClusterID map[string]map[string]struct{}
	ByReferenceKey    map[string]map[string]struct{}
}

func (s *Service) BuildReviewWeek(ctx context.Context, requestedWeek string) (ReviewWeekSnapshot, error) {
	window, err := s.ResolveWeekWindow(ctx, requestedWeek, time.Time{})
	if err != nil {
		return ReviewWeekSnapshot{}, err
	}
	week := window.CurrentWeek
	store, err := s.OpenStoreForWeek(week)
	if err != nil {
		return ReviewWeekSnapshot{}, fmt.Errorf("open semantic store for semantic week %q: %w", week, err)
	}
	defer func() {
		_ = store.Close()
	}()

	weekData, err := semanticquery.LoadWeekData(ctx, store, semanticquery.LoadWeekDataOptions{
		IncludeRawFailures: true,
	})
	if err != nil {
		return ReviewWeekSnapshot{}, err
	}

	sourceClusters := append([]semanticcontracts.FailurePatternRecord(nil), weekData.SourceFailurePatterns...)
	reviewQueue := append([]semanticcontracts.ReviewItemRecord(nil), weekData.ReviewQueue...)
	links := append([]semanticcontracts.Phase3LinkRecord(nil), weekData.Phase3Links...)

	childAnchorsByClusterID := map[string][]ReviewPhase3Anchor{}
	childLaneKeysByClusterID := map[string][]string{}
	for _, cluster := range sourceClusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		if clusterID == "" {
			continue
		}
		selectionID := reviewRowSelectionID(environment, clusterID)
		anchors := reviewAnchorsForCluster(environment, cluster.References)
		laneKeys := reviewLaneKeysForContributingTests(cluster.ContributingTests)
		if selectionID != "" {
			childAnchorsByClusterID[selectionID] = reviewDedupeAnchors(append(childAnchorsByClusterID[selectionID], anchors...))
			childLaneKeysByClusterID[selectionID] = reviewMergeLaneKeys(childLaneKeysByClusterID[selectionID], laneKeys)
		}
		childAnchorsByClusterID[clusterID] = reviewDedupeAnchors(append(childAnchorsByClusterID[clusterID], anchors...))
		childLaneKeysByClusterID[clusterID] = reviewMergeLaneKeys(childLaneKeysByClusterID[clusterID], laneKeys)
	}

	linkedChildrenBySelectionID, err := reviewLinkedChildrenByMergedSelectionID(sourceClusters, links)
	if err != nil {
		return ReviewWeekSnapshot{}, fmt.Errorf("build linked child clusters: %w", err)
	}

	clusters := append([]semanticcontracts.FailurePatternRecord(nil), weekData.FailurePatterns...)
	reviewIndex := buildReviewSignalIndex(reviewQueue)
	rawTextIndex := semanticquery.RawFailureTextByEnvironmentRow(weekData.RawFailures)
	phase3ClusterByAnchor := map[string]string{}
	for _, row := range links {
		anchor := ReviewPhase3Anchor{
			Environment: row.Environment,
			RunURL:      row.RunURL,
			RowID:       row.RowID,
		}
		key := anchor.Key()
		if key == "" {
			continue
		}
		phase3ClusterID := strings.TrimSpace(row.IssueID)
		if phase3ClusterID == "" {
			continue
		}
		phase3ClusterByAnchor[key] = phase3ClusterID
	}

	totalSupportByEnvironment := map[string]int{}
	phraseEnvironments := map[string]map[string]struct{}{}
	for _, cluster := range clusters {
		environment := normalizeEnvironment(cluster.Environment)
		totalSupportByEnvironment[environment] += cluster.SupportCount
		phraseKey := normalizePhrase(cluster.CanonicalEvidencePhrase)
		if phraseKey == "" {
			continue
		}
		envSet := phraseEnvironments[phraseKey]
		if envSet == nil {
			envSet = map[string]struct{}{}
			phraseEnvironments[phraseKey] = envSet
		}
		envSet[environment] = struct{}{}
	}

	metricWindowStart := time.Time{}
	metricWindowEnd := time.Time{}
	if weekStart, ok := reviewParseSemanticWeek(week); ok {
		metricWindowStart = weekStart.UTC()
		metricWindowEnd = weekStart.AddDate(0, 0, 7).UTC()
	}
	overallJobsByEnv, err := reviewMetricRunTotalsByEnvironment(
		ctx,
		store,
		reviewSortedEnvironmentKeys(totalSupportByEnvironment),
		metricWindowStart,
		metricWindowEnd,
	)
	if err != nil {
		return ReviewWeekSnapshot{}, fmt.Errorf("load overall metric run counts: %w", err)
	}

	historyResolver, err := s.BuildHistoryResolverForWeek(ctx, week, weekData.WeekSchemaVersion)
	if err != nil {
		return ReviewWeekSnapshot{}, fmt.Errorf("build signature history resolver: %w", err)
	}
	trendAnchor := time.Now().UTC()
	if weekStart, ok := reviewParseSemanticWeek(week); ok {
		trendAnchor = weekStart.AddDate(0, 0, reviewTrendWindowDays-1).UTC()
	}

	rows := make([]FailurePatternRow, 0, len(clusters))
	anchorsByClusterID := map[string][]ReviewPhase3Anchor{}
	for key, anchors := range childAnchorsByClusterID {
		anchorsByClusterID[key] = append([]ReviewPhase3Anchor(nil), anchors...)
	}
	laneKeysByClusterID := map[string][]string{}
	for key, laneKeys := range childLaneKeysByClusterID {
		laneKeysByClusterID[key] = append([]string(nil), laneKeys...)
	}

	aggregatedSelections := map[string]struct{}{}
	unassignedCount := 0
	missingAnchorCount := 0
	anchoredClusterCount := 0

	for _, cluster := range clusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		selectionID := reviewRowSelectionID(environment, clusterID)
		anchors := reviewAnchorsForCluster(environment, cluster.References)
		if len(anchors) == 0 {
			missingAnchorCount++
		} else {
			anchoredClusterCount++
		}
		if selectionID != "" {
			anchorsByClusterID[selectionID] = reviewDedupeAnchors(append(anchorsByClusterID[selectionID], anchors...))
		}
		laneKeys := reviewLaneKeysForContributingTests(cluster.ContributingTests)
		if selectionID != "" {
			laneKeysByClusterID[selectionID] = reviewMergeLaneKeys(laneKeysByClusterID[selectionID], laneKeys)
		}
		if clusterID != "" {
			anchorsByClusterID[clusterID] = reviewDedupeAnchors(append(anchorsByClusterID[clusterID], anchors...))
			laneKeysByClusterID[clusterID] = reviewMergeLaneKeys(laneKeysByClusterID[clusterID], laneKeys)
		}

		phase3ClusterIDs := reviewPhase3ClusterIDsForAnchors(anchors, phase3ClusterByAnchor)
		manualIssueID := ""
		switch len(phase3ClusterIDs) {
		case 0:
			manualIssueID = ""
		case 1:
			manualIssueID = phase3ClusterIDs[0]
		default:
			return ReviewWeekSnapshot{}, fmt.Errorf(
				"phase3 conflict: semantic cluster %s resolves to multiple phase3 cluster IDs (%s); unlink and relink this cluster",
				clusterID,
				strings.Join(phase3ClusterIDs, ", "),
			)
		}
		if len(phase3ClusterIDs) == 0 {
			unassignedCount++
		}

		qualityCodes := QualityIssueCodes(cluster.CanonicalEvidencePhrase)
		qualityLabels := make([]string, 0, len(qualityCodes))
		for _, code := range qualityCodes {
			qualityLabels = append(qualityLabels, QualityIssueLabel(code))
		}
		reviewReasons := reviewReasonsForCluster(cluster, reviewIndex)
		qualityScore := QualityScore(qualityCodes) + (len(reviewReasons) * 2)
		alsoSeenIn := reviewEnvironmentsForPhrase(
			phraseEnvironments[normalizePhrase(cluster.CanonicalEvidencePhrase)],
			environment,
		)
		primary := primaryContributingTest(cluster.ContributingTests)
		linkedChildren := reviewBuildLinkedChildFailurePatternRows(
			manualIssueID,
			linkedChildrenBySelectionID[selectionID],
			totalSupportByEnvironment[environment],
			reviewIndex,
			rawTextIndex,
		)
		isAggregatedRow := strings.TrimSpace(manualIssueID) != "" && len(linkedChildren) > 0
		if isAggregatedRow {
			if selectionID != "" {
				aggregatedSelections[selectionID] = struct{}{}
			}
			if clusterID != "" {
				aggregatedSelections[clusterID] = struct{}{}
			}
		}

		displayReferences := reviewToRunReferences(cluster.References, 0)
		scoreReferences := []RunReference(nil)
		trendSparkline := ""
		trendCounts := []int(nil)
		trendRange := ""
		if isAggregatedRow {
			scoreReferences = reviewToRunReferences(cluster.References, 0)
			if sparkline, counts, sparkRange, ok := DailyDensitySparkline(
				scoreReferences,
				reviewTrendWindowDays,
				trendAnchor,
			); ok {
				trendSparkline = sparkline
				trendCounts = append([]int(nil), counts...)
				trendRange = sparkRange
			}
		}

		historyPresence := semhistory.FailurePatternPresence{}
		if historyResolver != nil && isAggregatedRow {
			historyPresence = historyResolver.PresenceForPhase3Cluster(environment, manualIssueID)
		}
		priorLastSeenAt := ""
		if !historyPresence.PriorLastSeenAt.IsZero() {
			priorLastSeenAt = historyPresence.PriorLastSeenAt.UTC().Format(time.RFC3339)
		}

		rows = append(rows, FailurePatternRow{
			Environment:         environment,
			FailedAt:            strings.TrimSpace(primary.Lane),
			JobName:             strings.TrimSpace(primary.JobName),
			TestName:            strings.TrimSpace(primary.TestName),
			FailurePattern:      strings.TrimSpace(cluster.CanonicalEvidencePhrase),
			FailurePatternID:    clusterID,
			SearchQuery:         strings.TrimSpace(cluster.SearchQueryPhrase),
			Occurrences:         cluster.SupportCount,
			OccurrenceShare:     reviewSupportShare(cluster.SupportCount, totalSupportByEnvironment[environment]),
			AfterLastPushCount:  cluster.PostGoodCommitCount,
			AlsoIn:              alsoSeenIn,
			QualityScore:        qualityScore,
			QualityNoteLabels:   qualityLabels,
			ReviewNoteLabels:    reviewReasons,
			ContributingTests:   reviewToContributingTests(cluster.ContributingTests),
			FullErrorSamples:    reviewFullErrorSamplesForReferences(environment, cluster.References, rawTextIndex, 0),
			AffectedRuns:        displayReferences,
			ScoringReferences:   scoreReferences,
			TrendSparkline:      trendSparkline,
			TrendCounts:         trendCounts,
			TrendRange:          trendRange,
			PriorWeeksPresent:   historyPresence.PriorWeeksPresent,
			PriorWeekStarts:     append([]string(nil), historyPresence.PriorWeekStarts...),
			PriorRunsAffected:   historyPresence.PriorJobsAffected,
			PriorLastSeenAt:     priorLastSeenAt,
			ManualIssueID:       manualIssueID,
			ManualIssueConflict: false,
			SelectionValue:      selectionID,
			LinkedPatterns:      linkedChildren,
			SearchIndex: strings.Join([]string{
				environment,
				strings.TrimSpace(primary.Lane),
				strings.TrimSpace(primary.JobName),
				strings.TrimSpace(primary.TestName),
				strings.TrimSpace(cluster.CanonicalEvidencePhrase),
				clusterID,
				selectionID,
				strings.Join(qualityLabels, " "),
				strings.Join(reviewReasons, " "),
				manualIssueID,
			}, " "),
		})
	}

	return ReviewWeekSnapshot{
		Weeks:                append([]string(nil), window.Weeks...),
		Week:                 week,
		PreviousWeek:         window.PreviousWeek,
		NextWeek:             window.NextWeek,
		Rows:                 rows,
		OverallJobsByEnv:     overallJobsByEnv,
		AnchorsByClusterID:   anchorsByClusterID,
		LaneKeysByClusterID:  laneKeysByClusterID,
		AggregatedSelection:  aggregatedSelections,
		UnassignedCount:      unassignedCount,
		MissingAnchorCount:   missingAnchorCount,
		TotalClusters:        len(clusters),
		AnchoredClusterCount: anchoredClusterCount,
	}, nil
}

func buildReviewSignalIndex(rows []semanticcontracts.ReviewItemRecord) reviewSignalIndex {
	index := reviewSignalIndex{
		ByPhase1ClusterID: map[string]map[string]struct{}{},
		ByReferenceKey:    map[string]map[string]struct{}{},
	}
	for _, row := range rows {
		reason := strings.TrimSpace(row.Reason)
		if reason == "" || strings.EqualFold(reason, reviewInformationalReason) {
			continue
		}
		for _, phase1ID := range row.SourcePhase1ClusterIDs {
			key := strings.TrimSpace(phase1ID)
			if key == "" {
				continue
			}
			set := index.ByPhase1ClusterID[key]
			if set == nil {
				set = map[string]struct{}{}
				index.ByPhase1ClusterID[key] = set
			}
			set[reason] = struct{}{}
		}
		for _, key := range reviewReferenceKeys(row.Environment, row.References) {
			set := index.ByReferenceKey[key]
			if set == nil {
				set = map[string]struct{}{}
				index.ByReferenceKey[key] = set
			}
			set[reason] = struct{}{}
		}
	}
	return index
}

func reviewReasonsForCluster(cluster semanticcontracts.FailurePatternRecord, index reviewSignalIndex) []string {
	set := map[string]struct{}{}
	for _, phase1ID := range cluster.MemberPhase1ClusterIDs {
		for reason := range index.ByPhase1ClusterID[strings.TrimSpace(phase1ID)] {
			set[reason] = struct{}{}
		}
	}
	for _, key := range reviewReferenceKeys(cluster.Environment, cluster.References) {
		for reason := range index.ByReferenceKey[key] {
			set[reason] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for reason := range set {
		out = append(out, reason)
	}
	sort.Strings(out)
	return out
}

func reviewReferenceKeys(environment string, references []semanticcontracts.ReferenceRecord) []string {
	if len(references) == 0 {
		return nil
	}
	normalizedEnvironment := normalizeEnvironment(environment)
	if normalizedEnvironment == "" {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(references)*2)
	appendKey := func(key string) {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			return
		}
		if _, ok := seen[trimmed]; ok {
			return
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	for _, reference := range references {
		if rowKey := semanticquery.EnvironmentRowKey(normalizedEnvironment, reference.RowID); rowKey != "" {
			appendKey("row|" + rowKey)
		}
		appendKey(reviewReferenceTupleKey(normalizedEnvironment, reference.RunURL, reference.OccurredAt, reference.SignatureID))
	}
	sort.Strings(out)
	return out
}

func reviewReferenceTupleKey(environment string, runURL string, occurredAt string, signatureID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedRunURL := strings.TrimSpace(runURL)
	trimmedOccurredAt := strings.TrimSpace(occurredAt)
	trimmedSignatureID := strings.TrimSpace(signatureID)
	if normalizedEnvironment == "" {
		return ""
	}
	if trimmedRunURL == "" && trimmedOccurredAt == "" && trimmedSignatureID == "" {
		return ""
	}
	return "ref|" + normalizedEnvironment + "|" + trimmedRunURL + "|" + trimmedOccurredAt + "|" + trimmedSignatureID
}

func reviewAnchorsForCluster(environment string, references []semanticcontracts.ReferenceRecord) []ReviewPhase3Anchor {
	set := map[string]ReviewPhase3Anchor{}
	for _, reference := range references {
		runURL := strings.TrimSpace(reference.RunURL)
		rowID := strings.TrimSpace(reference.RowID)
		if runURL == "" || rowID == "" {
			continue
		}
		anchor := ReviewPhase3Anchor{
			Environment: environment,
			RunURL:      runURL,
			RowID:       rowID,
		}
		key := anchor.Key()
		if key == "" {
			continue
		}
		set[key] = anchor
	}
	return reviewAnchorValues(set)
}

func reviewAnchorValues(set map[string]ReviewPhase3Anchor) []ReviewPhase3Anchor {
	if len(set) == 0 {
		return nil
	}
	out := make([]ReviewPhase3Anchor, 0, len(set))
	for _, anchor := range set {
		out = append(out, anchor)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Environment != out[j].Environment {
			return out[i].Environment < out[j].Environment
		}
		if out[i].RunURL != out[j].RunURL {
			return out[i].RunURL < out[j].RunURL
		}
		return out[i].RowID < out[j].RowID
	})
	return out
}

func reviewDedupeAnchors(anchors []ReviewPhase3Anchor) []ReviewPhase3Anchor {
	set := map[string]ReviewPhase3Anchor{}
	for _, anchor := range anchors {
		key := anchor.Key()
		if key == "" {
			continue
		}
		set[key] = anchor
	}
	return reviewAnchorValues(set)
}

func reviewFullErrorSamplesForReferences(
	environment string,
	references []semanticcontracts.ReferenceRecord,
	rawTextByRowKey map[string]string,
	limit int,
) []string {
	if len(references) == 0 {
		return nil
	}
	maxCount := len(references)
	if limit > 0 && limit < maxCount {
		maxCount = limit
	}
	out := make([]string, 0, maxCount)
	seen := map[string]struct{}{}
	for _, reference := range references {
		rowKey := semanticquery.EnvironmentRowKey(environment, reference.RowID)
		if rowKey == "" {
			continue
		}
		fullError := strings.TrimSpace(rawTextByRowKey[rowKey])
		if fullError == "" {
			continue
		}
		if _, exists := seen[fullError]; exists {
			continue
		}
		seen[fullError] = struct{}{}
		out = append(out, fullError)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func reviewToRunReferences(rows []semanticcontracts.ReferenceRecord, limit int) []RunReference {
	if len(rows) == 0 {
		return nil
	}
	maxCount := len(rows)
	if limit > 0 && limit < maxCount {
		maxCount = limit
	}
	out := make([]RunReference, 0, maxCount)
	for _, row := range rows {
		out = append(out, RunReference{
			RunURL:      strings.TrimSpace(row.RunURL),
			OccurredAt:  strings.TrimSpace(row.OccurredAt),
			SignatureID: strings.TrimSpace(row.SignatureID),
			PRNumber:    row.PRNumber,
		})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func reviewToContributingTests(rows []semanticcontracts.ContributingTestRecord) []ContributingTest {
	if len(rows) == 0 {
		return nil
	}
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

func reviewPhase3ClusterIDsForAnchors(anchors []ReviewPhase3Anchor, phase3ClusterByAnchor map[string]string) []string {
	set := map[string]struct{}{}
	for _, anchor := range anchors {
		phase3ClusterID := strings.TrimSpace(phase3ClusterByAnchor[anchor.Key()])
		if phase3ClusterID == "" {
			continue
		}
		set[phase3ClusterID] = struct{}{}
	}
	return sortedStringSet(set)
}

func reviewLinkedChildrenByMergedSelectionID(
	clusters []semanticcontracts.FailurePatternRecord,
	phase3Links []semanticcontracts.Phase3LinkRecord,
) (map[string][]semanticcontracts.FailurePatternRecord, error) {
	phase3ClusterByAnchor := map[string]string{}
	for _, row := range phase3Links {
		phase3ClusterID := strings.TrimSpace(row.IssueID)
		if phase3ClusterID == "" {
			continue
		}
		key := phase3AnchorKey(row.Environment, row.RunURL, row.RowID)
		if key == "" {
			continue
		}
		phase3ClusterByAnchor[key] = phase3ClusterID
	}

	grouped := map[string][]semanticcontracts.FailurePatternRecord{}
	for _, cluster := range clusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		if environment == "" || clusterID == "" {
			return nil, fmt.Errorf("phase2 cluster record missing environment and/or phase2_cluster_id")
		}
		anchors := reviewAnchorsForCluster(environment, cluster.References)
		phase3ClusterIDs := reviewPhase3ClusterIDsForAnchors(anchors, phase3ClusterByAnchor)
		if len(phase3ClusterIDs) > 1 {
			return nil, fmt.Errorf(
				"phase3 conflict: semantic cluster %s resolves to multiple phase3 cluster IDs (%s)",
				clusterID,
				strings.Join(phase3ClusterIDs, ", "),
			)
		}
		if len(phase3ClusterIDs) == 0 {
			continue
		}
		selectionID := reviewRowSelectionID(environment, phase3ClusterIDs[0])
		grouped[selectionID] = append(grouped[selectionID], cluster)
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

func reviewBuildLinkedChildFailurePatternRows(
	manualIssueID string,
	childClusters []semanticcontracts.FailurePatternRecord,
	totalSupportByEnvironment int,
	reviewIndex reviewSignalIndex,
	rawTextIndex map[string]string,
) []FailurePatternRow {
	if strings.TrimSpace(manualIssueID) == "" || len(childClusters) == 0 {
		return nil
	}
	out := make([]FailurePatternRow, 0, len(childClusters))
	for _, cluster := range childClusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		qualityCodes := QualityIssueCodes(cluster.CanonicalEvidencePhrase)
		qualityLabels := make([]string, 0, len(qualityCodes))
		for _, code := range qualityCodes {
			qualityLabels = append(qualityLabels, QualityIssueLabel(code))
		}
		reviewReasons := reviewReasonsForCluster(cluster, reviewIndex)
		qualityScore := QualityScore(qualityCodes) + (len(reviewReasons) * 2)
		primary := primaryContributingTest(cluster.ContributingTests)

		out = append(out, FailurePatternRow{
			Environment:         environment,
			FailedAt:            strings.TrimSpace(primary.Lane),
			JobName:             strings.TrimSpace(primary.JobName),
			TestName:            strings.TrimSpace(primary.TestName),
			FailurePattern:      strings.TrimSpace(cluster.CanonicalEvidencePhrase),
			FailurePatternID:    clusterID,
			SearchQuery:         strings.TrimSpace(cluster.SearchQueryPhrase),
			Occurrences:         cluster.SupportCount,
			OccurrenceShare:     reviewSupportShare(cluster.SupportCount, totalSupportByEnvironment),
			AfterLastPushCount:  cluster.PostGoodCommitCount,
			QualityScore:        qualityScore,
			QualityNoteLabels:   qualityLabels,
			ReviewNoteLabels:    reviewReasons,
			ContributingTests:   reviewToContributingTests(cluster.ContributingTests),
			FullErrorSamples:    reviewFullErrorSamplesForReferences(environment, cluster.References, rawTextIndex, 0),
			AffectedRuns:        reviewToRunReferences(cluster.References, 0),
			ManualIssueID:       strings.TrimSpace(manualIssueID),
			ManualIssueConflict: false,
			SelectionValue:      reviewRowSelectionID(environment, clusterID),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Occurrences != out[j].Occurrences {
			return out[i].Occurrences > out[j].Occurrences
		}
		if out[i].AfterLastPushCount != out[j].AfterLastPushCount {
			return out[i].AfterLastPushCount > out[j].AfterLastPushCount
		}
		if strings.TrimSpace(out[i].FailurePattern) != strings.TrimSpace(out[j].FailurePattern) {
			return strings.TrimSpace(out[i].FailurePattern) < strings.TrimSpace(out[j].FailurePattern)
		}
		return strings.TrimSpace(out[i].FailurePatternID) < strings.TrimSpace(out[j].FailurePatternID)
	})
	return out
}

func reviewRowSelectionID(environment string, clusterID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedClusterID := strings.TrimSpace(clusterID)
	if normalizedEnvironment == "" || trimmedClusterID == "" {
		return trimmedClusterID
	}
	return normalizedEnvironment + "|" + trimmedClusterID
}

func reviewMergeLaneKeys(existing []string, incoming []string) []string {
	set := map[string]struct{}{}
	for _, value := range existing {
		if normalized := normalizeLaneKey(value); normalized != "" {
			set[normalized] = struct{}{}
		}
	}
	for _, value := range incoming {
		if normalized := normalizeLaneKey(value); normalized != "" {
			set[normalized] = struct{}{}
		}
	}
	return sortedStringSet(set)
}

func reviewLaneKeysForContributingTests(rows []semanticcontracts.ContributingTestRecord) []string {
	if len(rows) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	for _, row := range rows {
		normalized := normalizeLaneKey(row.Lane)
		if normalized == "" {
			continue
		}
		set[normalized] = struct{}{}
	}
	return sortedStringSet(set)
}

func reviewSupportShare(value int, total int) float64 {
	if total <= 0 || value <= 0 {
		return 0
	}
	return (float64(value) / float64(total)) * 100.0
}

func reviewSortedEnvironmentKeys(values map[string]int) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for environment := range values {
		normalized := normalizeEnvironment(environment)
		if normalized == "" {
			continue
		}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func reviewMetricRunTotalsByEnvironment(
	ctx context.Context,
	store storecontracts.Store,
	environments []string,
	windowStart time.Time,
	windowEnd time.Time,
) (map[string]int, error) {
	totals := map[string]int{}
	if store == nil {
		return totals, nil
	}
	environmentSet := map[string]struct{}{}
	for _, environment := range environments {
		normalized := normalizeEnvironment(environment)
		if normalized == "" {
			continue
		}
		environmentSet[normalized] = struct{}{}
	}
	if len(environmentSet) == 0 {
		return totals, nil
	}
	normalizedEnvironments := make([]string, 0, len(environmentSet))
	for environment := range environmentSet {
		normalizedEnvironments = append(normalizedEnvironments, environment)
	}
	sort.Strings(normalizedEnvironments)
	if metricDates := metricDateLabelsFromWindow(windowStart, windowEnd); len(metricDates) > 0 {
		return sumMetricByEnvironmentForDates(ctx, store, reviewMetricRunCount, normalizedEnvironments, metricDates)
	}
	rows, err := store.ListMetricsDaily(ctx)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		environment := normalizeEnvironment(row.Environment)
		if _, ok := environmentSet[environment]; !ok {
			continue
		}
		if strings.TrimSpace(row.Metric) != reviewMetricRunCount {
			continue
		}
		if !windowStart.IsZero() && !windowEnd.IsZero() {
			dateValue, ok := reviewParseMetricDate(row.Date)
			if !ok {
				continue
			}
			if dateValue.Before(windowStart) || !dateValue.Before(windowEnd) {
				continue
			}
		}
		value := int(row.Value)
		if value <= 0 {
			continue
		}
		totals[environment] += value
	}
	return totals, nil
}

func reviewParseMetricDate(value string) (time.Time, bool) {
	parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func reviewParseSemanticWeek(value string) (time.Time, bool) {
	parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func reviewEnvironmentsForPhrase(set map[string]struct{}, currentEnvironment string) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	current := normalizeEnvironment(currentEnvironment)
	for environment := range set {
		normalized := normalizeEnvironment(environment)
		if normalized == "" || normalized == current {
			continue
		}
		out = append(out, strings.ToUpper(normalized))
	}
	sort.Strings(out)
	return out
}

func normalizeLaneKey(value string) string {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" {
		return "unknown"
	}
	return trimmed
}
