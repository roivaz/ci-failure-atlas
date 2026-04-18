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
	postgresstore "ci-failure-atlas/pkg/store/postgres"
)

const failurePatternReportFullErrorExamplesLimit = 3

type FailurePatternReportBuildOptions struct {
	Week                string
	Environments        []string
	HistoryHorizonWeeks int
	HistoryResolver     semhistory.FailurePatternHistoryResolver
}

type FailurePatternReportReference struct {
	RunURL         string `json:"run_url"`
	OccurredAt     string `json:"occurred_at"`
	SignatureID    string `json:"signature_id"`
	PRNumber       int    `json:"pr_number"`
	PostGoodCommit bool   `json:"after_last_push_of_merged_pr"`
}

type FailurePatternReportContributingTest struct {
	Lane         string `json:"failed_at"`
	JobName      string `json:"job_name"`
	TestName     string `json:"test_name"`
	SupportCount int    `json:"occurrences"`
}

type FailurePatternReportCluster struct {
	Environment             string                                 `json:"environment"`
	SchemaVersion           string                                 `json:"schema_version"`
	Phase2ClusterID         string                                 `json:"failure_pattern_id"`
	CanonicalEvidencePhrase string                                 `json:"failure_pattern"`
	SearchQueryPhrase       string                                 `json:"search_query"`
	SupportCount            int                                    `json:"occurrences"`
	SeenPostGoodCommit      bool                                   `json:"after_last_push_seen"`
	PostGoodCommitCount     int                                    `json:"after_last_push_count"`
	ContributingTestsCount  int                                    `json:"contributing_test_count"`
	ContributingTests       []FailurePatternReportContributingTest `json:"contributing_tests"`
	MemberPhase1ClusterIDs  []string                               `json:"member_phase1_cluster_ids"`
	MemberSignatureIDs      []string                               `json:"member_signature_ids"`
	References              []FailurePatternReportReference        `json:"affected_runs"`
	FullErrorSamples        []string                               `json:"full_error_samples,omitempty"`
	LinkedChildren          []FailurePatternReportCluster          `json:"linked_failure_patterns,omitempty"`
}

type FailurePatternReportData struct {
	FailurePatternClusters         []FailurePatternReportCluster
	TargetEnvironments             []string
	OverallJobsByEnvironment       map[string]int
	WindowStartRaw                 string
	WindowEndRaw                   string
	HistoryResolver                semhistory.FailurePatternHistoryResolver
	GeneratedAt                    time.Time
	TestClusterCountsByEnvironment map[string]int
	ReviewItemCountsByEnvironment  map[string]int
}

func BuildFailurePatternReportData(ctx context.Context, store storecontracts.Store, opts FailurePatternReportBuildOptions) (FailurePatternReportData, error) {
	if store == nil {
		return FailurePatternReportData{}, fmt.Errorf("store is required")
	}

	weekData, err := semanticquery.LoadWeekData(ctx, store, semanticquery.LoadWeekDataOptions{
		IncludeRawFailures: true,
	})
	if err != nil {
		return FailurePatternReportData{}, err
	}

	sourceClusterRows := append([]semanticcontracts.FailurePatternRecord(nil), weekData.SourceFailurePatterns...)
	phase3Links := append([]semanticcontracts.Phase3LinkRecord(nil), weekData.Phase3Links...)
	materializedClusterRows := append([]semanticcontracts.FailurePatternRecord(nil), weekData.FailurePatterns...)
	linkedChildrenByClusterKey, err := failurePatternReportLinkedChildrenByMergedClusterKey(sourceClusterRows, phase3Links)
	if err != nil {
		return FailurePatternReportData{}, fmt.Errorf("build linked child clusters: %w", err)
	}

	reportRows := toFailurePatternReportClusters(materializedClusterRows)
	reportLinkedChildrenByClusterKey := toFailurePatternReportClusterGroupMap(linkedChildrenByClusterKey)
	rawFailuresByRun := failurePatternReportIndexRawFailuresByEnvironmentRun(weekData.RawFailures)
	reportLinkedChildrenByClusterKey = failurePatternReportAttachFullErrorSamplesByGroup(
		reportLinkedChildrenByClusterKey,
		failurePatternReportFullErrorExamplesLimit,
		rawFailuresByRun,
	)

	targetEnvironments := semanticquery.ResolveTargetEnvironments(opts.Environments, weekData)
	metricWindowStart, metricWindowEnd := failurePatternReportMetricWindowBounds(opts.Week)
	windowStartRaw, windowEndRaw := failurePatternReportMetricWindowStrings(metricWindowStart, metricWindowEnd)
	overallJobsByEnvironment, err := failurePatternReportMetricRunTotalsByEnvironment(
		ctx,
		store,
		targetEnvironments,
		metricWindowStart,
		metricWindowEnd,
	)
	if err != nil {
		return FailurePatternReportData{}, fmt.Errorf("load overall metric run counts: %w", err)
	}

	historyResolver := opts.HistoryResolver
	if historyResolver == nil {
		lookbackWeeks := opts.HistoryHorizonWeeks
		if lookbackWeeks <= 0 {
			lookbackWeeks = DefaultHistoryWeeks
		}
		historyResolver, err = semhistory.BuildFailurePatternHistoryResolver(ctx, semhistory.BuildOptions{
			CurrentWeek:                        strings.TrimSpace(opts.Week),
			FailurePatternHistoryLookbackWeeks: lookbackWeeks,
		})
		if err != nil {
			return FailurePatternReportData{}, fmt.Errorf("build failure-pattern history resolver: %w", err)
		}
	}

	failurePatternRows := failurePatternReportAttachFullErrorSamples(reportRows, failurePatternReportFullErrorExamplesLimit, rawFailuresByRun)
	failurePatternRows = failurePatternReportAttachLinkedChildren(failurePatternRows, reportLinkedChildrenByClusterKey)

	return FailurePatternReportData{
		FailurePatternClusters:         failurePatternRows,
		TargetEnvironments:             append([]string(nil), targetEnvironments...),
		OverallJobsByEnvironment:       cloneIntMap(overallJobsByEnvironment),
		WindowStartRaw:                 windowStartRaw,
		WindowEndRaw:                   windowEndRaw,
		HistoryResolver:                historyResolver,
		GeneratedAt:                    time.Now().UTC(),
		TestClusterCountsByEnvironment: cloneIntMap(weekData.TestClusterCountsByEnv),
		ReviewItemCountsByEnvironment:  cloneIntMap(weekData.ReviewQueueCountsByEnv),
	}, nil
}

func cloneIntMap(source map[string]int) map[string]int {
	if len(source) == 0 {
		return map[string]int{}
	}
	out := make(map[string]int, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func failurePatternReportMetricWindowBounds(week string) (time.Time, time.Time) {
	normalizedWeek, err := postgresstore.NormalizeWeek(week)
	if err != nil || normalizedWeek == "" {
		return time.Time{}, time.Time{}
	}
	start, err := time.Parse("2006-01-02", normalizedWeek)
	if err != nil {
		return time.Time{}, time.Time{}
	}
	start = start.UTC()
	return start, start.AddDate(0, 0, 7)
}

func failurePatternReportMetricWindowStrings(start time.Time, end time.Time) (string, string) {
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return "", ""
	}
	return start.Format(time.RFC3339), end.Format(time.RFC3339)
}

func failurePatternReportMetricRunTotalsByEnvironment(
	ctx context.Context,
	store storecontracts.Store,
	environments []string,
	windowStart time.Time,
	windowEnd time.Time,
) (map[string]int, error) {
	totals := map[string]int{}
	normalizedEnvironments := normalizeStringSlice(environments)
	if len(normalizedEnvironments) == 0 {
		return totals, nil
	}
	if metricDates := metricDateLabelsFromWindow(windowStart, windowEnd); len(metricDates) > 0 {
		return sumMetricByEnvironmentForDates(ctx, store, "run_count", normalizedEnvironments, metricDates)
	}
	environmentSet := make(map[string]struct{}, len(normalizedEnvironments))
	for _, environment := range normalizedEnvironments {
		environmentSet[environment] = struct{}{}
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
		if strings.TrimSpace(row.Metric) != "run_count" {
			continue
		}
		if !windowStart.IsZero() && !windowEnd.IsZero() {
			dateValue, ok := failurePatternReportParseMetricDate(row.Date)
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

func failurePatternReportParseMetricDate(value string) (time.Time, bool) {
	parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func failurePatternReportLinkedChildrenByMergedClusterKey(
	sourceClusters []semanticcontracts.FailurePatternRecord,
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
	for _, cluster := range sourceClusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		if environment == "" || clusterID == "" {
			return nil, fmt.Errorf("phase2 cluster record missing environment and/or phase2_cluster_id")
		}
		phase3ClusterIDs := failurePatternReportPhase3ClusterIDsForCluster(cluster, phase3ClusterByAnchor)
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
		groupKey := failurePatternReportClusterKey(environment, phase3ClusterIDs[0])
		grouped[groupKey] = append(grouped[groupKey], cluster)
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

func failurePatternReportPhase3ClusterIDsForCluster(
	cluster semanticcontracts.FailurePatternRecord,
	phase3ClusterByAnchor map[string]string,
) []string {
	set := map[string]struct{}{}
	environment := normalizeEnvironment(cluster.Environment)
	for _, reference := range cluster.References {
		key := phase3AnchorKey(environment, reference.RunURL, reference.RowID)
		if key == "" {
			continue
		}
		phase3ClusterID := strings.TrimSpace(phase3ClusterByAnchor[key])
		if phase3ClusterID == "" {
			continue
		}
		set[phase3ClusterID] = struct{}{}
	}
	return sortedStringSet(set)
}

func failurePatternReportClusterKey(environment string, clusterID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedClusterID := strings.TrimSpace(clusterID)
	if normalizedEnvironment == "" || trimmedClusterID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedClusterID
}

func toFailurePatternReportClusterGroupMap(groups map[string][]semanticcontracts.FailurePatternRecord) map[string][]FailurePatternReportCluster {
	if len(groups) == 0 {
		return nil
	}
	out := make(map[string][]FailurePatternReportCluster, len(groups))
	for key, rows := range groups {
		out[key] = toFailurePatternReportClusters(rows)
	}
	return out
}

func toFailurePatternReportClusters(rows []semanticcontracts.FailurePatternRecord) []FailurePatternReportCluster {
	out := make([]FailurePatternReportCluster, 0, len(rows))
	for _, row := range rows {
		out = append(out, FailurePatternReportCluster{
			Environment:             normalizeEnvironment(row.Environment),
			SchemaVersion:           strings.TrimSpace(row.SchemaVersion),
			Phase2ClusterID:         strings.TrimSpace(row.Phase2ClusterID),
			CanonicalEvidencePhrase: strings.TrimSpace(row.CanonicalEvidencePhrase),
			SearchQueryPhrase:       strings.TrimSpace(row.SearchQueryPhrase),
			SupportCount:            row.SupportCount,
			SeenPostGoodCommit:      row.SeenPostGoodCommit,
			PostGoodCommitCount:     row.PostGoodCommitCount,
			ContributingTestsCount:  row.ContributingTestsCount,
			ContributingTests:       toFailurePatternReportContributingTests(row.ContributingTests),
			MemberPhase1ClusterIDs:  append([]string(nil), row.MemberPhase1ClusterIDs...),
			MemberSignatureIDs:      append([]string(nil), row.MemberSignatureIDs...),
			References:              toFailurePatternReportReferences(row.References),
		})
	}
	return out
}

func toFailurePatternReportContributingTests(rows []semanticcontracts.ContributingTestRecord) []FailurePatternReportContributingTest {
	out := make([]FailurePatternReportContributingTest, 0, len(rows))
	for _, row := range rows {
		out = append(out, FailurePatternReportContributingTest{
			Lane:         strings.TrimSpace(row.Lane),
			JobName:      strings.TrimSpace(row.JobName),
			TestName:     strings.TrimSpace(row.TestName),
			SupportCount: row.SupportCount,
		})
	}
	return out
}

func toFailurePatternReportReferences(rows []semanticcontracts.ReferenceRecord) []FailurePatternReportReference {
	out := make([]FailurePatternReportReference, 0, len(rows))
	for _, row := range rows {
		out = append(out, FailurePatternReportReference{
			RunURL:         strings.TrimSpace(row.RunURL),
			OccurredAt:     strings.TrimSpace(row.OccurredAt),
			SignatureID:    strings.TrimSpace(row.SignatureID),
			PRNumber:       row.PRNumber,
			PostGoodCommit: row.PostGoodCommit,
		})
	}
	return out
}

func failurePatternReportIndexRawFailuresByEnvironmentRun(rows []storecontracts.RawFailureRecord) map[string][]storecontracts.RawFailureRecord {
	byRun := map[string][]storecontracts.RawFailureRecord{}
	for _, row := range rows {
		environment := normalizeEnvironment(row.Environment)
		runURL := strings.TrimSpace(row.RunURL)
		if environment == "" || runURL == "" {
			continue
		}
		key := environment + "|" + runURL
		byRun[key] = append(byRun[key], row)
	}
	for key := range byRun {
		runRows := byRun[key]
		sort.Slice(runRows, func(i, j int) bool {
			if runRows[i].OccurredAt != runRows[j].OccurredAt {
				return runRows[i].OccurredAt < runRows[j].OccurredAt
			}
			if runRows[i].RowID != runRows[j].RowID {
				return runRows[i].RowID < runRows[j].RowID
			}
			return runRows[i].SignatureID < runRows[j].SignatureID
		})
		byRun[key] = runRows
	}
	return byRun
}

func failurePatternReportAttachFullErrorSamples(
	clusters []FailurePatternReportCluster,
	limit int,
	runFailuresByRun map[string][]storecontracts.RawFailureRecord,
) []FailurePatternReportCluster {
	if len(clusters) == 0 || limit <= 0 {
		return append([]FailurePatternReportCluster(nil), clusters...)
	}
	out := append([]FailurePatternReportCluster(nil), clusters...)
	for index := range out {
		cluster := out[index]
		signatureIDs := map[string]struct{}{}
		for _, signatureID := range cluster.MemberSignatureIDs {
			trimmed := strings.TrimSpace(signatureID)
			if trimmed == "" {
				continue
			}
			signatureIDs[trimmed] = struct{}{}
		}
		for _, ref := range cluster.References {
			trimmed := strings.TrimSpace(ref.SignatureID)
			if trimmed == "" {
				continue
			}
			signatureIDs[trimmed] = struct{}{}
		}

		samples := make([]string, 0, limit)
		orderedRefs := append([]FailurePatternReportReference(nil), cluster.References...)
		sort.Slice(orderedRefs, func(i, j int) bool {
			ti, okI := ParseReferenceTimestamp(orderedRefs[i].OccurredAt)
			tj, okJ := ParseReferenceTimestamp(orderedRefs[j].OccurredAt)
			switch {
			case okI && okJ && !ti.Equal(tj):
				return ti.After(tj)
			case okI != okJ:
				return okI
			}
			return strings.TrimSpace(orderedRefs[i].RunURL) < strings.TrimSpace(orderedRefs[j].RunURL)
		})

		environment := normalizeEnvironment(cluster.Environment)
		for _, ref := range orderedRefs {
			if len(samples) >= limit {
				break
			}
			runURL := strings.TrimSpace(ref.RunURL)
			if runURL == "" || environment == "" {
				continue
			}
			runRows := runFailuresByRun[environment+"|"+runURL]
			for _, runRow := range runRows {
				if len(samples) >= limit {
					break
				}
				signatureID := strings.TrimSpace(runRow.SignatureID)
				if len(signatureIDs) > 0 {
					if _, ok := signatureIDs[signatureID]; !ok {
						continue
					}
				}
				sample := strings.TrimSpace(runRow.RawText)
				if sample == "" {
					sample = strings.TrimSpace(runRow.NormalizedText)
				}
				samples = failurePatternReportAppendUniqueLimitedSample(samples, sample, limit)
			}
		}
		out[index].FullErrorSamples = samples
	}
	return out
}

func failurePatternReportAttachFullErrorSamplesByGroup(
	groups map[string][]FailurePatternReportCluster,
	limit int,
	runFailuresByRun map[string][]storecontracts.RawFailureRecord,
) map[string][]FailurePatternReportCluster {
	if len(groups) == 0 {
		return nil
	}
	out := make(map[string][]FailurePatternReportCluster, len(groups))
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out[key] = failurePatternReportAttachFullErrorSamples(groups[key], limit, runFailuresByRun)
	}
	return out
}

func failurePatternReportAttachLinkedChildren(
	rows []FailurePatternReportCluster,
	linkedChildrenByClusterKey map[string][]FailurePatternReportCluster,
) []FailurePatternReportCluster {
	if len(rows) == 0 || len(linkedChildrenByClusterKey) == 0 {
		return rows
	}
	out := append([]FailurePatternReportCluster(nil), rows...)
	for index := range out {
		key := failurePatternReportClusterKey(out[index].Environment, out[index].Phase2ClusterID)
		children := linkedChildrenByClusterKey[key]
		if len(children) == 0 {
			continue
		}
		out[index].LinkedChildren = append([]FailurePatternReportCluster(nil), children...)
	}
	return out
}

func failurePatternReportAppendUniqueLimitedSample(existing []string, candidate string, limit int) []string {
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
