package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"ci-failure-atlas/pkg/report/triagehtml"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	semanticquery "ci-failure-atlas/pkg/semantic/query"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"
)

const globalReportFullErrorExamplesLimit = 3

type GlobalReportBuildOptions struct {
	Week                string
	Environments        []string
	HistoryHorizonWeeks int
	HistoryResolver     semhistory.GlobalSignatureResolver
}

type GlobalReportReference struct {
	RunURL         string `json:"run_url"`
	OccurredAt     string `json:"occurred_at"`
	SignatureID    string `json:"signature_id"`
	PRNumber       int    `json:"pr_number"`
	PostGoodCommit bool   `json:"post_good_commit"`
}

type GlobalReportContributingTest struct {
	Lane         string `json:"lane"`
	JobName      string `json:"job_name"`
	TestName     string `json:"test_name"`
	SupportCount int    `json:"support_count"`
}

type GlobalReportCluster struct {
	Environment             string                         `json:"environment"`
	SchemaVersion           string                         `json:"schema_version"`
	Phase2ClusterID         string                         `json:"phase2_cluster_id"`
	CanonicalEvidencePhrase string                         `json:"canonical_evidence_phrase"`
	SearchQueryPhrase       string                         `json:"search_query_phrase"`
	SupportCount            int                            `json:"support_count"`
	SeenPostGoodCommit      bool                           `json:"seen_post_good_commit"`
	PostGoodCommitCount     int                            `json:"post_good_commit_count"`
	ContributingTestsCount  int                            `json:"contributing_tests_count"`
	ContributingTests       []GlobalReportContributingTest `json:"contributing_tests"`
	MemberPhase1ClusterIDs  []string                       `json:"member_phase1_cluster_ids"`
	MemberSignatureIDs      []string                       `json:"member_signature_ids"`
	References              []GlobalReportReference        `json:"references"`
	FullErrorSamples        []string                       `json:"full_error_samples,omitempty"`
	LinkedChildren          []GlobalReportCluster          `json:"linked_children,omitempty"`
}

type GlobalReportData struct {
	GlobalClusters                 []GlobalReportCluster
	TargetEnvironments             []string
	OverallJobsByEnvironment       map[string]int
	WindowStartRaw                 string
	WindowEndRaw                   string
	HistoryResolver                semhistory.GlobalSignatureResolver
	GeneratedAt                    time.Time
	TestClusterCountsByEnvironment map[string]int
	ReviewItemCountsByEnvironment  map[string]int
}

func BuildGlobalReportData(ctx context.Context, store storecontracts.Store, opts GlobalReportBuildOptions) (GlobalReportData, error) {
	if store == nil {
		return GlobalReportData{}, fmt.Errorf("store is required")
	}

	weekData, err := semanticquery.LoadWeekData(ctx, store, semanticquery.LoadWeekDataOptions{
		IncludeRawFailures: true,
	})
	if err != nil {
		return GlobalReportData{}, err
	}

	sourceGlobalRows := append([]semanticcontracts.GlobalClusterRecord(nil), weekData.SourceGlobalClusters...)
	phase3Links := append([]semanticcontracts.Phase3LinkRecord(nil), weekData.Phase3Links...)
	globalRows := append([]semanticcontracts.GlobalClusterRecord(nil), weekData.GlobalClusters...)
	linkedChildrenByClusterKey, err := globalReportLinkedChildrenByMergedClusterKey(sourceGlobalRows, phase3Links)
	if err != nil {
		return GlobalReportData{}, fmt.Errorf("build linked child clusters: %w", err)
	}

	reportGlobalRows := toGlobalReportClusters(globalRows)
	reportLinkedChildrenByClusterKey := toGlobalReportClusterGroupMap(linkedChildrenByClusterKey)
	rawFailuresByRun := globalReportIndexRawFailuresByEnvironmentRun(weekData.RawFailures)
	reportLinkedChildrenByClusterKey = globalReportAttachFullErrorSamplesByGroup(
		reportLinkedChildrenByClusterKey,
		globalReportFullErrorExamplesLimit,
		rawFailuresByRun,
	)

	targetEnvironments := semanticquery.ResolveTargetEnvironments(opts.Environments, weekData)
	metricWindowStart, metricWindowEnd := globalReportMetricWindowBounds(opts.Week)
	windowStartRaw, windowEndRaw := globalReportMetricWindowStrings(metricWindowStart, metricWindowEnd)
	overallJobsByEnvironment, err := globalReportMetricRunTotalsByEnvironment(
		ctx,
		store,
		targetEnvironments,
		metricWindowStart,
		metricWindowEnd,
	)
	if err != nil {
		return GlobalReportData{}, fmt.Errorf("load overall metric run counts: %w", err)
	}

	historyResolver := opts.HistoryResolver
	if historyResolver == nil {
		lookbackWeeks := opts.HistoryHorizonWeeks
		if lookbackWeeks <= 0 {
			lookbackWeeks = DefaultHistoryWeeks
		}
		historyResolver, err = semhistory.BuildGlobalSignatureResolver(ctx, semhistory.BuildOptions{
			CurrentWeek:                  strings.TrimSpace(opts.Week),
			GlobalSignatureLookbackWeeks: lookbackWeeks,
		})
		if err != nil {
			return GlobalReportData{}, fmt.Errorf("build global signature history resolver: %w", err)
		}
	}

	htmlGlobalRows := globalReportAttachFullErrorSamples(reportGlobalRows, globalReportFullErrorExamplesLimit, rawFailuresByRun)
	htmlGlobalRows = globalReportAttachLinkedChildren(htmlGlobalRows, reportLinkedChildrenByClusterKey)

	return GlobalReportData{
		GlobalClusters:                 htmlGlobalRows,
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

func globalReportMetricWindowBounds(week string) (time.Time, time.Time) {
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

func globalReportMetricWindowStrings(start time.Time, end time.Time) (string, string) {
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return "", ""
	}
	return start.Format(time.RFC3339), end.Format(time.RFC3339)
}

func globalReportMetricRunTotalsByEnvironment(
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
			dateValue, ok := globalReportParseMetricDate(row.Date)
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

func globalReportParseMetricDate(value string) (time.Time, bool) {
	parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func globalReportLinkedChildrenByMergedClusterKey(
	globalClusters []semanticcontracts.GlobalClusterRecord,
	phase3Links []semanticcontracts.Phase3LinkRecord,
) (map[string][]semanticcontracts.GlobalClusterRecord, error) {
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

	grouped := map[string][]semanticcontracts.GlobalClusterRecord{}
	for _, cluster := range globalClusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		if environment == "" || clusterID == "" {
			return nil, fmt.Errorf("global cluster record missing environment and/or phase2_cluster_id")
		}
		phase3ClusterIDs := globalReportPhase3ClusterIDsForGlobalCluster(cluster, phase3ClusterByAnchor)
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
		groupKey := globalReportClusterKey(environment, phase3ClusterIDs[0])
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

func globalReportPhase3ClusterIDsForGlobalCluster(
	cluster semanticcontracts.GlobalClusterRecord,
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

func globalReportClusterKey(environment string, clusterID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedClusterID := strings.TrimSpace(clusterID)
	if normalizedEnvironment == "" || trimmedClusterID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedClusterID
}

func toGlobalReportClusterGroupMap(groups map[string][]semanticcontracts.GlobalClusterRecord) map[string][]GlobalReportCluster {
	if len(groups) == 0 {
		return nil
	}
	out := make(map[string][]GlobalReportCluster, len(groups))
	for key, rows := range groups {
		out[key] = toGlobalReportClusters(rows)
	}
	return out
}

func toGlobalReportClusters(rows []semanticcontracts.GlobalClusterRecord) []GlobalReportCluster {
	out := make([]GlobalReportCluster, 0, len(rows))
	for _, row := range rows {
		out = append(out, GlobalReportCluster{
			Environment:             normalizeEnvironment(row.Environment),
			SchemaVersion:           strings.TrimSpace(row.SchemaVersion),
			Phase2ClusterID:         strings.TrimSpace(row.Phase2ClusterID),
			CanonicalEvidencePhrase: strings.TrimSpace(row.CanonicalEvidencePhrase),
			SearchQueryPhrase:       strings.TrimSpace(row.SearchQueryPhrase),
			SupportCount:            row.SupportCount,
			SeenPostGoodCommit:      row.SeenPostGoodCommit,
			PostGoodCommitCount:     row.PostGoodCommitCount,
			ContributingTestsCount:  row.ContributingTestsCount,
			ContributingTests:       toGlobalReportContributingTests(row.ContributingTests),
			MemberPhase1ClusterIDs:  append([]string(nil), row.MemberPhase1ClusterIDs...),
			MemberSignatureIDs:      append([]string(nil), row.MemberSignatureIDs...),
			References:              toGlobalReportReferences(row.References),
		})
	}
	return out
}

func toGlobalReportContributingTests(rows []semanticcontracts.ContributingTestRecord) []GlobalReportContributingTest {
	out := make([]GlobalReportContributingTest, 0, len(rows))
	for _, row := range rows {
		out = append(out, GlobalReportContributingTest{
			Lane:         strings.TrimSpace(row.Lane),
			JobName:      strings.TrimSpace(row.JobName),
			TestName:     strings.TrimSpace(row.TestName),
			SupportCount: row.SupportCount,
		})
	}
	return out
}

func toGlobalReportReferences(rows []semanticcontracts.ReferenceRecord) []GlobalReportReference {
	out := make([]GlobalReportReference, 0, len(rows))
	for _, row := range rows {
		out = append(out, GlobalReportReference{
			RunURL:         strings.TrimSpace(row.RunURL),
			OccurredAt:     strings.TrimSpace(row.OccurredAt),
			SignatureID:    strings.TrimSpace(row.SignatureID),
			PRNumber:       row.PRNumber,
			PostGoodCommit: row.PostGoodCommit,
		})
	}
	return out
}

func globalReportIndexRawFailuresByEnvironmentRun(rows []storecontracts.RawFailureRecord) map[string][]storecontracts.RawFailureRecord {
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

func globalReportAttachFullErrorSamples(
	clusters []GlobalReportCluster,
	limit int,
	runFailuresByRun map[string][]storecontracts.RawFailureRecord,
) []GlobalReportCluster {
	if len(clusters) == 0 || limit <= 0 {
		return append([]GlobalReportCluster(nil), clusters...)
	}
	out := append([]GlobalReportCluster(nil), clusters...)
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
		orderedRefs := append([]GlobalReportReference(nil), cluster.References...)
		sort.Slice(orderedRefs, func(i, j int) bool {
			ti, okI := triagehtml.ParseReferenceTimestamp(orderedRefs[i].OccurredAt)
			tj, okJ := triagehtml.ParseReferenceTimestamp(orderedRefs[j].OccurredAt)
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
				samples = globalReportAppendUniqueLimitedSample(samples, sample, limit)
			}
		}
		out[index].FullErrorSamples = samples
	}
	return out
}

func globalReportAttachFullErrorSamplesByGroup(
	groups map[string][]GlobalReportCluster,
	limit int,
	runFailuresByRun map[string][]storecontracts.RawFailureRecord,
) map[string][]GlobalReportCluster {
	if len(groups) == 0 {
		return nil
	}
	out := make(map[string][]GlobalReportCluster, len(groups))
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out[key] = globalReportAttachFullErrorSamples(groups[key], limit, runFailuresByRun)
	}
	return out
}

func globalReportAttachLinkedChildren(
	rows []GlobalReportCluster,
	linkedChildrenByClusterKey map[string][]GlobalReportCluster,
) []GlobalReportCluster {
	if len(rows) == 0 || len(linkedChildrenByClusterKey) == 0 {
		return rows
	}
	out := append([]GlobalReportCluster(nil), rows...)
	for index := range out {
		key := globalReportClusterKey(out[index].Environment, out[index].Phase2ClusterID)
		children := linkedChildrenByClusterKey[key]
		if len(children) == 0 {
			continue
		}
		out[index].LinkedChildren = append([]GlobalReportCluster(nil), children...)
	}
	return out
}

func globalReportAppendUniqueLimitedSample(existing []string, candidate string, limit int) []string {
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
