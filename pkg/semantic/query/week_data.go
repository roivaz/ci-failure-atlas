package query

import (
	"context"
	"fmt"
	"sort"
	"strings"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	phase3engine "ci-failure-atlas/pkg/semantic/engine/phase3"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

type LoadWeekDataOptions struct {
	IncludeRawFailures bool
}

type WeekData struct {
	SourceGlobalClusters       []semanticcontracts.GlobalClusterRecord
	GlobalClusters             []semanticcontracts.GlobalClusterRecord
	ReviewQueue                []semanticcontracts.ReviewItemRecord
	Phase3Links                []semanticcontracts.Phase3LinkRecord
	RawFailures                []storecontracts.RawFailureRecord
	TestClusterCountsByEnv     map[string]int
	ReviewQueueCountsByEnv     map[string]int
	GlobalClusterCountsByEnv   map[string]int
	GlobalSupportTotalsByEnv   map[string]int
	AvailableEnvironments      []string
}

func LoadWeekData(ctx context.Context, store storecontracts.Store, opts LoadWeekDataOptions) (WeekData, error) {
	if store == nil {
		return WeekData{}, fmt.Errorf("store is required")
	}

	sourceGlobalClusters, err := store.ListGlobalClusters(ctx)
	if err != nil {
		return WeekData{}, fmt.Errorf("list global clusters: %w", err)
	}
	reviewQueue, err := store.ListReviewQueue(ctx)
	if err != nil {
		return WeekData{}, fmt.Errorf("list review queue: %w", err)
	}
	phase3Links, err := store.ListPhase3Links(ctx)
	if err != nil {
		return WeekData{}, fmt.Errorf("list phase3 links: %w", err)
	}
	globalClusters, err := phase3engine.Merge(sourceGlobalClusters, phase3Links)
	if err != nil {
		return WeekData{}, fmt.Errorf("apply phase3 materialized view: %w", err)
	}

	rawFailures := []storecontracts.RawFailureRecord(nil)
	if opts.IncludeRawFailures {
		rawFailures, err = store.ListRawFailures(ctx)
		if err != nil {
			return WeekData{}, fmt.Errorf("list raw failures: %w", err)
		}
	}

	testClusterIDsByEnv := map[string]map[string]struct{}{}
	globalClusterCountsByEnv := map[string]int{}
	globalSupportTotalsByEnv := map[string]int{}
	envSet := map[string]struct{}{}
	for _, row := range sourceGlobalClusters {
		environment := normalizeEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		envSet[environment] = struct{}{}
		globalClusterCountsByEnv[environment]++
		globalSupportTotalsByEnv[environment] += row.SupportCount
		if _, ok := testClusterIDsByEnv[environment]; !ok {
			testClusterIDsByEnv[environment] = map[string]struct{}{}
		}
		for _, phase1ClusterID := range row.MemberPhase1ClusterIDs {
			trimmedPhase1ClusterID := strings.TrimSpace(phase1ClusterID)
			if trimmedPhase1ClusterID == "" {
				continue
			}
			testClusterIDsByEnv[environment][trimmedPhase1ClusterID] = struct{}{}
		}
	}

	testClusterCountsByEnv := map[string]int{}
	for environment, clusterIDs := range testClusterIDsByEnv {
		testClusterCountsByEnv[environment] = len(clusterIDs)
	}

	reviewQueueCountsByEnv := map[string]int{}
	for _, row := range reviewQueue {
		environment := normalizeEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		envSet[environment] = struct{}{}
		reviewQueueCountsByEnv[environment]++
	}

	availableEnvironments := make([]string, 0, len(envSet))
	for environment := range envSet {
		availableEnvironments = append(availableEnvironments, environment)
	}
	sort.Strings(availableEnvironments)

	return WeekData{
		SourceGlobalClusters:     sourceGlobalClusters,
		GlobalClusters:           globalClusters,
		ReviewQueue:              reviewQueue,
		Phase3Links:              phase3Links,
		RawFailures:              rawFailures,
		TestClusterCountsByEnv:   testClusterCountsByEnv,
		ReviewQueueCountsByEnv:   reviewQueueCountsByEnv,
		GlobalClusterCountsByEnv: globalClusterCountsByEnv,
		GlobalSupportTotalsByEnv: globalSupportTotalsByEnv,
		AvailableEnvironments:    availableEnvironments,
	}, nil
}

func ResolveTargetEnvironments(configured []string, data WeekData) []string {
	normalizedConfigured := normalizeEnvironments(configured)
	if len(normalizedConfigured) > 0 {
		return normalizedConfigured
	}
	return append([]string(nil), data.AvailableEnvironments...)
}

func RawFailureTextByEnvironmentRow(rows []storecontracts.RawFailureRecord) map[string]string {
	byRowKey := map[string]string{}
	for _, row := range rows {
		environment := normalizeEnvironment(row.Environment)
		rowID := strings.TrimSpace(row.RowID)
		rawText := strings.TrimSpace(row.RawText)
		if environment == "" || rowID == "" || rawText == "" {
			continue
		}
		rowKey := EnvironmentRowKey(environment, rowID)
		if rowKey == "" {
			continue
		}
		if _, exists := byRowKey[rowKey]; !exists {
			byRowKey[rowKey] = rawText
		}
	}
	return byRowKey
}

func EnvironmentRowKey(environment string, rowID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedRowID := strings.TrimSpace(rowID)
	if normalizedEnvironment == "" || trimmedRowID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedRowID
}

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeEnvironments(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		normalized := normalizeEnvironment(value)
		if normalized == "" {
			continue
		}
		set[normalized] = struct{}{}
	}
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
