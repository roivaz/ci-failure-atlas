package history

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

const defaultLookbackWeeks = 4

type BuildOptions struct {
	DataDirectory                string
	CurrentSemanticSubdir        string
	GlobalSignatureLookbackWeeks int
	ListSemanticWeeks            func(context.Context) ([]string, error)
	OpenStore                    func(context.Context, string) (storecontracts.Store, error)
	ReadWindowMetadata           func(string) (WindowMetadata, bool, error)
}

type SignatureKey struct {
	Environment string
	Phrase      string
	SearchQuery string
}

type SignaturePresence struct {
	PriorWeeksPresent int
	PriorWeekStarts   []string
	PriorJobsAffected int
	PriorLastSeenAt   time.Time
}

type GlobalSignatureResolver interface {
	PresenceFor(SignatureKey) SignaturePresence
	PresenceForPhase3Cluster(environment string, phase3ClusterID string) SignaturePresence
}

type globalSignatureResolver struct {
	byKey              map[string]SignaturePresence
	byPhase3ClusterKey map[string]SignaturePresence
}

type signaturePresenceAggregate struct {
	weeks    map[string]struct{}
	jobs     map[string]struct{}
	lastSeen time.Time
}

func (r *globalSignatureResolver) PresenceFor(key SignatureKey) SignaturePresence {
	if r == nil || len(r.byKey) == 0 {
		return SignaturePresence{}
	}
	presence, ok := r.byKey[signatureHistoryKey(key.Environment, key.Phrase, key.SearchQuery)]
	if !ok {
		return SignaturePresence{}
	}
	return presence
}

func (r *globalSignatureResolver) PresenceForPhase3Cluster(environment string, phase3ClusterID string) SignaturePresence {
	if r == nil || len(r.byPhase3ClusterKey) == 0 {
		return SignaturePresence{}
	}
	presence, ok := r.byPhase3ClusterKey[phase3ClusterHistoryKey(environment, phase3ClusterID)]
	if !ok {
		return SignaturePresence{}
	}
	return presence
}

func BuildGlobalSignatureResolver(ctx context.Context, opts BuildOptions) (GlobalSignatureResolver, error) {
	currentSubdir := strings.TrimSpace(opts.CurrentSemanticSubdir)
	if currentSubdir == "" {
		return &globalSignatureResolver{
			byKey:              map[string]SignaturePresence{},
			byPhase3ClusterKey: map[string]SignaturePresence{},
		}, nil
	}

	currentWeek, ok := parseWeekStart(currentSubdir)
	if !ok {
		return &globalSignatureResolver{
			byKey:              map[string]SignaturePresence{},
			byPhase3ClusterKey: map[string]SignaturePresence{},
		}, nil
	}

	windowMetadataReader := opts.ReadWindowMetadata
	if windowMetadataReader != nil {
		if metadata, exists, err := windowMetadataReader(currentSubdir); err != nil {
			return nil, fmt.Errorf("read current semantic window metadata: %w", err)
		} else if exists && !isCanonicalSevenDayWindow(metadata) {
			return &globalSignatureResolver{
				byKey:              map[string]SignaturePresence{},
				byPhase3ClusterKey: map[string]SignaturePresence{},
			}, nil
		}
	}

	lookbackWeeks := opts.GlobalSignatureLookbackWeeks
	if lookbackWeeks <= 0 {
		lookbackWeeks = defaultLookbackWeeks
	}
	lookbackStart := currentWeek.AddDate(0, 0, -(lookbackWeeks * 7))

	semanticWeeksLister := opts.ListSemanticWeeks
	if semanticWeeksLister == nil {
		return &globalSignatureResolver{
			byKey:              map[string]SignaturePresence{},
			byPhase3ClusterKey: map[string]SignaturePresence{},
		}, nil
	}
	weekNames, err := semanticWeeksLister(ctx)
	if err != nil {
		return nil, fmt.Errorf("list semantic weeks: %w", err)
	}

	storeOpener := opts.OpenStore
	if storeOpener == nil {
		return &globalSignatureResolver{
			byKey:              map[string]SignaturePresence{},
			byPhase3ClusterKey: map[string]SignaturePresence{},
		}, nil
	}

	signatureAggregates := map[string]*signaturePresenceAggregate{}
	phase3ClusterAggregates := map[string]*signaturePresenceAggregate{}
	for _, rawWeekName := range weekNames {
		weekName := strings.TrimSpace(rawWeekName)
		if weekName == "" {
			continue
		}
		weekStart, ok := parseWeekStart(weekName)
		if !ok {
			continue
		}
		if !weekStart.Before(currentWeek) {
			continue
		}
		if weekStart.Before(lookbackStart) {
			continue
		}
		if windowMetadataReader != nil {
			metadata, exists, metadataErr := windowMetadataReader(weekName)
			if metadataErr != nil {
				return nil, fmt.Errorf("read semantic window metadata for week %q: %w", weekName, metadataErr)
			}
			if !exists || !isCanonicalSevenDayWindow(metadata) {
				continue
			}
		}

		weekStore, err := storeOpener(ctx, weekName)
		if err != nil {
			return nil, fmt.Errorf("open semantic store for week %q: %w", weekName, err)
		}
		rows, err := weekStore.ListGlobalClusters(ctx)
		if err != nil {
			_ = weekStore.Close()
			return nil, fmt.Errorf("list global clusters for week %q: %w", weekName, err)
		}
		phase3Links, err := weekStore.ListPhase3Links(ctx)
		_ = weekStore.Close()
		if err != nil {
			return nil, fmt.Errorf("list phase3 links for week %q: %w", weekName, err)
		}
		collectGlobalSignaturePresence(rows, weekName, signatureAggregates)
		if err := collectPhase3ClusterPresence(rows, phase3Links, weekName, phase3ClusterAggregates); err != nil {
			return nil, fmt.Errorf("collect phase3-cluster presence for week %q: %w", weekName, err)
		}
	}

	byKey := map[string]SignaturePresence{}
	for key, item := range signatureAggregates {
		weeks := make([]string, 0, len(item.weeks))
		for week := range item.weeks {
			weeks = append(weeks, week)
		}
		sort.Strings(weeks)
		byKey[key] = SignaturePresence{
			PriorWeeksPresent: len(weeks),
			PriorWeekStarts:   weeks,
			PriorJobsAffected: len(item.jobs),
			PriorLastSeenAt:   item.lastSeen,
		}
	}
	byPhase3ClusterKey := map[string]SignaturePresence{}
	for key, item := range phase3ClusterAggregates {
		weeks := make([]string, 0, len(item.weeks))
		for week := range item.weeks {
			weeks = append(weeks, week)
		}
		sort.Strings(weeks)
		byPhase3ClusterKey[key] = SignaturePresence{
			PriorWeeksPresent: len(weeks),
			PriorWeekStarts:   weeks,
			PriorJobsAffected: len(item.jobs),
			PriorLastSeenAt:   item.lastSeen,
		}
	}
	return &globalSignatureResolver{
		byKey:              byKey,
		byPhase3ClusterKey: byPhase3ClusterKey,
	}, nil
}

func collectGlobalSignaturePresence(rows []semanticcontracts.GlobalClusterRecord, weekName string, aggregates map[string]*signaturePresenceAggregate) {
	for _, row := range rows {
		key := signatureHistoryKey(row.Environment, row.CanonicalEvidencePhrase, row.SearchQueryPhrase)
		if key == "" {
			continue
		}
		item, ok := aggregates[key]
		if !ok {
			item = &signaturePresenceAggregate{
				weeks: map[string]struct{}{},
				jobs:  map[string]struct{}{},
			}
			aggregates[key] = item
		}
		item.weeks[weekName] = struct{}{}
		for _, reference := range row.References {
			runKey := normalizedRunReferenceKey(
				reference.RunURL,
				reference.SignatureID,
				reference.OccurredAt,
				reference.PRNumber,
			)
			if runKey != "" {
				item.jobs[runKey] = struct{}{}
			}
			if ts, ok := parseReferenceTimestamp(reference.OccurredAt); ok {
				if item.lastSeen.IsZero() || ts.After(item.lastSeen) {
					item.lastSeen = ts
				}
			}
		}
	}
}

func collectPhase3ClusterPresence(
	rows []semanticcontracts.GlobalClusterRecord,
	phase3Links []semanticcontracts.Phase3LinkRecord,
	weekName string,
	aggregates map[string]*signaturePresenceAggregate,
) error {
	phase3ClusterByAnchor := map[string]string{}
	for _, row := range phase3Links {
		phase3ClusterID := strings.TrimSpace(row.IssueID)
		if phase3ClusterID == "" {
			continue
		}
		key := phase3AnchorHistoryKey(row.Environment, row.RunURL, row.RowID)
		if key == "" {
			continue
		}
		phase3ClusterByAnchor[key] = phase3ClusterID
	}
	for _, row := range rows {
		environment := normalizeEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		phase3ClusterIDSet := map[string]struct{}{}
		for _, reference := range row.References {
			anchorKey := phase3AnchorHistoryKey(environment, reference.RunURL, reference.RowID)
			if anchorKey == "" {
				continue
			}
			phase3ClusterID := strings.TrimSpace(phase3ClusterByAnchor[anchorKey])
			if phase3ClusterID == "" {
				continue
			}
			phase3ClusterIDSet[phase3ClusterID] = struct{}{}
		}
		if len(phase3ClusterIDSet) == 0 {
			continue
		}
		phase3ClusterIDs := make([]string, 0, len(phase3ClusterIDSet))
		for phase3ClusterID := range phase3ClusterIDSet {
			phase3ClusterIDs = append(phase3ClusterIDs, phase3ClusterID)
		}
		sort.Strings(phase3ClusterIDs)
		if len(phase3ClusterIDs) > 1 {
			return fmt.Errorf(
				"phase3 conflict: global cluster %s resolves to multiple phase3 cluster IDs (%s)",
				strings.TrimSpace(row.Phase2ClusterID),
				strings.Join(phase3ClusterIDs, ", "),
			)
		}
		key := phase3ClusterHistoryKey(environment, phase3ClusterIDs[0])
		if key == "" {
			continue
		}
		item, ok := aggregates[key]
		if !ok {
			item = &signaturePresenceAggregate{
				weeks: map[string]struct{}{},
				jobs:  map[string]struct{}{},
			}
			aggregates[key] = item
		}
		item.weeks[weekName] = struct{}{}
		for _, reference := range row.References {
			runKey := normalizedRunReferenceKey(
				reference.RunURL,
				reference.SignatureID,
				reference.OccurredAt,
				reference.PRNumber,
			)
			if runKey != "" {
				item.jobs[runKey] = struct{}{}
			}
			if ts, ok := parseReferenceTimestamp(reference.OccurredAt); ok {
				if item.lastSeen.IsZero() || ts.After(item.lastSeen) {
					item.lastSeen = ts
				}
			}
		}
	}
	return nil
}

func normalizedRunReferenceKey(runURL string, signatureID string, occurredAt string, prNumber int) string {
	if trimmed := strings.TrimSpace(runURL); trimmed != "" {
		return trimmed
	}
	parts := []string{
		strings.TrimSpace(signatureID),
		strings.TrimSpace(occurredAt),
		fmt.Sprintf("%d", prNumber),
	}
	key := strings.TrimSpace(strings.Join(parts, "|"))
	if key == "||0" {
		return ""
	}
	return key
}

func signatureHistoryKey(environment string, phrase string, searchQuery string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	signatureText := normalizedSignatureText(phrase, searchQuery)
	if normalizedEnvironment == "" || signatureText == "" {
		return ""
	}
	return normalizedEnvironment + "|" + signatureText
}

func phase3ClusterHistoryKey(environment string, phase3ClusterID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedPhase3ClusterID := strings.TrimSpace(phase3ClusterID)
	if normalizedEnvironment == "" || trimmedPhase3ClusterID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedPhase3ClusterID
}

func phase3AnchorHistoryKey(environment string, runURL string, rowID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedRunURL := strings.TrimSpace(runURL)
	trimmedRowID := strings.TrimSpace(rowID)
	if normalizedEnvironment == "" || trimmedRunURL == "" || trimmedRowID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedRunURL + "|" + trimmedRowID
}

func normalizedSignatureText(phrase string, searchQuery string) string {
	canonical := normalizePhrase(phrase)
	if canonical != "" {
		return canonical
	}
	return normalizePhrase(searchQuery)
}

func normalizePhrase(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	return strings.Join(strings.Fields(trimmed), " ")
}

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func parseWeekStart(value string) (time.Time, bool) {
	parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func parseReferenceTimestamp(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, false
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts.UTC(), true
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts.UTC(), true
	}
	return time.Time{}, false
}
