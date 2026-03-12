package history

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

const defaultLookbackDays = 30

type BuildOptions struct {
	DataDirectory               string
	CurrentSemanticSubdir       string
	GlobalSignatureLookbackDays int
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
}

type globalSignatureResolver struct {
	byKey map[string]SignaturePresence
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

func BuildGlobalSignatureResolver(ctx context.Context, opts BuildOptions) (GlobalSignatureResolver, error) {
	dataDirectory := strings.TrimSpace(opts.DataDirectory)
	currentSubdir := strings.TrimSpace(opts.CurrentSemanticSubdir)
	if dataDirectory == "" || currentSubdir == "" {
		return &globalSignatureResolver{byKey: map[string]SignaturePresence{}}, nil
	}

	currentWeek, ok := parseWeekStart(currentSubdir)
	if !ok {
		return &globalSignatureResolver{byKey: map[string]SignaturePresence{}}, nil
	}
	if metadata, exists, err := ReadWindowMetadata(dataDirectory, currentSubdir); err != nil {
		return nil, fmt.Errorf("read current semantic window metadata: %w", err)
	} else if exists && !isCanonicalSevenDayWindow(metadata) {
		return &globalSignatureResolver{byKey: map[string]SignaturePresence{}}, nil
	}

	lookbackDays := opts.GlobalSignatureLookbackDays
	if lookbackDays <= 0 {
		lookbackDays = defaultLookbackDays
	}
	lookbackStart := currentWeek.AddDate(0, 0, -lookbackDays)

	semanticRoot := filepath.Join(dataDirectory, "semantic")
	entries, err := os.ReadDir(semanticRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return &globalSignatureResolver{byKey: map[string]SignaturePresence{}}, nil
		}
		return nil, fmt.Errorf("read semantic root directory %q: %w", semanticRoot, err)
	}

	aggregates := map[string]*signaturePresenceAggregate{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		weekName := strings.TrimSpace(entry.Name())
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
		metadata, exists, metadataErr := ReadWindowMetadata(dataDirectory, weekName)
		if metadataErr != nil {
			return nil, fmt.Errorf("read semantic window metadata for week %q: %w", weekName, metadataErr)
		}
		if !exists || !isCanonicalSevenDayWindow(metadata) {
			continue
		}

		weekStore, err := ndjson.NewWithOptions(dataDirectory, ndjson.Options{
			SemanticSubdirectory: weekName,
		})
		if err != nil {
			return nil, fmt.Errorf("open semantic store for week %q: %w", weekName, err)
		}
		rows, err := weekStore.ListGlobalClusters(ctx)
		_ = weekStore.Close()
		if err != nil {
			return nil, fmt.Errorf("list global clusters for week %q: %w", weekName, err)
		}
		collectGlobalSignaturePresence(rows, weekName, aggregates)
	}

	byKey := map[string]SignaturePresence{}
	for key, item := range aggregates {
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
	return &globalSignatureResolver{byKey: byKey}, nil
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
