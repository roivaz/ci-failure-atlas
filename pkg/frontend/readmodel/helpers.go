package readmodel

import (
	"fmt"
	"sort"
	"strings"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

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
