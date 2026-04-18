package service

import (
	"sort"
	"strings"
	"time"
)

func CurrentWeekStart(now time.Time) time.Time {
	normalized := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
	offset := int(normalized.Weekday())
	return normalized.AddDate(0, 0, -offset)
}

func CurrentWeekLabel(now time.Time) string {
	return CurrentWeekStart(now).Format("2006-01-02")
}

func LatestCompleteWeek(weeks []string, now time.Time) string {
	ordered := normalizedWeeks(weeks)
	if len(ordered) == 0 {
		return ""
	}
	currentWeek := CurrentWeekLabel(now)
	for i := len(ordered) - 1; i >= 0; i-- {
		if ordered[i] < currentWeek {
			return ordered[i]
		}
	}
	return ordered[len(ordered)-1]
}

func ResolveWindow(weeks []string, requestedWeek string, defaultWeek string, now time.Time) (string, string, string, int) {
	ordered := normalizedWeeks(weeks)
	if len(ordered) == 0 {
		return "", "", "", -1
	}

	currentWeek := CurrentWeekLabel(now)
	week := strings.TrimSpace(requestedWeek)
	if week == "" || !contains(ordered, week) {
		week = ""
		trimmedDefaultWeek := strings.TrimSpace(defaultWeek)
		if trimmedDefaultWeek != "" && contains(ordered, trimmedDefaultWeek) {
			if trimmedDefaultWeek != currentWeek || onlyCurrentWeekAvailable(ordered, currentWeek) {
				week = trimmedDefaultWeek
			}
		}
	}
	if week == "" {
		week = LatestCompleteWeek(ordered, now)
	}

	index := sort.SearchStrings(ordered, week)
	if index < 0 || index >= len(ordered) || ordered[index] != week {
		index = len(ordered) - 1
		week = ordered[index]
	}

	previous := ""
	next := ""
	if index > 0 {
		previous = ordered[index-1]
	}
	if index+1 < len(ordered) {
		next = ordered[index+1]
	}
	return week, previous, next, index
}

func normalizedWeeks(weeks []string) []string {
	if len(weeks) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	for _, week := range weeks {
		trimmedWeek := strings.TrimSpace(week)
		if trimmedWeek == "" {
			continue
		}
		set[trimmedWeek] = struct{}{}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for week := range set {
		out = append(out, week)
	}
	sort.Strings(out)
	return out
}

func contains(weeks []string, target string) bool {
	trimmedTarget := strings.TrimSpace(target)
	if trimmedTarget == "" {
		return false
	}
	index := sort.SearchStrings(weeks, trimmedTarget)
	return index >= 0 && index < len(weeks) && weeks[index] == trimmedTarget
}

func onlyCurrentWeekAvailable(weeks []string, currentWeek string) bool {
	if len(weeks) == 0 {
		return false
	}
	for _, week := range weeks {
		if week != currentWeek {
			return false
		}
	}
	return true
}
