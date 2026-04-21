package readmodel

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type presentationWindowDefaultMode string

const (
	presentationWindowDefaultNone         presentationWindowDefaultMode = ""
	presentationWindowDefaultLatestWeek   presentationWindowDefaultMode = "latest_week"
	presentationWindowDefaultRolling      presentationWindowDefaultMode = "rolling"
	presentationWindowDefaultLatestSprint presentationWindowDefaultMode = "latest_sprint"
)

type presentationWindowRequest struct {
	Date        string
	StartDate   string
	EndDate     string
	Week        string
	Now         time.Time
	DefaultMode presentationWindowDefaultMode
	RollingDays int
}

const signalHorizonMinWeeks = 3

type presentationWindow struct {
	StartDate          string
	EndDate            string
	StartTime          time.Time
	EndTime            time.Time
	DateLabels         []string
	SemanticWeeks      []string
	AnchorWeek         string
	SignalHorizonWeeks []string
}

type WindowDefaultMode = presentationWindowDefaultMode

const (
	WindowDefaultNone         WindowDefaultMode = presentationWindowDefaultNone
	WindowDefaultLatestWeek   WindowDefaultMode = presentationWindowDefaultLatestWeek
	WindowDefaultRolling      WindowDefaultMode = presentationWindowDefaultRolling
	WindowDefaultLatestSprint WindowDefaultMode = presentationWindowDefaultLatestSprint
)

type WindowRequest struct {
	Date        string
	StartDate   string
	EndDate     string
	Week        string
	Now         time.Time
	DefaultMode WindowDefaultMode
	RollingDays int
}

type WindowScope struct {
	StartDate          string
	EndDate            string
	StartTime          time.Time
	EndTime            time.Time
	DateLabels         []string
	SemanticWeeks      []string
	AnchorWeek         string
	SignalHorizonWeeks []string
}

func (s *Service) ResolveWindow(ctx context.Context, request WindowRequest) (WindowScope, error) {
	window, err := s.resolvePresentationWindow(ctx, presentationWindowRequest{
		Date:        request.Date,
		StartDate:   request.StartDate,
		EndDate:     request.EndDate,
		Week:        request.Week,
		Now:         request.Now,
		DefaultMode: presentationWindowDefaultMode(request.DefaultMode),
		RollingDays: request.RollingDays,
	})
	if err != nil {
		return WindowScope{}, err
	}
	return WindowScope{
		StartDate:          window.StartDate,
		EndDate:            window.EndDate,
		StartTime:          window.StartTime,
		EndTime:            window.EndTime,
		DateLabels:         append([]string(nil), window.DateLabels...),
		SemanticWeeks:      append([]string(nil), window.SemanticWeeks...),
		AnchorWeek:         window.AnchorWeek,
		SignalHorizonWeeks: append([]string(nil), window.SignalHorizonWeeks...),
	}, nil
}

func (s *Service) resolvePresentationWindow(
	ctx context.Context,
	request presentationWindowRequest,
) (presentationWindow, error) {
	if s == nil {
		return presentationWindow{}, fmt.Errorf("service is required")
	}

	startDate := strings.TrimSpace(request.StartDate)
	endDate := strings.TrimSpace(request.EndDate)
	week := strings.TrimSpace(request.Week)
	date := strings.TrimSpace(request.Date)

	switch {
	case date != "":
		startDate = date
		endDate = date
	case startDate != "" || endDate != "":
		if startDate == "" || endDate == "" {
			return presentationWindow{}, fmt.Errorf("start_date and end_date must both be set")
		}
	case week != "":
		startDate, endDate = semanticWeekDateRange(week)
		if startDate == "" || endDate == "" {
			return presentationWindow{}, fmt.Errorf("invalid week %q", week)
		}
	default:
		switch request.DefaultMode {
		case presentationWindowDefaultLatestWeek:
			window, err := s.ResolveWeekWindow(ctx, "", request.Now)
			if err != nil {
				return presentationWindow{}, err
			}
			startDate, endDate = semanticWeekDateRange(window.CurrentWeek)
		case presentationWindowDefaultRolling:
			now := request.Now
			if now.IsZero() {
				now = time.Now().UTC()
			}
			rollingDays := request.RollingDays
			if rollingDays <= 0 {
				rollingDays = 7
			}
			endValue := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
			startValue := endValue.AddDate(0, 0, -(rollingDays - 1))
			startDate = startValue.Format("2006-01-02")
			endDate = endValue.Format("2006-01-02")
		case presentationWindowDefaultLatestSprint:
			now := request.Now
			if now.IsZero() {
				now = time.Now().UTC()
			}
			sprintStart, sprintEnd := SprintWindowForDate(now)
			startDate = sprintStart.Format("2006-01-02")
			endDate = sprintEnd.Format("2006-01-02")
		default:
			return presentationWindow{}, fmt.Errorf("start_date and end_date are required")
		}
	}

	startLabel, startValue, err := normalizeDateLabel(startDate)
	if err != nil {
		return presentationWindow{}, fmt.Errorf("invalid start_date: %w", err)
	}
	endLabel, endValue, err := normalizeDateLabel(endDate)
	if err != nil {
		return presentationWindow{}, fmt.Errorf("invalid end_date: %w", err)
	}
	if endValue.Before(startValue) {
		return presentationWindow{}, fmt.Errorf("end_date %s must be on or after start_date %s", endLabel, startLabel)
	}

	startTime := time.Date(startValue.Year(), startValue.Month(), startValue.Day(), 0, 0, 0, 0, time.UTC)
	endInclusive := time.Date(endValue.Year(), endValue.Month(), endValue.Day(), 0, 0, 0, 0, time.UTC)
	semanticWeeks := intersectingSemanticWeeks(startTime, endInclusive)
	if len(semanticWeeks) == 0 {
		return presentationWindow{}, fmt.Errorf("no semantic weeks intersect window %s..%s", startLabel, endLabel)
	}

	availableWeeks, err := s.DiscoverSemanticWeeks(ctx)
	if err != nil {
		return presentationWindow{}, err
	}
	loadableWeeks := filterAvailableWeeks(semanticWeeks, availableWeeks)
	if len(loadableWeeks) == 0 {
		return presentationWindow{}, s.explainUnavailableWeek(ctx, semanticWeeks[0])
	}
	if gap := interiorGap(loadableWeeks); gap != "" {
		return presentationWindow{}, s.explainUnavailableWeek(ctx, gap)
	}

	endTime := endInclusive.AddDate(0, 0, 1).UTC()

	signalHorizonWeeks := computeSignalHorizonWeeks(loadableWeeks, availableWeeks, endInclusive)

	return presentationWindow{
		StartDate:          startLabel,
		EndDate:            endLabel,
		StartTime:          startTime,
		EndTime:            endTime,
		DateLabels:         metricDateLabelsFromWindow(startTime, endTime),
		SemanticWeeks:      loadableWeeks,
		AnchorWeek:         loadableWeeks[len(loadableWeeks)-1],
		SignalHorizonWeeks: signalHorizonWeeks,
	}, nil
}

func intersectingSemanticWeeks(startTime time.Time, endInclusive time.Time) []string {
	if startTime.IsZero() || endInclusive.IsZero() || endInclusive.Before(startTime) {
		return nil
	}
	startWeek := weekStartForDate(startTime)
	endWeek := weekStartForDate(endInclusive)
	if startWeek.IsZero() || endWeek.IsZero() {
		return nil
	}
	out := make([]string, 0, 4)
	for current := startWeek; !current.After(endWeek); current = current.AddDate(0, 0, 7) {
		out = append(out, current.Format("2006-01-02"))
	}
	return out
}

func weekStartForDate(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	date := time.Date(value.UTC().Year(), value.UTC().Month(), value.UTC().Day(), 0, 0, 0, 0, time.UTC)
	return date.AddDate(0, 0, -int((date.Weekday()+6)%7)).UTC()
}

func filterAvailableWeeks(requested []string, available []string) []string {
	out := make([]string, 0, len(requested))
	for _, week := range requested {
		index := sort.SearchStrings(available, week)
		if index < len(available) && available[index] == week {
			out = append(out, week)
		}
	}
	return out
}

// interiorGap returns the first missing week label between the first and last
// entries of a sorted, non-empty week list. Returns "" if there are no gaps.
func interiorGap(weeks []string) string {
	if len(weeks) < 2 {
		return ""
	}
	first, err := time.Parse("2006-01-02", weeks[0])
	if err != nil {
		return ""
	}
	expected := first.UTC()
	for i := 1; i < len(weeks); i++ {
		expected = expected.AddDate(0, 0, 7)
		if weeks[i] != expected.Format("2006-01-02") {
			return expected.Format("2006-01-02")
		}
	}
	return ""
}

// computeSignalHorizonWeeks returns the set of semantic weeks needed for
// signal computation. It extends the presentation weeks backwards to cover
// at least signalHorizonMinWeeks, filtered to weeks that are actually
// available in the store. Gaps in the signal horizon are tolerated (unlike
// the presentation window) because partial history is better than none.
func computeSignalHorizonWeeks(presentationWeeks []string, availableWeeks []string, endInclusive time.Time) []string {
	if len(presentationWeeks) == 0 {
		return nil
	}
	anchorWeek := presentationWeeks[len(presentationWeeks)-1]
	anchorTime, err := time.Parse("2006-01-02", anchorWeek)
	if err != nil {
		return append([]string(nil), presentationWeeks...)
	}
	horizonStart := anchorTime.UTC().AddDate(0, 0, -(signalHorizonMinWeeks * 7))
	if !endInclusive.IsZero() {
		endWeek := weekStartForDate(endInclusive)
		candidate := endWeek.AddDate(0, 0, -(signalHorizonMinWeeks * 7))
		if candidate.Before(horizonStart) {
			horizonStart = candidate
		}
	}

	candidateWeeks := intersectingSemanticWeeks(horizonStart, anchorTime.UTC().AddDate(0, 0, 6))
	horizonWeeks := filterAvailableWeeks(candidateWeeks, availableWeeks)

	presentationSet := make(map[string]struct{}, len(presentationWeeks))
	for _, w := range presentationWeeks {
		presentationSet[w] = struct{}{}
	}
	for _, w := range horizonWeeks {
		if _, exists := presentationSet[w]; !exists {
			presentationSet[w] = struct{}{}
		}
	}

	merged := make([]string, 0, len(presentationSet))
	for w := range presentationSet {
		merged = append(merged, w)
	}
	sort.Strings(merged)
	return merged
}

func semanticWeekDateRange(week string) (string, string) {
	normalizedWeek, err := normalizeWeekLabel(week)
	if err != nil {
		return "", ""
	}
	startDate, err := time.Parse("2006-01-02", normalizedWeek)
	if err != nil {
		return "", ""
	}
	startDate = startDate.UTC()
	return startDate.Format("2006-01-02"), startDate.AddDate(0, 0, 6).Format("2006-01-02")
}
