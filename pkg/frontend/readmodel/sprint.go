package readmodel

import "time"

const (
	alignmentAnchorDateStr = "2026-01-05"
	sprintDurationDays     = 14
)

func alignmentAnchorDate() time.Time {
	t, _ := time.Parse("2006-01-02", alignmentAnchorDateStr)
	return t.UTC()
}

// SprintWindowForDate returns the inclusive start and end dates of the sprint
// containing the given date, aligned to the two-week cadence anchored on
// 2026-01-05.
func SprintWindowForDate(now time.Time) (time.Time, time.Time) {
	anchor := alignmentAnchorDate()
	today := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
	daysSinceAnchor := int(today.Sub(anchor).Hours() / 24)
	windowNumber := daysSinceAnchor / sprintDurationDays
	start := anchor.AddDate(0, 0, windowNumber*sprintDurationDays)
	end := start.AddDate(0, 0, sprintDurationDays-1)
	return start, end
}

// ShiftSprintWindow returns the sprint window shifted by the given number of
// sprints relative to the sprint containing referenceDate.
func ShiftSprintWindow(referenceDate time.Time, sprints int) (time.Time, time.Time) {
	start, _ := SprintWindowForDate(referenceDate)
	shifted := start.AddDate(0, 0, sprints*sprintDurationDays)
	return shifted, shifted.AddDate(0, 0, sprintDurationDays-1)
}

// SprintDurationDays returns the sprint length (exported for use in server.go).
func SprintDurationDays() int {
	return sprintDurationDays
}
