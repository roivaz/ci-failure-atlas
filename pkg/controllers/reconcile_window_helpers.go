package controllers

import (
	"time"

	"ci-failure-atlas/pkg/sourceoptions"
	"ci-failure-atlas/pkg/store/contracts"
)

const (
	defaultHistoryHorizonWeeks = 4
	daysPerWeek                = 7
)

func activeReconcileWindow(source *sourceoptions.Options) time.Duration {
	weeks := defaultHistoryHorizonWeeks
	if source != nil && source.HistoryHorizonWeeks > 0 {
		weeks = source.HistoryHorizonWeeks
	}
	return time.Duration(weeks*daysPerWeek) * 24 * time.Hour
}

func isTimestampWithinWindow(value string, window time.Duration, now time.Time) bool {
	if window <= 0 {
		return true
	}
	ts, ok := parseTimestamp(value)
	if !ok {
		return true
	}
	return !ts.UTC().Before(now.UTC().Add(-window))
}

func isRunWithinActiveWindow(run contracts.RunRecord, window time.Duration, now time.Time) bool {
	return isTimestampWithinWindow(run.OccurredAt, window, now)
}
