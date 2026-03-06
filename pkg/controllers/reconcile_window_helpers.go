package controllers

import (
	"time"

	"ci-failure-atlas/pkg/sourceoptions"
	"ci-failure-atlas/pkg/store/contracts"
)

const (
	defaultActiveReconcileWindow   = 14 * 24 * time.Hour
	defaultUnresolvedPRRetryWindow = 7 * 24 * time.Hour
)

func activeReconcileWindow(source *sourceoptions.Options) time.Duration {
	if source != nil && source.ReconcileActiveWindow > 0 {
		return source.ReconcileActiveWindow
	}
	return defaultActiveReconcileWindow
}

func unresolvedPRRetryWindow(source *sourceoptions.Options) time.Duration {
	if source != nil && source.UnresolvedPRRetryWindow > 0 {
		return source.UnresolvedPRRetryWindow
	}
	return defaultUnresolvedPRRetryWindow
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

func isRunWithinUnresolvedRetryWindow(run contracts.RunRecord, window time.Duration, now time.Time) bool {
	if run.MergedPR || run.PRNumber <= 0 {
		return false
	}
	return isTimestampWithinWindow(run.OccurredAt, window, now)
}
