package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func loadMetricsDailyForDates(
	ctx context.Context,
	store storecontracts.Store,
	environments []string,
	dates []string,
) ([]storecontracts.MetricDailyRecord, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	normalizedEnvironments := normalizeStringSlice(environments)
	normalizedDates := normalizeMetricDateLabels(dates)
	if len(normalizedEnvironments) == 0 || len(normalizedDates) == 0 {
		return nil, nil
	}
	return store.ListMetricsDailyForDates(ctx, normalizedEnvironments, normalizedDates)
}

func sumMetricByEnvironmentForDates(
	ctx context.Context,
	store storecontracts.Store,
	metric string,
	environments []string,
	dates []string,
) (map[string]int, error) {
	totals := map[string]int{}
	if store == nil {
		return totals, nil
	}
	trimmedMetric := strings.TrimSpace(metric)
	if trimmedMetric == "" {
		return totals, nil
	}
	normalizedEnvironments := normalizeStringSlice(environments)
	normalizedDates := normalizeMetricDateLabels(dates)
	if len(normalizedEnvironments) == 0 || len(normalizedDates) == 0 {
		return totals, nil
	}
	sums, err := store.SumMetricByEnvironmentForDates(ctx, trimmedMetric, normalizedEnvironments, normalizedDates)
	if err != nil {
		return nil, err
	}
	for environment, value := range sums {
		normalizedEnvironment := normalizeEnvironment(environment)
		intValue := int(value)
		if normalizedEnvironment == "" || intValue <= 0 {
			continue
		}
		totals[normalizedEnvironment] += intValue
	}
	return totals, nil
}

func metricDateLabelsFromWindow(start time.Time, end time.Time) []string {
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return nil
	}
	out := make([]string, 0, int(end.Sub(start)/(24*time.Hour)))
	for date := start.UTC(); date.Before(end.UTC()); date = date.AddDate(0, 0, 1) {
		out = append(out, date.Format("2006-01-02"))
	}
	return normalizeMetricDateLabels(out)
}

func normalizeMetricDateLabels(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		set[trimmed] = struct{}{}
	}
	return sortedStringSet(set)
}
