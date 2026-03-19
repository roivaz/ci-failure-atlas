package input

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/testrules"
)

type EnrichedFailure struct {
	Environment    string
	RunURL         string
	RowID          string
	OccurredAt     string
	JobName        string
	Lane           string
	PRNumber       int
	PostGoodCommit bool
	TestName       string
	TestSuite      string
	SignatureID    string
	RawText        string
	NormalizedText string
}

type BuildOptions struct {
	EnvironmentSet map[string]struct{}
	WindowStart    *time.Time
	WindowEnd      *time.Time
}

type Diagnostics struct {
	RawRowsTotal             int
	RowsIncluded             int
	RowsSkippedOutsideWindow int
	RowsSkippedNonArtifact   int
	RowsSkippedInvalid       int
	MissingRunMetadata       int
	MissingOccurredAt        int
	MissingJobName           int
	MissingRowID             int
	MissingSignatureID       int
	MissingRawText           int
	MissingNormalizedText    int
}

type BuildIssue struct {
	Reason      string
	Environment string
	RunURL      string
	RowID       string
}

type BuildResult struct {
	Rows        []EnrichedFailure
	Diagnostics Diagnostics
	InvalidRows []BuildIssue
}

const invalidRowsSampleLimit = 10

func BuildEnrichedFailures(ctx context.Context, store storecontracts.Store, opts BuildOptions) (BuildResult, error) {
	if store == nil {
		return BuildResult{}, fmt.Errorf("store is required")
	}

	runs, err := store.ListRuns(ctx)
	if err != nil {
		return BuildResult{}, fmt.Errorf("list runs: %w", err)
	}
	runByKey := make(map[string]storecontracts.RunRecord, len(runs))
	for _, run := range runs {
		normalizedRun := normalizeRun(run)
		key := enrichedRunLookupKey(normalizedRun.Environment, normalizedRun.RunURL)
		if key == "" {
			continue
		}
		runByKey[key] = normalizedRun
	}

	rawRows, err := store.ListRawFailures(ctx)
	if err != nil {
		return BuildResult{}, fmt.Errorf("list raw failures: %w", err)
	}

	result := BuildResult{
		Rows:        make([]EnrichedFailure, 0),
		Diagnostics: Diagnostics{},
		InvalidRows: make([]BuildIssue, 0),
	}

	for _, row := range rawRows {
		normalizedRow := normalizeRawFailure(row, row.Environment, row.RunURL)
		environment := normalizeEnvironment(normalizedRow.Environment)
		runURL := strings.TrimSpace(normalizedRow.RunURL)
		if environment == "" || runURL == "" {
			continue
		}
		if !isEnvironmentEnabled(environment, opts.EnvironmentSet) {
			continue
		}

		result.Diagnostics.RawRowsTotal++
		if normalizedRow.NonArtifactBacked {
			result.Diagnostics.RowsSkippedNonArtifact++
			continue
		}

		runKey := enrichedRunLookupKey(environment, runURL)
		normalizedRun, runFound := runByKey[runKey]
		if !runFound {
			normalizedRun = storecontracts.RunRecord{}
		}

		issues := validateRow(normalizedRow, normalizedRun, runFound)
		if len(issues) > 0 {
			result.Diagnostics.RowsSkippedInvalid++
			for _, issue := range issues {
				incrementIssueCounters(&result.Diagnostics, issue.Reason)
				if len(result.InvalidRows) < invalidRowsSampleLimit {
					result.InvalidRows = append(result.InvalidRows, issue)
				}
			}
			continue
		}

		if !isRowWithinWindow(normalizedRow, normalizedRun, opts.WindowStart, opts.WindowEnd) {
			result.Diagnostics.RowsSkippedOutsideWindow++
			continue
		}

		occurredAt := normalizedRow.OccurredAt
		if occurredAt == "" {
			occurredAt = strings.TrimSpace(normalizedRun.OccurredAt)
		}

		result.Rows = append(result.Rows, EnrichedFailure{
			Environment:    environment,
			RunURL:         runURL,
			RowID:          normalizedRow.RowID,
			OccurredAt:     occurredAt,
			JobName:        strings.TrimSpace(normalizedRun.JobName),
			Lane:           string(testrules.ClassifyLane(environment, normalizedRow.TestSuite, normalizedRow.TestName)),
			PRNumber:       normalizedRun.PRNumber,
			PostGoodCommit: normalizedRun.PostGoodCommit,
			TestName:       normalizedRow.TestName,
			TestSuite:      normalizedRow.TestSuite,
			SignatureID:    normalizedRow.SignatureID,
			RawText:        normalizedRow.RawText,
			NormalizedText: normalizedRow.NormalizedText,
		})
		result.Diagnostics.RowsIncluded++
	}

	sort.Slice(result.Rows, func(i, j int) bool {
		if result.Rows[i].Environment != result.Rows[j].Environment {
			return result.Rows[i].Environment < result.Rows[j].Environment
		}
		if result.Rows[i].Lane != result.Rows[j].Lane {
			return result.Rows[i].Lane < result.Rows[j].Lane
		}
		if result.Rows[i].JobName != result.Rows[j].JobName {
			return result.Rows[i].JobName < result.Rows[j].JobName
		}
		if result.Rows[i].TestName != result.Rows[j].TestName {
			return result.Rows[i].TestName < result.Rows[j].TestName
		}
		if result.Rows[i].OccurredAt != result.Rows[j].OccurredAt {
			return result.Rows[i].OccurredAt < result.Rows[j].OccurredAt
		}
		if result.Rows[i].RunURL != result.Rows[j].RunURL {
			return result.Rows[i].RunURL < result.Rows[j].RunURL
		}
		if result.Rows[i].SignatureID != result.Rows[j].SignatureID {
			return result.Rows[i].SignatureID < result.Rows[j].SignatureID
		}
		return result.Rows[i].RowID < result.Rows[j].RowID
	})

	if result.Diagnostics.RowsSkippedInvalid > 0 {
		return result, fmt.Errorf(
			"incomplete semantic input rows detected: invalid=%d missing_run=%d missing_occurred_at=%d missing_job_name=%d missing_row_id=%d missing_signature_id=%d missing_raw_text=%d missing_normalized_text=%d sample=%s",
			result.Diagnostics.RowsSkippedInvalid,
			result.Diagnostics.MissingRunMetadata,
			result.Diagnostics.MissingOccurredAt,
			result.Diagnostics.MissingJobName,
			result.Diagnostics.MissingRowID,
			result.Diagnostics.MissingSignatureID,
			result.Diagnostics.MissingRawText,
			result.Diagnostics.MissingNormalizedText,
			formatIssueSample(result.InvalidRows),
		)
	}

	return result, nil
}

func enrichedRunLookupKey(environment string, runURL string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	normalizedRunURL := strings.TrimSpace(runURL)
	if normalizedEnvironment == "" || normalizedRunURL == "" {
		return ""
	}
	return normalizedEnvironment + "|" + normalizedRunURL
}

func isEnvironmentEnabled(environment string, envSet map[string]struct{}) bool {
	if len(envSet) == 0 {
		return true
	}
	_, ok := envSet[normalizeEnvironment(environment)]
	return ok
}

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeRun(run storecontracts.RunRecord) storecontracts.RunRecord {
	return storecontracts.RunRecord{
		Environment:    normalizeEnvironment(run.Environment),
		RunURL:         strings.TrimSpace(run.RunURL),
		JobName:        strings.TrimSpace(run.JobName),
		PRNumber:       run.PRNumber,
		PostGoodCommit: run.PostGoodCommit,
		OccurredAt:     strings.TrimSpace(run.OccurredAt),
	}
}

func normalizeRawFailure(row storecontracts.RawFailureRecord, fallbackEnvironment string, fallbackRunURL string) storecontracts.RawFailureRecord {
	normalizedEnvironment := normalizeEnvironment(row.Environment)
	if normalizedEnvironment == "" {
		normalizedEnvironment = normalizeEnvironment(fallbackEnvironment)
	}
	normalizedRunURL := strings.TrimSpace(row.RunURL)
	if normalizedRunURL == "" {
		normalizedRunURL = strings.TrimSpace(fallbackRunURL)
	}
	return storecontracts.RawFailureRecord{
		Environment:       normalizedEnvironment,
		RowID:             strings.TrimSpace(row.RowID),
		RunURL:            normalizedRunURL,
		NonArtifactBacked: row.NonArtifactBacked,
		TestName:          strings.TrimSpace(row.TestName),
		TestSuite:         strings.TrimSpace(row.TestSuite),
		SignatureID:       strings.TrimSpace(row.SignatureID),
		OccurredAt:        strings.TrimSpace(row.OccurredAt),
		RawText:           strings.TrimSpace(row.RawText),
		NormalizedText:    strings.TrimSpace(row.NormalizedText),
	}
}

func validateRow(row storecontracts.RawFailureRecord, run storecontracts.RunRecord, runFound bool) []BuildIssue {
	issues := make([]BuildIssue, 0, 6)
	base := BuildIssue{
		Environment: row.Environment,
		RunURL:      row.RunURL,
		RowID:       row.RowID,
	}
	if !runFound {
		issue := base
		issue.Reason = "missing_run_metadata"
		issues = append(issues, issue)
		return issues
	}
	if strings.TrimSpace(run.JobName) == "" {
		issue := base
		issue.Reason = "missing_job_name"
		issues = append(issues, issue)
	}
	occurredAt := strings.TrimSpace(row.OccurredAt)
	if occurredAt == "" {
		occurredAt = strings.TrimSpace(run.OccurredAt)
	}
	if occurredAt == "" {
		issue := base
		issue.Reason = "missing_occurred_at"
		issues = append(issues, issue)
	}
	if strings.TrimSpace(row.RowID) == "" {
		issue := base
		issue.Reason = "missing_row_id"
		issues = append(issues, issue)
	}
	if strings.TrimSpace(row.SignatureID) == "" {
		issue := base
		issue.Reason = "missing_signature_id"
		issues = append(issues, issue)
	}
	if strings.TrimSpace(row.RawText) == "" {
		issue := base
		issue.Reason = "missing_raw_text"
		issues = append(issues, issue)
	}
	if strings.TrimSpace(row.NormalizedText) == "" {
		issue := base
		issue.Reason = "missing_normalized_text"
		issues = append(issues, issue)
	}
	return issues
}

func parseTimestamp(raw string) (time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return parsed.UTC(), true
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return parsed.UTC(), true
	}
	return time.Time{}, false
}

func isRowWithinWindow(row storecontracts.RawFailureRecord, run storecontracts.RunRecord, start *time.Time, end *time.Time) bool {
	if start == nil || end == nil {
		return true
	}
	timestamp := strings.TrimSpace(row.OccurredAt)
	if timestamp == "" {
		timestamp = strings.TrimSpace(run.OccurredAt)
	}
	parsed, ok := parseTimestamp(timestamp)
	if !ok {
		return false
	}
	if parsed.Before(*start) {
		return false
	}
	if !parsed.Before(*end) {
		return false
	}
	return true
}

func incrementIssueCounters(diag *Diagnostics, reason string) {
	if diag == nil {
		return
	}
	switch strings.TrimSpace(reason) {
	case "missing_run_metadata":
		diag.MissingRunMetadata++
	case "missing_occurred_at":
		diag.MissingOccurredAt++
	case "missing_job_name":
		diag.MissingJobName++
	case "missing_row_id":
		diag.MissingRowID++
	case "missing_signature_id":
		diag.MissingSignatureID++
	case "missing_raw_text":
		diag.MissingRawText++
	case "missing_normalized_text":
		diag.MissingNormalizedText++
	}
}

func formatIssueSample(issues []BuildIssue) string {
	if len(issues) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(issues))
	for _, issue := range issues {
		parts = append(parts, fmt.Sprintf("%s:%s|%s|%s", issue.Reason, issue.Environment, issue.RunURL, issue.RowID))
	}
	return "[" + strings.Join(parts, ",") + "]"
}
