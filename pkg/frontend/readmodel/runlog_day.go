package readmodel

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	semanticquery "ci-failure-atlas/pkg/semantic/query"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

type RunLogDayQuery struct {
	Date         string
	Week         string
	Environments []string
	GeneratedAt  time.Time
}

type RunLogDayData struct {
	Meta         RunLogDayMeta          `json:"meta"`
	Environments []RunLogDayEnvironment `json:"environments"`
}

type RunLogDayMeta struct {
	Date         string   `json:"date"`
	AnchorWeek   string   `json:"-"`
	Timezone     string   `json:"timezone"`
	GeneratedAt  string   `json:"generated_at"`
	Environments []string `json:"environments"`
}

type RunLogDayEnvironment struct {
	Environment string             `json:"environment"`
	Summary     RunLogDaySummary   `json:"summary"`
	Runs        []JobHistoryRunRow `json:"runs"`
}

type RunLogDaySummary struct {
	TotalRuns                  int `json:"total_runs"`
	FailedRuns                 int `json:"failed_runs"`
	RunsWithRawFailures        int `json:"runs_with_occurrences"`
	RunsWithSemanticAttachment int `json:"runs_with_failure_pattern_matches"`
	RunsUnmatchedSignatures    int `json:"runs_with_unmatched_occurrences"`
	RunsNoFailureRows          int `json:"runs_without_occurrence_rows"`
	FailedRunsWithoutRawRows   int `json:"failed_runs_without_occurrence_rows"`
}

type JobHistoryRunRow struct {
	Run             storecontracts.RunRecord  `json:"run"`
	Lanes           []string                  `json:"failed_at,omitempty"`
	FailedTestCount int                       `json:"failed_test_count"`
	FailureRows     []JobHistoryFailureRow    `json:"occurrences,omitempty"`
	SemanticRollups JobHistorySemanticRollups `json:"failure_pattern_summary"`
	BadPRScore      int                       `json:"pr_caused_score,omitempty"`
	BadPRReasons    []string                  `json:"pr_caused_reasons,omitempty"`
}

type JobHistoryFailureRow struct {
	RowID              string                       `json:"row_id"`
	RunURL             string                       `json:"run_url"`
	OccurredAt         string                       `json:"occurred_at"`
	Lane               string                       `json:"failed_at,omitempty"`
	SignatureID        string                       `json:"signature_id,omitempty"`
	TestName           string                       `json:"test_name,omitempty"`
	TestSuite          string                       `json:"test_suite,omitempty"`
	FailureText        string                       `json:"failure_text,omitempty"`
	NonArtifactBacked  bool                         `json:"non_artifact_backed,omitempty"`
	SemanticAttachment JobHistorySemanticAttachment `json:"failure_pattern_match"`
	Phase3IssueID      string                       `json:"phase3_issue_id,omitempty"`
	BadPRScore         int                          `json:"-"`
	BadPRReasons       []string                     `json:"-"`
}

type JobHistorySemanticAttachment struct {
	Status                  string `json:"status"`
	ClusterID               string `json:"failure_pattern_id,omitempty"`
	CanonicalEvidencePhrase string `json:"failure_pattern,omitempty"`
	SearchQueryPhrase       string `json:"search_query,omitempty"`
}

type JobHistorySemanticRollups struct {
	SignatureCount     int      `json:"signature_count"`
	DistinctClusterIDs []string `json:"failure_pattern_ids,omitempty"`
	ClusteredRows      int      `json:"matched_occurrences"`
	UnmatchedRows      int      `json:"unmatched_occurrences"`
	AttachmentSummary  string   `json:"match_summary"`
}

type runLogDayScope struct {
	Date         string
	DateValue    time.Time
	ResolvedWeek string
}

type jobHistoryReferenceCluster struct {
	ClusterID               string
	CanonicalEvidencePhrase string
	SearchQueryPhrase       string
	Lane                    string
	SupportCount            int
	BadPRScore              int
	BadPRReasons            []string
}

func (s *Service) BuildRunLogDay(ctx context.Context, query RunLogDayQuery) (RunLogDayData, error) {
	if s == nil {
		return RunLogDayData{}, fmt.Errorf("service is required")
	}
	dateLabel, _, err := normalizeDateLabel(query.Date)
	if err != nil {
		return RunLogDayData{}, fmt.Errorf("invalid date: %w", err)
	}

	window, err := s.resolvePresentationWindow(ctx, presentationWindowRequest{
		Date: dateLabel,
		Week: query.Week,
	})
	if err != nil {
		return RunLogDayData{}, err
	}

	store, err := s.OpenStoreForWeek(window.AnchorWeek)
	if err != nil {
		return RunLogDayData{}, err
	}
	defer func() {
		_ = store.Close()
	}()

	weekData, err := semanticquery.LoadWeekData(ctx, store, semanticquery.LoadWeekDataOptions{})
	if err != nil {
		return RunLogDayData{}, fmt.Errorf("load semantic week data for run history: %w", err)
	}

	targetEnvironments := semanticquery.ResolveTargetEnvironments(query.Environments, weekData)
	if len(targetEnvironments) == 0 {
		targetEnvironments = normalizeStringSlice(query.Environments)
	}

	factsByEnvironment, err := loadFailurePatternsFacts(ctx, store, targetEnvironments, window)
	if err != nil {
		return RunLogDayData{}, fmt.Errorf("load run history day facts: %w", err)
	}

	clusterByReference := buildJobHistoryReferenceIndex(weekData.FailurePatterns)
	phase3IssueByAnchor := buildJobHistoryPhase3IssueIndex(weekData.Phase3Links)

	generatedAt := query.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	environments := make([]RunLogDayEnvironment, 0, len(targetEnvironments))
	for _, environment := range targetEnvironments {
		normalizedEnvironment := normalizeEnvironment(environment)
		if normalizedEnvironment == "" {
			continue
		}
		runs := buildJobHistoryRunRows(
			normalizedEnvironment,
			factsByEnvironment[normalizedEnvironment],
			clusterByReference,
			phase3IssueByAnchor,
		)
		environments = append(environments, RunLogDayEnvironment{
			Environment: normalizedEnvironment,
			Summary:     buildRunLogDaySummary(runs),
			Runs:        runs,
		})
	}

	return RunLogDayData{
		Meta: RunLogDayMeta{
			Date:         window.StartDate,
			AnchorWeek:   window.AnchorWeek,
			Timezone:     "UTC",
			GeneratedAt:  generatedAt.UTC().Format(time.RFC3339),
			Environments: append([]string(nil), targetEnvironments...),
		},
		Environments: environments,
	}, nil
}

func resolveRunLogDayScope(query RunLogDayQuery) (runLogDayScope, error) {
	dateLabel, dateValue, err := normalizeDateLabel(query.Date)
	if err != nil {
		return runLogDayScope{}, fmt.Errorf("invalid date: %w", err)
	}
	resolvedWeek, err := resolveFailurePatternsWeekLabel(dateValue, dateValue, query.Week)
	if err != nil {
		return runLogDayScope{}, err
	}
	return runLogDayScope{
		Date:         dateLabel,
		DateValue:    dateValue,
		ResolvedWeek: resolvedWeek,
	}, nil
}

func buildJobHistoryRunRows(
	environment string,
	facts failurePatternsEnvironmentFacts,
	clusterByReference map[string]jobHistoryReferenceCluster,
	phase3IssueByAnchor map[string]string,
) []JobHistoryRunRow {
	rawFailuresByRun := map[string][]storecontracts.RawFailureRecord{}
	for _, row := range facts.RawFailures {
		runURL := strings.TrimSpace(row.RunURL)
		if runURL == "" {
			continue
		}
		rawFailuresByRun[runURL] = append(rawFailuresByRun[runURL], row)
	}

	runRows := make([]JobHistoryRunRow, 0, len(facts.RunsByURL))
	for _, run := range facts.RunsByURL {
		runURL := strings.TrimSpace(run.RunURL)
		if runURL == "" {
			continue
		}
		failures := buildJobHistoryFailureRows(
			environment,
			rawFailuresByRun[runURL],
			clusterByReference,
			phase3IssueByAnchor,
		)
		badPRScore, badPRReasons := jobHistoryWeeklyBadPR(failures)
		runRows = append(runRows, JobHistoryRunRow{
			Run:             run,
			Lanes:           jobHistoryRunLanes(failures),
			FailedTestCount: jobHistoryFailedTestCount(failures),
			FailureRows:     failures,
			SemanticRollups: buildJobHistorySemanticRollups(run, failures),
			BadPRScore:      badPRScore,
			BadPRReasons:    badPRReasons,
		})
	}

	sortJobHistoryRunRows(runRows)
	return runRows
}

func buildJobHistoryFailureRows(
	environment string,
	rawFailures []storecontracts.RawFailureRecord,
	clusterByReference map[string]jobHistoryReferenceCluster,
	phase3IssueByAnchor map[string]string,
) []JobHistoryFailureRow {
	rows := make([]JobHistoryFailureRow, 0, len(rawFailures))
	for _, row := range rawFailures {
		signatureID := strings.TrimSpace(row.SignatureID)
		cluster, matched := findJobHistoryClusterForRawFailure(environment, row, clusterByReference)

		attachment := JobHistorySemanticAttachment{
			Status: "unmatched",
		}
		if matched {
			attachment = JobHistorySemanticAttachment{
				Status:                  "clustered",
				ClusterID:               strings.TrimSpace(cluster.ClusterID),
				CanonicalEvidencePhrase: strings.TrimSpace(cluster.CanonicalEvidencePhrase),
				SearchQueryPhrase:       strings.TrimSpace(cluster.SearchQueryPhrase),
			}
		}

		rows = append(rows, JobHistoryFailureRow{
			RowID:              strings.TrimSpace(row.RowID),
			RunURL:             strings.TrimSpace(row.RunURL),
			OccurredAt:         strings.TrimSpace(row.OccurredAt),
			Lane:               strings.TrimSpace(cluster.Lane),
			SignatureID:        signatureID,
			TestName:           strings.TrimSpace(row.TestName),
			TestSuite:          strings.TrimSpace(row.TestSuite),
			FailureText:        jobHistoryFailureText(row),
			NonArtifactBacked:  row.NonArtifactBacked,
			SemanticAttachment: attachment,
			Phase3IssueID:      strings.TrimSpace(phase3IssueByAnchor[phase3AnchorKey(environment, row.RunURL, row.RowID)]),
			BadPRScore:         cluster.BadPRScore,
			BadPRReasons:       append([]string(nil), cluster.BadPRReasons...),
		})
	}

	sortJobHistoryFailureRows(rows)
	return rows
}

func buildJobHistorySemanticRollups(run storecontracts.RunRecord, failures []JobHistoryFailureRow) JobHistorySemanticRollups {
	signatureIDs := map[string]struct{}{}
	clusterIDs := map[string]struct{}{}
	clusteredRows := 0
	unmatchedRows := 0

	for _, row := range failures {
		if trimmedSignatureID := strings.TrimSpace(row.SignatureID); trimmedSignatureID != "" {
			signatureIDs[trimmedSignatureID] = struct{}{}
		}
		switch strings.TrimSpace(row.SemanticAttachment.Status) {
		case "clustered":
			clusteredRows++
			if trimmedClusterID := strings.TrimSpace(row.SemanticAttachment.ClusterID); trimmedClusterID != "" {
				clusterIDs[trimmedClusterID] = struct{}{}
			}
		default:
			unmatchedRows++
		}
	}

	return JobHistorySemanticRollups{
		SignatureCount:     len(signatureIDs),
		DistinctClusterIDs: sortedStringSet(clusterIDs),
		ClusteredRows:      clusteredRows,
		UnmatchedRows:      unmatchedRows,
		AttachmentSummary:  buildJobHistoryAttachmentSummary(run, len(signatureIDs), len(clusterIDs), clusteredRows, unmatchedRows),
	}
}

func buildJobHistoryAttachmentSummary(
	run storecontracts.RunRecord,
	signatureCount int,
	distinctClusterCount int,
	clusteredRows int,
	unmatchedRows int,
) string {
	if signatureCount == 0 {
		if run.Failed {
			return "failed_without_raw_rows"
		}
		return "no_failures"
	}
	if clusteredRows == 0 {
		return "unmatched_only"
	}
	if unmatchedRows > 0 {
		return "mixed"
	}
	if signatureCount == 1 && distinctClusterCount == 1 {
		return "single_clustered"
	}
	return "multiple_clustered"
}

func buildRunLogDaySummary(runs []JobHistoryRunRow) RunLogDaySummary {
	summary := RunLogDaySummary{
		TotalRuns: len(runs),
	}
	for _, row := range runs {
		if row.Run.Failed {
			summary.FailedRuns++
		}
		if len(row.FailureRows) == 0 {
			summary.RunsNoFailureRows++
			if row.Run.Failed {
				summary.FailedRunsWithoutRawRows++
			}
			continue
		}
		summary.RunsWithRawFailures++
		if row.SemanticRollups.ClusteredRows > 0 {
			summary.RunsWithSemanticAttachment++
		}
		if row.SemanticRollups.UnmatchedRows > 0 {
			summary.RunsUnmatchedSignatures++
		}
	}
	return summary
}

func buildJobHistoryReferenceIndex(clusters []semanticcontracts.FailurePatternRecord) map[string]jobHistoryReferenceCluster {
	index := map[string]jobHistoryReferenceCluster{}
	phraseEnvironments := jobHistoryPhraseEnvironments(clusters)
	for _, cluster := range clusters {
		environment := normalizeEnvironment(cluster.Environment)
		if environment == "" {
			continue
		}
		otherEnvironments := windowedSeenInOtherEnvironments(
			phraseEnvironments[normalizePhrase(cluster.CanonicalEvidencePhrase)],
			environment,
		)
		badPRScore, badPRReasons := BadPRScoreAndReasons(FailurePatternRow{
			Environment:        environment,
			AfterLastPushCount: cluster.PostGoodCommitCount,
			AlsoIn:             otherEnvironments,
			AffectedRuns:       jobHistoryRunReferences(cluster.References),
		})
		candidate := jobHistoryReferenceCluster{
			ClusterID:               strings.TrimSpace(cluster.Phase2ClusterID),
			CanonicalEvidencePhrase: strings.TrimSpace(cluster.CanonicalEvidencePhrase),
			SearchQueryPhrase:       strings.TrimSpace(cluster.SearchQueryPhrase),
			Lane:                    strings.TrimSpace(primaryContributingTest(cluster.ContributingTests).Lane),
			SupportCount:            cluster.SupportCount,
			BadPRScore:              badPRScore,
			BadPRReasons:            append([]string(nil), badPRReasons...),
		}
		for _, key := range jobHistoryReferenceKeys(environment, cluster.References) {
			if key == "" {
				continue
			}
			current, exists := index[key]
			if !exists || jobHistoryPrefersClusterCandidate(current, candidate) {
				index[key] = candidate
			}
		}
	}
	return index
}

func jobHistoryPhraseEnvironments(clusters []semanticcontracts.FailurePatternRecord) map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}
	for _, cluster := range clusters {
		environment := normalizeEnvironment(cluster.Environment)
		phraseKey := normalizePhrase(cluster.CanonicalEvidencePhrase)
		if environment == "" || phraseKey == "" {
			continue
		}
		set := out[phraseKey]
		if set == nil {
			set = map[string]struct{}{}
			out[phraseKey] = set
		}
		set[environment] = struct{}{}
	}
	return out
}

func jobHistoryRunReferences(rows []semanticcontracts.ReferenceRecord) []RunReference {
	out := make([]RunReference, 0, len(rows))
	for _, row := range rows {
		out = append(out, RunReference{
			RunURL:      strings.TrimSpace(row.RunURL),
			OccurredAt:  strings.TrimSpace(row.OccurredAt),
			SignatureID: strings.TrimSpace(row.SignatureID),
			PRNumber:    row.PRNumber,
		})
	}
	return out
}

func buildJobHistoryPhase3IssueIndex(links []semanticcontracts.Phase3LinkRecord) map[string]string {
	index := map[string]string{}
	for _, link := range links {
		key := phase3AnchorKey(link.Environment, link.RunURL, link.RowID)
		issueID := strings.TrimSpace(link.IssueID)
		if key == "" || issueID == "" {
			continue
		}
		index[key] = issueID
	}
	return index
}

func jobHistoryPrefersClusterCandidate(current jobHistoryReferenceCluster, candidate jobHistoryReferenceCluster) bool {
	if candidate.SupportCount != current.SupportCount {
		return candidate.SupportCount > current.SupportCount
	}
	currentPhrase := strings.TrimSpace(current.CanonicalEvidencePhrase)
	candidatePhrase := strings.TrimSpace(candidate.CanonicalEvidencePhrase)
	if candidatePhrase != currentPhrase {
		return candidatePhrase < currentPhrase
	}
	return strings.TrimSpace(candidate.ClusterID) < strings.TrimSpace(current.ClusterID)
}

func findJobHistoryClusterForRawFailure(
	environment string,
	row storecontracts.RawFailureRecord,
	clusterByReference map[string]jobHistoryReferenceCluster,
) (jobHistoryReferenceCluster, bool) {
	for _, key := range jobHistoryRawFailureKeys(environment, row) {
		cluster, ok := clusterByReference[key]
		if ok {
			return cluster, true
		}
	}
	return jobHistoryReferenceCluster{}, false
}

func jobHistoryReferenceKeys(environment string, references []semanticcontracts.ReferenceRecord) []string {
	if len(references) == 0 {
		return nil
	}
	normalizedEnvironment := normalizeEnvironment(environment)
	if normalizedEnvironment == "" {
		return nil
	}
	out := make([]string, 0, len(references)*2)
	seen := map[string]struct{}{}
	appendKey := func(key string) {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			return
		}
		if _, ok := seen[trimmed]; ok {
			return
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	for _, reference := range references {
		appendKey(jobHistoryReferenceRowKey(normalizedEnvironment, reference.RowID))
		appendKey(jobHistoryReferenceTupleKey(normalizedEnvironment, reference.RunURL, reference.OccurredAt, reference.SignatureID))
	}
	return out
}

func jobHistoryRawFailureKeys(environment string, row storecontracts.RawFailureRecord) []string {
	normalizedEnvironment := normalizeEnvironment(environment)
	if normalizedEnvironment == "" {
		return nil
	}
	out := make([]string, 0, 2)
	if key := jobHistoryReferenceRowKey(normalizedEnvironment, row.RowID); key != "" {
		out = append(out, key)
	}
	if key := jobHistoryReferenceTupleKey(normalizedEnvironment, row.RunURL, row.OccurredAt, row.SignatureID); key != "" {
		out = append(out, key)
	}
	return out
}

func jobHistoryReferenceRowKey(environment string, rowID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	rowKey := semanticquery.EnvironmentRowKey(normalizedEnvironment, rowID)
	if rowKey == "" {
		return ""
	}
	return "row|" + rowKey
}

func jobHistoryReferenceTupleKey(environment string, runURL string, occurredAt string, signatureID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedRunURL := strings.TrimSpace(runURL)
	trimmedOccurredAt := strings.TrimSpace(occurredAt)
	trimmedSignatureID := strings.TrimSpace(signatureID)
	if normalizedEnvironment == "" {
		return ""
	}
	if trimmedRunURL == "" && trimmedOccurredAt == "" && trimmedSignatureID == "" {
		return ""
	}
	return "ref|" + normalizedEnvironment + "|" + trimmedRunURL + "|" + trimmedOccurredAt + "|" + trimmedSignatureID
}

func sortJobHistoryRunRows(rows []JobHistoryRunRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		left := strings.TrimSpace(rows[i].Run.OccurredAt)
		right := strings.TrimSpace(rows[j].Run.OccurredAt)
		if left != right {
			return left > right
		}
		leftJob := strings.TrimSpace(rows[i].Run.JobName)
		rightJob := strings.TrimSpace(rows[j].Run.JobName)
		if leftJob != rightJob {
			return leftJob < rightJob
		}
		return strings.TrimSpace(rows[i].Run.RunURL) < strings.TrimSpace(rows[j].Run.RunURL)
	})
}

func sortJobHistoryFailureRows(rows []JobHistoryFailureRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		left := strings.TrimSpace(rows[i].OccurredAt)
		right := strings.TrimSpace(rows[j].OccurredAt)
		if left != right {
			return left < right
		}
		return strings.TrimSpace(rows[i].RowID) < strings.TrimSpace(rows[j].RowID)
	})
}

func jobHistoryRunLanes(rows []JobHistoryFailureRow) []string {
	set := map[string]struct{}{}
	for _, row := range rows {
		if lane := strings.TrimSpace(row.Lane); lane != "" {
			set[lane] = struct{}{}
		}
	}
	return sortedStringSet(set)
}

func jobHistoryFailedTestCount(rows []JobHistoryFailureRow) int {
	if len(rows) == 0 {
		return 0
	}
	set := map[string]struct{}{}
	for _, row := range rows {
		testName := strings.TrimSpace(row.TestName)
		testSuite := strings.TrimSpace(row.TestSuite)
		switch {
		case testName != "" || testSuite != "":
			set[testSuite+"|"+testName] = struct{}{}
		case strings.TrimSpace(row.RowID) != "":
			set["row|"+strings.TrimSpace(row.RowID)] = struct{}{}
		default:
			set["failure|"+strings.TrimSpace(row.OccurredAt)+"|"+strings.TrimSpace(row.RunURL)] = struct{}{}
		}
	}
	return len(set)
}

func jobHistoryWeeklyBadPR(failures []JobHistoryFailureRow) (int, []string) {
	bestScore := 0
	var bestReasons []string
	seen := map[string]struct{}{}
	for _, row := range failures {
		if strings.TrimSpace(row.SemanticAttachment.Status) != "clustered" {
			continue
		}
		key := strings.TrimSpace(row.SemanticAttachment.ClusterID)
		if key == "" {
			key = strings.TrimSpace(row.SignatureID)
		}
		if key != "" {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
		}
		if row.BadPRScore <= bestScore {
			continue
		}
		bestScore = row.BadPRScore
		bestReasons = append([]string(nil), row.BadPRReasons...)
	}
	return bestScore, bestReasons
}

func jobHistoryFailureText(row storecontracts.RawFailureRecord) string {
	text := strings.TrimSpace(row.RawText)
	if text != "" {
		return text
	}
	return strings.TrimSpace(row.NormalizedText)
}
