package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"ci-failure-atlas/pkg/report/triagehtml"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	semanticquery "ci-failure-atlas/pkg/semantic/query"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

type JobHistoryDayQuery struct {
	Date         string
	Week         string
	Environments []string
	GeneratedAt  time.Time
}

type JobHistoryDayData struct {
	Meta         JobHistoryDayMeta          `json:"meta"`
	Environments []JobHistoryDayEnvironment `json:"environments"`
}

type JobHistoryDayMeta struct {
	Date         string   `json:"date"`
	ResolvedWeek string   `json:"resolved_week"`
	Timezone     string   `json:"timezone"`
	GeneratedAt  string   `json:"generated_at"`
	Environments []string `json:"environments"`
}

type JobHistoryDayEnvironment struct {
	Environment string               `json:"environment"`
	Summary     JobHistoryDaySummary `json:"summary"`
	Runs        []JobHistoryRunRow   `json:"runs"`
}

type JobHistoryDaySummary struct {
	TotalRuns                  int `json:"total_runs"`
	FailedRuns                 int `json:"failed_runs"`
	RunsWithRawFailures        int `json:"runs_with_raw_failures"`
	RunsWithSemanticAttachment int `json:"runs_with_semantic_attachment"`
	RunsUnmatchedSignatures    int `json:"runs_unmatched_signatures"`
	RunsNoFailureRows          int `json:"runs_no_failure_rows"`
	FailedRunsWithoutRawRows   int `json:"failed_runs_without_raw_rows"`
}

type JobHistoryRunRow struct {
	Run             storecontracts.RunRecord  `json:"run"`
	Lanes           []string                  `json:"lanes,omitempty"`
	FailedTestCount int                       `json:"failed_test_count"`
	FailureRows     []JobHistoryFailureRow    `json:"failure_rows,omitempty"`
	SemanticRollups JobHistorySemanticRollups `json:"semantic_rollups"`
	BadPRScore      int                       `json:"bad_pr_score,omitempty"`
	BadPRReasons    []string                  `json:"bad_pr_reasons,omitempty"`
}

type JobHistoryFailureRow struct {
	RowID              string                       `json:"row_id"`
	RunURL             string                       `json:"run_url"`
	OccurredAt         string                       `json:"occurred_at"`
	Lane               string                       `json:"lane,omitempty"`
	SignatureID        string                       `json:"signature_id,omitempty"`
	TestName           string                       `json:"test_name,omitempty"`
	TestSuite          string                       `json:"test_suite,omitempty"`
	FailureText        string                       `json:"failure_text,omitempty"`
	NonArtifactBacked  bool                         `json:"non_artifact_backed,omitempty"`
	SemanticAttachment JobHistorySemanticAttachment `json:"semantic_attachment"`
	Phase3IssueID      string                       `json:"phase3_issue_id,omitempty"`
	BadPRScore         int                          `json:"-"`
	BadPRReasons       []string                     `json:"-"`
}

type JobHistorySemanticAttachment struct {
	Status                  string `json:"status"`
	ClusterID               string `json:"cluster_id,omitempty"`
	CanonicalEvidencePhrase string `json:"canonical_evidence_phrase,omitempty"`
	SearchQueryPhrase       string `json:"search_query_phrase,omitempty"`
}

type JobHistorySemanticRollups struct {
	SignatureCount     int      `json:"signature_count"`
	DistinctClusterIDs []string `json:"distinct_cluster_ids,omitempty"`
	ClusteredRows      int      `json:"clustered_rows"`
	UnmatchedRows      int      `json:"unmatched_rows"`
	AttachmentSummary  string   `json:"attachment_summary"`
}

type jobHistoryDayScope struct {
	Date         string
	DateValue    time.Time
	ResolvedWeek string
}

type jobHistorySignatureCluster struct {
	ClusterID               string
	CanonicalEvidencePhrase string
	SearchQueryPhrase       string
	Lane                    string
	SupportCount            int
	BadPRScore              int
	BadPRReasons            []string
}

func (s *Service) BuildJobHistoryDay(ctx context.Context, query JobHistoryDayQuery) (JobHistoryDayData, error) {
	if s == nil {
		return JobHistoryDayData{}, fmt.Errorf("service is required")
	}

	scope, err := resolveJobHistoryDayScope(query)
	if err != nil {
		return JobHistoryDayData{}, err
	}
	scope.ResolvedWeek, err = s.ensureWeekExists(ctx, scope.ResolvedWeek)
	if err != nil {
		return JobHistoryDayData{}, err
	}

	store, err := s.OpenStoreForWeek(scope.ResolvedWeek)
	if err != nil {
		return JobHistoryDayData{}, err
	}
	defer func() {
		_ = store.Close()
	}()

	weekData, err := semanticquery.LoadWeekData(ctx, store, semanticquery.LoadWeekDataOptions{})
	if err != nil {
		return JobHistoryDayData{}, fmt.Errorf("load semantic week data for run history: %w", err)
	}

	targetEnvironments := semanticquery.ResolveTargetEnvironments(query.Environments, weekData)
	if len(targetEnvironments) == 0 {
		targetEnvironments = normalizeStringSlice(query.Environments)
	}

	factsByEnvironment, err := loadWindowedTriageFacts(ctx, store, targetEnvironments, windowedTriageScope{
		DateLabels: []string{scope.Date},
	})
	if err != nil {
		return JobHistoryDayData{}, fmt.Errorf("load run history day facts: %w", err)
	}

	clusterBySignature := buildJobHistorySignatureIndex(weekData.GlobalClusters)
	phase3IssueByAnchor := buildJobHistoryPhase3IssueIndex(weekData.Phase3Links)

	generatedAt := query.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	environments := make([]JobHistoryDayEnvironment, 0, len(targetEnvironments))
	for _, environment := range targetEnvironments {
		normalizedEnvironment := normalizeEnvironment(environment)
		if normalizedEnvironment == "" {
			continue
		}
		runs := buildJobHistoryRunRows(
			normalizedEnvironment,
			factsByEnvironment[normalizedEnvironment],
			clusterBySignature,
			phase3IssueByAnchor,
		)
		environments = append(environments, JobHistoryDayEnvironment{
			Environment: normalizedEnvironment,
			Summary:     buildJobHistoryDaySummary(runs),
			Runs:        runs,
		})
	}

	return JobHistoryDayData{
		Meta: JobHistoryDayMeta{
			Date:         scope.Date,
			ResolvedWeek: scope.ResolvedWeek,
			Timezone:     "UTC",
			GeneratedAt:  generatedAt.UTC().Format(time.RFC3339),
			Environments: append([]string(nil), targetEnvironments...),
		},
		Environments: environments,
	}, nil
}

func resolveJobHistoryDayScope(query JobHistoryDayQuery) (jobHistoryDayScope, error) {
	dateLabel, dateValue, err := normalizeDateLabel(query.Date)
	if err != nil {
		return jobHistoryDayScope{}, fmt.Errorf("invalid date: %w", err)
	}
	resolvedWeek, err := resolveWindowedTriageWeekLabel(dateValue, dateValue, query.Week)
	if err != nil {
		return jobHistoryDayScope{}, err
	}
	return jobHistoryDayScope{
		Date:         dateLabel,
		DateValue:    dateValue,
		ResolvedWeek: resolvedWeek,
	}, nil
}

func buildJobHistoryRunRows(
	environment string,
	facts windowedTriageEnvironmentFacts,
	clusterBySignature map[string]jobHistorySignatureCluster,
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
			clusterBySignature,
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
	clusterBySignature map[string]jobHistorySignatureCluster,
	phase3IssueByAnchor map[string]string,
) []JobHistoryFailureRow {
	rows := make([]JobHistoryFailureRow, 0, len(rawFailures))
	for _, row := range rawFailures {
		signatureID := strings.TrimSpace(row.SignatureID)
		signatureKey := jobHistorySignatureKey(environment, signatureID)
		cluster, matched := clusterBySignature[signatureKey]

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

func buildJobHistoryDaySummary(runs []JobHistoryRunRow) JobHistoryDaySummary {
	summary := JobHistoryDaySummary{
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

func buildJobHistorySignatureIndex(clusters []semanticcontracts.GlobalClusterRecord) map[string]jobHistorySignatureCluster {
	index := map[string]jobHistorySignatureCluster{}
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
		badPRScore, badPRReasons := triagehtml.BadPRScoreAndReasons(triagehtml.SignatureRow{
			Environment:   environment,
			PostGoodCount: cluster.PostGoodCommitCount,
			AlsoSeenIn:    otherEnvironments,
			References:    jobHistoryRunReferences(cluster.References),
		})
		candidate := jobHistorySignatureCluster{
			ClusterID:               strings.TrimSpace(cluster.Phase2ClusterID),
			CanonicalEvidencePhrase: strings.TrimSpace(cluster.CanonicalEvidencePhrase),
			SearchQueryPhrase:       strings.TrimSpace(cluster.SearchQueryPhrase),
			Lane:                    strings.TrimSpace(primaryContributingTest(cluster.ContributingTests).Lane),
			SupportCount:            cluster.SupportCount,
			BadPRScore:              badPRScore,
			BadPRReasons:            append([]string(nil), badPRReasons...),
		}
		for _, signatureID := range cluster.MemberSignatureIDs {
			key := jobHistorySignatureKey(environment, signatureID)
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

func jobHistoryPhraseEnvironments(clusters []semanticcontracts.GlobalClusterRecord) map[string]map[string]struct{} {
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

func jobHistoryRunReferences(rows []semanticcontracts.ReferenceRecord) []triagehtml.RunReference {
	out := make([]triagehtml.RunReference, 0, len(rows))
	for _, row := range rows {
		out = append(out, triagehtml.RunReference{
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

func jobHistoryPrefersClusterCandidate(current jobHistorySignatureCluster, candidate jobHistorySignatureCluster) bool {
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

func jobHistorySignatureKey(environment string, signatureID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedSignatureID := strings.TrimSpace(signatureID)
	if normalizedEnvironment == "" || trimmedSignatureID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedSignatureID
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
