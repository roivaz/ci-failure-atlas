package postgres

import (
	"fmt"
	"sort"
	"strings"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeSemanticEnvironment(value string) string {
	normalized := normalizeEnvironment(value)
	if normalized == "" {
		return "unknown"
	}
	return normalized
}

func normalizeEnvironmentSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	for _, value := range values {
		normalized := normalizeEnvironment(value)
		if normalized == "" {
			continue
		}
		set[normalized] = struct{}{}
	}
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

func normalizeDate(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("date is empty")
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return "", err
	}
	return parsed.UTC().Format("2006-01-02"), nil
}

func dateFromTimestamp(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", false
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts.UTC().Format("2006-01-02"), true
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts.UTC().Format("2006-01-02"), true
	}
	return "", false
}

func normalizeRunRecord(row storecontracts.RunRecord) storecontracts.RunRecord {
	prNumber := row.PRNumber
	if prNumber < 0 {
		prNumber = 0
	}
	prState := strings.ToLower(strings.TrimSpace(row.PRState))
	switch prState {
	case "open", "closed":
	default:
		prState = ""
	}
	return storecontracts.RunRecord{
		Environment:    normalizeEnvironment(row.Environment),
		RunURL:         strings.TrimSpace(row.RunURL),
		JobName:        strings.TrimSpace(row.JobName),
		PRNumber:       prNumber,
		PRState:        prState,
		PRSHA:          strings.TrimSpace(row.PRSHA),
		FinalMergedSHA: strings.TrimSpace(row.FinalMergedSHA),
		MergedPR:       row.MergedPR,
		PostGoodCommit: row.PostGoodCommit,
		Failed:         row.Failed,
		OccurredAt:     strings.TrimSpace(row.OccurredAt),
	}
}

func normalizePullRequestRecord(row storecontracts.PullRequestRecord) storecontracts.PullRequestRecord {
	prNumber := row.PRNumber
	if prNumber < 0 {
		prNumber = 0
	}
	state := strings.ToLower(strings.TrimSpace(row.State))
	switch state {
	case "open", "closed":
	default:
		state = ""
	}
	merged := row.Merged
	if merged {
		state = "closed"
	}
	return storecontracts.PullRequestRecord{
		PRNumber:       prNumber,
		State:          state,
		Merged:         merged,
		HeadSHA:        strings.TrimSpace(row.HeadSHA),
		MergeCommitSHA: strings.TrimSpace(row.MergeCommitSHA),
		MergedAt:       strings.TrimSpace(row.MergedAt),
		ClosedAt:       strings.TrimSpace(row.ClosedAt),
		UpdatedAt:      strings.TrimSpace(row.UpdatedAt),
		LastCheckedAt:  strings.TrimSpace(row.LastCheckedAt),
	}
}

func normalizeArtifactFailureRecord(row storecontracts.ArtifactFailureRecord) storecontracts.ArtifactFailureRecord {
	return storecontracts.ArtifactFailureRecord{
		Environment:   normalizeEnvironment(row.Environment),
		ArtifactRowID: strings.TrimSpace(row.ArtifactRowID),
		RunURL:        strings.TrimSpace(row.RunURL),
		TestName:      strings.TrimSpace(row.TestName),
		TestSuite:     strings.TrimSpace(row.TestSuite),
		SignatureID:   strings.TrimSpace(row.SignatureID),
		FailureText:   strings.TrimSpace(row.FailureText),
	}
}

func normalizeRawFailureRecord(row storecontracts.RawFailureRecord) storecontracts.RawFailureRecord {
	return storecontracts.RawFailureRecord{
		Environment:       normalizeEnvironment(row.Environment),
		RowID:             strings.TrimSpace(row.RowID),
		RunURL:            strings.TrimSpace(row.RunURL),
		NonArtifactBacked: row.NonArtifactBacked,
		TestName:          strings.TrimSpace(row.TestName),
		TestSuite:         strings.TrimSpace(row.TestSuite),
		SignatureID:       strings.TrimSpace(row.SignatureID),
		OccurredAt:        strings.TrimSpace(row.OccurredAt),
		RawText:           strings.TrimSpace(row.RawText),
		NormalizedText:    strings.TrimSpace(row.NormalizedText),
	}
}

func normalizeMetricDailyRecord(row storecontracts.MetricDailyRecord) storecontracts.MetricDailyRecord {
	return storecontracts.MetricDailyRecord{
		Environment: normalizeEnvironment(row.Environment),
		Date:        strings.TrimSpace(row.Date),
		Metric:      strings.TrimSpace(row.Metric),
		Value:       row.Value,
	}
}

func normalizeTestMetadataPeriod(value string) string {
	period := strings.TrimSpace(value)
	if period == "" {
		return "default"
	}
	return period
}

func normalizeTestMetadataDailyRecord(row storecontracts.TestMetadataDailyRecord) storecontracts.TestMetadataDailyRecord {
	return storecontracts.TestMetadataDailyRecord{
		Environment:            normalizeEnvironment(row.Environment),
		Date:                   strings.TrimSpace(row.Date),
		Release:                strings.TrimSpace(row.Release),
		Period:                 normalizeTestMetadataPeriod(row.Period),
		TestName:               strings.TrimSpace(row.TestName),
		TestSuite:              strings.TrimSpace(row.TestSuite),
		CurrentPassPercentage:  row.CurrentPassPercentage,
		CurrentRuns:            row.CurrentRuns,
		PreviousPassPercentage: row.PreviousPassPercentage,
		PreviousRuns:           row.PreviousRuns,
		NetImprovement:         row.NetImprovement,
		IngestedAt:             strings.TrimSpace(row.IngestedAt),
	}
}

func normalizeCheckpointRecord(row storecontracts.CheckpointRecord) storecontracts.CheckpointRecord {
	return storecontracts.CheckpointRecord{
		Name:      strings.TrimSpace(row.Name),
		Value:     strings.TrimSpace(row.Value),
		UpdatedAt: strings.TrimSpace(row.UpdatedAt),
	}
}

func normalizeDeadLetterRecord(row storecontracts.DeadLetterRecord) storecontracts.DeadLetterRecord {
	return storecontracts.DeadLetterRecord{
		Controller: strings.TrimSpace(row.Controller),
		Key:        strings.TrimSpace(row.Key),
		Error:      strings.TrimSpace(row.Error),
		FailedAt:   strings.TrimSpace(row.FailedAt),
	}
}

func normalizeReferenceRecord(row semanticcontracts.ReferenceRecord) semanticcontracts.ReferenceRecord {
	prNumber := row.PRNumber
	if prNumber < 0 {
		prNumber = 0
	}
	return semanticcontracts.ReferenceRecord{
		RowID:          strings.TrimSpace(row.RowID),
		RunURL:         strings.TrimSpace(row.RunURL),
		OccurredAt:     strings.TrimSpace(row.OccurredAt),
		SignatureID:    strings.TrimSpace(row.SignatureID),
		PRNumber:       prNumber,
		PostGoodCommit: row.PostGoodCommit,
	}
}

func normalizeReferenceSlice(rows []semanticcontracts.ReferenceRecord) []semanticcontracts.ReferenceRecord {
	if len(rows) == 0 {
		return nil
	}
	out := make([]semanticcontracts.ReferenceRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizeReferenceRecord(row)
		if normalized.RowID == "" && normalized.RunURL == "" && normalized.SignatureID == "" && normalized.OccurredAt == "" {
			continue
		}
		out = append(out, normalized)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OccurredAt != out[j].OccurredAt {
			return out[i].OccurredAt < out[j].OccurredAt
		}
		if out[i].RunURL != out[j].RunURL {
			return out[i].RunURL < out[j].RunURL
		}
		if out[i].RowID != out[j].RowID {
			return out[i].RowID < out[j].RowID
		}
		if out[i].SignatureID != out[j].SignatureID {
			return out[i].SignatureID < out[j].SignatureID
		}
		if out[i].PRNumber != out[j].PRNumber {
			return out[i].PRNumber < out[j].PRNumber
		}
		if out[i].PostGoodCommit != out[j].PostGoodCommit {
			return !out[i].PostGoodCommit && out[j].PostGoodCommit
		}
		return false
	})
	return out
}

func normalizeStringSlice(values []string) []string {
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
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizeDateSlice(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	set := map[string]struct{}{}
	for _, value := range values {
		normalized, err := normalizeDate(value)
		if err != nil {
			return nil, err
		}
		if normalized == "" {
			continue
		}
		set[normalized] = struct{}{}
	}
	if len(set) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out, nil
}

func normalizeContributingTests(rows []semanticcontracts.ContributingTestRecord) []semanticcontracts.ContributingTestRecord {
	if len(rows) == 0 {
		return nil
	}
	merged := map[string]semanticcontracts.ContributingTestRecord{}
	for _, row := range rows {
		lane := strings.TrimSpace(row.Lane)
		jobName := strings.TrimSpace(row.JobName)
		testName := strings.TrimSpace(row.TestName)
		if lane == "" && jobName == "" && testName == "" {
			continue
		}
		supportCount := row.SupportCount
		if supportCount < 0 {
			supportCount = 0
		}
		key := lane + "|" + jobName + "|" + testName
		existing := merged[key]
		if existing.Lane == "" {
			existing.Lane = lane
		}
		if existing.JobName == "" {
			existing.JobName = jobName
		}
		if existing.TestName == "" {
			existing.TestName = testName
		}
		existing.SupportCount += supportCount
		merged[key] = existing
	}

	out := make([]semanticcontracts.ContributingTestRecord, 0, len(merged))
	for _, row := range merged {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Lane != out[j].Lane {
			return out[i].Lane < out[j].Lane
		}
		if out[i].JobName != out[j].JobName {
			return out[i].JobName < out[j].JobName
		}
		return out[i].TestName < out[j].TestName
	})
	return out
}

func normalizeFailurePatternRecord(row semanticcontracts.FailurePatternRecord) semanticcontracts.FailurePatternRecord {
	supportCount := row.SupportCount
	if supportCount < 0 {
		supportCount = 0
	}
	postGoodCommitCount := row.PostGoodCommitCount
	if postGoodCommitCount < 0 {
		postGoodCommitCount = 0
	}
	contributingTests := normalizeContributingTests(row.ContributingTests)
	return semanticcontracts.FailurePatternRecord{
		SchemaVersion:                semanticcontracts.NormalizeSchemaVersion(row.SchemaVersion),
		Environment:                  normalizeSemanticEnvironment(row.Environment),
		Phase2ClusterID:              strings.TrimSpace(row.Phase2ClusterID),
		CanonicalEvidencePhrase:      strings.TrimSpace(row.CanonicalEvidencePhrase),
		SearchQueryPhrase:            strings.TrimSpace(row.SearchQueryPhrase),
		SearchQuerySourceRunURL:      strings.TrimSpace(row.SearchQuerySourceRunURL),
		SearchQuerySourceSignatureID: strings.TrimSpace(row.SearchQuerySourceSignatureID),
		SupportCount:                 supportCount,
		SeenPostGoodCommit:           row.SeenPostGoodCommit || postGoodCommitCount > 0,
		PostGoodCommitCount:          postGoodCommitCount,
		ContributingTestsCount:       len(contributingTests),
		ContributingTests:            contributingTests,
		MemberPhase1ClusterIDs:       normalizeStringSlice(row.MemberPhase1ClusterIDs),
		MemberSignatureIDs:           normalizeStringSlice(row.MemberSignatureIDs),
		References:                   normalizeReferenceSlice(row.References),
	}
}

func normalizeReviewItemRecord(row semanticcontracts.ReviewItemRecord) semanticcontracts.ReviewItemRecord {
	return semanticcontracts.ReviewItemRecord{
		SchemaVersion:                        semanticcontracts.NormalizeSchemaVersion(row.SchemaVersion),
		Environment:                          normalizeSemanticEnvironment(row.Environment),
		ReviewItemID:                         strings.TrimSpace(row.ReviewItemID),
		Phase:                                strings.TrimSpace(row.Phase),
		Reason:                               strings.TrimSpace(row.Reason),
		ProposedCanonicalEvidencePhrase:      strings.TrimSpace(row.ProposedCanonicalEvidencePhrase),
		ProposedSearchQueryPhrase:            strings.TrimSpace(row.ProposedSearchQueryPhrase),
		ProposedSearchQuerySourceRunURL:      strings.TrimSpace(row.ProposedSearchQuerySourceRunURL),
		ProposedSearchQuerySourceSignatureID: strings.TrimSpace(row.ProposedSearchQuerySourceSignatureID),
		SourcePhase1ClusterIDs:               normalizeStringSlice(row.SourcePhase1ClusterIDs),
		MemberSignatureIDs:                   normalizeStringSlice(row.MemberSignatureIDs),
		References:                           normalizeReferenceSlice(row.References),
	}
}

func normalizePhase3IssueRecord(row semanticcontracts.Phase3IssueRecord) semanticcontracts.Phase3IssueRecord {
	return semanticcontracts.Phase3IssueRecord{
		SchemaVersion: semanticcontracts.NormalizeSchemaVersion(row.SchemaVersion),
		IssueID:       strings.TrimSpace(row.IssueID),
		Title:         strings.TrimSpace(row.Title),
		CreatedAt:     strings.TrimSpace(row.CreatedAt),
		UpdatedAt:     strings.TrimSpace(row.UpdatedAt),
	}
}

func normalizePhase3LinkRecord(row semanticcontracts.Phase3LinkRecord) semanticcontracts.Phase3LinkRecord {
	return semanticcontracts.Phase3LinkRecord{
		SchemaVersion: semanticcontracts.NormalizeSchemaVersion(row.SchemaVersion),
		IssueID:       strings.TrimSpace(row.IssueID),
		Environment:   normalizeSemanticEnvironment(row.Environment),
		RunURL:        strings.TrimSpace(row.RunURL),
		RowID:         strings.TrimSpace(row.RowID),
		UpdatedAt:     strings.TrimSpace(row.UpdatedAt),
	}
}

func normalizePhase3EventRecord(row semanticcontracts.Phase3EventRecord) semanticcontracts.Phase3EventRecord {
	return semanticcontracts.Phase3EventRecord{
		SchemaVersion: semanticcontracts.NormalizeSchemaVersion(row.SchemaVersion),
		EventID:       strings.TrimSpace(row.EventID),
		Action:        strings.TrimSpace(row.Action),
		IssueID:       strings.TrimSpace(row.IssueID),
		Environment:   normalizeSemanticEnvironment(row.Environment),
		RunURL:        strings.TrimSpace(row.RunURL),
		RowID:         strings.TrimSpace(row.RowID),
		At:            strings.TrimSpace(row.At),
	}
}

func runRecordKey(row storecontracts.RunRecord) string {
	if row.Environment == "" || row.RunURL == "" {
		return ""
	}
	return row.Environment + "|" + row.RunURL
}

func artifactFailureKey(row storecontracts.ArtifactFailureRecord) string {
	if row.Environment == "" || row.ArtifactRowID == "" {
		return ""
	}
	return row.Environment + "|" + row.ArtifactRowID
}

func rawFailureKey(row storecontracts.RawFailureRecord) string {
	if row.Environment == "" || row.RowID == "" {
		return ""
	}
	return row.Environment + "|" + row.RowID
}

func metricDailyKey(row storecontracts.MetricDailyRecord) string {
	if row.Environment == "" || row.Date == "" || row.Metric == "" {
		return ""
	}
	return row.Environment + "|" + row.Date + "|" + row.Metric
}

func testMetadataDailyKey(row storecontracts.TestMetadataDailyRecord) string {
	if row.Environment == "" || row.Date == "" || row.Period == "" || row.TestName == "" {
		return ""
	}
	return row.Environment + "|" + row.Date + "|" + row.Period + "|" + row.TestSuite + "|" + row.TestName
}

func globalClusterKey(row semanticcontracts.FailurePatternRecord) string {
	environment := normalizeSemanticEnvironment(row.Environment)
	clusterID := strings.TrimSpace(row.Phase2ClusterID)
	if environment == "" || clusterID == "" {
		return ""
	}
	return environment + "|" + clusterID
}

func reviewItemKey(row semanticcontracts.ReviewItemRecord) string {
	environment := normalizeSemanticEnvironment(row.Environment)
	reviewID := strings.TrimSpace(row.ReviewItemID)
	if environment == "" || reviewID == "" {
		return ""
	}
	return environment + "|" + reviewID
}

func phase3IssueKey(row semanticcontracts.Phase3IssueRecord) string {
	issueID := strings.TrimSpace(row.IssueID)
	if issueID == "" {
		return ""
	}
	return issueID
}

func phase3LinkKey(row semanticcontracts.Phase3LinkRecord) string {
	environment := normalizeSemanticEnvironment(row.Environment)
	runURL := strings.TrimSpace(row.RunURL)
	rowID := strings.TrimSpace(row.RowID)
	if environment == "" || runURL == "" || rowID == "" {
		return ""
	}
	return environment + "|" + runURL + "|" + rowID
}

func phase3EventKey(row semanticcontracts.Phase3EventRecord) string {
	eventID := strings.TrimSpace(row.EventID)
	if eventID == "" {
		return ""
	}
	return eventID
}
