package phase3

import (
	"fmt"
	"sort"
	"strings"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

type mergeGroup struct {
	Environment     string
	OutputClusterID string
	Members         []semanticcontracts.FailurePatternRecord
}

// Merge applies manual phase3 links on top of phase2 failure patterns.
// Linked clusters are merged by environment + phase3 cluster ID; unlinked
// clusters are preserved as standalone entries.
func Merge(
	globalClusters []semanticcontracts.FailurePatternRecord,
	phase3Links []semanticcontracts.Phase3LinkRecord,
) ([]semanticcontracts.FailurePatternRecord, error) {
	if len(globalClusters) == 0 {
		return nil, nil
	}

	phase3ClusterByAnchor := map[string]string{}
	for _, row := range phase3Links {
		phase3ClusterID := strings.TrimSpace(row.IssueID)
		if phase3ClusterID == "" {
			continue
		}
		key := phase3AnchorKey(row.Environment, row.RunURL, row.RowID)
		if key == "" {
			continue
		}
		phase3ClusterByAnchor[key] = phase3ClusterID
	}

	groupByKey := map[string]*mergeGroup{}
	for _, cluster := range globalClusters {
		normalized := normalizeGlobalCluster(cluster)
		environment := strings.TrimSpace(normalized.Environment)
		phase2ClusterID := strings.TrimSpace(normalized.Phase2ClusterID)
		if environment == "" || phase2ClusterID == "" {
			return nil, fmt.Errorf("failure-pattern record missing environment and/or phase2_cluster_id")
		}

		phase3ClusterIDs := phase3ClusterIDsForCluster(normalized, phase3ClusterByAnchor)
		if len(phase3ClusterIDs) > 1 {
			return nil, fmt.Errorf(
				"phase3 conflict: semantic cluster %s resolves to multiple phase3 cluster IDs (%s)",
				phase2ClusterID,
				strings.Join(phase3ClusterIDs, ", "),
			)
		}

		groupKey := environment + "|unlinked|" + phase2ClusterID
		outputClusterID := phase2ClusterID
		if len(phase3ClusterIDs) == 1 {
			outputClusterID = phase3ClusterIDs[0]
			groupKey = environment + "|linked|" + outputClusterID
		}

		group := groupByKey[groupKey]
		if group == nil {
			group = &mergeGroup{
				Environment:     environment,
				OutputClusterID: outputClusterID,
				Members:         make([]semanticcontracts.FailurePatternRecord, 0, 1),
			}
			groupByKey[groupKey] = group
		}
		group.Members = append(group.Members, normalized)
	}

	merged := make([]semanticcontracts.FailurePatternRecord, 0, len(groupByKey))
	for _, group := range groupByKey {
		merged = append(merged, compileMergedGroup(*group))
	}
	sortFailurePatterns(merged)
	return merged, nil
}

func compileMergedGroup(group mergeGroup) semanticcontracts.FailurePatternRecord {
	representative := representativeCluster(group.Members)
	memberPhase1Set := map[string]struct{}{}
	memberSignatureSet := map[string]struct{}{}
	contributingTestsByKey := map[string]semanticcontracts.ContributingTestRecord{}
	referencesByKey := map[string]semanticcontracts.ReferenceRecord{}

	supportCount := 0
	postGoodCommitCount := 0
	seenPostGoodCommit := false

	for _, member := range group.Members {
		supportCount += member.SupportCount
		postGoodCommitCount += member.PostGoodCommitCount
		if member.SeenPostGoodCommit || member.PostGoodCommitCount > 0 {
			seenPostGoodCommit = true
		}

		for _, phase1ClusterID := range member.MemberPhase1ClusterIDs {
			trimmed := strings.TrimSpace(phase1ClusterID)
			if trimmed == "" {
				continue
			}
			memberPhase1Set[trimmed] = struct{}{}
		}
		for _, signatureID := range member.MemberSignatureIDs {
			trimmed := strings.TrimSpace(signatureID)
			if trimmed == "" {
				continue
			}
			memberSignatureSet[trimmed] = struct{}{}
		}

		for _, contributing := range member.ContributingTests {
			normalized := semanticcontracts.ContributingTestRecord{
				Lane:         strings.TrimSpace(contributing.Lane),
				JobName:      strings.TrimSpace(contributing.JobName),
				TestName:     strings.TrimSpace(contributing.TestName),
				SupportCount: contributing.SupportCount,
			}
			key := normalized.Lane + "|" + normalized.JobName + "|" + normalized.TestName
			if strings.Trim(strings.ReplaceAll(key, "|", ""), " ") == "" {
				continue
			}
			aggregated := contributingTestsByKey[key]
			aggregated.Lane = normalized.Lane
			aggregated.JobName = normalized.JobName
			aggregated.TestName = normalized.TestName
			aggregated.SupportCount += normalized.SupportCount
			contributingTestsByKey[key] = aggregated
		}

		for _, reference := range member.References {
			normalized := normalizeReference(reference)
			key := referenceKey(normalized)
			if key == "" {
				continue
			}
			referencesByKey[key] = normalized
		}
	}

	memberPhase1ClusterIDs := sortedStringSet(memberPhase1Set)
	memberSignatureIDs := sortedStringSet(memberSignatureSet)

	contributingTests := make([]semanticcontracts.ContributingTestRecord, 0, len(contributingTestsByKey))
	for _, row := range contributingTestsByKey {
		contributingTests = append(contributingTests, row)
	}
	sort.Slice(contributingTests, func(i, j int) bool {
		if contributingTests[i].Lane != contributingTests[j].Lane {
			return contributingTests[i].Lane < contributingTests[j].Lane
		}
		if contributingTests[i].JobName != contributingTests[j].JobName {
			return contributingTests[i].JobName < contributingTests[j].JobName
		}
		return contributingTests[i].TestName < contributingTests[j].TestName
	})

	references := make([]semanticcontracts.ReferenceRecord, 0, len(referencesByKey))
	for _, row := range referencesByKey {
		references = append(references, row)
	}
	sortReferences(references)

	searchQuerySourceRunURL := strings.TrimSpace(representative.SearchQuerySourceRunURL)
	searchQuerySourceSignatureID := strings.TrimSpace(representative.SearchQuerySourceSignatureID)
	if len(references) > 0 && (searchQuerySourceRunURL == "" || searchQuerySourceSignatureID == "") {
		searchQuerySourceRunURL = strings.TrimSpace(references[0].RunURL)
		searchQuerySourceSignatureID = strings.TrimSpace(references[0].SignatureID)
	}

	return semanticcontracts.FailurePatternRecord{
		SchemaVersion:                semanticcontracts.CurrentSchemaVersion,
		Environment:                  group.Environment,
		Phase2ClusterID:              strings.TrimSpace(group.OutputClusterID),
		CanonicalEvidencePhrase:      strings.TrimSpace(representative.CanonicalEvidencePhrase),
		SearchQueryPhrase:            strings.TrimSpace(representative.SearchQueryPhrase),
		SearchQuerySourceRunURL:      searchQuerySourceRunURL,
		SearchQuerySourceSignatureID: searchQuerySourceSignatureID,
		SupportCount:                 supportCount,
		SeenPostGoodCommit:           seenPostGoodCommit,
		PostGoodCommitCount:          postGoodCommitCount,
		ContributingTestsCount:       len(contributingTests),
		ContributingTests:            contributingTests,
		MemberPhase1ClusterIDs:       memberPhase1ClusterIDs,
		MemberSignatureIDs:           memberSignatureIDs,
		References:                   references,
	}
}

func representativeCluster(rows []semanticcontracts.FailurePatternRecord) semanticcontracts.FailurePatternRecord {
	if len(rows) == 0 {
		return semanticcontracts.FailurePatternRecord{}
	}
	sorted := append([]semanticcontracts.FailurePatternRecord(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].SupportCount != sorted[j].SupportCount {
			return sorted[i].SupportCount > sorted[j].SupportCount
		}
		if strings.TrimSpace(sorted[i].CanonicalEvidencePhrase) != strings.TrimSpace(sorted[j].CanonicalEvidencePhrase) {
			return strings.TrimSpace(sorted[i].CanonicalEvidencePhrase) < strings.TrimSpace(sorted[j].CanonicalEvidencePhrase)
		}
		return strings.TrimSpace(sorted[i].Phase2ClusterID) < strings.TrimSpace(sorted[j].Phase2ClusterID)
	})
	return sorted[0]
}

func phase3ClusterIDsForCluster(
	cluster semanticcontracts.FailurePatternRecord,
	phase3ClusterByAnchor map[string]string,
) []string {
	set := map[string]struct{}{}
	environment := normalizeEnvironment(cluster.Environment)
	for _, reference := range cluster.References {
		key := phase3AnchorKey(environment, reference.RunURL, reference.RowID)
		if key == "" {
			continue
		}
		phase3ClusterID := strings.TrimSpace(phase3ClusterByAnchor[key])
		if phase3ClusterID == "" {
			continue
		}
		set[phase3ClusterID] = struct{}{}
	}
	return sortedStringSet(set)
}

func phase3AnchorKey(environment string, runURL string, rowID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedRunURL := strings.TrimSpace(runURL)
	trimmedRowID := strings.TrimSpace(rowID)
	if normalizedEnvironment == "" || trimmedRunURL == "" || trimmedRowID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedRunURL + "|" + trimmedRowID
}

func normalizeGlobalCluster(row semanticcontracts.FailurePatternRecord) semanticcontracts.FailurePatternRecord {
	normalized := row
	normalized.SchemaVersion = semanticcontracts.CurrentSchemaVersion
	normalized.Environment = normalizeEnvironment(row.Environment)
	normalized.Phase2ClusterID = strings.TrimSpace(row.Phase2ClusterID)
	normalized.CanonicalEvidencePhrase = strings.TrimSpace(row.CanonicalEvidencePhrase)
	normalized.SearchQueryPhrase = strings.TrimSpace(row.SearchQueryPhrase)
	normalized.SearchQuerySourceRunURL = strings.TrimSpace(row.SearchQuerySourceRunURL)
	normalized.SearchQuerySourceSignatureID = strings.TrimSpace(row.SearchQuerySourceSignatureID)
	normalized.MemberPhase1ClusterIDs = sortedTrimmedSlice(row.MemberPhase1ClusterIDs)
	normalized.MemberSignatureIDs = sortedTrimmedSlice(row.MemberSignatureIDs)

	normalizedReferences := make([]semanticcontracts.ReferenceRecord, 0, len(row.References))
	for _, reference := range row.References {
		normalizedReferences = append(normalizedReferences, normalizeReference(reference))
	}
	sortReferences(normalizedReferences)
	normalized.References = normalizedReferences
	return normalized
}

func normalizeReference(reference semanticcontracts.ReferenceRecord) semanticcontracts.ReferenceRecord {
	return semanticcontracts.ReferenceRecord{
		RowID:          strings.TrimSpace(reference.RowID),
		RunURL:         strings.TrimSpace(reference.RunURL),
		OccurredAt:     strings.TrimSpace(reference.OccurredAt),
		SignatureID:    strings.TrimSpace(reference.SignatureID),
		PRNumber:       reference.PRNumber,
		PostGoodCommit: reference.PostGoodCommit,
	}
}

func referenceKey(reference semanticcontracts.ReferenceRecord) string {
	rowID := strings.TrimSpace(reference.RowID)
	runURL := strings.TrimSpace(reference.RunURL)
	if rowID != "" {
		return runURL + "|" + rowID
	}
	return strings.TrimSpace(reference.RunURL) + "|" +
		strings.TrimSpace(reference.OccurredAt) + "|" +
		strings.TrimSpace(reference.SignatureID) + "|" +
		fmt.Sprintf("%d", reference.PRNumber) + "|" +
		fmt.Sprintf("%t", reference.PostGoodCommit)
}

func sortReferences(rows []semanticcontracts.ReferenceRecord) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].RunURL != rows[j].RunURL {
			return rows[i].RunURL < rows[j].RunURL
		}
		if rows[i].OccurredAt != rows[j].OccurredAt {
			return rows[i].OccurredAt < rows[j].OccurredAt
		}
		if rows[i].SignatureID != rows[j].SignatureID {
			return rows[i].SignatureID < rows[j].SignatureID
		}
		if rows[i].RowID != rows[j].RowID {
			return rows[i].RowID < rows[j].RowID
		}
		if rows[i].PRNumber != rows[j].PRNumber {
			return rows[i].PRNumber < rows[j].PRNumber
		}
		if rows[i].PostGoodCommit != rows[j].PostGoodCommit {
			return !rows[i].PostGoodCommit && rows[j].PostGoodCommit
		}
		return false
	})
}

func sortedTrimmedSlice(values []string) []string {
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

func sortedStringSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sortFailurePatterns(rows []semanticcontracts.FailurePatternRecord) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].SupportCount != rows[j].SupportCount {
			return rows[i].SupportCount > rows[j].SupportCount
		}
		if rows[i].ContributingTestsCount != rows[j].ContributingTestsCount {
			return rows[i].ContributingTestsCount > rows[j].ContributingTestsCount
		}
		if rows[i].Environment != rows[j].Environment {
			return rows[i].Environment < rows[j].Environment
		}
		return rows[i].Phase2ClusterID < rows[j].Phase2ClusterID
	})
}

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
