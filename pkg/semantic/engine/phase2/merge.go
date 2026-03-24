package phase2

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

var (
	reCollapseWhitespace   = regexp.MustCompile(`\s+`)
	reProviderPath         = regexp.MustCompile(`/providers/(Microsoft\.[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*)/`)
	reProviderText         = regexp.MustCompile(`(Microsoft\.[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)?)`)
	rePlaceholderToken     = regexp.MustCompile(`<uuid>|<hex>|<url>`)
	reContainsErrorSignals = regexp.MustCompile(`(?i)(error|failed|timeout|not found|forbidden|denied|deadline|conflict)`)
)

var assertionTailPrefixes = []string{
	"to be true",
	"to be false",
	"to equal",
	"to have occurred",
	"to match error",
	"to match",
	"to contain substring",
	"to be nil",
	"to be empty",
	"to be numerically",
	"to have len",
	"to have length",
	"to have key",
	"to consist of",
}

var genericPhase2CanonicalPhrases = map[string]struct{}{
	"interrupted by user":         {},
	"cluster provisioning failed": {},
	"context deadline exceeded":   {},
	"timeout during createhcpclusterfromparam; context deadline exceeded": {},
}

type globalBucket struct {
	key     string
	members []semanticcontracts.TestClusterRecord
}

// Merge builds deterministic global phase2 clusters from phase1 test clusters
// and returns a merged review queue that includes phase2-level review items.
func Merge(
	testClusters []semanticcontracts.TestClusterRecord,
	reviewItems []semanticcontracts.ReviewItemRecord,
) ([]semanticcontracts.GlobalClusterRecord, []semanticcontracts.ReviewItemRecord, error) {
	if len(testClusters) == 0 {
		finalizedReview := finalizeReviewIDs(reviewItems)
		sortReviewItems(finalizedReview)
		return nil, finalizedReview, nil
	}

	buckets := map[string]*globalBucket{}
	for index, cluster := range testClusters {
		phase1ClusterID := strings.TrimSpace(cluster.Phase1ClusterID)
		if phase1ClusterID == "" {
			return nil, nil, fmt.Errorf("test cluster at index %d missing phase1_cluster_id", index)
		}
		normalizedCluster := normalizeTestCluster(cluster)
		key := normalizedCluster.Environment + "|" + phase2Key(normalizedCluster)
		bucket, exists := buckets[key]
		if !exists {
			bucket = &globalBucket{key: key, members: []semanticcontracts.TestClusterRecord{}}
			buckets[key] = bucket
		}
		bucket.members = append(bucket.members, normalizedCluster)
	}

	globalClusters := make([]semanticcontracts.GlobalClusterRecord, 0, len(buckets))
	for _, bucket := range buckets {
		cluster, err := compileGlobalCluster(bucket.members)
		if err != nil {
			return nil, nil, err
		}
		globalClusters = append(globalClusters, cluster)
	}
	sortGlobalClusters(globalClusters)

	mergedReview := append([]semanticcontracts.ReviewItemRecord(nil), reviewItems...)
	mergedReview = append(mergedReview, buildPhase2AmbiguousProviderReviewItems(testClusters)...)
	mergedReview = finalizeReviewIDs(mergedReview)
	sortReviewItems(mergedReview)
	return globalClusters, mergedReview, nil
}

func compileGlobalCluster(members []semanticcontracts.TestClusterRecord) (semanticcontracts.GlobalClusterRecord, error) {
	if len(members) == 0 {
		return semanticcontracts.GlobalClusterRecord{}, fmt.Errorf("cannot compile global cluster from empty members")
	}

	sort.Slice(members, func(i, j int) bool {
		if members[i].Lane != members[j].Lane {
			return members[i].Lane < members[j].Lane
		}
		if members[i].JobName != members[j].JobName {
			return members[i].JobName < members[j].JobName
		}
		if members[i].TestName != members[j].TestName {
			return members[i].TestName < members[j].TestName
		}
		return members[i].Phase1ClusterID < members[j].Phase1ClusterID
	})

	memberPhase1IDs := make([]string, 0, len(members))
	memberSignaturesSet := map[string]struct{}{}
	contributingTests := map[string]semanticcontracts.ContributingTestRecord{}
	referencesByKey := map[string]semanticcontracts.ReferenceRecord{}
	environment := strings.TrimSpace(members[0].Environment)
	if environment == "" {
		environment = "unknown"
	}

	supportCount := 0
	postGoodCommitCount := 0

	for _, member := range members {
		memberPhase1IDs = append(memberPhase1IDs, member.Phase1ClusterID)
		for _, sigID := range member.MemberSignatureIDs {
			trimmed := strings.TrimSpace(sigID)
			if trimmed == "" {
				continue
			}
			memberSignaturesSet[trimmed] = struct{}{}
		}

		testKey := strings.TrimSpace(member.Lane) + "|" + strings.TrimSpace(member.JobName) + "|" + strings.TrimSpace(member.TestName)
		test := contributingTests[testKey]
		test.Lane = strings.TrimSpace(member.Lane)
		test.JobName = strings.TrimSpace(member.JobName)
		test.TestName = strings.TrimSpace(member.TestName)
		test.SupportCount += member.SupportCount
		contributingTests[testKey] = test

		supportCount += member.SupportCount
		postGoodCommitCount += member.PostGoodCommitCount

		for _, ref := range member.References {
			normalizedRef := normalizeReference(ref)
			key := referenceKey(normalizedRef)
			if key == "" {
				continue
			}
			referencesByKey[key] = normalizedRef
		}
	}

	sort.Strings(memberPhase1IDs)
	memberSignatures := sortedKeys(memberSignaturesSet)
	phase2ClusterID := fingerprint(strings.Join(memberPhase1IDs, ","))

	contributingList := make([]semanticcontracts.ContributingTestRecord, 0, len(contributingTests))
	for _, row := range contributingTests {
		contributingList = append(contributingList, row)
	}
	sort.Slice(contributingList, func(i, j int) bool {
		if contributingList[i].Lane != contributingList[j].Lane {
			return contributingList[i].Lane < contributingList[j].Lane
		}
		if contributingList[i].JobName != contributingList[j].JobName {
			return contributingList[i].JobName < contributingList[j].JobName
		}
		return contributingList[i].TestName < contributingList[j].TestName
	})

	references := make([]semanticcontracts.ReferenceRecord, 0, len(referencesByKey))
	for _, row := range referencesByKey {
		references = append(references, row)
	}
	sortReferences(references)

	representative := representativeCluster(members)
	searchQueryPhrase := strings.TrimSpace(representative.SearchQueryPhrase)
	searchSourceRunURL := strings.TrimSpace(representative.SearchQuerySourceRunURL)
	searchSourceSignatureID := strings.TrimSpace(representative.SearchQuerySourceSignatureID)
	if !hasValidSearchSource(searchQueryPhrase, searchSourceRunURL, searchSourceSignatureID, references) {
		searchQueryPhrase = fallbackSearchPhraseForCluster(representative)
		searchSourceRunURL = ""
		searchSourceSignatureID = ""
		if len(references) > 0 {
			searchSourceRunURL = strings.TrimSpace(references[0].RunURL)
			searchSourceSignatureID = strings.TrimSpace(references[0].SignatureID)
		}
	}

	return semanticcontracts.GlobalClusterRecord{
		SchemaVersion:                semanticcontracts.SchemaVersionV1,
		Environment:                  environment,
		Phase2ClusterID:              phase2ClusterID,
		CanonicalEvidencePhrase:      strings.TrimSpace(representative.CanonicalEvidencePhrase),
		SearchQueryPhrase:            searchQueryPhrase,
		SearchQuerySourceRunURL:      searchSourceRunURL,
		SearchQuerySourceSignatureID: searchSourceSignatureID,
		SupportCount:                 supportCount,
		SeenPostGoodCommit:           postGoodCommitCount > 0,
		PostGoodCommitCount:          postGoodCommitCount,
		ContributingTestsCount:       len(contributingList),
		ContributingTests:            contributingList,
		MemberPhase1ClusterIDs:       memberPhase1IDs,
		MemberSignatureIDs:           memberSignatures,
		References:                   references,
	}, nil
}

func representativeCluster(members []semanticcontracts.TestClusterRecord) semanticcontracts.TestClusterRecord {
	sorted := append([]semanticcontracts.TestClusterRecord(nil), members...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].SupportCount != sorted[j].SupportCount {
			return sorted[i].SupportCount > sorted[j].SupportCount
		}
		return sorted[i].Phase1ClusterID < sorted[j].Phase1ClusterID
	})
	return sorted[0]
}

func hasValidSearchSource(phrase, runURL, signatureID string, references []semanticcontracts.ReferenceRecord) bool {
	trimmedPhrase := strings.TrimSpace(phrase)
	if trimmedPhrase == "" {
		return false
	}
	if strings.Contains(trimmedPhrase, "<") && strings.Contains(trimmedPhrase, ">") {
		return false
	}
	trimmedRunURL := strings.TrimSpace(runURL)
	trimmedSignatureID := strings.TrimSpace(signatureID)
	for _, ref := range references {
		if strings.TrimSpace(ref.RunURL) != trimmedRunURL {
			continue
		}
		if strings.TrimSpace(ref.SignatureID) != trimmedSignatureID {
			continue
		}
		return true
	}
	return false
}

func buildPhase2AmbiguousProviderReviewItems(testClusters []semanticcontracts.TestClusterRecord) []semanticcontracts.ReviewItemRecord {
	baseToMembers := map[string][]semanticcontracts.TestClusterRecord{}
	baseToProviders := map[string]map[string]struct{}{}
	baseCanonicalByKey := map[string]string{}

	for _, cluster := range testClusters {
		normalized := normalizeTestCluster(cluster)
		base := strings.ToLower(collapseWS(normalized.CanonicalEvidencePhrase))
		if base == "" {
			continue
		}
		laneKey := phase2LaneKey(normalized.Lane)
		baseKey := normalized.Environment + "|lane:" + laneKey + "|phrase:" + base
		baseToMembers[baseKey] = append(baseToMembers[baseKey], normalized)
		baseCanonicalByKey[baseKey] = base

		provider := providerAnchorFromCluster(normalized)
		if _, ok := baseToProviders[baseKey]; !ok {
			baseToProviders[baseKey] = map[string]struct{}{}
		}
		if provider == "" {
			provider = "<none>"
		}
		baseToProviders[baseKey][provider] = struct{}{}
	}

	reviewItems := make([]semanticcontracts.ReviewItemRecord, 0)
	for baseKey, providerSet := range baseToProviders {
		if len(providerSet) <= 1 {
			continue
		}
		members := baseToMembers[baseKey]
		if len(members) == 0 {
			continue
		}
		sort.Slice(members, func(i, j int) bool {
			return strings.TrimSpace(members[i].Phase1ClusterID) < strings.TrimSpace(members[j].Phase1ClusterID)
		})
		environment := strings.TrimSpace(members[0].Environment)
		canonical := strings.TrimSpace(baseCanonicalByKey[baseKey])
		if canonical == "" {
			canonical = strings.ToLower(collapseWS(strings.TrimSpace(members[0].CanonicalEvidencePhrase)))
		}

		sourcePhase1IDsSet := map[string]struct{}{}
		memberSignaturesSet := map[string]struct{}{}
		referencesByKey := map[string]semanticcontracts.ReferenceRecord{}
		for _, member := range members {
			if strings.TrimSpace(member.Phase1ClusterID) != "" {
				sourcePhase1IDsSet[strings.TrimSpace(member.Phase1ClusterID)] = struct{}{}
			}
			for _, signatureID := range member.MemberSignatureIDs {
				trimmed := strings.TrimSpace(signatureID)
				if trimmed == "" {
					continue
				}
				memberSignaturesSet[trimmed] = struct{}{}
			}
			for _, ref := range member.References {
				normalizedRef := normalizeReference(ref)
				key := referenceKey(normalizedRef)
				if key == "" {
					continue
				}
				referencesByKey[key] = normalizedRef
			}
		}

		references := make([]semanticcontracts.ReferenceRecord, 0, len(referencesByKey))
		for _, ref := range referencesByKey {
			references = append(references, ref)
		}
		sortReferences(references)

		firstRef := semanticcontracts.ReferenceRecord{}
		if len(references) > 0 {
			firstRef = references[0]
		}
		reviewItems = append(reviewItems, semanticcontracts.ReviewItemRecord{
			SchemaVersion:                        semanticcontracts.SchemaVersionV1,
			Environment:                          environment,
			Phase:                                "phase2",
			Reason:                               "ambiguous_provider_merge",
			ProposedCanonicalEvidencePhrase:      truncate(canonical, 240),
			ProposedSearchQueryPhrase:            phase2ReviewSearchPhrase(members, canonical),
			ProposedSearchQuerySourceRunURL:      strings.TrimSpace(firstRef.RunURL),
			ProposedSearchQuerySourceSignatureID: strings.TrimSpace(firstRef.SignatureID),
			SourcePhase1ClusterIDs:               sortedKeys(sourcePhase1IDsSet),
			MemberSignatureIDs:                   sortedKeys(memberSignaturesSet),
			References:                           references,
		})
	}

	return reviewItems
}

func phase2Key(cluster semanticcontracts.TestClusterRecord) string {
	canonical := strings.ToLower(collapseWS(cluster.CanonicalEvidencePhrase))
	canonical = rePlaceholderToken.ReplaceAllString(canonical, "")
	canonical = collapseWS(canonical)
	laneKey := phase2LaneKey(cluster.Lane)

	if !isGenericCanonical(canonical) {
		return "lane:" + laneKey + "|phrase:" + canonical
	}

	anchor := ""
	anchor = providerAnchorFromCluster(cluster)
	if anchor == "" {
		anchor = "<none>"
	}
	return "lane:" + laneKey + "|phrase:" + canonical + "|provider:" + anchor
}

func phase2LaneKey(lane string) string {
	return strings.ToLower(defaultKeyPart(strings.TrimSpace(lane), "unknown"))
}

func fallbackSearchPhraseForCluster(cluster semanticcontracts.TestClusterRecord) string {
	if phrase := bestSearchPhraseFromText(cluster.CanonicalEvidencePhrase); phrase != "" {
		return phrase
	}
	if phrase := bestSearchPhraseFromText(cluster.SearchQueryPhrase); phrase != "" {
		return phrase
	}
	return "failure"
}

func phase2ReviewSearchPhrase(members []semanticcontracts.TestClusterRecord, fallbackCanonical string) string {
	for _, member := range members {
		if phrase := bestSearchPhraseFromText(member.SearchQueryPhrase); phrase != "" {
			return phrase
		}
	}
	if phrase := bestSearchPhraseFromText(fallbackCanonical); phrase != "" {
		return phrase
	}
	return "failure"
}

func bestSearchPhraseFromText(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	derived := strings.TrimSpace(safeSearchFromText(trimmed))
	if derived == "" || derived == "failure" {
		return truncate(trimmed, 220)
	}
	return derived
}

func providerAnchorFromCluster(cluster semanticcontracts.TestClusterRecord) string {
	if provider := providerAnchor(strings.TrimSpace(cluster.SearchQueryPhrase)); provider != "" {
		return provider
	}
	if provider := providerAnchor(strings.TrimSpace(cluster.CanonicalEvidencePhrase)); provider != "" {
		return provider
	}
	return ""
}

func isGenericCanonical(canonical string) bool {
	if strings.Contains(canonical, "error code:") {
		return true
	}
	_, ok := genericPhase2CanonicalPhrases[canonical]
	return ok
}

func providerAnchor(text string) string {
	pathMatches := reProviderPath.FindAllStringSubmatch(text, -1)
	for i := len(pathMatches) - 1; i >= 0; i-- {
		if len(pathMatches[i]) < 2 {
			continue
		}
		candidate := strings.TrimSpace(pathMatches[i][1])
		if candidate == "" || isIgnoredProvider(candidate) {
			continue
		}
		return candidate
	}

	textMatches := reProviderText.FindAllStringSubmatch(text, -1)
	for i := len(textMatches) - 1; i >= 0; i-- {
		if len(textMatches[i]) < 2 {
			continue
		}
		candidate := strings.TrimSpace(textMatches[i][1])
		if candidate == "" || isIgnoredProvider(candidate) {
			continue
		}
		return candidate
	}
	return ""
}

func isIgnoredProvider(value string) bool {
	switch value {
	case "Microsoft.Resources", "Microsoft.RedHatOpenShift", "Microsoft.Azure.ARO":
		return true
	default:
		return strings.HasPrefix(value, "Microsoft.Azure.ARO.HCP")
	}
}

func collapseWS(value string) string {
	return reCollapseWhitespace.ReplaceAllString(strings.TrimSpace(value), " ")
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}

func sortReferences(rows []semanticcontracts.ReferenceRecord) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].OccurredAt != rows[j].OccurredAt {
			return rows[i].OccurredAt < rows[j].OccurredAt
		}
		if rows[i].RunURL != rows[j].RunURL {
			return rows[i].RunURL < rows[j].RunURL
		}
		if rows[i].RowID != rows[j].RowID {
			return rows[i].RowID < rows[j].RowID
		}
		return rows[i].SignatureID < rows[j].SignatureID
	})
}

func sortGlobalClusters(rows []semanticcontracts.GlobalClusterRecord) {
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

func sortReviewItems(rows []semanticcontracts.ReviewItemRecord) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Environment != rows[j].Environment {
			return rows[i].Environment < rows[j].Environment
		}
		if rows[i].Phase != rows[j].Phase {
			return rows[i].Phase < rows[j].Phase
		}
		if rows[i].Reason != rows[j].Reason {
			return rows[i].Reason < rows[j].Reason
		}
		return rows[i].ReviewItemID < rows[j].ReviewItemID
	})
}

func normalizeTestCluster(row semanticcontracts.TestClusterRecord) semanticcontracts.TestClusterRecord {
	memberSignatures := map[string]struct{}{}
	for _, sigID := range row.MemberSignatureIDs {
		trimmed := strings.TrimSpace(sigID)
		if trimmed == "" {
			continue
		}
		memberSignatures[trimmed] = struct{}{}
	}
	memberSignatureIDs := sortedKeys(memberSignatures)

	referencesByKey := map[string]semanticcontracts.ReferenceRecord{}
	for _, ref := range row.References {
		normalized := normalizeReference(ref)
		key := referenceKey(normalized)
		if key == "" {
			continue
		}
		referencesByKey[key] = normalized
	}
	references := make([]semanticcontracts.ReferenceRecord, 0, len(referencesByKey))
	for _, ref := range referencesByKey {
		references = append(references, ref)
	}
	sortReferences(references)

	return semanticcontracts.TestClusterRecord{
		SchemaVersion:                semanticcontracts.SchemaVersionV1,
		Environment:                  defaultKeyPart(strings.TrimSpace(row.Environment), "unknown"),
		Phase1ClusterID:              strings.TrimSpace(row.Phase1ClusterID),
		Lane:                         strings.TrimSpace(row.Lane),
		JobName:                      strings.TrimSpace(row.JobName),
		TestName:                     strings.TrimSpace(row.TestName),
		TestSuite:                    strings.TrimSpace(row.TestSuite),
		CanonicalEvidencePhrase:      strings.TrimSpace(row.CanonicalEvidencePhrase),
		SearchQueryPhrase:            strings.TrimSpace(row.SearchQueryPhrase),
		SearchQuerySourceRunURL:      strings.TrimSpace(row.SearchQuerySourceRunURL),
		SearchQuerySourceSignatureID: strings.TrimSpace(row.SearchQuerySourceSignatureID),
		SupportCount:                 max(0, row.SupportCount),
		SeenPostGoodCommit:           row.SeenPostGoodCommit || row.PostGoodCommitCount > 0,
		PostGoodCommitCount:          max(0, row.PostGoodCommitCount),
		MemberSignatureIDs:           memberSignatureIDs,
		References:                   references,
	}
}

func normalizeReviewItem(row semanticcontracts.ReviewItemRecord) semanticcontracts.ReviewItemRecord {
	sourcePhase1Set := map[string]struct{}{}
	for _, id := range row.SourcePhase1ClusterIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		sourcePhase1Set[trimmed] = struct{}{}
	}
	memberSigSet := map[string]struct{}{}
	for _, id := range row.MemberSignatureIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		memberSigSet[trimmed] = struct{}{}
	}
	referencesByKey := map[string]semanticcontracts.ReferenceRecord{}
	for _, ref := range row.References {
		normalized := normalizeReference(ref)
		key := referenceKey(normalized)
		if key == "" {
			continue
		}
		referencesByKey[key] = normalized
	}
	references := make([]semanticcontracts.ReferenceRecord, 0, len(referencesByKey))
	for _, ref := range referencesByKey {
		references = append(references, ref)
	}
	sortReferences(references)

	return semanticcontracts.ReviewItemRecord{
		SchemaVersion:                        semanticcontracts.SchemaVersionV1,
		Environment:                          defaultKeyPart(strings.TrimSpace(row.Environment), "unknown"),
		ReviewItemID:                         strings.TrimSpace(row.ReviewItemID),
		Phase:                                strings.TrimSpace(row.Phase),
		Reason:                               strings.TrimSpace(row.Reason),
		ProposedCanonicalEvidencePhrase:      strings.TrimSpace(row.ProposedCanonicalEvidencePhrase),
		ProposedSearchQueryPhrase:            strings.TrimSpace(row.ProposedSearchQueryPhrase),
		ProposedSearchQuerySourceRunURL:      strings.TrimSpace(row.ProposedSearchQuerySourceRunURL),
		ProposedSearchQuerySourceSignatureID: strings.TrimSpace(row.ProposedSearchQuerySourceSignatureID),
		SourcePhase1ClusterIDs:               sortedKeys(sourcePhase1Set),
		MemberSignatureIDs:                   sortedKeys(memberSigSet),
		References:                           references,
	}
}

func normalizeReference(row semanticcontracts.ReferenceRecord) semanticcontracts.ReferenceRecord {
	return semanticcontracts.ReferenceRecord{
		RowID:          strings.TrimSpace(row.RowID),
		RunURL:         strings.TrimSpace(row.RunURL),
		OccurredAt:     strings.TrimSpace(row.OccurredAt),
		SignatureID:    strings.TrimSpace(row.SignatureID),
		PRNumber:       row.PRNumber,
		PostGoodCommit: row.PostGoodCommit,
	}
}

func finalizeReviewIDs(rows []semanticcontracts.ReviewItemRecord) []semanticcontracts.ReviewItemRecord {
	out := make([]semanticcontracts.ReviewItemRecord, 0, len(rows))
	for _, row := range rows {
		normalized := normalizeReviewItem(row)
		seed := strings.TrimSpace(normalized.Environment) +
			"|" + strings.TrimSpace(normalized.Phase) +
			"|" + strings.TrimSpace(normalized.Reason) +
			"|" + strings.Join(normalized.SourcePhase1ClusterIDs, ",") +
			"|" + strings.Join(normalized.MemberSignatureIDs, ",")
		normalized.ReviewItemID = fingerprint(seed)
		out = append(out, normalized)
	}

	merged := map[string]semanticcontracts.ReviewItemRecord{}
	for _, row := range out {
		if strings.TrimSpace(row.ReviewItemID) == "" {
			continue
		}
		merged[row.ReviewItemID] = row
	}

	finalized := make([]semanticcontracts.ReviewItemRecord, 0, len(merged))
	for _, row := range merged {
		finalized = append(finalized, row)
	}
	return finalized
}

func referenceKey(row semanticcontracts.ReferenceRecord) string {
	rowID := strings.TrimSpace(row.RowID)
	if rowID != "" {
		return "row|" + rowID
	}
	runURL := strings.TrimSpace(row.RunURL)
	occurredAt := strings.TrimSpace(row.OccurredAt)
	signatureID := strings.TrimSpace(row.SignatureID)
	if runURL == "" || occurredAt == "" || signatureID == "" {
		return ""
	}
	return runURL + "|" + occurredAt + "|" + signatureID
}

func fingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func isAssertionTail(line string) bool {
	normalized := strings.ToLower(collapseWS(line))
	for _, prefix := range assertionTailPrefixes {
		if strings.HasPrefix(normalized, prefix) {
			return true
		}
	}
	return false
}

func safeSearchFromText(text string) string {
	patterns := []string{
		`Deserializaion Error:[^\n]+`,
		`Command Error:[^\n]+`,
		`ERROR CODE:\s*[A-Za-z0-9_]+`,
		`context deadline exceeded`,
		`Interrupted by User`,
		`Cluster provisioning failed`,
		`failed to run ARM step:[^\n]+`,
		`error running Helm release deployment Step, failed to deploy helm release:[^\n]+`,
		`error running Image Mirror Step, failed to execute shell command:[^\n]+`,
		`failed to search for managed resource groups:[^\n]+`,
		`failed to create SRE breakglass session:[^\n]+`,
		`failed to gather logs[^\n]+`,
		`missing expected log sources[^\n]+`,
	}
	for _, rawPattern := range patterns {
		pattern := regexp.MustCompile("(?i)" + rawPattern)
		match := strings.TrimSpace(pattern.FindString(text))
		if match == "" {
			continue
		}
		if strings.Contains(match, "<") && strings.Contains(match, ">") {
			continue
		}
		if strings.Contains(text, match) {
			return truncate(match, 220)
		}
	}

	for _, line := range strings.Split(text, "\n") {
		token := strings.TrimSpace(line)
		if token == "" {
			continue
		}
		if isAssertionTail(token) {
			continue
		}
		if strings.Contains(token, "<") && strings.Contains(token, ">") {
			continue
		}
		if reContainsErrorSignals.MatchString(token) {
			return truncate(token, 220)
		}
	}

	if strings.Contains(strings.ToLower(text), "context deadline exceeded") {
		return "context deadline exceeded"
	}
	return "failure"
}

func truncate(value string, maxLen int) string {
	trimmed := strings.TrimSpace(value)
	if maxLen <= 0 {
		return ""
	}
	runes := []rune(trimmed)
	if len(runes) <= maxLen {
		return trimmed
	}
	return string(runes[:maxLen])
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func defaultKeyPart(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}
