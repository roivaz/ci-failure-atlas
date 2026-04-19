package phase1

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

var rePlaceholderToken = regexp.MustCompile(`<[^>]+>`)

type clusterAccumulator struct {
	Environment         string
	GroupKey            string
	LocalClusterKey     string
	Lane                string
	JobName             string
	TestName            string
	TestSuite           string
	Rows                []semanticcontracts.Phase1WorksetRecord
	MemberSignatures    map[string]struct{}
	CanonicalCandidates map[string]int
	SearchCandidates    map[string]int
	ReasonSet           map[string]struct{}
}

func Compile(
	workset []semanticcontracts.Phase1WorksetRecord,
	assignments []semanticcontracts.Phase1AssignmentRecord,
) ([]semanticcontracts.TestClusterRecord, []semanticcontracts.ReviewItemRecord, error) {
	worksetByRowID := make(map[string]semanticcontracts.Phase1WorksetRecord, len(workset))
	worksetRowIDs := make([]string, 0, len(workset))
	for _, row := range workset {
		rowID := strings.TrimSpace(row.RowID)
		if rowID == "" {
			return nil, nil, errors.New("workset contains empty row_id")
		}
		if _, exists := worksetByRowID[rowID]; exists {
			return nil, nil, fmt.Errorf("workset contains duplicate row_id %q", rowID)
		}
		worksetByRowID[rowID] = row
		worksetRowIDs = append(worksetRowIDs, rowID)
	}

	assignmentsByRowID := make(map[string]semanticcontracts.Phase1AssignmentRecord, len(assignments))
	accumulators := map[string]*clusterAccumulator{}
	compileErrors := []string{}

	for idx, assignment := range assignments {
		rowID := strings.TrimSpace(assignment.RowID)
		if rowID == "" {
			compileErrors = append(compileErrors, fmt.Sprintf("assignment[%d] missing row_id", idx))
			continue
		}
		if _, exists := assignmentsByRowID[rowID]; exists {
			compileErrors = append(compileErrors, fmt.Sprintf("duplicate assignment row_id %q", rowID))
			continue
		}

		row, ok := worksetByRowID[rowID]
		if !ok {
			compileErrors = append(compileErrors, fmt.Sprintf("assignment row_id %q not found in workset", rowID))
			continue
		}

		derivedGroupKey := buildGroupKey(row.Environment, row.Lane, row.JobName, row.TestName)
		assignmentGroupKey := strings.TrimSpace(assignment.GroupKey)
		if assignmentGroupKey == "" {
			assignmentGroupKey = derivedGroupKey
		}
		if assignmentGroupKey != derivedGroupKey {
			compileErrors = append(compileErrors, fmt.Sprintf("assignment row_id %q group_key mismatch: got=%q expected=%q", rowID, assignmentGroupKey, derivedGroupKey))
			continue
		}

		localClusterKey := strings.TrimSpace(assignment.Phase1LocalClusterKey)
		if localClusterKey == "" {
			compileErrors = append(compileErrors, fmt.Sprintf("assignment row_id %q missing phase1_local_cluster_key", rowID))
			continue
		}

		assignmentsByRowID[rowID] = assignment
		accKey := assignmentGroupKey + "|" + localClusterKey
		acc, exists := accumulators[accKey]
		if !exists {
			acc = &clusterAccumulator{
				Environment:         defaultKeyPart(row.Environment, "unknown"),
				GroupKey:            assignmentGroupKey,
				LocalClusterKey:     localClusterKey,
				Lane:                defaultKeyPart(row.Lane, "unknown"),
				JobName:             defaultKeyPart(row.JobName, "unknown"),
				TestName:            defaultKeyPart(row.TestName, "unknown"),
				TestSuite:           strings.TrimSpace(row.TestSuite),
				Rows:                []semanticcontracts.Phase1WorksetRecord{},
				MemberSignatures:    map[string]struct{}{},
				CanonicalCandidates: map[string]int{},
				SearchCandidates:    map[string]int{},
				ReasonSet:           map[string]struct{}{},
			}
			accumulators[accKey] = acc
		}

		acc.Rows = append(acc.Rows, row)
		if sig := strings.TrimSpace(row.SignatureID); sig != "" {
			acc.MemberSignatures[sig] = struct{}{}
		}
		if candidate := strings.TrimSpace(assignment.CanonicalEvidencePhraseCandidate); candidate != "" {
			acc.CanonicalCandidates[candidate]++
		}
		if candidate := strings.TrimSpace(assignment.SearchQueryPhraseCandidate); candidate != "" {
			acc.SearchCandidates[candidate]++
		}
		if confidence := strings.ToLower(strings.TrimSpace(assignment.Confidence)); confidence != "" && confidence != "high" {
			acc.ReasonSet["low_confidence_evidence"] = struct{}{}
		}
		for _, reason := range assignment.Reasons {
			normalizedReason := normalizeReason(reason)
			if normalizedReason != "" {
				acc.ReasonSet[normalizedReason] = struct{}{}
			}
		}
	}

	missingAssignments := make([]string, 0)
	for _, rowID := range worksetRowIDs {
		if _, ok := assignmentsByRowID[rowID]; !ok {
			missingAssignments = append(missingAssignments, rowID)
		}
	}
	if len(missingAssignments) > 0 {
		sort.Strings(missingAssignments)
		limit := minInt(len(missingAssignments), 10)
		compileErrors = append(
			compileErrors,
			fmt.Sprintf("workset rows missing assignments: %d (sample: %s)", len(missingAssignments), strings.Join(missingAssignments[:limit], ", ")),
		)
	}
	if len(compileErrors) > 0 {
		return nil, nil, errors.New(strings.Join(compileErrors, "; "))
	}

	clusters := make([]semanticcontracts.TestClusterRecord, 0, len(accumulators))
	reviewItems := make([]semanticcontracts.ReviewItemRecord, 0)
	clusterIDCounts := map[string]int{}

	for _, acc := range accumulators {
		sortRowsForReferences(acc.Rows)
		references := make([]semanticcontracts.ReferenceRecord, 0, len(acc.Rows))
		postGoodCount := 0
		for _, row := range acc.Rows {
			if row.PostGoodCommit {
				postGoodCount++
			}
			references = append(references, semanticcontracts.ReferenceRecord{
				RowID:          strings.TrimSpace(row.RowID),
				RunURL:         strings.TrimSpace(row.RunURL),
				OccurredAt:     strings.TrimSpace(row.OccurredAt),
				SignatureID:    strings.TrimSpace(row.SignatureID),
				PRNumber:       row.PRNumber,
				PostGoodCommit: row.PostGoodCommit,
			})
		}

		memberSignatures := sortedKeys(acc.MemberSignatures)
		clusterIDBase := fingerprint(acc.Environment + "|" + acc.GroupKey + "|" + acc.LocalClusterKey)
		clusterID := clusterIDBase
		clusterIDCounts[clusterIDBase]++
		if clusterIDCounts[clusterIDBase] > 1 {
			clusterID = clusterIDBase + "-" + fingerprint(acc.LocalClusterKey)[:8]
			acc.ReasonSet["phase1_cluster_id_collision"] = struct{}{}
		}

		fallbackCanonical := compactTextForPhrase(joinCandidateFallback(acc.Rows), 220)
		canonicalPhrase, canonicalHadFallback, canonicalHadConflict := pickPrimaryPhrase(acc.CanonicalCandidates, fallbackCanonical)
		canonicalPhrase = refineCanonicalPhrase(canonicalPhrase, acc.Rows)
		if canonicalHadFallback {
			acc.ReasonSet["missing_canonical_candidate"] = struct{}{}
		}
		if canonicalHadConflict {
			acc.ReasonSet["inconsistent_canonical_candidate"] = struct{}{}
		}

		searchFallback := buildFallbackSearchPhrase(acc.Rows)
		searchPhrase, searchHadFallback, searchHadConflict := pickPrimaryPhrase(acc.SearchCandidates, searchFallback)
		if searchHadConflict {
			acc.ReasonSet["inconsistent_search_candidate"] = struct{}{}
		}
		if searchHadFallback {
			acc.ReasonSet["missing_search_candidate"] = struct{}{}
		}

		searchPhrase, sourceRunURL, sourceSignatureID, sourceFound := resolveSearchPhrase(references, searchPhrase, canonicalPhrase, acc.Rows)
		if !sourceFound {
			sourceRunURL, sourceSignatureID, searchPhrase, sourceFound = fallbackSearchSource(acc.Rows)
			acc.ReasonSet["search_query_source_not_found"] = struct{}{}
		}
		if rePlaceholderToken.MatchString(searchPhrase) {
			acc.ReasonSet["placeholder_in_search_query"] = struct{}{}
		}

		cluster := semanticcontracts.TestClusterRecord{
			SchemaVersion:                semanticcontracts.CurrentSchemaVersion,
			Environment:                  acc.Environment,
			Phase1ClusterID:              clusterID,
			Lane:                         acc.Lane,
			JobName:                      acc.JobName,
			TestName:                     acc.TestName,
			TestSuite:                    acc.TestSuite,
			CanonicalEvidencePhrase:      canonicalPhrase,
			SearchQueryPhrase:            searchPhrase,
			SearchQuerySourceRunURL:      sourceRunURL,
			SearchQuerySourceSignatureID: sourceSignatureID,
			SupportCount:                 len(acc.Rows),
			SeenPostGoodCommit:           postGoodCount > 0,
			PostGoodCommitCount:          postGoodCount,
			MemberSignatureIDs:           memberSignatures,
			References:                   references,
		}

		if hasAmbiguousProviderMergeFromRows(acc.Rows, cluster.CanonicalEvidencePhrase, cluster.SearchQueryPhrase) {
			acc.ReasonSet["ambiguous_provider_merge"] = struct{}{}
		}
		clusters = append(clusters, cluster)

		for _, reason := range sortedKeys(acc.ReasonSet) {
			reviewItems = append(reviewItems, buildReviewItem(cluster, reason))
		}
	}

	sort.Slice(clusters, func(i, j int) bool {
		return testClusterSortLess(clusters[i], clusters[j])
	})
	sort.Slice(reviewItems, func(i, j int) bool {
		if reviewItems[i].Phase != reviewItems[j].Phase {
			return reviewItems[i].Phase < reviewItems[j].Phase
		}
		if reviewItems[i].Reason != reviewItems[j].Reason {
			return reviewItems[i].Reason < reviewItems[j].Reason
		}
		return reviewItems[i].ReviewItemID < reviewItems[j].ReviewItemID
	})

	return clusters, reviewItems, nil
}

func buildReviewItem(cluster semanticcontracts.TestClusterRecord, reason string) semanticcontracts.ReviewItemRecord {
	sourceIDs := []string{cluster.Phase1ClusterID}
	memberSignatures := append([]string(nil), cluster.MemberSignatureIDs...)
	referenceKeys := sortedReferenceKeys(cluster.References)
	reviewID := fingerprint(strings.TrimSpace(cluster.Environment) + "|phase1|" + reason + "|" + strings.Join(sourceIDs, ",") + "|" + strings.Join(referenceKeys, ","))
	return semanticcontracts.ReviewItemRecord{
		SchemaVersion:                        semanticcontracts.CurrentSchemaVersion,
		Environment:                          strings.TrimSpace(cluster.Environment),
		ReviewItemID:                         reviewID,
		Phase:                                "phase1",
		Reason:                               reason,
		ProposedCanonicalEvidencePhrase:      cluster.CanonicalEvidencePhrase,
		ProposedSearchQueryPhrase:            cluster.SearchQueryPhrase,
		ProposedSearchQuerySourceRunURL:      cluster.SearchQuerySourceRunURL,
		ProposedSearchQuerySourceSignatureID: cluster.SearchQuerySourceSignatureID,
		SourcePhase1ClusterIDs:               sourceIDs,
		MemberSignatureIDs:                   memberSignatures,
		References:                           append([]semanticcontracts.ReferenceRecord(nil), cluster.References...),
	}
}

func sortedReferenceKeys(rows []semanticcontracts.ReferenceRecord) []string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		key := referenceIdentityKey(row)
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func referenceIdentityKey(row semanticcontracts.ReferenceRecord) string {
	rowID := strings.TrimSpace(row.RowID)
	if rowID != "" {
		return "row|" + rowID
	}
	runURL := strings.TrimSpace(row.RunURL)
	occurredAt := strings.TrimSpace(row.OccurredAt)
	signatureID := strings.TrimSpace(row.SignatureID)
	if runURL == "" && occurredAt == "" && signatureID == "" {
		return ""
	}
	return "ref|" + runURL + "|" + occurredAt + "|" + signatureID
}

func sortRowsForReferences(rows []semanticcontracts.Phase1WorksetRecord) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].OccurredAt != rows[j].OccurredAt {
			return rows[i].OccurredAt < rows[j].OccurredAt
		}
		if rows[i].RunURL != rows[j].RunURL {
			return rows[i].RunURL < rows[j].RunURL
		}
		if rows[i].SignatureID != rows[j].SignatureID {
			return rows[i].SignatureID < rows[j].SignatureID
		}
		return rows[i].RowID < rows[j].RowID
	})
}

func resolveSearchPhrase(
	references []semanticcontracts.ReferenceRecord,
	preferred string,
	canonical string,
	rows []semanticcontracts.Phase1WorksetRecord,
) (string, string, string, bool) {
	seen := map[string]struct{}{}
	candidates := []string{}
	appendCandidate := func(value string) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return
		}
		if _, exists := seen[trimmed]; exists {
			return
		}
		seen[trimmed] = struct{}{}
		candidates = append(candidates, trimmed)
	}
	appendCandidate(preferred)
	appendCandidate(canonical)
	appendCandidate(buildFallbackSearchPhrase(rows))

	for _, candidate := range candidates {
		runURL, signatureID, excerpt, found := findSearchSourceWithExcerpt(rows, candidate)
		if !found {
			continue
		}
		refined := deriveConciseLiteralSearchPhrase(excerpt, candidate)
		if refined != "" {
			if refinedRunURL, refinedSignatureID, refinedFound := findSearchSource(rows, refined); refinedFound {
				return refined, refinedRunURL, refinedSignatureID, true
			}
		}
		return candidate, runURL, signatureID, true
	}

	for _, row := range rows {
		text := sourceTextForSearch(row)
		derived := deriveConciseLiteralSearchPhrase(text, "")
		if derived == "" || !strings.Contains(text, derived) {
			continue
		}
		runURL := strings.TrimSpace(row.RunURL)
		signatureID := strings.TrimSpace(row.SignatureID)
		if runURL == "" || signatureID == "" {
			continue
		}
		return derived, runURL, signatureID, true
	}
	return "", "", "", false
}

func findSearchSource(rows []semanticcontracts.Phase1WorksetRecord, phrase string) (string, string, bool) {
	target := strings.TrimSpace(phrase)
	if target == "" {
		return "", "", false
	}
	for _, row := range rows {
		text := sourceTextForSearch(row)
		if !strings.Contains(text, target) {
			continue
		}
		runURL := strings.TrimSpace(row.RunURL)
		signatureID := strings.TrimSpace(row.SignatureID)
		if runURL == "" || signatureID == "" {
			continue
		}
		return runURL, signatureID, true
	}
	return "", "", false
}

func findSearchSourceWithExcerpt(rows []semanticcontracts.Phase1WorksetRecord, phrase string) (string, string, string, bool) {
	target := strings.TrimSpace(phrase)
	if target == "" {
		return "", "", "", false
	}
	for _, row := range rows {
		text := sourceTextForSearch(row)
		if !strings.Contains(text, target) {
			continue
		}
		runURL := strings.TrimSpace(row.RunURL)
		signatureID := strings.TrimSpace(row.SignatureID)
		if runURL == "" || signatureID == "" {
			continue
		}
		return runURL, signatureID, text, true
	}
	return "", "", "", false
}

func fallbackSearchSource(rows []semanticcontracts.Phase1WorksetRecord) (string, string, string, bool) {
	if len(rows) == 0 {
		return "", "", "", false
	}
	for _, row := range rows {
		runURL := strings.TrimSpace(row.RunURL)
		signatureID := strings.TrimSpace(row.SignatureID)
		if runURL == "" || signatureID == "" {
			continue
		}
		text := sourceTextForSearch(row)
		phrase := deriveConciseLiteralSearchPhrase(text, "")
		if phrase == "" || !strings.Contains(text, phrase) {
			continue
		}
		return runURL, signatureID, phrase, true
	}
	return "", "", "", false
}

func sourceTextForSearch(row semanticcontracts.Phase1WorksetRecord) string {
	raw := strings.TrimSpace(row.RawText)
	if raw != "" {
		return raw
	}
	return strings.TrimSpace(row.NormalizedText)
}

func deriveConciseLiteralSearchPhrase(source string, hint string) string {
	raw := strings.TrimSpace(source)
	if raw == "" {
		return ""
	}
	candidate := strings.TrimSpace(hint)
	if candidate != "" && strings.Contains(raw, candidate) && !isNoisySearchPhrase(candidate) {
		return compactLiteralWindow(candidate, 220)
	}
	derived := literalSearchPhraseFromText(raw)
	if derived != "" && strings.Contains(raw, derived) {
		return compactLiteralWindow(derived, 220)
	}
	if candidate != "" && strings.Contains(raw, candidate) {
		return compactLiteralWindow(candidate, 220)
	}
	return ""
}

func literalSearchPhraseFromText(text string) string {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return ""
	}
	token := safeSearchFromText(raw)
	if token == "" {
		return ""
	}
	return compactLiteralWindow(token, 220)
}

func refineCanonicalPhrase(canonical string, rows []semanticcontracts.Phase1WorksetRecord) string {
	current := strings.TrimSpace(canonical)
	if !isWeakCanonicalPhrase(current) {
		return current
	}
	for _, row := range rows {
		raw := strings.TrimSpace(row.RawText)
		if raw == "" {
			continue
		}
		refined := literalSearchPhraseFromText(raw)
		if refined == "" || isNoisySearchPhrase(refined) {
			continue
		}
		return compactTextForPhrase(refined, 220)
	}
	return current
}

func isWeakCanonicalPhrase(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return true
	}
	lower := strings.ToLower(trimmed)
	switch lower {
	case "error", "failed", "deploymentfailed", "internalservererror", "context deadline exceeded", "unexpected error", "operation failed", "cluster provisioning failed", "multipleerrorsoccurred":
		return true
	default:
		return false
	}
}

func isNoisySearchPhrase(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return true
	}
	lower := strings.ToLower(trimmed)
	switch lower {
	case "error", "failed", "deploymentfailed", "internalservererror", "conflict", "badrequest", "multipleerrorsoccurred", "unexpected error", "operation failed", "context deadline exceeded", "cluster provisioning failed":
		return true
	default:
		return len(trimmed) > 220
	}
}

func compactLiteralWindow(text string, maxChars int) string {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return ""
	}
	if maxChars <= 0 || len(raw) <= maxChars {
		return raw
	}
	return strings.TrimSpace(raw[:maxChars])
}

func pickPrimaryPhrase(candidates map[string]int, fallback string) (string, bool, bool) {
	normalized := make(map[string]int, len(candidates))
	for phrase, count := range candidates {
		trimmed := strings.TrimSpace(phrase)
		if trimmed == "" {
			continue
		}
		normalized[trimmed] += count
	}
	if len(normalized) == 0 {
		return strings.TrimSpace(fallback), true, false
	}

	type phraseCount struct {
		phrase string
		count  int
	}
	items := make([]phraseCount, 0, len(normalized))
	for phrase, count := range normalized {
		items = append(items, phraseCount{phrase: phrase, count: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count != items[j].count {
			return items[i].count > items[j].count
		}
		return items[i].phrase < items[j].phrase
	})
	return items[0].phrase, false, len(items) > 1
}

func compactTextForPhrase(value string, max int) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(value, "\n", " "), "\r", " "), "\t", " "))
	normalized = strings.Join(strings.Fields(normalized), " ")
	if max <= 0 {
		return normalized
	}
	if len(normalized) <= max {
		return normalized
	}
	return normalized[:max]
}

func buildFallbackSearchPhrase(rows []semanticcontracts.Phase1WorksetRecord) string {
	return compactTextForPhrase(joinCandidateFallback(rows), 160)
}

func joinCandidateFallback(rows []semanticcontracts.Phase1WorksetRecord) string {
	if len(rows) == 0 {
		return ""
	}
	for _, row := range rows {
		if raw := strings.TrimSpace(row.RawText); raw != "" {
			return raw
		}
	}
	for _, row := range rows {
		if normalized := strings.TrimSpace(row.NormalizedText); normalized != "" {
			return normalized
		}
	}
	return ""
}

func hasAmbiguousProviderMergeFromRows(rows []semanticcontracts.Phase1WorksetRecord, canonical string, search string) bool {
	providers := map[string]struct{}{}
	for _, row := range rows {
		if provider := providerAnchor(sourceTextForSearch(row)); provider != "" {
			providers[provider] = struct{}{}
		}
	}
	if len(providers) <= 1 {
		return false
	}
	return isGenericEvidenceText(canonical, search)
}

func isGenericEvidenceText(canonical string, search string) bool {
	text := strings.ToLower(strings.TrimSpace(canonical + " " + search))
	for _, token := range []string{
		"internalservererror",
		"internal server error",
		"deploymentfailed",
		"badrequest",
		"conflict",
		"operation failed due to an internal server error",
		"multipleerrorsoccurred",
	} {
		if strings.Contains(text, token) {
			return true
		}
	}
	return false
}

func testClusterSortLess(a semanticcontracts.TestClusterRecord, b semanticcontracts.TestClusterRecord) bool {
	aLane := defaultKeyPart(a.Lane, "unknown")
	bLane := defaultKeyPart(b.Lane, "unknown")
	if aLane != bLane {
		return aLane < bLane
	}
	aJob := defaultKeyPart(a.JobName, "unknown")
	bJob := defaultKeyPart(b.JobName, "unknown")
	if aJob != bJob {
		return aJob < bJob
	}
	aTest := defaultKeyPart(a.TestName, "unknown")
	bTest := defaultKeyPart(b.TestName, "unknown")
	if aTest != bTest {
		return aTest < bTest
	}
	if a.SupportCount != b.SupportCount {
		return a.SupportCount > b.SupportCount
	}
	return strings.TrimSpace(a.Phase1ClusterID) < strings.TrimSpace(b.Phase1ClusterID)
}
