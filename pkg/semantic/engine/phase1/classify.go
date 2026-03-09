package phase1

import (
	"sort"
	"strings"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

type testKey struct {
	environment string
	lane        string
	jobName     string
	test        string
}

func Classify(rows []semanticcontracts.Phase1NormalizedRecord) []semanticcontracts.Phase1AssignmentRecord {
	byTest := map[testKey][]semanticcontracts.Phase1NormalizedRecord{}
	for _, row := range rows {
		key := testKey{
			environment: defaultKeyPart(row.Environment, "unknown"),
			lane:        defaultKeyPart(row.Lane, "unknown"),
			jobName:     defaultKeyPart(row.JobName, "unknown"),
			test:        defaultKeyPart(row.TestName, "unknown"),
		}
		byTest[key] = append(byTest[key], row)
	}

	assignments := make([]semanticcontracts.Phase1AssignmentRecord, 0, len(rows))
	for _, testRows := range byTest {
		sort.Slice(testRows, func(i, j int) bool {
			if testRows[i].OccurredAt != testRows[j].OccurredAt {
				return testRows[i].OccurredAt < testRows[j].OccurredAt
			}
			if testRows[i].RunURL != testRows[j].RunURL {
				return testRows[i].RunURL < testRows[j].RunURL
			}
			return testRows[i].SignatureID < testRows[j].SignatureID
		})

		keyToLocal := map[string]string{}
		localToRows := map[string][]semanticcontracts.Phase1NormalizedRecord{}
		for _, row := range testRows {
			phase1 := normalizePhase1Key(row.Phase1Key)
			if phase1 == "missing-phase1-key" {
				phase1 = normalizePhase1Key(strings.ToLower(collapseWS(row.CanonicalEvidencePhrase)))
			}
			local, ok := keyToLocal[phase1]
			if !ok {
				local = localClusterKeyForPhase1Key(phase1)
				keyToLocal[phase1] = local
			}
			localToRows[local] = append(localToRows[local], row)
		}

		baseToProviders := map[string]map[string]struct{}{}
		baseToLocalKeys := map[string]map[string]struct{}{}
		for localKey, clusterRows := range localToRows {
			for _, row := range clusterRows {
				base := canonicalBase(row)
				provider := row.ProviderAnchor
				if strings.TrimSpace(provider) == "" {
					provider = "<none>"
				}
				if _, ok := baseToProviders[base]; !ok {
					baseToProviders[base] = map[string]struct{}{}
				}
				baseToProviders[base][provider] = struct{}{}
				if _, ok := baseToLocalKeys[base]; !ok {
					baseToLocalKeys[base] = map[string]struct{}{}
				}
				baseToLocalKeys[base][localKey] = struct{}{}
			}
		}

		ambiguousLocals := map[string]struct{}{}
		for base, providers := range baseToProviders {
			if len(providers) > 1 {
				for local := range baseToLocalKeys[base] {
					ambiguousLocals[local] = struct{}{}
				}
			}
		}

		for localKey, clusterRows := range localToRows {
			canonicalCounts := map[string]int{}
			for _, row := range clusterRows {
				phrase := collapseWS(row.CanonicalEvidencePhrase)
				if phrase == "" {
					continue
				}
				canonicalCounts[phrase]++
			}
			canonicalPhrase := "failure"
			if len(canonicalCounts) > 0 {
				canonicalPhrase = mostCommon(canonicalCounts)
			}

			representative := clusterRows[0]
			for _, row := range clusterRows {
				if collapseWS(row.CanonicalEvidencePhrase) == canonicalPhrase {
					representative = row
					break
				}
			}
			searchPhrase := chooseSearchPhrase(representative.RawText, []string{
				representative.SearchQueryPhrase,
				canonicalPhrase,
			})

			reasons := map[string]struct{}{}
			if _, ambiguous := ambiguousLocals[localKey]; ambiguous {
				reasons["ambiguous_provider_merge"] = struct{}{}
			}
			if isWeakCanonical(canonicalPhrase) {
				reasons["insufficient_inner_error"] = struct{}{}
			}

			reasonSlice := make([]string, 0, len(reasons))
			for reason := range reasons {
				normalized := normalizeReason(reason)
				if normalized == "" {
					continue
				}
				reasonSlice = append(reasonSlice, normalized)
			}
			sort.Strings(reasonSlice)

			confidence := "high"
			if len(reasonSlice) > 0 {
				confidence = "low"
			}

			for _, row := range clusterRows {
				groupKey := strings.TrimSpace(row.GroupKey)
				if groupKey == "" {
					groupKey = buildGroupKey(row.Environment, row.Lane, row.JobName, row.TestName)
				}
				assignments = append(assignments, semanticcontracts.Phase1AssignmentRecord{
					SchemaVersion:                    semanticcontracts.SchemaVersionV1,
					Environment:                      strings.TrimSpace(row.Environment),
					RowID:                            row.RowID,
					GroupKey:                         groupKey,
					Phase1LocalClusterKey:            localKey,
					CanonicalEvidencePhraseCandidate: canonicalPhrase,
					SearchQueryPhraseCandidate:       searchPhrase,
					Confidence:                       confidence,
					Reasons:                          reasonSlice,
				})
			}
		}
	}

	sort.Slice(assignments, func(i, j int) bool {
		if assignments[i].GroupKey != assignments[j].GroupKey {
			return assignments[i].GroupKey < assignments[j].GroupKey
		}
		if assignments[i].Phase1LocalClusterKey != assignments[j].Phase1LocalClusterKey {
			return assignments[i].Phase1LocalClusterKey < assignments[j].Phase1LocalClusterKey
		}
		return assignments[i].RowID < assignments[j].RowID
	})
	return assignments
}

func mostCommon(values map[string]int) string {
	type kv struct {
		key   string
		count int
	}
	list := make([]kv, 0, len(values))
	for key, count := range values {
		list = append(list, kv{key: key, count: count})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].count != list[j].count {
			return list[i].count > list[j].count
		}
		return list[i].key < list[j].key
	})
	return list[0].key
}

func normalizePhase1Key(value string) string {
	normalized := collapseWS(value)
	if normalized == "" {
		return "missing-phase1-key"
	}
	return normalized
}

func localClusterKeyForPhase1Key(phase1Key string) string {
	key := strings.TrimSpace(phase1Key)
	if key == "" {
		key = "missing-phase1-key"
	}
	return "k-" + fingerprint(key)[:16]
}

func canonicalBase(row semanticcontracts.Phase1NormalizedRecord) string {
	return strings.ToLower(collapseWS(row.CanonicalEvidencePhrase))
}

func isWeakCanonical(canonical string) bool {
	value := strings.ToLower(collapseWS(canonical))
	switch {
	case value == "interrupted by user",
		value == "failure",
		value == "failure occurred",
		strings.HasPrefix(value, "unexpected error"),
		value == "msg:",
		value == "err:",
		value == "caused by:":
		return true
	default:
		return false
	}
}
