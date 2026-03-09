package phase1

import (
	"sort"
	"strings"

	factsnormalize "ci-failure-atlas/pkg/facts/normalize"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func BuildWorkset(rawFailures []storecontracts.RawFailureRecord, runs []storecontracts.RunRecord) []semanticcontracts.Phase1WorksetRecord {
	runsByKey := map[string]storecontracts.RunRecord{}
	for _, run := range runs {
		normalized := storecontracts.RunRecord{
			Environment:    strings.ToLower(strings.TrimSpace(run.Environment)),
			RunURL:         strings.TrimSpace(run.RunURL),
			JobName:        strings.TrimSpace(run.JobName),
			PRNumber:       run.PRNumber,
			PRState:        strings.TrimSpace(run.PRState),
			PRSHA:          strings.TrimSpace(run.PRSHA),
			FinalMergedSHA: strings.TrimSpace(run.FinalMergedSHA),
			MergedPR:       run.MergedPR,
			PostGoodCommit: run.PostGoodCommit,
			OccurredAt:     strings.TrimSpace(run.OccurredAt),
		}
		if normalized.Environment == "" || normalized.RunURL == "" {
			continue
		}
		runsByKey[normalized.Environment+"|"+normalized.RunURL] = normalized
	}

	workset := make([]semanticcontracts.Phase1WorksetRecord, 0, len(rawFailures))
	for _, row := range rawFailures {
		if row.NonArtifactBacked {
			continue
		}
		environment := strings.ToLower(strings.TrimSpace(row.Environment))
		runURL := strings.TrimSpace(row.RunURL)
		if environment == "" || runURL == "" {
			continue
		}

		run := runsByKey[environment+"|"+runURL]
		jobName := strings.TrimSpace(run.JobName)
		testName := strings.TrimSpace(row.TestName)
		testSuite := strings.TrimSpace(row.TestSuite)
		lane := deriveLane(jobName, testName, testSuite)
		occurredAt := strings.TrimSpace(row.OccurredAt)
		if occurredAt == "" {
			occurredAt = strings.TrimSpace(run.OccurredAt)
		}

		signatureID := strings.TrimSpace(row.SignatureID)
		rawText := strings.TrimSpace(row.RawText)
		normalizedText := strings.TrimSpace(row.NormalizedText)
		if normalizedText == "" && rawText != "" {
			normalizedText = factsnormalize.Text(rawText)
		}
		if signatureID == "" {
			base := normalizedText
			if base == "" {
				base = rawText
			}
			if base == "" {
				continue
			}
			signatureID = fingerprint(base)
		}
		rowID := strings.TrimSpace(row.RowID)
		if rowID == "" {
			rowID = buildRowIDWithEnvironment(environment, runURL, signatureID, occurredAt)
		}

		workset = append(workset, semanticcontracts.Phase1WorksetRecord{
			SchemaVersion:  semanticcontracts.SchemaVersionV1,
			Environment:    environment,
			RowID:          rowID,
			GroupKey:       buildGroupKey(environment, lane, defaultKeyPart(jobName, "unknown"), defaultKeyPart(testName, "unknown")),
			Lane:           defaultKeyPart(lane, "unknown"),
			JobName:        defaultKeyPart(jobName, "unknown"),
			TestName:       defaultKeyPart(testName, "unknown"),
			TestSuite:      strings.TrimSpace(testSuite),
			SignatureID:    signatureID,
			OccurredAt:     occurredAt,
			RunURL:         runURL,
			PRNumber:       run.PRNumber,
			PostGoodCommit: run.PostGoodCommit,
			RawText:        rawText,
			NormalizedText: normalizedText,
		})
	}

	sort.Slice(workset, func(i, j int) bool {
		if workset[i].Lane != workset[j].Lane {
			return workset[i].Lane < workset[j].Lane
		}
		if workset[i].JobName != workset[j].JobName {
			return workset[i].JobName < workset[j].JobName
		}
		if workset[i].TestName != workset[j].TestName {
			return workset[i].TestName < workset[j].TestName
		}
		if workset[i].OccurredAt != workset[j].OccurredAt {
			return workset[i].OccurredAt < workset[j].OccurredAt
		}
		if workset[i].RunURL != workset[j].RunURL {
			return workset[i].RunURL < workset[j].RunURL
		}
		if workset[i].SignatureID != workset[j].SignatureID {
			return workset[i].SignatureID < workset[j].SignatureID
		}
		return workset[i].RowID < workset[j].RowID
	})

	return workset
}

func deriveLane(jobName string, testName string, testSuite string) string {
	normalizedJob := strings.ToLower(strings.TrimSpace(jobName))
	normalizedName := strings.ToLower(strings.TrimSpace(testName))
	normalizedSuite := strings.ToLower(strings.TrimSpace(testSuite))

	switch {
	case strings.Contains(normalizedSuite, "step graph"):
		return "provision"
	case strings.HasPrefix(normalizedName, "run pipeline step "):
		return "provision"
	case strings.Contains(normalizedJob, "provision"):
		return "provision"
	case strings.Contains(normalizedJob, "e2e"):
		return "e2e"
	default:
		return "unknown"
	}
}
