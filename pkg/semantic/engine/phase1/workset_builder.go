package phase1

import (
	"sort"
	"strings"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	semanticinput "ci-failure-atlas/pkg/semantic/input"
)

func BuildWorkset(rows []semanticinput.EnrichedFailure) []semanticcontracts.Phase1WorksetRecord {
	workset := make([]semanticcontracts.Phase1WorksetRecord, 0, len(rows))
	for _, row := range rows {
		environment := strings.ToLower(strings.TrimSpace(row.Environment))
		runURL := strings.TrimSpace(row.RunURL)
		rowID := strings.TrimSpace(row.RowID)
		signatureID := strings.TrimSpace(row.SignatureID)
		if environment == "" || runURL == "" || rowID == "" || signatureID == "" {
			continue
		}

		jobName := strings.TrimSpace(row.JobName)
		testName := strings.TrimSpace(row.TestName)
		testSuite := strings.TrimSpace(row.TestSuite)
		lane := strings.TrimSpace(row.Lane)
		rawText := strings.TrimSpace(row.RawText)
		normalizedText := strings.TrimSpace(row.NormalizedText)
		occurredAt := strings.TrimSpace(row.OccurredAt)

		workset = append(workset, semanticcontracts.Phase1WorksetRecord{
			SchemaVersion:  semanticcontracts.CurrentSchemaVersion,
			Environment:    environment,
			RowID:          rowID,
			GroupKey:       buildGroupKey(environment, lane, defaultKeyPart(jobName, "unknown"), defaultKeyPart(testName, "unknown")),
			Lane:           defaultKeyPart(lane, "unknown"),
			JobName:        defaultKeyPart(jobName, "unknown"),
			TestName:       defaultKeyPart(testName, "unknown"),
			TestSuite:      testSuite,
			SignatureID:    signatureID,
			OccurredAt:     occurredAt,
			RunURL:         runURL,
			PRNumber:       row.PRNumber,
			PostGoodCommit: row.PostGoodCommit,
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
