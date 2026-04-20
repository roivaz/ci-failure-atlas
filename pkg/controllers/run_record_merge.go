package controllers

import (
	"strings"

	"ci-failure-atlas/pkg/store/contracts"
)

func mergeRunRecordFromSippy(existing contracts.RunRecord, existingFound bool, candidate contracts.RunRecord) contracts.RunRecord {
	if !existingFound {
		return candidate
	}

	next := candidate
	if strings.TrimSpace(next.JobName) == "" {
		next.JobName = strings.TrimSpace(existing.JobName)
	}
	if next.PRNumber <= 0 && existing.PRNumber > 0 {
		next.PRNumber = existing.PRNumber
	}
	if strings.TrimSpace(next.PRSHA) == "" && existing.PRNumber == next.PRNumber {
		next.PRSHA = strings.TrimSpace(existing.PRSHA)
	}
	if !hasValidOccurredAt(next.OccurredAt) {
		next.OccurredAt = strings.TrimSpace(existing.OccurredAt)
	}

	if existing.PRNumber == next.PRNumber && next.PRNumber > 0 {
		if strings.TrimSpace(next.PRState) == "" {
			next.PRState = strings.TrimSpace(existing.PRState)
		}
		if strings.TrimSpace(next.FinalMergedSHA) == "" {
			next.FinalMergedSHA = strings.TrimSpace(existing.FinalMergedSHA)
		}
		if !next.MergedPR {
			next.MergedPR = existing.MergedPR
		}
		if !next.PostGoodCommit {
			next.PostGoodCommit = existing.PostGoodCommit
		}
	}

	return next
}

func mergeRunRecordFromProw(existing contracts.RunRecord, existingFound bool, candidate contracts.RunRecord) contracts.RunRecord {
	if !existingFound {
		return candidate
	}

	next := existing
	if strings.TrimSpace(next.JobName) == "" {
		next.JobName = strings.TrimSpace(candidate.JobName)
	}
	if next.PRNumber <= 0 && candidate.PRNumber > 0 {
		next.PRNumber = candidate.PRNumber
	}
	if strings.TrimSpace(next.PRSHA) == "" && next.PRNumber == candidate.PRNumber {
		next.PRSHA = strings.TrimSpace(candidate.PRSHA)
	}
	if !hasValidOccurredAt(next.OccurredAt) {
		next.OccurredAt = strings.TrimSpace(candidate.OccurredAt)
	}
	if strings.TrimSpace(next.PRState) == "" {
		next.PRState = strings.TrimSpace(candidate.PRState)
	}
	if strings.TrimSpace(next.FinalMergedSHA) == "" {
		next.FinalMergedSHA = strings.TrimSpace(candidate.FinalMergedSHA)
	}
	if !next.MergedPR {
		next.MergedPR = candidate.MergedPR
	}
	if !next.PostGoodCommit {
		next.PostGoodCommit = candidate.PostGoodCommit
	}

	return next
}

func hasValidOccurredAt(value string) bool {
	_, ok := parseTimestamp(strings.TrimSpace(value))
	return ok
}
