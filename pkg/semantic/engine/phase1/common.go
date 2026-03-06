package phase1

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"sort"
	"strings"
)

var (
	reCollapseWhitespace = regexp.MustCompile(`\s+`)
)

var genericCodes = map[string]struct{}{
	"deploymentfailed":       {},
	"internalservererror":    {},
	"conflict":               {},
	"badrequest":             {},
	"multipleerrorsoccurred": {},
}

var wrapperOnly = map[string]struct{}{
	"unexpected error": {},
	"msg:":             {},
	"err:":             {},
	"caused by:":       {},
	"step errored":     {},
}

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

func collapseWS(value string) string {
	return reCollapseWhitespace.ReplaceAllString(strings.TrimSpace(value), " ")
}

func defaultKeyPart(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func buildGroupKey(lane string, jobName string, testName string) string {
	return defaultKeyPart(lane, "unknown") + "|" + defaultKeyPart(jobName, "unknown") + "|" + defaultKeyPart(testName, "unknown")
}

func fingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func buildRowID(runURL string, signatureID string, occurredAt string) string {
	seed := strings.TrimSpace(runURL) + "|" + strings.TrimSpace(signatureID) + "|" + strings.TrimSpace(occurredAt)
	return fingerprint(seed)
}

func normalizeReason(value string) string {
	normalized := strings.ToLower(collapseWS(value))
	return strings.ReplaceAll(normalized, " ", "_")
}

func isGenericCode(value string) bool {
	_, ok := genericCodes[strings.ToLower(strings.TrimSpace(value))]
	return ok
}

func sortedKeys[T any](values map[string]T) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		if strings.TrimSpace(key) == "" {
			continue
		}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
