package phase1

import (
	"regexp"
	"sort"
	"strings"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

var (
	reProviderPath                = regexp.MustCompile(`/providers/(Microsoft\.[A-Za-z]+(?:\.[A-Za-z]+)?)/`)
	reProviderText                = regexp.MustCompile(`(Microsoft\.[A-Za-z]+(?:\.[A-Za-z]+)?)`)
	reCleanFmtWrap                = regexp.MustCompile(`<\*[^>]+\|\s*0x[0-9a-fA-F]+>\s*:?`)
	reCleanHexAddress             = regexp.MustCompile(`\b0x[0-9a-fA-F]+\b`)
	reCleanGoFileLine             = regexp.MustCompile(`\b\w[\w./-]*\.go:\d+\b`)
	reCleanUnexpectedPrefix       = regexp.MustCompile(`(?i)^\s*unexpected error:\s*`)
	reCleanWrapperPrefix          = regexp.MustCompile(`(?i)^\s*(msg:|err:|caused by:)\s*`)
	reCleanURL                    = regexp.MustCompile(`https?://\S+`)
	reCleanSubscription           = regexp.MustCompile(`/subscriptions/[0-9a-fA-F-]+`)
	reCleanUUID                   = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F-]{27,}\b`)
	reCleanHexLong                = regexp.MustCompile(`\b[0-9a-fA-F]{32,}\b`)
	reCleanResourceGroupQuoted    = regexp.MustCompile(`(?i)resourcegroup="[^"]+"`)
	reCleanClusterQuoted          = regexp.MustCompile(`(?i)cluster="[^"]+"`)
	reBoolAssertionContext        = regexp.MustCompile(`(?is)Timed out after [0-9.]+s\.\s*\n(?P<context>[^\n]+)\s*\nExpected\s*\n\s*<bool>:\s*false\s*\n\s*to be true`)
	reAssertionRegexHint          = regexp.MustCompile(`Regexp:\s*"([^"]+)"`)
	reAssertionErrorSignal        = regexp.MustCompile(`(?i)(error|failed|timeout|forbidden|denied|conflict|deadline|not found|invalid|http2:)`)
	reSafeErrorLineSignal         = regexp.MustCompile(`(?i)(error|failed|timeout|not found|forbidden|denied|deadline|conflict)`)
	reCodeField                   = regexp.MustCompile(`"code"\s*:\s*"([A-Za-z0-9_]+)"`)
	reCauseBySplit                = regexp.MustCompile(`(?i)caused by:`)
	reErrorCode                   = regexp.MustCompile(`(?i)ERROR CODE:\s*([A-Za-z0-9_]+)`)
	rePickErrorSignal             = regexp.MustCompile(`(?i)(error|failed|timeout|forbidden|denied|conflict|deadline|not found)`)
	reUnexpectedOnly              = regexp.MustCompile(`(?i)unexpected error:?`)
	rePhase1Placeholder           = regexp.MustCompile(`<uuid>|<hex>|<url>`)
	reDeserializationLiteral      = regexp.MustCompile(`(?i)Deserializaion Error:[^\n]+`)
	reWrapperStepErroredContainer = regexp.MustCompile(`(?i)step errored`)
)

var normalizePickPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)Deserializaion Error:[^\n]+`),
	regexp.MustCompile(`(?i)Command Error:[^\n]+`),
	regexp.MustCompile(`(?i)missing expected log sources[^\n]+`),
	regexp.MustCompile(`(?i)failed to gather logs[^\n]+`),
	regexp.MustCompile(`(?i)failed to get service aro-hcp-exporter/aro-hcp-exporter: services "aro-hcp-exporter" not found`),
	regexp.MustCompile(`(?i)failed to search for managed resource groups:[^\n]+`),
	regexp.MustCompile(`(?i)failed to create SRE breakglass session:[^\n]+`),
	regexp.MustCompile(`(?i)response 404:[^\n]{0,240}`),
	regexp.MustCompile(`(?i)timeout '\d+\.\d+' minutes exceeded during CreateNodePoolFromParam[^\n]*`),
	regexp.MustCompile(`(?i)failed waiting for nodepool[^\n]+(?:updating|to finish creating)[^\n]*`),
	regexp.MustCompile(`(?i)UpdateNodePoolAndWait[^\n]+minutes exceeded[^\n]*`),
	regexp.MustCompile(`(?i)timeout '\d+\.\d+' minutes exceeded during CreateHCPClusterFromParam[^\n]*`),
	regexp.MustCompile(`(?i)error running Image Mirror Step, failed to execute shell command:[^\n]+`),
	regexp.MustCompile(`(?i)error running Helm release deployment Step, failed to deploy helm release:[^\n]+`),
	regexp.MustCompile(`(?i)error running Shell Step, failed to execute shell command:[^\n]+`),
	regexp.MustCompile(`(?i)failed to run ARM step:[^\n]+`),
	regexp.MustCompile(`(?i)Cluster provisioning failed`),
	regexp.MustCompile(`(?i)Interrupted by User`),
	regexp.MustCompile(`(?i)ERROR CODE:\s*[A-Za-z0-9_]+`),
}

var safeSearchPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)Deserializaion Error:[^\n]+`),
	regexp.MustCompile(`(?i)Command Error:[^\n]+`),
	regexp.MustCompile(`(?i)ERROR CODE:\s*[A-Za-z0-9_]+`),
	regexp.MustCompile(`(?i)timeout '\d+\.\d+' minutes exceeded during [A-Za-z0-9_]+`),
	regexp.MustCompile(`(?i)failed waiting for nodepool[^\n]+(?:updating|to finish creating)[^\n]*`),
	regexp.MustCompile(`(?i)failed to get service aro-hcp-exporter/aro-hcp-exporter: services "aro-hcp-exporter" not found`),
	regexp.MustCompile(`(?i)failed to search for managed resource groups:[^\n]+`),
	regexp.MustCompile(`(?i)failed to create SRE breakglass session:[^\n]+`),
	regexp.MustCompile(`(?i)error running Image Mirror Step, failed to execute shell command:[^\n]+`),
	regexp.MustCompile(`(?i)error running Helm release deployment Step, failed to deploy helm release:[^\n]+`),
	regexp.MustCompile(`(?i)error running Shell Step, failed to execute shell command:[^\n]+`),
	regexp.MustCompile(`(?i)failed to run ARM step:[^\n]+`),
	regexp.MustCompile(`(?i)response 404:[^\n]+`),
	regexp.MustCompile(`(?i)missing expected log sources[^\n]+`),
	regexp.MustCompile(`(?i)failed to gather logs[^\n]+`),
	regexp.MustCompile(`(?i)context deadline exceeded`),
	regexp.MustCompile(`(?i)Interrupted by User`),
	regexp.MustCompile(`(?i)Cluster provisioning failed`),
}

type extractedEvidence struct {
	CanonicalEvidencePhrase string
	SearchQueryPhrase       string
	ProviderAnchor          string
	GenericPhrase           bool
}

func Normalize(workset []semanticcontracts.Phase1WorksetRecord) []semanticcontracts.Phase1NormalizedRecord {
	out := make([]semanticcontracts.Phase1NormalizedRecord, 0, len(workset))
	for _, row := range workset {
		evidence := extractEvidence(row.RawText)
		out = append(out, semanticcontracts.Phase1NormalizedRecord{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			RowID:                   row.RowID,
			GroupKey:                row.GroupKey,
			Lane:                    row.Lane,
			JobName:                 row.JobName,
			TestName:                row.TestName,
			TestSuite:               row.TestSuite,
			SignatureID:             row.SignatureID,
			OccurredAt:              row.OccurredAt,
			RunURL:                  row.RunURL,
			PRNumber:                row.PRNumber,
			PostGoodCommit:          row.PostGoodCommit,
			RawText:                 row.RawText,
			NormalizedText:          row.NormalizedText,
			CanonicalEvidencePhrase: evidence.CanonicalEvidencePhrase,
			SearchQueryPhrase:       evidence.SearchQueryPhrase,
			ProviderAnchor:          evidence.ProviderAnchor,
			GenericPhrase:           evidence.GenericPhrase,
			Phase1Key:               phase1Key(evidence),
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Lane != out[j].Lane {
			return out[i].Lane < out[j].Lane
		}
		if out[i].JobName != out[j].JobName {
			return out[i].JobName < out[j].JobName
		}
		if out[i].TestName != out[j].TestName {
			return out[i].TestName < out[j].TestName
		}
		if out[i].OccurredAt != out[j].OccurredAt {
			return out[i].OccurredAt < out[j].OccurredAt
		}
		if out[i].RunURL != out[j].RunURL {
			return out[i].RunURL < out[j].RunURL
		}
		if out[i].SignatureID != out[j].SignatureID {
			return out[i].SignatureID < out[j].SignatureID
		}
		return out[i].RowID < out[j].RowID
	})

	return out
}

func phase1Key(evidence extractedEvidence) string {
	base := strings.ToLower(collapseWS(evidence.CanonicalEvidencePhrase))
	base = rePhase1Placeholder.ReplaceAllString(base, "")
	base = collapseWS(base)
	if evidence.GenericPhrase {
		provider := evidence.ProviderAnchor
		if strings.TrimSpace(provider) == "" {
			provider = "<none>"
		}
		return base + "|provider:" + provider
	}
	return base
}

func extractEvidence(text string) extractedEvidence {
	raw := text
	lowered := strings.ToLower(raw)
	provider := providerAnchor(raw)
	assertionContext := extractAssertionContext(raw)
	if assertionContext != "" {
		canonical := cleanCanonical(assertionContext)
		searchPhrase := chooseSearchPhrase(raw, []string{assertionContext, canonical})
		return extractedEvidence{
			CanonicalEvidencePhrase: canonical,
			SearchQueryPhrase:       searchPhrase,
			ProviderAnchor:          provider,
			GenericPhrase:           false,
		}
	}

	picked := ""
	for _, pattern := range normalizePickPatterns {
		if match := pattern.FindString(raw); match != "" {
			picked = match
			break
		}
	}

	if picked == "" {
		parts := reCauseBySplit.Split(raw, -1)
		if len(parts) > 1 {
			picked = truncateText(parts[len(parts)-1], 260)
		} else {
			lines := splitNonEmptyLines(raw)
			errorLines := make([]string, 0, len(lines))
			for _, line := range lines {
				if rePickErrorSignal.MatchString(line) && !isAssertionTail(line) {
					errorLines = append(errorLines, line)
				}
			}
			fallback := "failure occurred"
			if len(lines) > 0 {
				for i := len(lines) - 1; i >= 0; i-- {
					if !isAssertionTail(lines[i]) {
						fallback = lines[i]
						break
					}
				}
			}
			if len(errorLines) > 0 {
				picked = truncateText(errorLines[len(errorLines)-1], 260)
			} else {
				picked = truncateText(fallback, 260)
			}
		}
	}

	if strings.EqualFold(strings.TrimSpace(picked), "cluster provisioning failed") {
		if codePick := regexp.MustCompile(`(?i)ERROR CODE:\s*[A-Za-z0-9_]+`).FindString(raw); codePick != "" {
			picked = codePick
		}
	}

	code := ""
	if match := reErrorCode.FindStringSubmatch(picked); len(match) > 1 {
		code = strings.TrimSpace(match[1])
	}
	canonical := cleanCanonical(picked)

	if code != "" && isGenericCode(code) {
		leafCode := extractLeafCode(raw)
		parts := []string{"ERROR CODE: " + code}
		if provider != "" {
			parts = append(parts, "provider "+provider)
		}
		if leafCode != "" {
			parts = append(parts, "detail code "+leafCode)
		}
		canonical = strings.Join(parts, "; ")
	}

	if strings.Contains(lowered, "context deadline exceeded") && strings.Contains(lowered, "createhcpclusterfromparam") {
		canonical = "timeout during CreateHCPClusterFromParam; context deadline exceeded"
	}
	if strings.Contains(lowered, "interrupted by user") {
		canonical = "Interrupted by User"
	}
	if strings.Contains(lowered, "deserializaion error") {
		match := reDeserializationLiteral.FindString(raw)
		if match == "" {
			canonical = "Deserializaion Error"
		} else {
			canonical = cleanCanonical(match)
		}
	}

	normalizedCanonical := strings.ToLower(canonical)
	if _, found := wrapperOnly[normalizedCanonical]; found || reUnexpectedOnly.MatchString(canonical) {
		parts := reCauseBySplit.Split(raw, -1)
		if len(parts) > 1 {
			canonical = cleanCanonical(truncateText(parts[len(parts)-1], 260))
		}
	}

	searchPhrase := chooseSearchPhrase(raw, []string{picked, canonical})
	_, genericCanonical := map[string]struct{}{
		"interrupted by user":         {},
		"cluster provisioning failed": {},
		"context deadline exceeded":   {},
		"timeout during createhcpclusterfromparam; context deadline exceeded": {},
	}[strings.ToLower(canonical)]
	genericPhrase := (code != "" && isGenericCode(code)) || genericCanonical

	return extractedEvidence{
		CanonicalEvidencePhrase: canonical,
		SearchQueryPhrase:       searchPhrase,
		ProviderAnchor:          provider,
		GenericPhrase:           genericPhrase,
	}
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

func cleanCanonical(value string) string {
	text := value
	text = strings.ReplaceAll(text, "\r", " ")
	text = strings.ReplaceAll(text, "\n", " ")
	text = reCleanFmtWrap.ReplaceAllString(text, " ")
	text = reCleanHexAddress.ReplaceAllString(text, " ")
	text = reCleanGoFileLine.ReplaceAllString(text, " ")
	text = reCleanUnexpectedPrefix.ReplaceAllString(text, "")
	text = reCleanWrapperPrefix.ReplaceAllString(text, "")
	text = reCleanURL.ReplaceAllString(text, "<url>")
	text = reCleanSubscription.ReplaceAllString(text, "/subscriptions/<subscription>")
	text = reCleanUUID.ReplaceAllString(text, "<uuid>")
	text = reCleanHexLong.ReplaceAllString(text, "<hex>")
	text = reCleanResourceGroupQuoted.ReplaceAllString(text, `resourcegroup="<resource-group>"`)
	text = reCleanClusterQuoted.ReplaceAllString(text, `cluster="<cluster>"`)
	text = collapseWS(text)
	if len(text) > 260 {
		text = strings.TrimRight(text[:260], " ,;:")
	}
	return text
}

func extractAssertionContext(text string) string {
	if boolContext := extractBoolAssertionContext(text); boolContext != "" {
		return boolContext
	}

	lines := strings.Split(text, "\n")
	for index, line := range lines {
		if !isAssertionTail(line) {
			continue
		}

		tail := strings.ToLower(collapseWS(line))
		if strings.HasPrefix(tail, "to match error") {
			stop := minInt(len(lines), index+12)
			for _, candidateLine := range lines[index+1 : stop] {
				match := reAssertionRegexHint.FindStringSubmatch(candidateLine)
				if len(match) > 1 {
					regexHint := collapseWS(match[1])
					if regexHint != "" {
						return regexHint
					}
				}
			}
		}

		best := ""
		start := maxInt(0, index-30)
		for i := index - 1; i >= start; i-- {
			candidate := collapseWS(lines[i])
			if isNoiseAssertionContextLine(candidate) {
				continue
			}
			if reAssertionErrorSignal.MatchString(candidate) {
				return candidate
			}
			if best == "" && regexp.MustCompile(`[A-Za-z]`).MatchString(candidate) {
				best = candidate
			}
		}
		if best != "" {
			return best
		}
	}
	return ""
}

func extractBoolAssertionContext(text string) string {
	match := reBoolAssertionContext.FindStringSubmatch(text)
	if len(match) == 0 {
		return ""
	}
	for i, name := range reBoolAssertionContext.SubexpNames() {
		if name == "context" && i < len(match) {
			return collapseWS(match[i])
		}
	}
	return ""
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

func isNoiseAssertionContextLine(line string) bool {
	normalized := collapseWS(line)
	lowered := strings.ToLower(normalized)
	if normalized == "" {
		return true
	}
	if isAssertionTail(normalized) {
		return true
	}
	switch lowered {
	case "expected", "{", "}", "},", "]", "],", "(", ")":
		return true
	}
	if strings.HasPrefix(lowered, "expected") ||
		strings.HasPrefix(lowered, "learn more here:") ||
		strings.HasPrefix(lowered, "gomega truncated") ||
		strings.HasPrefix(lowered, "consider having") {
		return true
	}
	if strings.HasPrefix(lowered, "fail [") && strings.HasSuffix(lowered, ": expected") {
		return true
	}
	return strings.HasPrefix(normalized, "<") || strings.HasPrefix(normalized, "{") || strings.HasPrefix(normalized, "}")
}

func chooseSearchPhrase(text string, candidates []string) string {
	for _, candidate := range candidates {
		token := strings.TrimSpace(candidate)
		if token == "" {
			continue
		}
		if containsPlaceholderToken(token) {
			continue
		}
		if strings.Contains(text, token) {
			return token
		}
	}
	return safeSearchFromText(text)
}

func safeSearchFromText(text string) string {
	assertionContext := extractAssertionContext(text)
	if assertionContext != "" && strings.Contains(text, assertionContext) {
		return assertionContext
	}

	for _, pattern := range safeSearchPatterns {
		if match := pattern.FindString(text); match != "" {
			token := strings.TrimSpace(match)
			if token == "" || containsPlaceholderToken(token) {
				continue
			}
			if strings.Contains(text, token) {
				return token
			}
		}
	}

	for _, line := range strings.Split(text, "\n") {
		token := strings.TrimSpace(line)
		if token == "" || isAssertionTail(token) || containsPlaceholderToken(token) {
			continue
		}
		if reSafeErrorLineSignal.MatchString(token) {
			return truncateText(token, 220)
		}
	}

	if strings.Contains(strings.ToLower(text), "context deadline exceeded") {
		return "context deadline exceeded"
	}
	return "failure"
}

func extractLeafCode(text string) string {
	matches := reCodeField.FindAllStringSubmatch(text, -1)
	for i := len(matches) - 1; i >= 0; i-- {
		if len(matches[i]) < 2 {
			continue
		}
		code := strings.TrimSpace(matches[i][1])
		lowered := strings.ToLower(code)
		if lowered == "" {
			continue
		}
		if _, generic := genericCodes[lowered]; generic {
			continue
		}
		if lowered == "resourcedeploymentfailure" || lowered == "deploymentfailed" {
			continue
		}
		return code
	}
	return ""
}

func splitNonEmptyLines(text string) []string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func containsPlaceholderToken(value string) bool {
	return strings.Contains(value, "<") && strings.Contains(value, ">")
}

func truncateText(value string, max int) string {
	trimmed := strings.TrimSpace(value)
	if max <= 0 || len(trimmed) <= max {
		return trimmed
	}
	return strings.TrimSpace(trimmed[:max])
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
