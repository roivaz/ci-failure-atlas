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
	reCleanLookupHostOnServer     = regexp.MustCompile(`(?i)\blookup\s+[a-z0-9.-]+\s+on\s+\d{1,3}(?:\.\d{1,3}){3}:\d+\b`)
	reCleanResourceGroupQuoted    = regexp.MustCompile(`(?i)resourcegroup="[^"]+"`)
	reCleanResourceGroupBare      = regexp.MustCompile(`(?i)\bresource group [a-z0-9-]+\b`)
	reCleanClusterQuoted          = regexp.MustCompile(`(?i)cluster="[^"]+"`)
	reCleanClusterPhraseQuoted    = regexp.MustCompile(`(?i)\bcluster "[^"]+"`)
	reCleanClusterCreationQuoted  = regexp.MustCompile(`(?i)\bcluster creation ["'][^"']+["']`)
	reCleanForClusterPhrase       = regexp.MustCompile(`(?i)\bfor cluster [a-z0-9-]+\b`)
	reCleanInClusterPhrase        = regexp.MustCompile(`(?i)\bin cluster [a-z0-9-]+\b`)
	reCleanHCPClusterPhrase       = regexp.MustCompile(`(?i)\bhcp cluster [a-z0-9-]+\b`)
	reCleanExternalAuthQuoted     = regexp.MustCompile(`(?i)external auth "[^"]+"`)
	reCleanExternalAuthBare       = regexp.MustCompile(`(?i)\bexternal auth [a-z0-9-]+\b`)
	reCleanNodePoolQuoted         = regexp.MustCompile(`(?i)nodepool="[^"]+"`)
	reCleanNodePoolPhrase         = regexp.MustCompile(`(?i)\bnode pool [a-z0-9-]+\b`)
	reBoolAssertionContext        = regexp.MustCompile(`(?is)Timed out after [0-9.]+s\.\s*\n(?P<context>[^\n]+)\s*\nExpected\s*\n\s*<bool>:\s*false\s*\n\s*to be true`)
	reAssertionRegexHint          = regexp.MustCompile(`Regexp:\s*"([^"]+)"`)
	reAssertionErrorSignal        = regexp.MustCompile(`(?i)(error|failed|timeout|forbidden|denied|conflict|deadline|not found|invalid|http2:)`)
	reSafeErrorLineSignal         = regexp.MustCompile(`(?i)(error|failed|timeout|not found|forbidden|denied|deadline|conflict)`)
	reEventuallyWrapperLine       = regexp.MustCompile(`(?i)^the function passed to eventually failed at .+ with:?$`)
	reTimedOutAfterLine           = regexp.MustCompile(`(?i)^timed out after [0-9.]+s\.`)
	reCodeField                   = regexp.MustCompile(`"code"\s*:\s*"([A-Za-z0-9_]+)"`)
	reCauseBySplit                = regexp.MustCompile(`(?i)caused by:`)
	reErrorCode                   = regexp.MustCompile(`(?i)ERROR CODE:\s*([A-Za-z0-9_]+)`)
	rePickErrorSignal             = regexp.MustCompile(`(?i)(error|failed|timeout|forbidden|denied|conflict|deadline|not found)`)
	reHTTPResponseStatusLine      = regexp.MustCompile(`(?i)^response [45][0-9]{2}:\s*.+$`)
	reRouteHostNeverFound         = regexp.MustCompile(`(?i)route host was never found:[^\n]+`)
	reClusterOperatorsUnavailable = regexp.MustCompile(`(?i)cluster operators not available:[^\n]+`)
	reRateLimiterDeadline         = regexp.MustCompile(`(?i)client rate limiter wait returned an error: context deadline exceeded`)
	reUnexpectedOnly              = regexp.MustCompile(`(?i)unexpected error:?`)
	rePhase1Placeholder           = regexp.MustCompile(`<uuid>|<hex>|<url>`)
	reDeserializationLiteral      = regexp.MustCompile(`(?i)Deserializa(?:ti|i)on Error:[^\n]+`)
	reDeserializationNoOutput     = regexp.MustCompile(`(?i)Deserializa(?:ti|i)on Error:\s*no output from command`)
	reDeserializationToken        = regexp.MustCompile(`(?i)deserializa(?:ti|i)on error`)
	reCommandErrorLine            = regexp.MustCompile(`(?im)^Command Error:\s*[^\n]+$`)
	reQuotaRequiredAvailable      = regexp.MustCompile(`(?i)\brequired\s+['"]?\d+['"]?\s*,\s*available\s+['"]?\d+['"]?\b`)
	reWrapperStepErroredContainer = regexp.MustCompile(`(?i)step errored`)

	// Dial-TCP address: normalize raw IPs left behind after URL masking.
	reCleanDialTCPAddress = regexp.MustCompile(`\bdial tcp \d{1,3}(?:\.\d{1,3}){3}:\d+\b`)
	// Istio/envoy "dialing <ip>:<port>" — same IP/port noise, different verb.
	reCleanDialingAddress = regexp.MustCompile(`\bdialing \d{1,3}(?:\.\d{1,3}){3}:\d+\b`)
	// Logfmt-style timestamp (e.g. time=2026-04-17T11:04:19.211Z).
	reCleanLogfmtTimestamp = regexp.MustCompile(`\btime=[0-9]{4}-[0-9]{2}-[0-9]{2}T[A-Z0-9:.]+\s*`)
	// JSON "time" field with ISO-8601 value (e.g. prow entrypoint logs).
	reCleanJSONTimeField = regexp.MustCompile(`"time"\s*:\s*"[0-9]{4}-[0-9]{2}-[0-9]{2}T[^"]*",?\s*`)
	// Prow entrypoint single-line JSON: extract just the msg value.
	reProwEntrypointMsg = regexp.MustCompile(`"component"\s*:\s*"entrypoint"[^}]{0,400}"msg"\s*:\s*"([^"]+)"`)
	// All CreateHCPCluster*FromParam / *AndWait helper variants.
	reCreateHCPClusterTimeout = regexp.MustCompile(`(?i)createhcpcluster\w*(?:fromparam|andwait)`)
	// Gomega "Expected success, but got an error:" followed by optional type
	// wrapper line, then the real error message.
	reGomegaSuccessFailure = regexp.MustCompile(`(?i)Expected success, but got an error:\s*\n(?:[ \t]*<[^>\n]*>[ \t]*:?[ \t]*\n)?[ \t]*([^\n.]+)`)
	// HCP API / reserved hostnames (e.g. api.<cluster>.<stamp>.<region>.aroapp-hcp.io).
	// These appear in x509 certificate-mismatch error text and are cluster/stamp-
	// specific; normalize so the same class of cert error merges across clusters.
	reCleanHCPApiHost = regexp.MustCompile(`(?i)\b(?:api|reserved)\.[a-z0-9][a-z0-9.-]*\.aroapp-hcp\.io\b`)
	// OCP version strings like openshift-v4.22.0-candidate (the version number
	// is instance-specific; normalize so the same class of error merges).
	reCleanOCPVersion = regexp.MustCompile(`\bopenshift-v[0-9]+\.[0-9]+\.[0-9]+-[a-z]+\b`)
	// Single-quoted opaque alphanumeric IDs (≥20 chars) such as OCM cluster
	// internal IDs that appear in Azure RP error messages.
	reCleanQuotedOpaqueID = regexp.MustCompile(`'[a-z0-9]{20,}'`)
	// logfmt err= field value extracted from a step-error log line.
	// The pattern matches level=error … msg="Step errored." … err="<value>" to
	// capture the actionable error message without logfmt boilerplate fields.
	reLogfmtStepErroredErr = regexp.MustCompile(`(?i)level=error[^"]*msg="step errored\."[^"]*err="([^"]+)"`)

	// Kubernetes klog / structured-log prefix: E<MMDD> <HH:MM:SS>.<us> <goroutine>
	// The file:line portion is already stripped by reCleanGoFileLine; this
	// strips the remaining severity+timestamp+goroutine token so the real
	// message (e.g. "Unhandled Error" err=…) becomes the canonical phrase.
	reCleanK8sLogPrefix = regexp.MustCompile(`[EWI][0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+ +[0-9]+\s*\]?`)

	// Bare nodepool name in the form "nodepool <name>" (single token, no quotes)
	// that appears in UpdateNodePoolAndWait / timeout messages.  The quoted form
	// nodepool="<name>" is handled by reCleanNodePoolQuoted above; this pattern
	// covers the unquoted counterpart so the same failure class merges.
	reCleanNodePoolBare = regexp.MustCompile(`(?i)\bnodepool [a-z0-9][a-z0-9-]+\b`)

	// "make[N]: Entering/Leaving directory '...'" lines emitted by GNU Make
	// when shell steps run sub-makes.  These are build preamble noise that
	// precedes the real error in err= log fields; strip them so the canonical
	// phrase reflects the actual failure rather than the directory banner.
	reCleanMakeDirectory = regexp.MustCompile(`(?i)make\[\d+\]: (?:Entering|Leaving) directory\s+'[^']*'\s*\.?\s*`)
)

var normalizePickPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)Deserializa(?:ti|i)on Error:[^\n]+`),
	regexp.MustCompile(`(?i)Command Error:[^\n]+`),
	regexp.MustCompile(`(?i)route host was never found:[^\n]+`),
	regexp.MustCompile(`(?i)cluster operators not available:[^\n]+`),
	regexp.MustCompile(`(?i)client rate limiter wait returned an error: context deadline exceeded`),
	regexp.MustCompile(`(?i)missing expected log sources[^\n]+`),
	regexp.MustCompile(`(?i)failed to gather logs[^\n]+`),
	regexp.MustCompile(`(?i)failed to get service aro-hcp-exporter/aro-hcp-exporter: services "aro-hcp-exporter" not found`),
	regexp.MustCompile(`(?i)failed to search for managed resource groups:[^\n]+`),
	regexp.MustCompile(`(?i)failed to create SRE breakglass session:[^\n]+`),
	// ERROR CODE must come before the generic response-status line so that a
	// richer error code (e.g. NotFound with a detail message) is preferred
	// over the bare HTTP status text.
	regexp.MustCompile(`(?i)ERROR CODE:\s*[A-Za-z0-9_]+`),
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
}

var safeSearchPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)Deserializa(?:ti|i)on Error:[^\n]+`),
	regexp.MustCompile(`(?i)Command Error:[^\n]+`),
	regexp.MustCompile(`(?i)route host was never found:[^\n]+`),
	regexp.MustCompile(`(?i)cluster operators not available:[^\n]+`),
	regexp.MustCompile(`(?i)client rate limiter wait returned an error: context deadline exceeded`),
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

type azureCodeHit struct {
	Code  string
	Index int
}

func Normalize(workset []semanticcontracts.Phase1WorksetRecord) []semanticcontracts.Phase1NormalizedRecord {
	out := make([]semanticcontracts.Phase1NormalizedRecord, 0, len(workset))
	for _, row := range workset {
		evidence := extractEvidence(row.RawText)
		out = append(out, semanticcontracts.Phase1NormalizedRecord{
			SchemaVersion:           semanticcontracts.CurrentSchemaVersion,
			Environment:             strings.TrimSpace(row.Environment),
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

	// Prow entrypoint JSON lines are single-line structured logs whose only
	// varying field across occurrences is "time".  Extract just the "msg"
	// value so all instances of the same timeout canonicalize identically.
	// This must run before bestSignalErrorLine so that the JSON line itself
	// is not claimed as the canonical phrase.
	if picked == "" {
		if match := reProwEntrypointMsg.FindStringSubmatch(raw); len(match) > 1 {
			picked = strings.TrimSpace(match[1])
		}
	}

	// Logfmt step-error lines: extract only the err= value from the raw text
	// before bestSignalErrorLine can claim the full (potentially long) line and
	// truncate it before the closing quote, preventing extraction of err=.
	if picked == "" {
		if match := reLogfmtStepErroredErr.FindStringSubmatch(raw); len(match) > 1 {
			picked = strings.TrimSpace(match[1])
		}
	}

	if picked == "" {
		picked = bestSignalErrorLine(raw)
	}

	if picked == "" {
		picked = bestHTTPResponseStatusLine(raw)
	}

	if picked == "" {
		parts := reCauseBySplit.Split(raw, -1)
		if len(parts) > 1 {
			picked = truncateText(parts[len(parts)-1], 260)
		} else {
			lines := splitNonEmptyLines(raw)
			errorLines := make([]string, 0, len(lines))
			for _, line := range lines {
				if rePickErrorSignal.MatchString(line) && !isAssertionTail(line) && !isWrapperNoiseLine(line) && !isStructFieldNoiseLine(line) && !isStatusBannerLine(line) {
					errorLines = append(errorLines, line)
				}
			}
			fallback := "failure occurred"
			if len(lines) > 0 {
				for i := len(lines) - 1; i >= 0; i-- {
					if !isAssertionTail(lines[i]) && !isStructFieldNoiseLine(lines[i]) && !isStatusBannerLine(lines[i]) {
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
	picked = refineDeserializationNoOutputPicked(raw, picked)
	// Only attempt command-error refinement when the deserialization-no-output
	// path was not responsible for choosing the current picked value; that path
	// deliberately elevates the Command Error line as the best signal.
	if !containsDeserializationNoOutputSignal(raw) && !containsDeserializationNoOutputSignal(picked) {
		picked = refineCommandErrorExitStatusOnly(raw, picked)
	}

	code := ""
	leafCode := ""
	leafMessage := ""
	if match := reErrorCode.FindStringSubmatch(picked); len(match) > 1 {
		code = strings.TrimSpace(match[1])
	}
	if code == "" {
		code = rootAzureErrorCode(raw)
	}
	canonical := cleanCanonical(picked)

	if code != "" && isGenericCode(code) {
		leafCode, leafMessage = extractLeafAzureDetail(raw, code)
		parts := []string{"ERROR CODE: " + code}
		if leafCode != "" {
			parts = append(parts, "detail code "+leafCode)
		}
		if leafMessage != "" {
			parts = append(parts, "detail message "+leafMessage)
		}
		if provider != "" {
			parts = append(parts, "provider "+provider)
		}
		canonical = strings.Join(parts, "; ")
	}

	if strings.Contains(lowered, "context deadline exceeded") && reCreateHCPClusterTimeout.MatchString(lowered) {
		canonical = "timeout during CreateHCPClusterAndWait; context deadline exceeded"
	}
	if strings.Contains(lowered, "getadminrestconfigforhcpcluster") && strings.Contains(lowered, "timeout") {
		canonical = "timeout during GetAdminRESTConfigForHCPCluster while waiting for hcpcluster creds"
	}
	if strings.Contains(lowered, "interrupted by user") {
		canonical = "Interrupted by User"
	}
	if containsDeserializationErrorToken(picked) || containsDeserializationErrorToken(canonical) {
		match := reDeserializationLiteral.FindString(raw)
		if match == "" {
			canonical = "Deserializaion Error"
		} else {
			canonical = cleanCanonical(match)
		}
	}
	if strings.EqualFold(strings.TrimSpace(canonical), "context deadline exceeded") {
		if refined := bestContextDeadlineDetail(raw); refined != "" {
			canonical = cleanCanonical(refined)
		}
	}

	normalizedCanonical := strings.ToLower(canonical)
	if _, found := wrapperOnly[normalizedCanonical]; found || reUnexpectedOnly.MatchString(canonical) {
		parts := reCauseBySplit.Split(raw, -1)
		if len(parts) > 1 {
			canonical = cleanCanonical(truncateText(parts[len(parts)-1], 260))
		}
	}
	if isLowInformationCanonical(canonical) {
		if refined := bestSignalErrorLine(raw); refined != "" {
			canonical = cleanCanonical(refined)
		}
	}

	// For non-generic error codes the provider is not added by the block above;
	// append it here so that every ERROR CODE canonical phrase identifies the
	// resource provider that returned the error.
	if code != "" && !isGenericCode(code) && provider != "" &&
		strings.HasPrefix(strings.ToLower(canonical), "error code:") &&
		!strings.Contains(strings.ToLower(canonical), "; provider ") {
		canonical += "; provider " + provider
	}

	searchPhrase := chooseSearchPhrase(raw, []string{picked, canonical})
	_, genericCanonical := map[string]struct{}{
		"interrupted by user":         {},
		"cluster provisioning failed": {},
		"context deadline exceeded":   {},
		"timeout during createhcpclusterandwait; context deadline exceeded": {},
	}[strings.ToLower(canonical)]
	genericPhrase := genericCanonical
	if code != "" && isGenericCode(code) {
		genericPhrase = leafCode == "" && leafMessage == "" && provider == ""
	}

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
	case "Microsoft.Resources", "Microsoft.Azure.ARO":
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
	text = reCleanLookupHostOnServer.ReplaceAllString(text, "lookup <host> on <dns-server>")
	text = reCleanResourceGroupQuoted.ReplaceAllString(text, `resourcegroup="<resource-group>"`)
	text = reCleanResourceGroupBare.ReplaceAllString(text, "resource group <resource-group>")
	text = reCleanClusterQuoted.ReplaceAllString(text, `cluster="<cluster>"`)
	text = reCleanClusterPhraseQuoted.ReplaceAllString(text, `cluster "<cluster>"`)
	text = reCleanClusterCreationQuoted.ReplaceAllString(text, `cluster creation "<cluster>"`)
	text = reCleanForClusterPhrase.ReplaceAllString(text, "for cluster <cluster>")
	text = reCleanInClusterPhrase.ReplaceAllString(text, "in cluster <cluster>")
	text = reCleanHCPClusterPhrase.ReplaceAllString(text, "HCP cluster <cluster>")
	text = reCleanExternalAuthQuoted.ReplaceAllString(text, `external auth "<external-auth>"`)
	text = reCleanExternalAuthBare.ReplaceAllString(text, "external auth <external-auth>")
	text = reCleanNodePoolQuoted.ReplaceAllString(text, `nodepool="<nodepool>"`)
	text = reCleanNodePoolPhrase.ReplaceAllString(text, "node pool <nodepool>")
	text = reCleanNodePoolBare.ReplaceAllString(text, "nodepool <nodepool>")
	text = reCleanDialTCPAddress.ReplaceAllString(text, "dial tcp <ip>:<port>")
	text = reCleanDialingAddress.ReplaceAllString(text, "dialing <ip>:<port>")
	text = reCleanLogfmtTimestamp.ReplaceAllString(text, "")
	text = reCleanJSONTimeField.ReplaceAllString(text, "")
	text = reCleanHCPApiHost.ReplaceAllString(text, "<hcp-api-host>")
	text = reCleanOCPVersion.ReplaceAllString(text, "openshift-v<version>")
	text = reCleanQuotedOpaqueID.ReplaceAllString(text, "'<id>'")
	// Strip klog severity+timestamp+goroutine prefix AFTER reCleanGoFileLine has
	// already removed the file:line token, so the real log message surfaces.
	text = reCleanK8sLogPrefix.ReplaceAllString(text, "")
	// Strip GNU Make directory-entering banners that appear as build preamble
	// before the real error in multi-line err= log fields.
	text = reCleanMakeDirectory.ReplaceAllString(text, "")
	text = collapseWS(text)
	if len(text) > 260 {
		text = truncateCanonical(text, 260)
	}
	return text
}

func truncateCanonical(value string, max int) string {
	trimmed := strings.TrimSpace(value)
	if max <= 0 || len(trimmed) <= max {
		return strings.TrimRight(trimmed, " ,;:-")
	}
	cut := strings.TrimSpace(trimmed[:max])
	if lastSpace := strings.LastIndex(cut, " "); lastSpace >= max/2 {
		cut = cut[:lastSpace]
	}
	return strings.TrimRight(cut, " ,;:-")
}

func extractAssertionContext(text string) string {
	if boolContext := extractBoolAssertionContext(text); boolContext != "" {
		return boolContext
	}
	if eventuallyContext := extractEventuallyFailureContext(text); eventuallyContext != "" {
		return eventuallyContext
	}
	if successContext := extractGomegaSuccessFailureContext(text); successContext != "" {
		return successContext
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

func extractEventuallyFailureContext(text string) string {
	lines := strings.Split(text, "\n")
	for index, line := range lines {
		if !isExpectedBlockStartLine(line) {
			continue
		}
		start := maxInt(0, index-12)
		for i := index - 1; i >= start; i-- {
			candidate := collapseWS(lines[i])
			if candidate == "" {
				continue
			}
			if isNoiseAssertionContextLine(candidate) || isEventuallyWrapperLine(candidate) || isTimedOutAfterLine(candidate) {
				continue
			}
			return candidate
		}
	}
	return ""
}

// extractGomegaSuccessFailureContext handles the Gomega pattern:
//
//	Expected success, but got an error:
//	    <*errors.errorString | 0x...>:
//	    <actual error message>
//	    ...
//
// It extracts the actual error message line, skipping the optional type-wrapper
// line and any label lines that end with a colon (e.g. "IDMS verification
// failed:"), so the canonical phrase reflects the real failure rather than the
// Gomega assertion boilerplate or a structural label.
func extractGomegaSuccessFailureContext(text string) string {
	matchIdx := reGomegaSuccessFailure.FindStringSubmatchIndex(text)
	if len(matchIdx) < 4 {
		return ""
	}
	candidate := strings.TrimSpace(text[matchIdx[2]:matchIdx[3]])
	if candidate == "" || candidate == "..." {
		return ""
	}
	// If the captured line is a label (ends with ':'), look ahead in the
	// remaining text for the first non-empty, non-label, non-ellipsis line.
	if strings.HasSuffix(candidate, ":") {
		afterMatch := text[matchIdx[1]:]
		for _, line := range strings.Split(afterMatch, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || trimmed == "..." || strings.HasSuffix(trimmed, ":") {
				continue
			}
			return trimmed
		}
	}
	return candidate
}

func isExpectedBlockStartLine(line string) bool {
	return strings.EqualFold(collapseWS(line), "expected")
}

func isEventuallyWrapperLine(line string) bool {
	return reEventuallyWrapperLine.MatchString(collapseWS(line))
}

func isTimedOutAfterLine(line string) bool {
	return reTimedOutAfterLine.MatchString(collapseWS(line))
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
	if isEventuallyWrapperLine(normalized) || isTimedOutAfterLine(normalized) {
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
		if token == "" || isAssertionTail(token) || containsPlaceholderToken(token) || isWrapperNoiseLine(token) || isStructFieldNoiseLine(token) || isStatusBannerLine(token) {
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

func extractLeafAzureDetail(text string, rootCode string) (string, string) {
	decoded := decodeEscapedErrorPayload(text)
	hits := collectAzureCodeHits(decoded)
	if len(hits) == 0 {
		return "", ""
	}

	root := strings.ToLower(strings.TrimSpace(rootCode))
	fallbackCode := ""
	genericFallbackCode := ""
	for i := len(hits) - 1; i >= 0; i-- {
		code := strings.TrimSpace(hits[i].Code)
		lowered := strings.ToLower(code)
		if lowered == "" || lowered == root {
			continue
		}
		if lowered == "resourcedeploymentfailure" || lowered == "deploymentfailed" {
			continue
		}
		if isLikelyTruncatedAzureCode(code, hits) {
			continue
		}

		message := summarizeAzureDetailMessage(extractAzureMessageForCode(decoded, code))
		if _, generic := genericCodes[lowered]; generic {
			if message != "" {
				return code, message
			}
			if genericFallbackCode == "" {
				genericFallbackCode = code
			}
			continue
		}
		if message != "" {
			return code, message
		}
		if fallbackCode == "" {
			fallbackCode = code
		}
	}
	if fallbackCode != "" {
		return fallbackCode, ""
	}
	if genericFallbackCode != "" {
		return genericFallbackCode, ""
	}
	rootMessage := summarizeAzureDetailMessage(extractAzureMessageForCode(decoded, rootCode))
	if rootMessage != "" {
		return "", rootMessage
	}
	return "", ""
}

func rootAzureErrorCode(text string) string {
	if match := reErrorCode.FindStringSubmatch(text); len(match) > 1 {
		code := strings.TrimSpace(match[1])
		if code != "" {
			return code
		}
	}
	hits := collectAzureCodeHits(decodeEscapedErrorPayload(text))
	for _, hit := range hits {
		code := strings.TrimSpace(hit.Code)
		if code == "" {
			continue
		}
		return code
	}
	return ""
}

func collectAzureCodeHits(text string) []azureCodeHit {
	out := make([]azureCodeHit, 0)
	errorCodeMatches := reErrorCode.FindAllStringSubmatchIndex(text, -1)
	for _, match := range errorCodeMatches {
		if len(match) < 4 {
			continue
		}
		code := strings.TrimSpace(text[match[2]:match[3]])
		if code == "" {
			continue
		}
		out = append(out, azureCodeHit{
			Code:  code,
			Index: match[0],
		})
	}

	codeFieldMatches := reCodeField.FindAllStringSubmatchIndex(text, -1)
	for _, match := range codeFieldMatches {
		if len(match) < 4 {
			continue
		}
		code := strings.TrimSpace(text[match[2]:match[3]])
		if code == "" {
			continue
		}
		out = append(out, azureCodeHit{
			Code:  code,
			Index: match[0],
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Index != out[j].Index {
			return out[i].Index < out[j].Index
		}
		return out[i].Code < out[j].Code
	})
	return out
}

func decodeEscapedErrorPayload(text string) string {
	out := text
	// Some payloads are nested JSON strings with double escaping (for example
	// \\\"code\\\"). Normalize both layers before scanning for inner codes.
	for range 2 {
		out = strings.ReplaceAll(out, `\\r\\n`, "\n")
		out = strings.ReplaceAll(out, `\\n`, "\n")
		out = strings.ReplaceAll(out, `\\t`, " ")
		out = strings.ReplaceAll(out, `\\\"`, `"`)
		out = strings.ReplaceAll(out, `\r\n`, "\n")
		out = strings.ReplaceAll(out, `\n`, "\n")
		out = strings.ReplaceAll(out, `\t`, " ")
		out = strings.ReplaceAll(out, `\"`, `"`)
	}
	return out
}

func isLikelyTruncatedAzureCode(code string, hits []azureCodeHit) bool {
	candidate := strings.ToLower(strings.TrimSpace(code))
	if candidate == "" {
		return true
	}
	for _, hit := range hits {
		other := strings.ToLower(strings.TrimSpace(hit.Code))
		if other == "" || len(other) <= len(candidate) {
			continue
		}
		if strings.HasPrefix(other, candidate) {
			return true
		}
	}
	return false
}

func extractAzureMessageForCode(text string, code string) string {
	targetCode := strings.TrimSpace(code)
	if targetCode == "" {
		return ""
	}
	// Capture the nearest message paired with the same code token, including
	// nested payloads that may have been JSON-escaped in the original raw text.
	pattern := `(?is)(?:ERROR CODE:\s*` + regexp.QuoteMeta(targetCode) + `|"code"\s*:\s*"` + regexp.QuoteMeta(targetCode) + `").{0,900}"message"\s*:\s*"([^"]+)"`
	reCodeMessage := regexp.MustCompile(pattern)
	matches := reCodeMessage.FindAllStringSubmatch(text, -1)
	for i := len(matches) - 1; i >= 0; i-- {
		if len(matches[i]) < 2 {
			continue
		}
		message := strings.TrimSpace(matches[i][1])
		if message == "" {
			continue
		}
		return message
	}
	return ""
}

func summarizeAzureDetailMessage(message string) string {
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return ""
	}

	loweredRaw := strings.ToLower(trimmed)
	if strings.Contains(loweredRaw, `"code":`) ||
		strings.Contains(loweredRaw, `\"code\"`) ||
		strings.Contains(loweredRaw, `"status":`) ||
		strings.Contains(loweredRaw, `\"status\"`) {
		return ""
	}
	if strings.Count(trimmed, "{")+strings.Count(trimmed, "}") >= 2 {
		return ""
	}

	normalized := cleanCanonical(trimmed)
	normalized = reQuotaRequiredAvailable.ReplaceAllString(normalized, "required <count>, available <count>")
	if idx := strings.Index(strings.ToLower(normalized), "allocation failed."); idx >= 0 {
		normalized = strings.TrimSpace(normalized[idx:])
	}
	lowered := strings.ToLower(normalized)
	for _, generic := range []string{
		"at least one resource deployment operation failed",
		"the resource write operation failed to complete successfully",
		"operation failed due to an internal server error",
		"internal server error",
	} {
		if strings.Contains(lowered, generic) {
			return ""
		}
	}

	if idx := strings.Index(lowered, " for more details,"); idx >= 0 {
		normalized = strings.TrimSpace(normalized[:idx])
		lowered = strings.ToLower(normalized)
	}
	if idx := strings.Index(lowered, " refer to "); idx >= 0 {
		normalized = strings.TrimSpace(normalized[:idx])
	}
	if idx := strings.Index(normalized, ". "); idx > 0 && idx < 220 {
		normalized = strings.TrimSpace(normalized[:idx+1])
	}

	if len(strings.Fields(normalized)) < 3 && !strings.EqualFold(strings.TrimSpace(normalized), "Allocation failed.") {
		return ""
	}
	return truncateCanonical(normalized, 180)
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

func refineDeserializationNoOutputPicked(raw string, picked string) string {
	if !containsDeserializationNoOutputSignal(raw) && !containsDeserializationNoOutputSignal(picked) {
		return picked
	}
	if commandLine := lastCommandErrorLine(raw); commandLine != "" {
		return commandLine
	}
	return picked
}

// refineCommandErrorExitStatusOnly replaces a bare "Command Error: exit status
// N" pick with a more informative signal line from the surrounding text when
// one is available.  A bare exit-status string carries no actionable detail.
func refineCommandErrorExitStatusOnly(raw string, picked string) string {
	normalized := strings.ToLower(collapseWS(strings.TrimSpace(picked)))
	if normalized != "command error: exit status 1" &&
		normalized != "command error: exit status 2" &&
		normalized != "command error: exit status 3" {
		return picked
	}
	if refined := bestSignalErrorLine(raw); refined != "" {
		return refined
	}
	return picked
}

func containsDeserializationNoOutputSignal(value string) bool {
	return reDeserializationNoOutput.MatchString(strings.TrimSpace(value))
}

func containsDeserializationErrorToken(value string) bool {
	return reDeserializationToken.MatchString(strings.TrimSpace(value))
}

func lastCommandErrorLine(value string) string {
	matches := reCommandErrorLine.FindAllString(value, -1)
	for i := len(matches) - 1; i >= 0; i-- {
		line := strings.TrimSpace(matches[i])
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "Command Error: no output from command") {
			continue
		}
		return line
	}
	return ""
}

func bestSignalErrorLine(text string) string {
	lines := splitNonEmptyLines(text)
	if len(lines) == 0 {
		return ""
	}

	preStruct := make([]string, 0, len(lines))
	postStruct := make([]string, 0, len(lines))
	inStructBlock := false
	for _, line := range lines {
		token := strings.TrimSpace(line)
		if token == "" {
			continue
		}
		if isStructBoundaryLine(token) {
			inStructBlock = true
			continue
		}
		if !rePickErrorSignal.MatchString(token) || isAssertionTail(token) {
			continue
		}
		if isWrapperNoiseLine(token) || isStructFieldNoiseLine(token) || isStatusBannerLine(token) {
			continue
		}
		if inStructBlock {
			postStruct = append(postStruct, token)
		} else {
			preStruct = append(preStruct, token)
		}
	}

	if len(preStruct) > 0 {
		return truncateText(preStruct[len(preStruct)-1], 260)
	}
	if len(postStruct) > 0 {
		return truncateText(postStruct[len(postStruct)-1], 260)
	}
	return ""
}

func bestHTTPResponseStatusLine(text string) string {
	lines := splitNonEmptyLines(text)
	for _, line := range lines {
		token := collapseWS(line)
		if reHTTPResponseStatusLine.MatchString(token) {
			return truncateText(token, 260)
		}
	}
	return ""
}

func isStructBoundaryLine(line string) bool {
	switch strings.TrimSpace(line) {
	case "{", "}", "},", "[", "]", "],":
		return true
	default:
		return false
	}
}

func isWrapperNoiseLine(line string) bool {
	normalized := strings.ToLower(collapseWS(line))
	if normalized == "" {
		return true
	}
	if isEventuallyWrapperLine(normalized) || isTimedOutAfterLine(normalized) {
		return true
	}
	return strings.HasPrefix(normalized, "fail [") ||
		strings.HasPrefix(normalized, "unexpected error") ||
		strings.HasPrefix(normalized, "<*fmt.wraperror") ||
		strings.HasPrefix(normalized, "<*errors.errorstring") ||
		strings.HasPrefix(normalized, "<*")
}

func isStructFieldNoiseLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	normalized := strings.ToLower(collapseWS(trimmed))
	switch normalized {
	case "", "{", "}", "},", "[]", "{}", "{},", "null":
		return true
	}
	if strings.HasPrefix(normalized, "msg:") || strings.HasPrefix(normalized, "err:") {
		return true
	}
	if strings.HasPrefix(normalized, "istimeout:") ||
		strings.HasPrefix(normalized, "istemporary:") ||
		strings.HasPrefix(normalized, "isnotfound:") ||
		strings.HasPrefix(normalized, "server:") {
		return true
	}
	if strings.HasPrefix(normalized, "errorcode:\"\"") || strings.HasPrefix(normalized, "errorcode: \"\"") || strings.HasPrefix(normalized, "errorcode:''") || strings.HasPrefix(normalized, "errorcode: ''") {
		return true
	}
	if strings.Contains(normalized, "<context.") && strings.Contains(normalized, "{") {
		return true
	}
	return strings.HasPrefix(trimmed, "<*") && strings.Contains(trimmed, "{")
}

func isStatusBannerLine(line string) bool {
	normalized := strings.ToLower(collapseWS(line))
	return strings.HasPrefix(normalized, "response ") ||
		strings.HasPrefix(normalized, "error code unavailable") ||
		strings.HasPrefix(normalized, "response contained no body")
}

func isLowInformationCanonical(value string) bool {
	canonical := strings.TrimSpace(value)
	normalized := strings.ToLower(collapseWS(canonical))
	if normalized == "" {
		return true
	}
	if _, found := wrapperOnly[normalized]; found {
		return true
	}
	if reUnexpectedOnly.MatchString(canonical) {
		return true
	}
	if isStructBoundaryLine(canonical) || isStructFieldNoiseLine(canonical) {
		return true
	}
	if strings.Contains(normalized, "<context.") && strings.Contains(normalized, "{") {
		return true
	}
	return strings.Contains(normalized, "errorcode:\"\"") || strings.Contains(normalized, "errorcode: \"\"") || strings.Contains(normalized, "errorcode:''") || strings.Contains(normalized, "errorcode: ''")
}

func bestContextDeadlineDetail(text string) string {
	lines := splitNonEmptyLines(text)
	best := ""
	for _, line := range lines {
		token := collapseWS(line)
		lowered := strings.ToLower(token)
		if token == "" || isWrapperNoiseLine(token) || isStructFieldNoiseLine(token) || isStatusBannerLine(token) || isAssertionTail(token) {
			continue
		}
		if reRateLimiterDeadline.MatchString(token) {
			return truncateText(token, 260)
		}
		if reClusterOperatorsUnavailable.MatchString(token) {
			best = token
			continue
		}
		if strings.Contains(lowered, "context deadline exceeded") && lowered != "context deadline exceeded" {
			best = token
		}
	}
	if best != "" {
		return truncateText(best, 260)
	}
	if route := reRouteHostNeverFound.FindString(text); route != "" {
		return truncateText(route, 260)
	}
	return ""
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
