package normalize

import (
	"regexp"
	"strings"
)

var (
	ansiRE                      = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)
	urlRE                       = regexp.MustCompile(`https?://[^\s"']+`)
	uuidRE                      = regexp.MustCompile(`\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	shaRE                       = regexp.MustCompile(`sha256:[0-9a-f]{16,}`)
	longHexRE                   = regexp.MustCompile(`\b[0-9a-f]{16,}\b`)
	isoTSRE                     = regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}t\d{2}:\d{2}:\d{2}(?:\.\d+)?z\b`)
	quotedTSRE                  = regexp.MustCompile(`timestamp '\d{8}t\d{6}z'`)
	goFileLocationRE            = regexp.MustCompile(`\b[a-z0-9._/\-]+\.go:\d+\b`)
	resourceGroupJobRE          = regexp.MustCompile(`hcp-underlay-prow-j[0-9]{6,}`)
	customerRGRE                = regexp.MustCompile(`customer-rg-[a-z0-9-]+`)
	dnsShardRE                  = regexp.MustCompile(`j[0-9]{6,}\.hcp\.osadev\.cloud`)
	clusterQuotedRE             = regexp.MustCompile(`\bcluster="[a-z0-9-]+"`)
	clusterBareRE               = regexp.MustCompile(`\bcluster=[a-z0-9<>-]+`)
	resourceGroupQuotedRE       = regexp.MustCompile(`\bresourcegroup="[a-z0-9-]+"`)
	resourceGroupBareRE         = regexp.MustCompile(`\bresourcegroup=[a-z0-9<>-]+`)
	resourceGroupSingleQuotedRE = regexp.MustCompile(`resource group '[^']+'`)
	nodepoolQuotedRE            = regexp.MustCompile(`\bnodepool="[a-z0-9-]+"`)
	nodepoolBareRE              = regexp.MustCompile(`\bnodepool=[a-z0-9<>-]+`)
	repeatedWhitespaceRE        = regexp.MustCompile(`\s+`)
)

// Text normalizes a raw failure snippet into a stable fingerprinting input.
func Text(raw string) string {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return ""
	}

	normalized = ansiRE.ReplaceAllString(normalized, " ")
	normalized = urlRE.ReplaceAllString(normalized, "<url>")
	normalized = uuidRE.ReplaceAllString(normalized, "<uuid>")
	normalized = shaRE.ReplaceAllString(normalized, "sha256:<hash>")
	normalized = longHexRE.ReplaceAllString(normalized, "<hex>")
	normalized = isoTSRE.ReplaceAllString(normalized, "<ts>")
	normalized = quotedTSRE.ReplaceAllString(normalized, "timestamp <ts>")
	normalized = goFileLocationRE.ReplaceAllString(normalized, "<file>:<line>")
	normalized = resourceGroupJobRE.ReplaceAllString(normalized, "hcp-underlay-prow-<job>")
	normalized = customerRGRE.ReplaceAllString(normalized, "customer-rg-<id>")
	normalized = clusterQuotedRE.ReplaceAllString(normalized, `cluster="<cluster>"`)
	normalized = clusterBareRE.ReplaceAllString(normalized, "cluster=<cluster>")
	normalized = resourceGroupQuotedRE.ReplaceAllString(normalized, `resourcegroup="<resourcegroup>"`)
	normalized = resourceGroupBareRE.ReplaceAllString(normalized, "resourcegroup=<resourcegroup>")
	normalized = resourceGroupSingleQuotedRE.ReplaceAllString(normalized, "resource group '<resourcegroup>'")
	normalized = nodepoolQuotedRE.ReplaceAllString(normalized, `nodepool="<nodepool>"`)
	normalized = nodepoolBareRE.ReplaceAllString(normalized, "nodepool=<nodepool>")
	normalized = dnsShardRE.ReplaceAllString(normalized, "<job>.hcp.osadev.cloud")
	normalized = repeatedWhitespaceRE.ReplaceAllString(normalized, " ")
	normalized = strings.TrimSpace(normalized)

	const maxLen = 1024
	if len(normalized) > maxLen {
		normalized = normalized[:maxLen]
	}

	return normalized
}
