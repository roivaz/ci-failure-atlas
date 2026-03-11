package phase1

import (
	"strings"
	"testing"
)

func TestExtractEvidencePrefersPreStructDeadlineLine(t *testing.T) {
	t.Parallel()

	raw := `fail [github.com/Azure/ARO-HCP/test/e2e/gpu_nodepools_create_delete.go:96]: Unexpected error:
    <*fmt.wrapError | 0xc0004ac420>: 
    failed waiting for deployment "aro-hcp-demo" in resourcegroup="gpu-nodepools-NC4asT4v3-z4g56q" to finish: context deadline exceeded
    {
        msg: "failed waiting for deployment \"aro-hcp-demo\" in resourcegroup=\"gpu-nodepools-NC4asT4v3-z4g56q\" to finish: context deadline exceeded",
        err: <context.deadlineExceededError>{},
    }`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	lowered := strings.ToLower(got)
	if strings.Contains(lowered, "<context.deadlineexceedederror>{},") {
		t.Fatalf("expected context type stub to be excluded from canonical phrase, got=%q", got)
	}
	if !strings.Contains(lowered, "failed waiting for deployment") {
		t.Fatalf("expected deployment timeout line in canonical phrase, got=%q", got)
	}
}

func TestExtractEvidenceAvoidsEmptyErrorCodeStructField(t *testing.T) {
	t.Parallel()

	raw := `fail [github.com/Azure/ARO-HCP/test/e2e/cluster_authorized_cidrs_connectivity.go:133]: Unexpected error:
    <*fmt.wrapError | 0xc00097b920>: 
    failed to create HCP cluster cidr-connectivity-test: failed starting cluster creation "cidr-connectivity-test" in resourcegroup="e2e-cidr-connectivity-f9k9vw": PUT https://management.azure.com/subscriptions/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/resourceGroups/e2e-cidr-connectivity-f9k9vw/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/cidr-connectivity-test
    --------------------------------------------------------------------------------
    RESPONSE 502: 502 Bad Gateway
    ERROR CODE UNAVAILABLE
    --------------------------------------------------------------------------------
    Response contained no body
    --------------------------------------------------------------------------------
    {
        msg: "failed to create HCP cluster cidr-connectivity-test",
        err: <*azcore.ResponseError | 0xc0002a6d80>{
            ErrorCode: "",
        },
    }`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	lowered := strings.ToLower(got)
	if strings.Contains(lowered, "errorcode: \"\"") || strings.Contains(lowered, "errorcode:\"\"") {
		t.Fatalf("expected empty error code struct field to be excluded from canonical phrase, got=%q", got)
	}
	if !strings.Contains(lowered, "failed to create hcp cluster") {
		t.Fatalf("expected cluster create failure line in canonical phrase, got=%q", got)
	}
}

func TestExtractEvidenceAvoidsBraceOnlyCanonicalFromWrappedErrors(t *testing.T) {
	t.Parallel()

	raw := `fail [github.com/Azure/ARO-HCP/test/e2e/cluster_version_backlevel.go:193]: Unexpected error:
    <*fmt.wrapError | 0xc000a823a0>: 
    route host was never found: Get "https://agnhost-e2e-serving-app-p8ds6.apps.aro.example.net": tls: failed to verify certificate: x509: certificate signed by unknown authority
    {
        msg: "route host was never found",
        err: <*url.Error | 0xc000e42f90>{
            Err: <*tls.CertificateVerificationError | 0xc000e42f60>{
                Err: <*x509.UnknownAuthorityError | 0xc0003ca3f0>{},
            },
        },
    }`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	lowered := strings.ToLower(got)
	if got == "{" || got == "}" || got == "{}" || got == "null" {
		t.Fatalf("expected non-struct canonical phrase, got=%q", got)
	}
	if !strings.Contains(lowered, "route host was never found") && !strings.Contains(lowered, "certificate signed by unknown authority") {
		t.Fatalf("expected wrapped error details in canonical phrase, got=%q", got)
	}
}

func TestSafeSearchFromTextSkipsFrameworkWrapperLine(t *testing.T) {
	t.Parallel()

	raw := `fail [github.com/Azure/ARO-HCP/test/e2e/cluster_authorized_cidrs_connectivity.go:133]: Unexpected error:
    <*fmt.wrapError | 0xc00097b920>: 
    failed to create HCP cluster cidr-connectivity-test: failed starting cluster creation "cidr-connectivity-test" in resourcegroup="e2e-cidr-connectivity-f9k9vw": PUT https://management.azure.com/subscriptions/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/resourceGroups/e2e-cidr-connectivity-f9k9vw/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/cidr-connectivity-test
    {
        msg: "failed to create HCP cluster cidr-connectivity-test",
        err: <*azcore.ResponseError | 0xc0002a6d80>{
            ErrorCode: "",
        },
    }`

	got := safeSearchFromText(raw)
	lowered := strings.ToLower(got)
	if strings.HasPrefix(lowered, "fail [") {
		t.Fatalf("expected safe search phrase to avoid framework wrapper line, got=%q", got)
	}
	if !strings.Contains(lowered, "failed to create hcp cluster") {
		t.Fatalf("expected safe search phrase to include actionable failure line, got=%q", got)
	}
}

func TestExtractEvidenceCollapsesGetAdminRESTConfigTimeoutVariants(t *testing.T) {
	t.Parallel()

	rawA := `failed waiting for hcpcluster="ea-list" in resourcegroup="external-auth-rg-pxk72q" to finish getting creds, caused by: timeout '10.000000' minutes exceeded during GetAdminRESTConfigForHCPCluster for cluster ea-list`
	rawB := `failed waiting for hcpcluster="ea-list" in resourcegroup="external-auth-rg-pxk72q" to finish getting creds, caused by: timeout '10.000000' minutes exceeded during GetAdminRESTConfigForHCPCluster for cluster ea-list in resource group external-auth-rg-pxk72q`

	gotA := extractEvidence(rawA).CanonicalEvidencePhrase
	gotB := extractEvidence(rawB).CanonicalEvidencePhrase
	want := "timeout during GetAdminRESTConfigForHCPCluster while waiting for hcpcluster creds"

	if gotA != want {
		t.Fatalf("unexpected canonical for short variant: got=%q want=%q", gotA, want)
	}
	if gotB != want {
		t.Fatalf("unexpected canonical for long variant: got=%q want=%q", gotB, want)
	}
}

func TestExtractEvidenceUsesHTTP502StatusLineWhenOnlySignal(t *testing.T) {
	t.Parallel()

	raw := `fail [github.com/Azure/ARO-HCP/test/e2e/cluster_versions.go:42]: Unexpected error:
    <*exported.ResponseError | 0xc000736630>:
    GET https://management.azure.com/subscriptions/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/providers/Microsoft.RedHatOpenShift/locations/uksouth/hcpOpenShiftVersions
    --------------------------------------------------------------------------------
    RESPONSE 502: 502 Bad Gateway
    ERROR CODE UNAVAILABLE
    --------------------------------------------------------------------------------
    Response contained no body
    --------------------------------------------------------------------------------
    {
        ErrorCode: "",
        StatusCode: 502,
    }`

	evidence := extractEvidence(raw)
	if evidence.CanonicalEvidencePhrase != "RESPONSE 502: 502 Bad Gateway" {
		t.Fatalf("expected canonical phrase to use HTTP response status line, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if evidence.SearchQueryPhrase != "RESPONSE 502: 502 Bad Gateway" {
		t.Fatalf("expected search phrase to use HTTP response status line, got=%q", evidence.SearchQueryPhrase)
	}
}
