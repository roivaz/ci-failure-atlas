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

func TestExtractEvidencePrefersCommandErrorWhenDeserializationNoOutputPresent(t *testing.T) {
	t.Parallel()

	raw := `goroutine 1383 gp=0xc00161cfc0 m=nil [sync.WaitGroup.Wait, 3 minutes]:
runtime.gopark(0xc001729af0?, 0x2a657d4?, 0x20?, 0xb9?, 0x7ffb3a4a5d06?)
Command Error: exit status 2
Deserializaion Error: no output from command
crypto/tls.(*Conn).readFromUntil(0xc000806e08, {0x81cbfc0, 0xc000d38128}, 0xc0003829d0?)`

	evidence := extractEvidence(raw)
	if evidence.CanonicalEvidencePhrase != "Command Error: exit status 2" {
		t.Fatalf("expected canonical phrase to use command error line, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if evidence.SearchQueryPhrase != "Command Error: exit status 2" {
		t.Fatalf("expected search phrase to use command error line, got=%q", evidence.SearchQueryPhrase)
	}
}

func TestExtractEvidenceKeepsDeserializationNoOutputWithoutCommandError(t *testing.T) {
	t.Parallel()

	raw := `Deserializaion Error: no output from command
goroutine 1 [running]:
runtime.throw({0x1, 0x2})`

	evidence := extractEvidence(raw)
	if evidence.CanonicalEvidencePhrase != "Deserializaion Error: no output from command" {
		t.Fatalf("expected canonical phrase to remain deserialization no-output fallback, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceUsesAzureInnerThrottlingCodeAndMessage(t *testing.T) {
	t.Parallel()

	raw := `ERROR CODE: InternalServerError
{
  "error": {
    "code": "InternalServerError",
    "message": "failed to get managed identity '/subscriptions/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/resourceGroups/aro-hcp-test-msi-containers-dev-297/providers/Microsoft.ManagedIdentity/userAssignedIdentities/image-registry': GET https://management.azure.com/subscriptions/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/resourceGroups/aro-hcp-test-msi-containers-dev-297/providers/Microsoft.ManagedIdentity/userAssignedIdentities/image-registry\n--------------------------------------------------------------------------------\nRESPONSE 429: 429 Too Many Requests\nERROR CODE: SubscriptionRequestsThrottled\n--------------------------------------------------------------------------------\n{\n  \"error\": {\n    \"code\": \"SubscriptionRequestsThrottled\",\n    \"message\": \"Number of 'read' requests for subscription 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' actor 'd6b62dfa-87f5-49b3-bbcb-4a687c4faa96' exceeded. Please try again after '10' seconds after additional tokens are available. Refer to https://aka.ms/arm-throttling for additional information.\"\n  }\n}\n--------------------------------------------------------------------------------\n"
  }
}`

	evidence := extractEvidence(raw)
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "ERROR CODE: InternalServerError") {
		t.Fatalf("expected canonical phrase to keep root error code, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "detail code SubscriptionRequestsThrottled") {
		t.Fatalf("expected canonical phrase to include inner throttling code, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if !strings.Contains(strings.ToLower(evidence.CanonicalEvidencePhrase), "detail message number of 'read' requests for subscription") {
		t.Fatalf("expected canonical phrase to include throttling message summary, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if strings.Contains(evidence.CanonicalEvidencePhrase, "provider Microsoft.ManagedIdentity") {
		t.Fatalf("expected provider-only fallback to be replaced by inner detail, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceUsesAzureNestedInvalidRequestDetail(t *testing.T) {
	t.Parallel()

	raw := `ERROR CODE: DeploymentFailed
{
  "status": "Failed",
  "error": {
    "code": "DeploymentFailed",
    "message": "At least one resource deployment operation failed.",
    "details": [
      {
        "code": "Conflict",
        "message": "{\r\n  \"status\": \"Failed\",\r\n  \"error\": {\r\n    \"code\": \"ResourceDeploymentFailure\",\r\n    \"message\": \"The resource write operation failed to complete successfully, because it reached terminal provisioning state 'Failed'.\",\r\n    \"details\": [\r\n      {\r\n        \"code\": \"DeploymentFailed\",\r\n        \"message\": \"At least one resource deployment operation failed.\",\r\n        \"details\": [\r\n          {\r\n            \"code\": \"InvalidRequest\",\r\n            \"message\": \"The current utilization does not meet the criteria for both MaxTimeSeries and MaxEventsPerMinute quota requested. Please reach the required usage threshold of 50% of desired limit before requesting an increase, or request a limit increase of up to 200% of your current usage. For more details, see https://go.microsoft.com/fwlink/?linkid=2270124\"\r\n          }\r\n        ]\r\n      }\r\n    ]\r\n  }\r\n}"
      }
    ]
  }
}`

	evidence := extractEvidence(raw)
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "ERROR CODE: DeploymentFailed") {
		t.Fatalf("expected canonical phrase to keep root deployment code, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "detail code InvalidRequest") {
		t.Fatalf("expected canonical phrase to include nested invalid request code, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if !strings.Contains(strings.ToLower(evidence.CanonicalEvidencePhrase), "detail message the current utilization does not meet the criteria") {
		t.Fatalf("expected canonical phrase to include quota message summary, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceKeepsGenericAzureConflictDetailCode(t *testing.T) {
	t.Parallel()

	raw := `time=2026-03-02T16:29:57.122Z level=ERROR msg="Step errored." err="failed to run ARM step: failed to wait for deployment completion: GET https://management.azure.com/subscriptions/123/resourcegroups/hcp-underlay/providers/Microsoft.EventGrid/namespaces/arohcp/providers/Microsoft.Resources/deployments/x
ERROR CODE: DeploymentFailed
{ "error": { "code": "DeploymentFailed", "details": [ { "code": "Conflict", "message": "operation failed due to an internal server error" } ] } }"`

	evidence := extractEvidence(raw)
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "detail code Conflict") {
		t.Fatalf("expected canonical phrase to keep generic inner conflict code, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if strings.Contains(strings.ToLower(evidence.CanonicalEvidencePhrase), "provider microsoft.eventgrid") {
		t.Fatalf("expected canonical phrase to prefer inner detail code over provider fallback, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceUsesAzureNestedRoleAssignmentLimitDetail(t *testing.T) {
	t.Parallel()

	raw := `ERROR CODE: DeploymentFailed
{
  "status": "Failed",
  "error": {
    "code": "DeploymentFailed",
    "message": "At least one resource deployment operation failed.",
    "details": [
      {
        "code": "Conflict",
        "message": "{\r\n  \"error\": {\r\n    \"code\": \"RoleAssignmentLimitExceeded\",\r\n    \"message\": \"The role assignment limit for the subscription has been reached.\"\r\n  }\r\n}"
      }
    ]
  }
}`

	evidence := extractEvidence(raw)
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "detail code RoleAssignmentLimitExceeded") {
		t.Fatalf("expected canonical phrase to include role-assignment detail code, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if !strings.Contains(strings.ToLower(evidence.CanonicalEvidencePhrase), "detail message the role assignment limit for the subscription has been reached") {
		t.Fatalf("expected canonical phrase to include role-assignment detail message, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceUsesAzureOverconstrainedAllocationDetail(t *testing.T) {
	t.Parallel()

	raw := `ERROR CODE: DeploymentFailed
{
  "status": "Failed",
  "error": {
    "code": "DeploymentFailed",
    "message": "At least one resource deployment operation failed.",
    "details": [
      {
        "code": "OverconstrainedZonalAllocationRequest",
        "message": "Allocation failed. We do not have sufficient capacity for the requested VM size in this zone. Please try again later."
      }
    ]
  }
}`

	evidence := extractEvidence(raw)
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "detail code OverconstrainedZonalAllocationRequest") {
		t.Fatalf("expected canonical phrase to include zonal allocation detail code, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if !strings.Contains(strings.ToLower(evidence.CanonicalEvidencePhrase), "detail message allocation failed.") {
		t.Fatalf("expected canonical phrase to include normalized allocation failure summary, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceSkipsTruncatedAzureDetailCodeSuffix(t *testing.T) {
	t.Parallel()

	raw := `ERROR CODE: InternalServerError
{
  "error": {
    "code": "InternalServerError",
    "message": "inner payload",
    "details": [
      {
        "code": "SubscriptionRequestsThrottled",
        "message": "Number of 'read' requests for subscription 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' actor '11111111-2222-3333-4444-555555555555' exceeded."
      }
    ]
  }
}
ERROR CODE: Subsc`

	evidence := extractEvidence(raw)
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "detail code SubscriptionRequestsThrottled") {
		t.Fatalf("expected canonical phrase to keep full inner code instead of truncated suffix, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if strings.Contains(evidence.CanonicalEvidencePhrase, "detail code Subsc;") {
		t.Fatalf("expected canonical phrase to exclude truncated inner code suffix, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceUsesRootAzureMessageWhenNoInnerCode(t *testing.T) {
	t.Parallel()

	raw := `ERROR CODE: InternalServerError
{
  "id": "/subscriptions/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/providers/Microsoft.RedHatOpenShift/locations/eastus2euap/hcpOperationStatuses/b4b3429d-a139-425f-b110-f68346a4fe8b",
  "error": {
    "code": "InternalServerError",
    "message": "insufficient public IP address quota: required 2, available 0"
  }
}`

	evidence := extractEvidence(raw)
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "ERROR CODE: InternalServerError") {
		t.Fatalf("expected canonical phrase to keep root code, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if !strings.Contains(strings.ToLower(evidence.CanonicalEvidencePhrase), "detail message insufficient public ip address quota: required <count>, available <count>") {
		t.Fatalf("expected canonical phrase to include normalized root Azure quota message, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceNormalizesQuotaCountVariants(t *testing.T) {
	t.Parallel()

	rawA := `ERROR CODE: InternalServerError
{
  "error": {
    "code": "InternalServerError",
    "message": "insufficient public IP address quota: required 2, available 0"
  }
}`
	rawB := `ERROR CODE: InternalServerError
{
  "error": {
    "code": "InternalServerError",
    "message": "insufficient public IP address quota: required 2, available 1"
  }
}`

	gotA := extractEvidence(rawA).CanonicalEvidencePhrase
	gotB := extractEvidence(rawB).CanonicalEvidencePhrase
	if gotA != gotB {
		t.Fatalf("expected quota count variants to normalize to same canonical phrase: gotA=%q gotB=%q", gotA, gotB)
	}
	if !strings.Contains(strings.ToLower(gotA), "required <count>, available <count>") {
		t.Fatalf("expected normalized quota count placeholders in canonical phrase, got=%q", gotA)
	}
}

func TestExtractEvidenceEventuallyWrapperPrefersLineBeforeExpected(t *testing.T) {
	t.Parallel()

	raw := `fail [github.com/Azure/ARO-HCP/test/e2e/cluster_authorized_cidrs_connectivity.go:390]: Timed out after 600.002s.
The function passed to Eventually failed at /opt/app-root/src/github.com/Azure/ARO-HCP/test/e2e/cluster_authorized_cidrs_connectivity.go:389 with:
All ClusterOperators should report Available=True, but these are not available: [image-registry (Available=False)]
Expected
    <[]string | len:1, cap:1>: [
        "image-registry (Available=False)",
    ]
to be empty`

	evidence := extractEvidence(raw)
	lowered := strings.ToLower(evidence.CanonicalEvidencePhrase)
	if !strings.Contains(lowered, "all clusteroperators should report available=true, but these are not available") {
		t.Fatalf("expected canonical phrase to use context line before Expected, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if strings.Contains(lowered, "the function passed to eventually failed at") {
		t.Fatalf("expected Eventually wrapper line to be excluded from canonical phrase, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceContextDeadlinePrefersInnerDetail(t *testing.T) {
	t.Parallel()

	raw := `Unexpected error:
    <*errors.joinError | 0xc001101680>: 
    context deadline exceeded
    cluster operators not available: image-registry (Available=False, Progressing=True, Degraded=True)
    {
        errs: [
            <context.deadlineExceededError>{},
            <*errors.errorString | 0xc0006ec9b0>{
                s: "cluster operators not available: image-registry (Available=False, Progressing=True, Degraded=True)",
            },
        ],
    }`

	evidence := extractEvidence(raw)
	lowered := strings.ToLower(evidence.CanonicalEvidencePhrase)
	if !strings.Contains(lowered, "cluster operators not available: image-registry") {
		t.Fatalf("expected canonical phrase to include inner cluster-operator detail, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if lowered == "context deadline exceeded" {
		t.Fatalf("expected canonical phrase to be more specific than generic context deadline wrapper, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceSkipsIsTimeoutStructFieldAndUsesRouteHostLine(t *testing.T) {
	t.Parallel()

	raw := `Err: "no such host",
Name: "agnhost-e2e-serving-app-sckjc.apps.aro.example.net",
Server: "172.30.0.10:53",
IsTimeout: false,
IsTemporary: false,
IsNotFound: true,
occurred
fail [github.com/Azure/ARO-HCP/test/e2e/complete_cluster_create.go:137]: Unexpected error:
    <*fmt.wrapError | 0xc000e143c0>: 
    route host was never found: Get "https://agnhost-e2e-serving-app-sckjc.apps.aro.example.net": dial tcp: lookup agnhost-e2e-serving-app-sckjc.apps.aro.example.net on 172.30.0.10:53: no such host`

	evidence := extractEvidence(raw)
	lowered := strings.ToLower(evidence.CanonicalEvidencePhrase)
	if strings.Contains(lowered, "istimeout: false") {
		t.Fatalf("expected struct field dump line to be excluded from canonical phrase, got=%q", evidence.CanonicalEvidencePhrase)
	}
	if !strings.Contains(lowered, "route host was never found") {
		t.Fatalf("expected canonical phrase to use route host failure line, got=%q", evidence.CanonicalEvidencePhrase)
	}
}

func TestExtractEvidenceNormalizesRouteHostLookupVariants(t *testing.T) {
	t.Parallel()

	rawA := `fail [github.com/Azure/ARO-HCP/test/e2e/complete_cluster_create.go:137]: Unexpected error:
route host was never found: Get "https://agnhost-e2e-serving-app-sckjc.apps.aro.u0e2e1n2t9u1a8h.4rck.j1302400.hcp.osadev.cloud": dial tcp: lookup agnhost-e2e-serving-app-sckjc.apps.aro.u0e2e1n2t9u1a8h.4rck.j1302400.hcp.osadev.cloud on 172.30.0.10:53: no such host`
	rawB := `fail [github.com/Azure/ARO-HCP/test/e2e/cluster_version_backlevel.go:194]: Unexpected error:
route host was never found: Get "https://agnhost-e2e-serving-app-9f7x5.apps.aro.l6q3l5t4y9r4i6k.15br.j8542976.hcp.osadev.cloud": dial tcp: lookup agnhost-e2e-serving-app-9f7x5.apps.aro.l6q3l5t4y9r4i6k.15br.j8542976.hcp.osadev.cloud on 172.30.0.10:53: no such host`

	gotA := extractEvidence(rawA).CanonicalEvidencePhrase
	gotB := extractEvidence(rawB).CanonicalEvidencePhrase
	if gotA != gotB {
		t.Fatalf("expected route-host variants to normalize to same canonical phrase: gotA=%q gotB=%q", gotA, gotB)
	}
	if !strings.Contains(strings.ToLower(gotA), "lookup <host> on <dns-server>") {
		t.Fatalf("expected canonical phrase to normalize lookup host/server details, got=%q", gotA)
	}
}
