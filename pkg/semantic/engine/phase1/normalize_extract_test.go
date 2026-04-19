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
	// Provider is always appended to ERROR CODE patterns; verify it is present
	// in addition to the richer inner detail (not instead of it).
	if !strings.Contains(evidence.CanonicalEvidencePhrase, "provider Microsoft.ManagedIdentity") {
		t.Fatalf("expected provider Microsoft.ManagedIdentity to be included alongside inner detail, got=%q", evidence.CanonicalEvidencePhrase)
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
	// Provider is always appended alongside the detail code.
	if !strings.Contains(strings.ToLower(evidence.CanonicalEvidencePhrase), "provider microsoft.eventgrid") {
		t.Fatalf("expected provider Microsoft.EventGrid to be appended alongside detail code, got=%q", evidence.CanonicalEvidencePhrase)
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

// --- Regression tests for semantic quality improvements ---

// Finding 1: prow entrypoint JSON lines that differ only by "time" field must
// produce the same canonical phrase.
func TestExtractEvidenceProwEntrypointTimestampsMerge(t *testing.T) {
	t.Parallel()

	rawA := `{"component":"entrypoint","file":"sigs.k8s.io/prow/pkg/entrypoint/run.go:169","func":"sigs.k8s.io/prow/pkg/entrypoint.Options.ExecuteProcess","level":"error","msg":"Process did not finish before 2h0m0s timeout","severity":"error","time":"2026-04-12T09:52:11Z"}`
	rawB := `{"component":"entrypoint","file":"sigs.k8s.io/prow/pkg/entrypoint/run.go:169","func":"sigs.k8s.io/prow/pkg/entrypoint.Options.ExecuteProcess","level":"error","msg":"Process did not finish before 2h0m0s timeout","severity":"error","time":"2026-04-16T22:04:25Z"}`

	gotA := extractEvidence(rawA).CanonicalEvidencePhrase
	gotB := extractEvidence(rawB).CanonicalEvidencePhrase
	if gotA != gotB {
		t.Fatalf("prow entrypoint lines with different timestamps must canonicalize identically:\n  A=%q\n  B=%q", gotA, gotB)
	}
	if !strings.Contains(gotA, "Process did not finish before 2h0m0s timeout") {
		t.Fatalf("expected prow entrypoint msg in canonical phrase, got=%q", gotA)
	}
}

// Finding 2: route-host errors that differ only by the raw IP in "dial tcp
// <IP>:443" must produce the same canonical phrase.
func TestExtractEvidenceDialTCPAddressNormalized(t *testing.T) {
	t.Parallel()

	rawA := `fail [github.com/Azure/ARO-HCP/test/e2e/complete_cluster_create_multiversion.go:183]: Unexpected error:
    <*fmt.wrapError | 0xc000c8e620>: 
    route host was never found: Get "https://agnhost-e2e-serving-app-k8g25.apps.aro.u2q3n3k0t9h9m8l.pb5a.hcp.osadev.cloud": dial tcp 134.33.16.231:443: connect: connection timed out`
	rawB := `fail [github.com/Azure/ARO-HCP/test/e2e/cluster_version_backlevel.go:194]: Unexpected error:
    <*fmt.wrapError | 0xc001096040>: 
    route host was never found: Get "https://agnhost-e2e-serving-app-mbnnt.apps.aro.h7i5w5u7j5b3g2u.fova.hcp.osadev.cloud": dial tcp 20.40.25.244:443: connect: connection timed out`

	gotA := extractEvidence(rawA).CanonicalEvidencePhrase
	gotB := extractEvidence(rawB).CanonicalEvidencePhrase
	if gotA != gotB {
		t.Fatalf("route-host dial-tcp errors with different IPs must canonicalize identically:\n  A=%q\n  B=%q", gotA, gotB)
	}
	if strings.Contains(gotA, "134.33") || strings.Contains(gotA, "20.40") {
		t.Fatalf("canonical phrase must not contain raw IP addresses, got=%q", gotA)
	}
}

// Finding 3: Gomega "expected N nodes, found M" assertion — the pattern must
// not be "..." (the Gomega truncation marker).
func TestExtractEvidenceGomegaEllipsisNotExtracted(t *testing.T) {
	t.Parallel()

	raw := `fail [github.com/Azure/ARO-HCP/test/e2e/nodepool_update_nodes.go:262]: Expected success, but got an error:
    <*errors.errorString | 0xc000981550>: 
    expected 4 nodes, found 5
    ...
fail [github.com/Azure/ARO-HCP/test/e2e/nodepool_update_nodes.go:262]: Expected success, but got an error:
    <*errors.errorString | 0xc000981550>: 
    expected 4 nodes, found 5
    ...`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	if got == "..." {
		t.Fatalf("canonical phrase must not be the Gomega ellipsis marker")
	}
	if !strings.Contains(got, "expected 4 nodes, found 5") {
		t.Fatalf("expected the inner assertion error in canonical phrase, got=%q", got)
	}
}

// Finding 4: all CreateHCPCluster* timeout variants (FromParam, AndWait,
// 20251223FromParam, 20251223AndWait) must share a single canonical phrase.
func TestExtractEvidenceCreateHCPClusterTimeoutVariantsUnify(t *testing.T) {
	t.Parallel()

	want := "timeout during CreateHCPClusterAndWait; context deadline exceeded"

	cases := []struct {
		name string
		raw  string
	}{
		{
			"FromParam",
			`failed to create HCP cluster hcp-cluster, caused by: timeout '45.000000' minutes exceeded during CreateHCPClusterFromParam for cluster hcp-cluster in resource group rg-abc, error: failed waiting for cluster="hcp-cluster" in resourcegroup="rg-abc" to finish creating, caused by: timeout '45.000000' minutes exceeded during CreateHCPClusterFromParam for cluster hcp-cluster in resource group rg-abc, error: context deadline exceeded`,
		},
		{
			"20251223FromParam",
			`failed to create HCP cluster idms-e2e-hcp-cluster, caused by: timeout '45.000000' minutes exceeded during CreateHCPCluster20251223FromParam for cluster idms-e2e-hcp-cluster in resource group idms-v9cd6x, error: failed waiting for cluster="idms-e2e-hcp-cluster" in resourcegroup="idms-v9cd6x" to finish creating, caused by: timeout '45.000000' minutes exceeded during CreateHCPCluster20251223FromParam for cluster idms-e2e-hcp-cluster in resource group idms-v9cd6x, error: context deadline exceeded`,
		},
		{
			"20251223AndWait",
			`failed waiting for cluster="cilium-cluster" in resourcegroup="e2e-cilium-hvlzkd" to finish creating, caused by: timeout '45.000000' minutes exceeded during CreateHCPCluster20251223AndWait for cluster cilium-cluster in resource group e2e-cilium-hvlzkd, error: context deadline exceeded`,
		},
		{
			"AndWait",
			`failed waiting for cluster="cluster-ver-4-19" in resourcegroup="rg-cluster-back-version-g5hsfc" to finish creating, caused by: timeout '45.000000' minutes exceeded during CreateHCPClusterAndWait for cluster cluster-ver-4-19 in resource group rg-cluster-back-version-g5hsfc, error: context deadline exceeded`,
		},
	}

	for _, tc := range cases {
		got := extractEvidence(tc.raw).CanonicalEvidencePhrase
		if got != want {
			t.Errorf("case %q: got=%q want=%q", tc.name, got, want)
		}
	}
}

// Finding 5: "Expected success, but got an error:" wrapper must yield the
// inner error, not the Gomega boilerplate phrase.
func TestExtractEvidenceGomegaSuccessFailureExtractsInnerError(t *testing.T) {
	t.Parallel()

	raw := `fail [github.com/Azure/ARO-HCP/test/e2e/cluster_create_private_kv.go:180]: Timed out after 600.005s.
router-default deployment logs should be fetchable
Expected success, but got an error:
    <*errors.errorString | 0xc001178270>: 
    deployment router-default -n openshift-ingress has no running pods
    ...`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	if strings.EqualFold(got, "Expected success, but got an error:") {
		t.Fatalf("canonical phrase must not be the Gomega wrapper line, got=%q", got)
	}
	if !strings.Contains(strings.ToLower(got), "router-default") || !strings.Contains(strings.ToLower(got), "no running pods") {
		t.Fatalf("expected inner deployment error in canonical phrase, got=%q", got)
	}
}

// Finding 6: when both a "RESPONSE 404" status line and an "ERROR CODE:" line
// are present, the ERROR CODE must win.
func TestExtractEvidenceErrorCodePreferredOverResponse404(t *testing.T) {
	t.Parallel()

	raw := `PUT https://management.azure.com/subscriptions/XXXX/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/id/federatedIdentityCredentials/fic
--------------------------------------------------------------------------------
RESPONSE 404: 404 Not Found
ERROR CODE: NotFound
--------------------------------------------------------------------------------
{
  "error": {
    "code": "NotFound",
    "message": "MS Graph resource not found during Federated Identity Credential creation."
  }
}`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	if strings.EqualFold(strings.TrimSpace(got), "RESPONSE 404: 404 Not Found") {
		t.Fatalf("ERROR CODE must be preferred over RESPONSE status line, got=%q", got)
	}
	if !strings.Contains(strings.ToLower(got), "notfound") {
		t.Fatalf("expected NotFound error code in canonical phrase, got=%q", got)
	}
}

// Finding 8: "Internal server error." detail message is generic noise and must
// not appear in the canonical phrase; patterns with and without it should have
// the same canonical text.
func TestExtractEvidenceInternalServerErrorDetailSuppressed(t *testing.T) {
	t.Parallel()

	rawWithDetail := `POST https://management.azure.com/subscriptions/XXXX/resourceGroups/rg/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/cluster/requestAdminCredential
RESPONSE 500: 500 Internal Server Error
ERROR CODE: InternalServerError
{
  "error": {
    "code": "InternalServerError",
    "message": "Internal server error."
  }
}`
	rawWithout := `GET https://rp.example/subscriptions/XXXX/providers/Microsoft.RedHatOpenShift/locations/westus3/hcpOperationStatuses/abc123
RESPONSE 200: 200 OK
ERROR CODE: InternalServerError`

	gotWith := extractEvidence(rawWithDetail).CanonicalEvidencePhrase
	gotWithout := extractEvidence(rawWithout).CanonicalEvidencePhrase
	if gotWith != gotWithout {
		t.Fatalf("InternalServerError with and without generic detail must canonicalize identically:\n  with=%q\n  without=%q", gotWith, gotWithout)
	}
}

// Finding 11: logfmt-style timestamps (time=<ISO8601>) must be stripped so
// that the same step error on different runs produces the same canonical phrase.
func TestExtractEvidenceLogfmtTimestampStripped(t *testing.T) {
	t.Parallel()

	rawA := `time=2026-04-17T11:04:19.211Z level=ERROR msg="Step errored." serviceGroup=Microsoft.Azure.ARO.HCP.Management.Infra resourceGroup=management step=delete-non-swift-user-nodepools err="failed to prepare kubeconfig: failed to ensure cluster admin role: /me request is only valid with delegated authentication flow."`
	rawB := `time=2026-04-18T09:00:00.000Z level=ERROR msg="Step errored." serviceGroup=Microsoft.Azure.ARO.HCP.Management.Infra resourceGroup=management step=delete-non-swift-user-nodepools err="failed to prepare kubeconfig: failed to ensure cluster admin role: /me request is only valid with delegated authentication flow."`

	gotA := extractEvidence(rawA).CanonicalEvidencePhrase
	gotB := extractEvidence(rawB).CanonicalEvidencePhrase
	if gotA != gotB {
		t.Fatalf("logfmt lines with different timestamps must canonicalize identically:\n  A=%q\n  B=%q", gotA, gotB)
	}
	if strings.Contains(gotA, "2026-04-17") || strings.Contains(gotA, "2026-04-18") {
		t.Fatalf("canonical phrase must not contain raw timestamps, got=%q", gotA)
	}
}

// Prow entrypoint: the "msg" field must be extracted so the canonical phrase is
// the human-readable message, not the full JSON object.
func TestExtractEvidenceProwEntrypointExtractsMsgField(t *testing.T) {
	t.Parallel()

	raw := `{"component":"entrypoint","file":"sigs.k8s.io/prow/pkg/entrypoint/run.go:169","func":"sigs.k8s.io/prow/pkg/entrypoint.Options.ExecuteProcess","level":"error","msg":"Process did not finish before 2h0m0s timeout","severity":"error","time":"2026-04-18T01:21:20Z"}`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	if got != "Process did not finish before 2h0m0s timeout" {
		t.Fatalf("expected clean msg value as canonical phrase, got=%q", got)
	}
}

// Provider must always be appended to ERROR CODE canonical phrases when a
// non-ignored resource provider is found in the error text.
func TestExtractEvidenceErrorCodeIncludesProvider(t *testing.T) {
	t.Parallel()

	raw := `GET https://management.azure.com/subscriptions/XXXX/providers/Microsoft.Network/virtualNetworks
RESPONSE 429: 429 Too Many Requests
ERROR CODE: ResourceCollectionRequestsThrottled`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	if !strings.Contains(strings.ToLower(got), "provider microsoft.network") {
		t.Fatalf("expected provider Microsoft.Network in canonical phrase, got=%q", got)
	}
	if !strings.Contains(got, "ERROR CODE: ResourceCollectionRequestsThrottled") {
		t.Fatalf("expected error code preserved in canonical phrase, got=%q", got)
	}
}

// Microsoft.RedHatOpenShift must not be ignored so that our own RP errors are
// attributed to their source in the canonical phrase.
func TestExtractEvidenceRedHatOpenShiftProviderIncluded(t *testing.T) {
	t.Parallel()

	raw := `GET https://rp.example/subscriptions/XXXX/providers/Microsoft.RedHatOpenShift/locations/westus3/hcpOperationStatuses/abc
RESPONSE 500: 500 Internal Server Error
ERROR CODE: InternalServerError`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	if !strings.Contains(strings.ToLower(got), "provider microsoft.redhatopenshift") {
		t.Fatalf("expected provider Microsoft.RedHatOpenShift in canonical phrase, got=%q", got)
	}
}

// OCP candidate version strings must be normalized so different versions that
// produce the same "doesn't exist" error merge into a single failure pattern.
func TestExtractEvidenceOCPVersionStringNormalized(t *testing.T) {
	t.Parallel()

	rawV4 := `PUT https://rp.example/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/cluster
RESPONSE 400: 400 Bad Request
ERROR CODE: InvalidRequestContent
{"error":{"code":"InvalidRequestContent","message":"Version 'openshift-v4.22.0-candidate' doesn't exist"}}`

	rawV5 := `PUT https://rp.example/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/cluster
RESPONSE 400: 400 Bad Request
ERROR CODE: InvalidRequestContent
{"error":{"code":"InvalidRequestContent","message":"Version 'openshift-v5.1.0-candidate' doesn't exist"}}`

	gotV4 := extractEvidence(rawV4).CanonicalEvidencePhrase
	gotV5 := extractEvidence(rawV5).CanonicalEvidencePhrase
	if gotV4 != gotV5 {
		t.Fatalf("different OCP candidate versions must produce the same canonical:\n  v4=%q\n  v5=%q", gotV4, gotV5)
	}
	if strings.Contains(gotV4, "4.22.0") || strings.Contains(gotV4, "5.1.0") {
		t.Fatalf("canonical phrase must not contain raw version numbers, got=%q", gotV4)
	}
}

// Cluster internal IDs (OCM-style 32-char alphanumeric) must be normalized so
// the same error class for different cluster instances merges into one pattern.
func TestExtractEvidenceClusterInternalIDNormalized(t *testing.T) {
	t.Parallel()

	rawA := `PATCH https://management.azure.com/subscriptions/XXXX/resourceGroups/rg-a/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/cluster
RESPONSE 400: 400 Bad Request
ERROR CODE: InvalidRequestContent
{"error":{"code":"InvalidRequestContent","message":"Cluster '2pmeojr923nt08rchn2mn56al24muh61' is in state 'pending_update', can't update"}}`

	rawB := `PATCH https://management.azure.com/subscriptions/XXXX/resourceGroups/rg-b/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/cluster
RESPONSE 400: 400 Bad Request
ERROR CODE: InvalidRequestContent
{"error":{"code":"InvalidRequestContent","message":"Cluster '2pni671e890elvabbe631mnqcb4pi6te' is in state 'pending_update', can't update"}}`

	gotA := extractEvidence(rawA).CanonicalEvidencePhrase
	gotB := extractEvidence(rawB).CanonicalEvidencePhrase
	if gotA != gotB {
		t.Fatalf("same cluster-state error with different cluster IDs must canonicalize identically:\n  A=%q\n  B=%q", gotA, gotB)
	}
	if strings.Contains(gotA, "2pmeojr923nt08rchn2mn56al24muh61") {
		t.Fatalf("canonical phrase must not contain raw cluster IDs, got=%q", gotA)
	}
}

// Logfmt step-error lines: the err= field must be extracted as the canonical
// so the actionable message is not truncated by the boilerplate prefix.
func TestExtractEvidenceLogfmtStepErrorExtracts(t *testing.T) {
	t.Parallel()

	raw := `time=2026-04-17T11:04:14.653Z level=INFO msg="Running step." serviceGroup=Microsoft.Azure.ARO.HCP.Management.Infra resourceGroup=management step=delete-non-swift-user-nodepools
time=2026-04-17T11:04:19.211Z level=ERROR msg="Step errored." serviceGroup=Microsoft.Azure.ARO.HCP.Management.Infra resourceGroup=management step=delete-non-swift-user-nodepools err="failed to prepare kubeconfig: failed to ensure cluster admin role: /me request is only valid with delegated authentication flow."`

	got := extractEvidence(raw).CanonicalEvidencePhrase
	if strings.Contains(strings.ToLower(got), "step errored") {
		t.Fatalf("canonical phrase should not contain logfmt boilerplate 'Step errored.', got=%q", got)
	}
	if !strings.Contains(strings.ToLower(got), "failed to prepare kubeconfig") {
		t.Fatalf("canonical phrase should contain the actionable err= value, got=%q", got)
	}
}
