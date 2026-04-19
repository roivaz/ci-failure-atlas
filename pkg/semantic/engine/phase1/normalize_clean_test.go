package phase1

import (
	"strings"
	"testing"
)

func TestCleanCanonicalScrubsResourceGroupAndClusterIDs(t *testing.T) {
	t.Parallel()

	input := "timeout '10.000000' minutes exceeded during GetAdminRESTConfigForHCPCluster for cluster ea-cluster in resource group external-auth-cluster-w6qnck, error: context deadline exceeded"
	got := cleanCanonical(input)

	if strings.Contains(got, "external-auth-cluster-w6qnck") {
		t.Fatalf("expected resource-group ID to be scrubbed, got=%q", got)
	}
	if strings.Contains(got, "for cluster ea-cluster") {
		t.Fatalf("expected cluster ID to be scrubbed, got=%q", got)
	}
	if !strings.Contains(got, "resource group <resource-group>") {
		t.Fatalf("expected resource-group placeholder in canonical phrase, got=%q", got)
	}
	if !strings.Contains(got, "for cluster <cluster>") {
		t.Fatalf("expected cluster placeholder in canonical phrase, got=%q", got)
	}
}

func TestCleanCanonicalScrubsNodePoolIDs(t *testing.T) {
	t.Parallel()

	input := "timeout '45.000000' minutes exceeded during CreateNodePoolFromParam for node pool ea-np-1 in resource group external-auth-cluster-h9gmhm, error: failed waiting for nodepool=\"ea-np-1\" for cluster \"ea-cluster\" in resourcegroup=\"external-auth-cluster-h9gmhm\" to finish creating"
	got := cleanCanonical(input)

	if strings.Contains(got, "ea-np-1") {
		t.Fatalf("expected node-pool ID to be scrubbed, got=%q", got)
	}
	if strings.Contains(got, "external-auth-cluster-h9gmhm") {
		t.Fatalf("expected resource-group ID to be scrubbed, got=%q", got)
	}
	if !strings.Contains(got, "node pool <nodepool>") {
		t.Fatalf("expected node-pool placeholder in canonical phrase, got=%q", got)
	}
	if !strings.Contains(got, `nodepool="<nodepool>"`) {
		t.Fatalf("expected quoted node-pool placeholder in canonical phrase, got=%q", got)
	}
}

func TestCleanCanonicalScrubsExternalAuthResourceGroupSample(t *testing.T) {
	t.Parallel()

	input := "timeout '10.000000' minutes exceeded during GetAdminRESTConfigForHCPCluster for cluster ea-cluster in resource group external-auth-cluster-shhqbw, error: context deadline exceeded\", errs: [ {"
	got := cleanCanonical(input)

	if strings.Contains(got, "external-auth-cluster-shhqbw") {
		t.Fatalf("expected external-auth resource-group ID to be scrubbed, got=%q", got)
	}
	if strings.Contains(got, "for cluster ea-cluster") {
		t.Fatalf("expected cluster ID to be scrubbed, got=%q", got)
	}
	if !strings.Contains(got, "resource group <resource-group>") {
		t.Fatalf("expected resource-group placeholder in canonical phrase, got=%q", got)
	}
}

func TestCleanCanonicalScrubsExternalAuthAndInClusterIDs(t *testing.T) {
	t.Parallel()

	input := `failed waiting for external auth "ea-list" in resourcegroup="ea-list-rg-pxk72q", caused by: timeout '15.000000' minutes exceeded during CreateOrUpdateExternalAuthAndWait for external auth ea-list in cluster ea-listxfk7fg`
	got := cleanCanonical(input)

	if strings.Contains(got, "ea-listxfk7fg") {
		t.Fatalf("expected dynamic cluster name to be scrubbed, got=%q", got)
	}
	if strings.Contains(got, `external auth "ea-list"`) || strings.Contains(got, "external auth ea-list") {
		t.Fatalf("expected external auth name to be scrubbed, got=%q", got)
	}
	if !strings.Contains(got, `external auth "<external-auth>"`) {
		t.Fatalf("expected quoted external-auth placeholder in canonical phrase, got=%q", got)
	}
	if !strings.Contains(got, "for external auth <external-auth>") {
		t.Fatalf("expected external-auth placeholder in canonical phrase, got=%q", got)
	}
	if !strings.Contains(got, "in cluster <cluster>") {
		t.Fatalf("expected in-cluster placeholder in canonical phrase, got=%q", got)
	}
}

func TestCleanCanonicalTruncatesAtWordBoundary(t *testing.T) {
	t.Parallel()

	input := `failed waiting for external auth "ea-list" in resourcegroup="ea-list-rg-pxk72q" for cluster="ea-list" to finish, caused by: timeout '15.000000' minutes exceeded during CreateOrUpdateExternalAuthAndWait for external auth ea-list in cluster ea-listxfk7fg in resourcegroup="ea-list-rg-pxk72q"`
	got := cleanCanonical(input)

	if strings.HasSuffix(got, "<clu") || strings.HasSuffix(got, "resourc") {
		t.Fatalf("expected canonical truncation to avoid partial tokens, got=%q", got)
	}
}

func TestCleanCanonicalScrubsBareNodePoolName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "update-nodes",
			input: "timeout '45.000000' minutes exceeded during UpdateNodePoolAndWait for nodepool np-update-nodes",
			want:  "nodepool <nodepool>",
		},
		{
			name:  "hyphen-name",
			input: "UpdateNodePoolAndWait for nodepool np-1 timed out",
			want:  "nodepool <nodepool>",
		},
		{
			name:  "one-node",
			input: "timeout exceeded during UpdateNodePoolAndWait for nodepool np-one-node",
			want:  "nodepool <nodepool>",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := cleanCanonical(tc.input)
			if !strings.Contains(got, tc.want) {
				t.Fatalf("expected %q in cleaned canonical, got=%q", tc.want, got)
			}
		})
	}
}

func TestCleanCanonicalScrubsDialingIPPort(t *testing.T) {
	t.Parallel()

	input := `proxyconnect tcp: dial tcp 127.0.0.1:8888: connect: connection refused; also dialing 10.128.64.38:15017 failed`
	got := cleanCanonical(input)

	if strings.Contains(got, "127.0.0.1") || strings.Contains(got, "10.128.64.38") {
		t.Fatalf("expected IP addresses to be scrubbed, got=%q", got)
	}
	if !strings.Contains(got, "dial tcp <ip>:<port>") {
		t.Fatalf("expected dial tcp placeholder, got=%q", got)
	}
	if !strings.Contains(got, "dialing <ip>:<port>") {
		t.Fatalf("expected dialing placeholder, got=%q", got)
	}
}

func TestCleanCanonicalStripsK8sLogPrefix(t *testing.T) {
	t.Parallel()

	input := `E0407 23:10:13.008148    2565 controller.go:123] "Unhandled Error" err="something went wrong" controller="cluster"`
	got := cleanCanonical(input)

	if strings.Contains(got, "E0407") || strings.Contains(got, "23:10:13") || strings.Contains(got, "2565") {
		t.Fatalf("expected klog prefix to be stripped, got=%q", got)
	}
	if !strings.Contains(strings.ToLower(got), "unhandled error") {
		t.Fatalf("expected log message content to remain, got=%q", got)
	}
}

func TestCleanCanonicalStripsMakeDirectoryBanner(t *testing.T) {
	t.Parallel()

	input := "make[2]: Entering directory '/go/src/github.com/openshift-kni/numaresources-operator'\nerror: something actually failed"
	got := cleanCanonical(input)

	if strings.Contains(strings.ToLower(got), "entering directory") {
		t.Fatalf("expected make directory banner to be stripped, got=%q", got)
	}
	if !strings.Contains(strings.ToLower(got), "something actually failed") {
		t.Fatalf("expected actual error to remain, got=%q", got)
	}
}

func TestCleanCanonicalScrubsClusterCreationQuotedName(t *testing.T) {
	t.Parallel()

	input := `failed to create HCP cluster np-autoscale-cluster: failed starting cluster creation "np-autoscale-cluster" in resourcegroup="stg-autoscale-rg-abc123": PUT https://management.azure.com/subscriptions/11111111-2222-3333-4444-555555555555/resourceGroups/stg-autoscale-rg-abc123/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/np-autoscale-cluster`
	got := cleanCanonical(input)

	if strings.Contains(got, "np-autoscale-cluster") {
		t.Fatalf("expected cluster creation name to be scrubbed, got=%q", got)
	}
	if !strings.Contains(got, `failed to create HCP cluster <cluster>`) {
		t.Fatalf("expected HCP cluster placeholder in canonical phrase, got=%q", got)
	}
	if !strings.Contains(got, `cluster creation "<cluster>"`) {
		t.Fatalf("expected cluster creation placeholder in canonical phrase, got=%q", got)
	}
	if !strings.Contains(got, `resourcegroup="<resource-group>"`) {
		t.Fatalf("expected resource-group placeholder in canonical phrase, got=%q", got)
	}
	if !strings.Contains(got, "PUT <url>") {
		t.Fatalf("expected URL placeholder in canonical phrase, got=%q", got)
	}
}
