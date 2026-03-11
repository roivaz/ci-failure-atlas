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
