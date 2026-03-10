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
