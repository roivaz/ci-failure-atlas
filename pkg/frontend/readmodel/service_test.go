package readmodel

import (
	"context"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

func TestDiscoverSemanticWeeksSkipsLegacyWeeks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := newIntegrationFixture(t, "")
	legacyStore := fixture.openWeekStore(t, "2026-03-09")
	currentStore := fixture.openWeekStore(t, "2026-03-16")

	if err := legacyStore.ReplaceMaterializedWeek(ctx, materializedWeekWithSchemaVersion(previousMaterializedWeek(), semanticcontracts.SchemaVersionV1)); err != nil {
		t.Fatalf("seed legacy materialized week: %v", err)
	}
	if err := currentStore.ReplaceMaterializedWeek(ctx, materializedWeekWithSchemaVersion(currentMaterializedWeek(), semanticcontracts.CurrentSchemaVersion)); err != nil {
		t.Fatalf("seed current materialized week: %v", err)
	}

	weeks, err := fixture.service.DiscoverSemanticWeeks(ctx)
	if err != nil {
		t.Fatalf("discover semantic weeks: %v", err)
	}
	if len(weeks) != 1 || weeks[0] != "2026-03-16" {
		t.Fatalf("unexpected loadable weeks: %+v", weeks)
	}
}
