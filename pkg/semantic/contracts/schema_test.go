package contracts

import "testing"

func TestRequireCompatibleWeekSchemasAllowsUnsetExpected(t *testing.T) {
	t.Parallel()

	if err := RequireCompatibleWeekSchemas("", SchemaVersionV2, "window load"); err != nil {
		t.Fatalf("expected unset expected schema to be accepted, got=%v", err)
	}
}

func TestRequireCurrentSchemaVersionRejectsLegacyV1(t *testing.T) {
	t.Parallel()

	err := RequireCurrentSchemaVersion(SchemaVersionV1, "semantic week 2026-03-15")
	if err == nil {
		t.Fatalf("expected legacy v1 schema to be rejected")
	}
	if got := err.Error(); got != "semantic week 2026-03-15 uses legacy semantic schema v1; rematerialize/backfill this week before loading it" {
		t.Fatalf("unexpected legacy schema error: %q", got)
	}
}

func TestRequireCurrentSchemaVersionAllowsUnsetAndCurrent(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", SchemaVersionV2} {
		if err := RequireCurrentSchemaVersion(value, "semantic week load"); err != nil {
			t.Fatalf("expected schema %q to be accepted, got=%v", value, err)
		}
	}
}
