package run

import (
	"context"
	"testing"

	"ci-failure-atlas/pkg/testsupport/pgtest"
)

func TestCompleteUsesPostgresStoreWhenEnabled(t *testing.T) {
	server, err := pgtest.StartEmbedded(t.TempDir())
	if err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Stop()
	})

	raw := DefaultOptions()
	raw.PostgresOptions.Enabled = true
	raw.PostgresOptions.Embedded = false
	raw.PostgresOptions.Hostname = server.Host
	raw.PostgresOptions.Port = server.Port
	raw.PostgresOptions.User = server.User
	raw.PostgresOptions.Password = server.Password
	raw.PostgresOptions.Database = server.Database
	raw.PostgresOptions.SSLMode = "disable"
	raw.PostgresOptions.Initialize = true

	validated, err := raw.Validate()
	if err != nil {
		t.Fatalf("validate options: %v", err)
	}
	completed, err := validated.Complete(context.Background())
	if err != nil {
		t.Fatalf("complete options: %v", err)
	}
	t.Cleanup(completed.Cleanup)

	if completed.Postgres == nil {
		t.Fatalf("expected postgres options to be set")
	}
	if completed.Store == nil {
		t.Fatalf("expected postgres-backed store to be created")
	}
	if _, err := completed.Store.ListRuns(context.Background()); err != nil {
		t.Fatalf("smoke list runs using postgres store: %v", err)
	}
}
