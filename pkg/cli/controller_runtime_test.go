package cli

import (
	"context"
	"testing"

	"ci-failure-atlas/pkg/testsupport/pgtest"
)

func TestControllerCommandCompleteUsesPostgresStoreForRunOnce(t *testing.T) {
	server, err := pgtest.StartEmbedded(t.TempDir())
	if err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Stop()
	})

	raw := defaultControllerCommandOptions()
	raw.ControllerName = "metrics.rollup.daily"
	raw.ControllerKey = "dev|2026-03-22"
	raw.PostgresOptions.Enabled = true
	raw.PostgresOptions.Embedded = false
	raw.PostgresOptions.Hostname = server.Host
	raw.PostgresOptions.Port = server.Port
	raw.PostgresOptions.User = server.User
	raw.PostgresOptions.Password = server.Password
	raw.PostgresOptions.Database = server.Database
	raw.PostgresOptions.SSLMode = "disable"
	raw.PostgresOptions.Initialize = true

	runtime, err := raw.complete(context.Background(), true)
	if err != nil {
		t.Fatalf("complete command options: %v", err)
	}
	t.Cleanup(runtime.Cleanup)

	if runtime.Postgres == nil {
		t.Fatalf("expected postgres options to be set")
	}
	if runtime.Store == nil {
		t.Fatalf("expected postgres-backed store to be created")
	}
	if _, err := runtime.Store.ListRuns(context.Background()); err != nil {
		t.Fatalf("smoke list runs using postgres store: %v", err)
	}
}

func TestControllerCommandCompleteUsesPostgresStoreForSyncOnce(t *testing.T) {
	server, err := pgtest.StartEmbedded(t.TempDir())
	if err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Stop()
	})

	raw := defaultControllerCommandOptions()
	raw.ControllerName = "source.sippy.runs"
	raw.PostgresOptions.Enabled = true
	raw.PostgresOptions.Embedded = false
	raw.PostgresOptions.Hostname = server.Host
	raw.PostgresOptions.Port = server.Port
	raw.PostgresOptions.User = server.User
	raw.PostgresOptions.Password = server.Password
	raw.PostgresOptions.Database = server.Database
	raw.PostgresOptions.SSLMode = "disable"
	raw.PostgresOptions.Initialize = true

	runtime, err := raw.complete(context.Background(), false)
	if err != nil {
		t.Fatalf("complete command options: %v", err)
	}
	t.Cleanup(runtime.Cleanup)

	if runtime.Postgres == nil {
		t.Fatalf("expected postgres options to be set")
	}
	if runtime.Store == nil {
		t.Fatalf("expected postgres-backed store to be created")
	}
	if _, err := runtime.Store.ListRuns(context.Background()); err != nil {
		t.Fatalf("smoke list runs using postgres store: %v", err)
	}
}
