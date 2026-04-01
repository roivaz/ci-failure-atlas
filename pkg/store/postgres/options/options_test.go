package options

import "testing"

func TestValidateDisabledBackendSucceeds(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.Enabled = false

	if _, err := opts.Validate(); err != nil {
		t.Fatalf("expected validate success for disabled backend: %v", err)
	}
}

func TestValidateRemoteBackendRequiresCoreConnectionFields(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.Enabled = true
	opts.Embedded = false
	opts.Hostname = ""
	opts.User = ""
	opts.Database = ""

	if _, err := opts.Validate(); err == nil {
		t.Fatalf("expected validate error for missing remote connection fields")
	}
}

func TestValidateEmbeddedBackendRequiresDataDir(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.Enabled = true
	opts.Embedded = true
	opts.EmbeddedDataDir = ""

	if _, err := opts.Validate(); err == nil {
		t.Fatalf("expected validate error when embedded data dir is missing")
	}
}
