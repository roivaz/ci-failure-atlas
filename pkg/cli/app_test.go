package cli

import "testing"

func TestSiteRunURLFromListenAddress(t *testing.T) {
	testCases := []struct {
		name     string
		listen   string
		expected string
	}{
		{
			name:     "defaults when empty",
			listen:   "",
			expected: "http://127.0.0.1:8080",
		},
		{
			name:     "loopback host and port",
			listen:   "127.0.0.1:9000",
			expected: "http://127.0.0.1:9000",
		},
		{
			name:     "wildcard host normalizes to localhost",
			listen:   "0.0.0.0:8080",
			expected: "http://localhost:8080",
		},
		{
			name:     "empty host normalizes to localhost",
			listen:   ":8080",
			expected: "http://localhost:8080",
		},
		{
			name:     "invalid hostport falls back to raw",
			listen:   "localhost",
			expected: "http://localhost",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := siteRunURLFromListenAddress(tc.listen)
			if got != tc.expected {
				t.Fatalf("unexpected URL: got %q want %q", got, tc.expected)
			}
		})
	}
}

func TestNewAppCommandDoesNotExposeExportSiteSubcommand(t *testing.T) {
	t.Parallel()

	cmd, err := NewAppCommand()
	if err != nil {
		t.Fatalf("create app command: %v", err)
	}

	if got, want := cmd.Name(), "app"; got != want {
		t.Fatalf("unexpected command name: got=%q want=%q", got, want)
	}

	exportCmd, _, err := cmd.Find([]string{"export-site"})
	if err == nil && exportCmd != nil && exportCmd.Name() == "export-site" {
		t.Fatalf("did not expect export-site subcommand to be present")
	}
}
