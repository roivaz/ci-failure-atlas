package cli

import "testing"

func TestResolveReportOutputPath(t *testing.T) {
	testCases := []struct {
		name                 string
		dataDirectory        string
		semanticSubdirectory string
		reportSubdirectory   string
		outputPath           string
		outputFlagChanged    bool
		expectedPath         string
		expectError          bool
	}{
		{
			name:                 "uses semantic subdir when reports flag unset",
			dataDirectory:        "data",
			semanticSubdirectory: "2026-03-01",
			reportSubdirectory:   "",
			outputPath:           "data/reports/weekly-metrics.html",
			outputFlagChanged:    false,
			expectedPath:         "data/reports/2026-03-01/weekly-metrics.html",
		},
		{
			name:                 "reports subdir overrides semantic subdir",
			dataDirectory:        "data",
			semanticSubdirectory: "2026-03-01",
			reportSubdirectory:   "mgmt-weekly",
			outputPath:           "data/reports/weekly-metrics.html",
			outputFlagChanged:    false,
			expectedPath:         "data/reports/mgmt-weekly/weekly-metrics.html",
		},
		{
			name:                 "writes to reports root when subdirs absent",
			dataDirectory:        "data",
			semanticSubdirectory: "",
			reportSubdirectory:   "",
			outputPath:           "data/reports/weekly-metrics.html",
			outputFlagChanged:    false,
			expectedPath:         "data/reports/weekly-metrics.html",
		},
		{
			name:                 "explicit output remains unchanged",
			dataDirectory:        "data",
			semanticSubdirectory: "2026-03-01",
			reportSubdirectory:   "mgmt-weekly",
			outputPath:           "/tmp/custom-report.html",
			outputFlagChanged:    true,
			expectedPath:         "/tmp/custom-report.html",
		},
		{
			name:                 "rejects path traversal in reports subdir",
			dataDirectory:        "data",
			semanticSubdirectory: "",
			reportSubdirectory:   "../escape",
			outputPath:           "data/reports/weekly-metrics.html",
			outputFlagChanged:    false,
			expectError:          true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveReportOutputPath(
				tc.dataDirectory,
				tc.semanticSubdirectory,
				tc.reportSubdirectory,
				tc.outputPath,
				tc.outputFlagChanged,
			)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got path %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveReportOutputPath returned error: %v", err)
			}
			if got != tc.expectedPath {
				t.Fatalf("unexpected path: got %q, want %q", got, tc.expectedPath)
			}
		})
	}
}
