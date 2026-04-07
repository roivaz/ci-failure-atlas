package lanes

import "testing"

func TestClassifyLane(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		environment string
		testSuite   string
		testName    string
		wantLane    Lane
	}{
		{
			name:        "dev provision by step graph regex",
			environment: "dev",
			testSuite:   "step graph",
			testName:    "Run pipeline step Microsoft.Azure.ARO.HCP.Region",
			wantLane:    LaneProvision,
		},
		{
			name:        "dev e2e by suite",
			environment: "dev",
			testSuite:   "rp-api-compat-all/parallel",
			testName:    "any",
			wantLane:    LaneE2E,
		},
		{
			name:        "int e2e by suite",
			environment: "int",
			testSuite:   "integration/parallel",
			testName:    "any",
			wantLane:    LaneE2E,
		},
		{
			name:        "dev step graph non aro test is unknown",
			environment: "dev",
			testSuite:   "step graph",
			testName:    "Run pipeline step Other.Service",
			wantLane:    LaneUnknown,
		},
		{
			name:        "unknown environment is unknown lane",
			environment: "qa",
			testSuite:   "integration/parallel",
			testName:    "any",
			wantLane:    LaneUnknown,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ClassifyLane(tc.environment, tc.testSuite, tc.testName)
			if got != tc.wantLane {
				t.Fatalf("unexpected lane: got=%q want=%q", got, tc.wantLane)
			}
		})
	}
}

func TestFiltersForEnvironment(t *testing.T) {
	t.Parallel()

	filters, ok := FiltersForEnvironment("dev")
	if !ok {
		t.Fatalf("expected filters for dev")
	}
	if len(filters) != 2 {
		t.Fatalf("unexpected filter count for dev: got=%d want=2", len(filters))
	}
	if filters[0].TestSuite != "rp-api-compat-all/parallel" {
		t.Fatalf("unexpected first suite filter: got=%q", filters[0].TestSuite)
	}
	if filters[1].TestSuite != "step graph" || filters[1].TestNameRegex == "" {
		t.Fatalf("unexpected second filter: %+v", filters[1])
	}

	if _, ok := FiltersForEnvironment("unknown"); ok {
		t.Fatalf("expected no filters for unknown environment")
	}
}
