package summary

import (
	"strings"
	"testing"
)

func TestBuildMarkdownIncludesCoreSections(t *testing.T) {
	t.Parallel()

	globals := []globalCluster{
		{
			SchemaVersion:           "v1",
			Phase2ClusterID:         "g1",
			CanonicalEvidencePhrase: "failed waiting for cluster operators",
			SearchQueryPhrase:       "cluster operators not available",
			SupportCount:            7,
			SeenPostGoodCommit:      true,
			PostGoodCommitCount:     5,
			ContributingTestsCount:  2,
			ContributingTests: []contributingTest{
				{Lane: "e2e", JobName: "job-a", TestName: "test-a", SupportCount: 4},
				{Lane: "e2e", JobName: "job-a", TestName: "test-b", SupportCount: 3},
			},
			MemberPhase1ClusterIDs: []string{"p1", "p2"},
			MemberSignatureIDs:     []string{"s1", "s2"},
			References: []reference{
				{
					RunURL:         "https://prow.example/run/1",
					OccurredAt:     "2026-03-03T12:00:00Z",
					SignatureID:    "s1",
					PRNumber:       100,
					PostGoodCommit: true,
					RawTextExcerpt: "cluster operators not available",
				},
			},
		},
	}

	tests := []testCluster{
		{
			SchemaVersion:           "v1",
			Phase1ClusterID:         "p1",
			Lane:                    "e2e",
			JobName:                 "job-a",
			TestName:                "test-a",
			TestSuite:               "suite-a",
			CanonicalEvidencePhrase: "failed waiting for cluster operators",
			SearchQueryPhrase:       "cluster operators not available",
			SupportCount:            7,
			SeenPostGoodCommit:      true,
			PostGoodCommitCount:     5,
			MemberSignatureIDs:      []string{"s1", "s2"},
			References: []reference{
				{
					RunURL:         "https://prow.example/run/1",
					OccurredAt:     "2026-03-03T12:00:00Z",
					SignatureID:    "s1",
					PRNumber:       100,
					PostGoodCommit: true,
					RawTextExcerpt: "cluster operators not available",
				},
			},
		},
	}

	reviews := []reviewItem{
		{
			SchemaVersion: "v1",
			ReviewItemID:  "r1",
			Phase:         "phase1",
			Reason:        "needs-review",
		},
	}

	report := buildMarkdown(globals, tests, reviews, 10, 1.0)

	required := []string{
		"# CI Failure Triage Summary",
		"## Overview",
		"## Top Global Failure Signatures",
		"## Top Failing Tests",
		"## Lane Breakdown",
		"## High-Impact Post-Good-Commit Signatures",
		"## Review Queue",
		"Total failure records analyzed: **7**",
		"Markdown focus: top **10** rows with at least **1.00%** of total failures",
	}
	for _, section := range required {
		if !strings.Contains(report, section) {
			t.Fatalf("expected report to include %q", section)
		}
	}
}

func TestBuildMarkdownAppliesMinPercentFilter(t *testing.T) {
	t.Parallel()

	globals := []globalCluster{
		{
			SchemaVersion:           "v1",
			Phase2ClusterID:         "g1",
			CanonicalEvidencePhrase: "high signal cluster",
			SearchQueryPhrase:       "high signal cluster",
			SupportCount:            50,
			ContributingTestsCount:  1,
			ContributingTests:       []contributingTest{{Lane: "e2e", JobName: "job-a", TestName: "test-a", SupportCount: 50}},
		},
		{
			SchemaVersion:           "v1",
			Phase2ClusterID:         "g2",
			CanonicalEvidencePhrase: "low signal cluster",
			SearchQueryPhrase:       "low signal cluster",
			SupportCount:            2,
			ContributingTestsCount:  1,
			ContributingTests:       []contributingTest{{Lane: "e2e", JobName: "job-a", TestName: "test-b", SupportCount: 2}},
		},
	}
	tests := []testCluster{
		{
			SchemaVersion:           "v1",
			Phase1ClusterID:         "p1",
			Lane:                    "e2e",
			JobName:                 "job-a",
			TestName:                "test-a",
			CanonicalEvidencePhrase: "high signal cluster",
			SearchQueryPhrase:       "high signal cluster",
			SupportCount:            50,
		},
		{
			SchemaVersion:           "v1",
			Phase1ClusterID:         "p2",
			Lane:                    "e2e",
			JobName:                 "job-a",
			TestName:                "test-b",
			CanonicalEvidencePhrase: "low signal cluster",
			SearchQueryPhrase:       "low signal cluster",
			SupportCount:            2,
		},
	}

	report := buildMarkdown(globals, tests, nil, 10, 5.0)
	if !strings.Contains(report, "high signal cluster") {
		t.Fatalf("expected high signal cluster to be present: %q", report)
	}
	if strings.Contains(report, "low signal cluster") {
		t.Fatalf("did not expect low signal cluster below threshold: %q", report)
	}
}
