package summary

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
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

func TestGenerateWritesSummaryFromStore(t *testing.T) {
	t.Parallel()

	ctx := logr.NewContext(context.Background(), logr.Discard())
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:                semanticcontracts.SchemaVersionV1,
			Phase2ClusterID:              "g-1",
			CanonicalEvidencePhrase:      "failed waiting for cluster operators",
			SearchQueryPhrase:            "cluster operators not available",
			SearchQuerySourceRunURL:      "https://prow.example/run/1",
			SearchQuerySourceSignatureID: "sig-1",
			SupportCount:                 7,
			SeenPostGoodCommit:           true,
			PostGoodCommitCount:          5,
			ContributingTests: []semanticcontracts.ContributingTestRecord{
				{Lane: "e2e", JobName: "job-a", TestName: "test-a", SupportCount: 4},
				{Lane: "e2e", JobName: "job-a", TestName: "test-b", SupportCount: 3},
			},
			MemberPhase1ClusterIDs: []string{"p1", "p2"},
			MemberSignatureIDs:     []string{"s1", "s2"},
			References: []semanticcontracts.ReferenceRecord{
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
	}); err != nil {
		t.Fatalf("seed global clusters: %v", err)
	}

	if err := store.UpsertTestClusters(ctx, []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
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
			References: []semanticcontracts.ReferenceRecord{
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
	}); err != nil {
		t.Fatalf("seed test clusters: %v", err)
	}

	if err := store.UpsertReviewQueue(ctx, []semanticcontracts.ReviewItemRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			ReviewItemID:  "r1",
			Phase:         "phase1",
			Reason:        "needs-review",
		},
	}); err != nil {
		t.Fatalf("seed review queue: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "triage-summary.md")
	opts := DefaultOptions()
	opts.OutputPath = outputPath
	opts.Top = 10
	opts.MinPercent = 1.0

	if err := Generate(ctx, store, opts); err != nil {
		t.Fatalf("generate summary: %v", err)
	}

	report, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read generated summary: %v", err)
	}
	content := string(report)
	if !strings.Contains(content, "# CI Failure Triage Summary") {
		t.Fatalf("expected summary header in output: %q", content)
	}
	if !strings.Contains(content, "Top Global Failure Signatures") {
		t.Fatalf("expected global section in output: %q", content)
	}
}

func TestGenerateWritesSummaryPerEnvironmentWhenSplitEnabled(t *testing.T) {
	t.Parallel()

	ctx := logr.NewContext(context.Background(), logr.Discard())
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "g-dev",
			CanonicalEvidencePhrase: "dev evidence",
			SearchQueryPhrase:       "dev query",
			SupportCount:            3,
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase2ClusterID:         "g-int",
			CanonicalEvidencePhrase: "int evidence",
			SearchQueryPhrase:       "int query",
			SupportCount:            5,
		},
	}); err != nil {
		t.Fatalf("seed global clusters: %v", err)
	}
	if err := store.UpsertTestClusters(ctx, []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase1ClusterID:         "p-dev",
			Lane:                    "e2e",
			JobName:                 "job-dev",
			TestName:                "test-dev",
			CanonicalEvidencePhrase: "dev evidence",
			SearchQueryPhrase:       "dev query",
			SupportCount:            3,
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase1ClusterID:         "p-int",
			Lane:                    "e2e",
			JobName:                 "job-int",
			TestName:                "test-int",
			CanonicalEvidencePhrase: "int evidence",
			SearchQueryPhrase:       "int query",
			SupportCount:            5,
		},
	}); err != nil {
		t.Fatalf("seed test clusters: %v", err)
	}
	if err := store.UpsertReviewQueue(ctx, []semanticcontracts.ReviewItemRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			Environment:   "dev",
			ReviewItemID:  "r-dev",
			Phase:         "phase1",
			Reason:        "dev-review",
		},
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			Environment:   "int",
			ReviewItemID:  "r-int",
			Phase:         "phase1",
			Reason:        "int-review",
		},
	}); err != nil {
		t.Fatalf("seed review queue: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "triage-summary.md")
	opts := DefaultOptions()
	opts.OutputPath = outputPath
	opts.SplitByEnvironment = true
	opts.Environments = []string{"dev", "int"}

	if err := Generate(ctx, store, opts); err != nil {
		t.Fatalf("generate summary: %v", err)
	}

	devPath := filepath.Join(filepath.Dir(outputPath), "triage-summary.dev.md")
	intPath := filepath.Join(filepath.Dir(outputPath), "triage-summary.int.md")
	devReport, err := os.ReadFile(devPath)
	if err != nil {
		t.Fatalf("read dev summary: %v", err)
	}
	intReport, err := os.ReadFile(intPath)
	if err != nil {
		t.Fatalf("read int summary: %v", err)
	}
	if !strings.Contains(string(devReport), "dev evidence") {
		t.Fatalf("expected dev report to include dev-only evidence: %q", string(devReport))
	}
	if strings.Contains(string(devReport), "int evidence") {
		t.Fatalf("did not expect dev report to include int evidence: %q", string(devReport))
	}
	if !strings.Contains(string(intReport), "int evidence") {
		t.Fatalf("expected int report to include int-only evidence: %q", string(intReport))
	}
	if strings.Contains(string(intReport), "dev evidence") {
		t.Fatalf("did not expect int report to include dev evidence: %q", string(intReport))
	}
}
