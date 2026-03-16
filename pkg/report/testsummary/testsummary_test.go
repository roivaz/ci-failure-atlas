package testsummary

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestLookupMetadataFallsBackWithoutSuite(t *testing.T) {
	t.Parallel()

	key := toTestKey("e2e", "job-a", "test-a", "suite-a")
	fallback := map[testKeyNoSuite]testMetadata{
		{Lane: "e2e", JobName: "job-a", TestName: "test-a"}: {
			PassRate: float64Ptr(78.5),
			Runs:     40,
		},
	}

	got := lookupMetadata(key, map[testKey]testMetadata{}, fallback)
	if got.PassRate == nil || *got.PassRate != 78.5 {
		t.Fatalf("expected fallback metadata pass rate 78.5, got %+v", got)
	}
	if got.Runs != 40 {
		t.Fatalf("expected fallback runs 40, got %d", got.Runs)
	}
}

func TestSignaturePRWarningForRecentConcentration(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		SupportCount:        9,
		PostGoodCommitCount: 0,
		References: []reference{
			{PRNumber: 4201},
			{PRNumber: 4202},
			{PRNumber: 4203},
		},
	}
	recent := []reference{
		{PRNumber: 4249},
		{PRNumber: 4249},
		{PRNumber: 4249},
		{PRNumber: 4249},
	}

	warning, ok := signaturePRWarning(cluster, recent)
	if !ok {
		t.Fatal("expected warning for recent PR concentration")
	}
	if !strings.Contains(warning, "recent failures are concentrated on PR #4249") {
		t.Fatalf("unexpected warning: %q", warning)
	}
}

func TestSignaturePRWarningForTentativeBadPRSignal(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		SupportCount:        12,
		PostGoodCommitCount: 0,
		References: []reference{
			{PRNumber: 4249},
			{PRNumber: 4249},
			{PRNumber: 4313},
		},
	}
	recent := []reference{
		{PRNumber: 4313},
		{PRNumber: 4249},
		{PRNumber: 4249},
		{PRNumber: 4313},
	}

	warning, ok := signaturePRWarning(cluster, recent)
	if !ok {
		t.Fatal("expected warning for missing post-merge signal")
	}
	if !strings.Contains(warning, "post-good-commit-runs=0 across 2 recent PRs") {
		t.Fatalf("unexpected warning: %q", warning)
	}
	if !strings.Contains(warning, "Bad-PR attribution is tentative") {
		t.Fatalf("unexpected warning: %q", warning)
	}
}

func TestSignaturePRWarningUsesRecentDistinctPRsForTentativeCount(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		SupportCount:        12,
		PostGoodCommitCount: 0,
		References: []reference{
			{PRNumber: 4208},
			{PRNumber: 4225},
			{PRNumber: 4249},
			{PRNumber: 4313},
		},
	}
	recent := []reference{
		{PRNumber: 4313},
		{PRNumber: 4249},
		{PRNumber: 4249},
		{PRNumber: 4313},
	}

	warning, ok := signaturePRWarning(cluster, recent)
	if !ok {
		t.Fatal("expected warning for missing post-merge signal")
	}
	if !strings.Contains(warning, "post-good-commit-runs=0 across 2 recent PRs") {
		t.Fatalf("unexpected warning: %q", warning)
	}
	if !strings.Contains(warning, "Bad-PR attribution is tentative") {
		t.Fatalf("unexpected warning: %q", warning)
	}
}

func TestReviewReasonsForClusterIgnoresInformationalReason(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		Phase1ClusterID:    "cluster-a",
		MemberSignatureIDs: []string{"sig-a"},
	}
	index := buildReviewSignalIndex([]reviewItem{
		{
			Reason:                 "phase1_cluster_id_collision",
			SourcePhase1ClusterIDs: []string{"cluster-a"},
		},
		{
			Reason:             "phase1_cluster_id_collision",
			MemberSignatureIDs: []string{"sig-a"},
		},
	})

	reasons := reviewReasonsForCluster(cluster, index)
	if len(reasons) != 0 {
		t.Fatalf("expected informational reasons to be filtered, got %v", reasons)
	}
}

func TestClusterDailyDensitySparkline(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		References: []reference{
			{OccurredAt: "2026-03-01T10:00:00Z"},
			{OccurredAt: "2026-03-02T10:00:00Z"},
			{OccurredAt: "2026-03-02T12:00:00Z"},
			{OccurredAt: "2026-03-04T10:00:00Z"},
		},
	}

	unicodeSpark, counts, dateRange, ok := clusterDailyDensitySparkline(cluster, 4, time.Date(2026, 3, 4, 16, 30, 0, 0, time.UTC))
	if !ok {
		t.Fatal("expected sparkline to be generated")
	}
	if unicodeSpark == "" {
		t.Fatalf("expected non-empty sparkline string, got unicode=%q", unicodeSpark)
	}
	if got, want := dateRange, "2026-03-01..2026-03-04"; got != want {
		t.Fatalf("unexpected date range: got %q want %q", got, want)
	}
	if got, want := formatCounts(counts), "1,2,0,1"; got != want {
		t.Fatalf("unexpected counts: got %q want %q", got, want)
	}
}

func TestClusterDailyDensitySparklineAnchorsToGeneratedAtWindow(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		References: []reference{
			{OccurredAt: "2026-03-01T10:00:00Z"},
			{OccurredAt: "2026-03-02T10:00:00Z"},
		},
	}

	unicodeSpark, counts, dateRange, ok := clusterDailyDensitySparkline(cluster, 4, time.Date(2026, 3, 7, 1, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("expected sparkline to be generated")
	}
	if unicodeSpark != "····" {
		t.Fatalf("unexpected sparkline: got %q want %q", unicodeSpark, "····")
	}
	if got, want := dateRange, "2026-03-04..2026-03-07"; got != want {
		t.Fatalf("unexpected date range: got %q want %q", got, want)
	}
	if got, want := formatCounts(counts), "0,0,0,0"; got != want {
		t.Fatalf("unexpected counts: got %q want %q", got, want)
	}
}

func TestQualityTrendAnchorUsesConfiguredWindowEnd(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	got := qualityTrendAnchor(generatedAt, "2026-03-08T00:00:00Z")
	want := time.Date(2026, 3, 7, 23, 59, 59, int(time.Second-time.Nanosecond), time.UTC)
	if !got.Equal(want) {
		t.Fatalf("unexpected trend anchor: got %s want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestGenerateWritesPerEnvironmentFilesWhenSplitEnabled(t *testing.T) {
	t.Parallel()

	ctx := logr.NewContext(context.Background(), logr.Discard())
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertTestClusters(ctx, []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase1ClusterID:         "cluster-dev",
			Lane:                    "e2e",
			JobName:                 "job-dev",
			TestName:                "dev test",
			TestSuite:               "suite-dev",
			CanonicalEvidencePhrase: "dev failure phrase",
			SearchQueryPhrase:       "dev query",
			SupportCount:            1,
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/dev-1",
					OccurredAt:  "2026-03-05T10:00:00Z",
					SignatureID: "sig-dev",
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase1ClusterID:         "cluster-int",
			Lane:                    "e2e",
			JobName:                 "job-int",
			TestName:                "int test",
			TestSuite:               "suite-int",
			CanonicalEvidencePhrase: "int failure phrase",
			SearchQueryPhrase:       "int query",
			SupportCount:            1,
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/int-1",
					OccurredAt:  "2026-03-05T11:00:00Z",
					SignatureID: "sig-int",
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed test clusters: %v", err)
	}

	if err := store.UpsertRuns(ctx, []storecontracts.RunRecord{
		{
			Environment: "dev",
			RunURL:      "https://prow.example/run/dev-1",
			JobName:     "job-dev",
			OccurredAt:  "2026-03-05T10:00:00Z",
		},
		{
			Environment: "int",
			RunURL:      "https://prow.example/run/int-1",
			JobName:     "job-int",
			OccurredAt:  "2026-03-05T11:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed runs: %v", err)
	}

	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment: "dev",
			RowID:       "dev-row-1",
			RunURL:      "https://prow.example/run/dev-1",
			SignatureID: "sig-dev",
			RawText:     "full dev error",
		},
		{
			Environment: "int",
			RowID:       "int-row-1",
			RunURL:      "https://prow.example/run/int-1",
			SignatureID: "sig-int",
			RawText:     "full int error",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	if err := store.UpsertReviewQueue(ctx, []semanticcontracts.ReviewItemRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			Environment:   "dev",
			ReviewItemID:  "review-dev",
			Phase:         "phase1",
			Reason:        "dev-review",
		},
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			Environment:   "int",
			ReviewItemID:  "review-int",
			Phase:         "phase1",
			Reason:        "int-review",
		},
	}); err != nil {
		t.Fatalf("seed review queue: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "semantic-quality.html")
	opts := DefaultOptions()
	opts.OutputPath = outputPath
	opts.SplitByEnvironment = true
	opts.Environments = []string{"dev", "int"}

	if err := Generate(ctx, store, opts); err != nil {
		t.Fatalf("generate test summary: %v", err)
	}

	devPath := filepath.Join(filepath.Dir(outputPath), "semantic-quality.dev.html")
	intPath := filepath.Join(filepath.Dir(outputPath), "semantic-quality.int.html")
	devReport, err := os.ReadFile(devPath)
	if err != nil {
		t.Fatalf("read dev report: %v", err)
	}
	intReport, err := os.ReadFile(intPath)
	if err != nil {
		t.Fatalf("read int report: %v", err)
	}
	if !strings.Contains(string(devReport), "dev test") {
		t.Fatalf("expected dev report to include dev test: %q", string(devReport))
	}
	if strings.Contains(string(devReport), "int test") {
		t.Fatalf("did not expect dev report to include int test: %q", string(devReport))
	}
	if !strings.Contains(string(intReport), "int test") {
		t.Fatalf("expected int report to include int test: %q", string(intReport))
	}
	if strings.Contains(string(intReport), "dev test") {
		t.Fatalf("did not expect int report to include dev test: %q", string(intReport))
	}
}

func TestGenerateMinRunsUsesLatestSippyTestMetadataRuns(t *testing.T) {
	t.Parallel()

	ctx := logr.NewContext(context.Background(), logr.Discard())
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	const (
		environment = "dev"
		runURL      = "https://prow.example/run/dev-1"
		testName    = "dev test"
		testSuite   = "rp-api-compat-all/parallel"
	)

	if err := store.UpsertTestClusters(ctx, []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             environment,
			Phase1ClusterID:         "cluster-dev",
			Lane:                    "e2e",
			JobName:                 "job-dev",
			TestName:                testName,
			TestSuite:               testSuite,
			CanonicalEvidencePhrase: "dev failure phrase",
			SearchQueryPhrase:       "dev query",
			SupportCount:            1,
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:         runURL,
					OccurredAt:     "2026-03-05T10:00:00Z",
					SignatureID:    "sig-dev",
					PRNumber:       4242,
					PostGoodCommit: false,
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed test clusters: %v", err)
	}

	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment:    environment,
			RowID:          "dev-row-1",
			RunURL:         runURL,
			SignatureID:    "sig-dev",
			OccurredAt:     "2026-03-05T10:00:00Z",
			RawText:        "full dev error",
			NormalizedText: "full dev error",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	metadataDate := time.Now().UTC().Format("2006-01-02")
	if err := store.UpsertTestMetadataDaily(ctx, []storecontracts.TestMetadataDailyRecord{
		{
			Environment:            environment,
			Date:                   metadataDate,
			Release:                "4.20",
			Period:                 "default",
			TestName:               testName,
			TestSuite:              testSuite,
			CurrentPassPercentage:  92.5,
			CurrentRuns:            25,
			PreviousPassPercentage: 88.0,
			PreviousRuns:           25,
			NetImprovement:         4.5,
			IngestedAt:             time.Now().UTC().Format(time.RFC3339),
		},
	}); err != nil {
		t.Fatalf("seed test metadata daily: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "semantic-quality.html")
	opts := DefaultOptions()
	opts.OutputPath = outputPath
	opts.Environments = []string{environment}
	opts.TopTests = 0
	opts.MinRuns = 10

	if err := Generate(ctx, store, opts); err != nil {
		t.Fatalf("generate test summary: %v", err)
	}

	report, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	text := string(report)
	if !strings.Contains(text, testName) {
		t.Fatalf("expected test to survive min-runs filter via sippy metadata, got report=%q", text)
	}
	if !strings.Contains(text, "92.50% (25 runs)") {
		t.Fatalf("expected report to include sippy runs/passrate, got report=%q", text)
	}
}

func TestValidateOptionsRejectsUnknownFormat(t *testing.T) {
	t.Parallel()

	_, err := validateOptions(Options{
		OutputPath: "data/reports/semantic-quality.html",
		Format:     "pdf",
		RecentRuns: 4,
	})
	if err == nil {
		t.Fatal("expected format validation error")
	}
	if !strings.Contains(err.Error(), "invalid --format") {
		t.Fatalf("expected invalid format error, got %v", err)
	}
}

func TestValidateOptionsRejectsPartialWindow(t *testing.T) {
	t.Parallel()

	_, err := validateOptions(Options{
		OutputPath:        "data/reports/semantic-quality.html",
		Format:            reportFormatHTML,
		WindowStart:       "2026-03-01",
		RecentRuns:        4,
		QualityExportPath: "",
	})
	if err == nil {
		t.Fatal("expected workflow window validation error")
	}
	if !strings.Contains(err.Error(), "both --workflow.window.start and --workflow.window.end must be set together") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateWritesHTMLQualityReportAndFlaggedExport(t *testing.T) {
	t.Parallel()

	ctx := logr.NewContext(context.Background(), logr.Discard())
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertTestClusters(ctx, []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase1ClusterID:         "cluster-suspicious-1",
			Lane:                    "e2e",
			JobName:                 "job-dev",
			TestName:                "quality signal test one",
			TestSuite:               "suite-dev",
			CanonicalEvidencePhrase: "<context.deadlineExceededError>{},",
			SearchQueryPhrase:       "deadline exceeded context",
			SupportCount:            3,
			PostGoodCommitCount:     1,
			MemberSignatureIDs:      []string{"sig-suspicious-1"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4201/pull-ci-Azure-ARO-HCP-main-e2e-parallel/10001",
					OccurredAt:  "2026-03-07T12:00:00Z",
					SignatureID: "sig-suspicious-1",
					PRNumber:    4201,
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase1ClusterID:         "cluster-suspicious-2",
			Lane:                    "e2e",
			JobName:                 "job-dev",
			TestName:                "quality signal test two",
			TestSuite:               "suite-dev",
			CanonicalEvidencePhrase: "ErrorCode: \"\",",
			SearchQueryPhrase:       "empty error code",
			SupportCount:            2,
			PostGoodCommitCount:     0,
			MemberSignatureIDs:      []string{"sig-suspicious-2"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4202/pull-ci-Azure-ARO-HCP-main-e2e-parallel/10002",
					OccurredAt:  "2026-03-06T12:00:00Z",
					SignatureID: "sig-suspicious-2",
					PRNumber:    4202,
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed test clusters: %v", err)
	}

	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment: "dev",
			RowID:       "row-suspicious-1",
			RunURL:      "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4201/pull-ci-Azure-ARO-HCP-main-e2e-parallel/10001",
			SignatureID: "sig-suspicious-1",
			RawText:     "failed waiting for cluster create with context deadline exceeded",
		},
		{
			Environment: "dev",
			RowID:       "row-suspicious-2",
			RunURL:      "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4202/pull-ci-Azure-ARO-HCP-main-e2e-parallel/10002",
			SignatureID: "sig-suspicious-2",
			RawText:     "provider response contained empty ErrorCode field",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	if err := store.UpsertReviewQueue(ctx, []semanticcontracts.ReviewItemRecord{
		{
			SchemaVersion:          semanticcontracts.SchemaVersionV1,
			Environment:            "dev",
			ReviewItemID:           "review-1",
			Phase:                  "phase1",
			Reason:                 "low_confidence_evidence",
			SourcePhase1ClusterIDs: []string{"cluster-suspicious-1"},
		},
	}); err != nil {
		t.Fatalf("seed review queue: %v", err)
	}

	outputDir := t.TempDir()
	opts := DefaultOptions()
	opts.OutputPath = filepath.Join(outputDir, "semantic-quality.html")
	opts.Format = reportFormatHTML
	opts.QualityExportPath = filepath.Join(outputDir, "flagged-signatures.ndjson")
	opts.WindowStart = "2026-03-01"
	opts.WindowEnd = "2026-03-08"
	opts.RecentRuns = 2
	opts.MinRuns = 0

	if err := Generate(ctx, store, opts); err != nil {
		t.Fatalf("generate html quality report: %v", err)
	}

	reportBytes, err := os.ReadFile(opts.OutputPath)
	if err != nil {
		t.Fatalf("read html report: %v", err)
	}
	report := string(reportBytes)
	requiredReportSnippets := []string{
		"CI Semantic Quality Report",
		"id=\"theme-toggle\"",
		"Window: <strong>2026-03-01</strong> to <strong>2026-03-07</strong> (7 days)",
		"2026-03-01..2026-03-07",
		"filter-env",
		"filter-lane",
		"filter-min-support",
		"filter-flagged-only",
		"filter-review-only",
		"filter-search",
		"<table class=\"triage-table\"",
		".triage-table { width: 100%;",
		"Quality score",
		"Quality flags",
		"Review flags",
		"Full failure examples (1)",
		"Affected runs (1)",
		"Associated PR",
		"https://github.com/Azure/ARO-HCP/pull/4201",
		"prow job",
		"&lt;context.deadlineExceededError&gt;{},",
		"context type stub leaked",
		"low_confidence_evidence",
		"contains empty ErrorCode",
	}
	for _, snippet := range requiredReportSnippets {
		if !strings.Contains(report, snippet) {
			t.Fatalf("expected html quality report to contain %q", snippet)
		}
	}
	for _, snippet := range []string{
		"<h3>Suspicious Signatures</h3>",
		"<th>Post-good</th>",
		"<th>Full errors</th>",
		"<th>Recent runs</th>",
		"Error Spread (last",
	} {
		if strings.Contains(report, snippet) {
			t.Fatalf("expected html quality report to not contain %q", snippet)
		}
	}

	exportBytes, err := os.ReadFile(opts.QualityExportPath)
	if err != nil {
		t.Fatalf("read quality export: %v", err)
	}
	exportText := string(exportBytes)
	requiredExportSnippets := []string{
		"cluster-suspicious-1",
		"cluster-suspicious-2",
		"quality_issue_codes",
		"context_type_stub",
		"empty_error_code",
	}
	for _, snippet := range requiredExportSnippets {
		if !strings.Contains(exportText, snippet) {
			t.Fatalf("expected quality export to contain %q", snippet)
		}
	}
}

func TestGeneratePrefersGlobalClustersForQualityReport(t *testing.T) {
	t.Parallel()

	ctx := logr.NewContext(context.Background(), logr.Discard())
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertTestClusters(ctx, []semanticcontracts.TestClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase1ClusterID:         "phase1-only-cluster",
			Lane:                    "e2e",
			JobName:                 "job-dev",
			TestName:                "per test quality row",
			TestSuite:               "suite-dev",
			CanonicalEvidencePhrase: "per-test phrase should not be used when global is present",
			SearchQueryPhrase:       "per-test phrase query",
			SupportCount:            2,
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/per-test-1",
					OccurredAt:  "2026-03-07T12:00:00Z",
					SignatureID: "sig-per-test-1",
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed test clusters: %v", err)
	}

	if err := store.UpsertGlobalClusters(ctx, []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "phase2-global-1",
			CanonicalEvidencePhrase: "<context.deadlineExceededError>{},",
			SearchQueryPhrase:       "deadline exceeded context",
			SupportCount:            3,
			PostGoodCommitCount:     1,
			ContributingTestsCount:  1,
			ContributingTests: []semanticcontracts.ContributingTestRecord{
				{
					Lane:         "e2e",
					JobName:      "pull-ci-dev-e2e",
					TestName:     "global quality row",
					SupportCount: 3,
				},
			},
			MemberPhase1ClusterIDs: []string{"phase1-global-member"},
			MemberSignatureIDs:     []string{"sig-global-1"},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.example/run/global-1",
					OccurredAt:  "2026-03-07T12:00:00Z",
					SignatureID: "sig-global-1",
					PRNumber:    4313,
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed global clusters: %v", err)
	}

	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment: "dev",
			RowID:       "global-row-1",
			RunURL:      "https://prow.example/run/global-1",
			SignatureID: "sig-global-1",
			RawText:     "failed waiting for cluster create with context deadline exceeded",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	if err := store.UpsertReviewQueue(ctx, []semanticcontracts.ReviewItemRecord{
		{
			SchemaVersion:          semanticcontracts.SchemaVersionV1,
			Environment:            "dev",
			ReviewItemID:           "review-global-1",
			Phase:                  "phase2",
			Reason:                 "low_confidence_evidence",
			SourcePhase1ClusterIDs: []string{"phase1-global-member"},
		},
	}); err != nil {
		t.Fatalf("seed review queue: %v", err)
	}

	outputDir := t.TempDir()
	opts := DefaultOptions()
	opts.OutputPath = filepath.Join(outputDir, "semantic-quality.html")
	opts.Format = reportFormatHTML
	opts.QualityExportPath = filepath.Join(outputDir, "flagged-signatures.ndjson")
	opts.WindowStart = "2026-03-01"
	opts.WindowEnd = "2026-03-08"
	opts.RecentRuns = 2

	if err := Generate(ctx, store, opts); err != nil {
		t.Fatalf("generate html quality report: %v", err)
	}

	reportBytes, err := os.ReadFile(opts.OutputPath)
	if err != nil {
		t.Fatalf("read html report: %v", err)
	}
	report := string(reportBytes)
	for _, snippet := range []string{
		"Source semantic clusters: <code>store:global_clusters</code>",
		"&lt;context.deadlineExceededError&gt;{},",
		"global quality row",
		"phase2-global-1",
		"context type stub leaked",
	} {
		if !strings.Contains(report, snippet) {
			t.Fatalf("expected html quality report to contain %q", snippet)
		}
	}
	if strings.Contains(report, "per-test phrase should not be used when global is present") {
		t.Fatalf("expected report to use global clusters over per-test clusters")
	}

	exportBytes, err := os.ReadFile(opts.QualityExportPath)
	if err != nil {
		t.Fatalf("read quality export: %v", err)
	}
	exportText := string(exportBytes)
	if !strings.Contains(exportText, "phase2-global-1") {
		t.Fatalf("expected quality export to include global cluster id, got %q", exportText)
	}
}

func TestBuildQualitySignatureRowsIgnoreCancellationReviewNoise(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		Environment:             "prod",
		Phase1ClusterID:         "cluster-cancel",
		Lane:                    "e2e",
		JobName:                 "periodic-prod-e2e-parallel",
		TestName:                "cluster cancellation test",
		TestSuite:               "prod/parallel",
		CanonicalEvidencePhrase: "Interrupted by User",
		SupportCount:            3,
		References: []reference{
			{
				RunURL:      "https://prow.example/run/cancel-1",
				OccurredAt:  "2026-03-07T12:00:00Z",
				SignatureID: "sig-cancel-1",
			},
		},
		MemberSignatureIDs: []string{"sig-cancel-1"},
	}
	reviewIndex := buildReviewSignalIndex([]reviewItem{
		{
			Reason:                 "insufficient_inner_error",
			SourcePhase1ClusterIDs: []string{"cluster-cancel"},
		},
		{
			Reason:             "low_confidence_evidence",
			MemberSignatureIDs: []string{"sig-cancel-1"},
		},
	})

	rows := buildQualitySignatureRows(
		[]testCluster{cluster},
		map[testKey]testMetadata{},
		map[testKeyNoSuite]testMetadata{},
		map[referenceKey]string{
			{
				RunURL:      "https://prow.example/run/cancel-1",
				SignatureID: "sig-cancel-1",
			}: "job timeout cancellation",
		},
		reviewIndex,
		time.Date(2026, 3, 7, 16, 30, 0, 0, time.UTC),
		0,
		4,
		0,
	)
	if len(rows) != 1 {
		t.Fatalf("expected one quality row, got %d", len(rows))
	}
	if len(rows[0].ReviewReasons) != 0 {
		t.Fatalf("expected cancellation review noise to be filtered, got %v", rows[0].ReviewReasons)
	}
	if rows[0].QualityScore != 0 {
		t.Fatalf("expected cancellation row score=0, got %d", rows[0].QualityScore)
	}
	if rows[0].isFlagged() {
		t.Fatalf("expected cancellation row to not be flagged")
	}
}

func TestBuildQualitySignatureRowsFlagsSourceDeserializationNoOutput(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		Environment:             "dev",
		Phase1ClusterID:         "cluster-deserialization",
		Lane:                    "e2e",
		JobName:                 "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		TestName:                "Customer should be able to create several HCP clusters in their customer resource group, but not in the same managed resource group",
		TestSuite:               "rp-api-compat-all/parallel",
		CanonicalEvidencePhrase: "Deserializaion Error: no output from command",
		SupportCount:            7,
		References: []reference{
			{
				RunURL:      "https://prow.example/run/deserialization-1",
				OccurredAt:  "2026-03-07T12:00:00Z",
				SignatureID: "sig-deserialization-1",
			},
		},
		MemberSignatureIDs: []string{"sig-deserialization-1"},
	}

	rows := buildQualitySignatureRows(
		[]testCluster{cluster},
		map[testKey]testMetadata{},
		map[testKeyNoSuite]testMetadata{},
		map[referenceKey]string{
			{
				RunURL:      "https://prow.example/run/deserialization-1",
				SignatureID: "sig-deserialization-1",
			}: "Deserializaion Error: no output from command",
		},
		reviewSignalIndex{},
		time.Date(2026, 3, 7, 16, 30, 0, 0, time.UTC),
		0,
		4,
		0,
	)
	if len(rows) != 1 {
		t.Fatalf("expected one quality row, got %d", len(rows))
	}

	row := rows[0]
	hasIssueCode := false
	for _, code := range row.IssueCodes {
		if code == "source_deserialization_no_output" {
			hasIssueCode = true
			break
		}
	}
	if !hasIssueCode {
		t.Fatalf("expected source_deserialization_no_output issue code, got %v", row.IssueCodes)
	}
	if row.QualityScore < 9 {
		t.Fatalf("expected source deserialization issue to be high severity (>=9), got %d", row.QualityScore)
	}
	if !row.isFlagged() {
		t.Fatalf("expected deserialization no-output row to be flagged")
	}
}

func TestBuildQualitySignatureRowsSkipsDeserializationNoOutputFlagWhenCommandErrorExists(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		Environment:             "dev",
		Phase1ClusterID:         "cluster-command-error",
		Lane:                    "e2e",
		JobName:                 "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		TestName:                "Customer should be able to update nodepool replicas and autoscaling",
		TestSuite:               "rp-api-compat-all/parallel",
		CanonicalEvidencePhrase: "Command Error: exit status 2",
		SupportCount:            1,
		References: []reference{
			{
				RunURL:      "https://prow.example/run/command-error-1",
				OccurredAt:  "2026-03-16T11:48:11Z",
				SignatureID: "sig-command-error-1",
			},
		},
		MemberSignatureIDs: []string{"sig-command-error-1"},
	}

	rows := buildQualitySignatureRows(
		[]testCluster{cluster},
		map[testKey]testMetadata{},
		map[testKeyNoSuite]testMetadata{},
		map[referenceKey]string{
			{
				RunURL:      "https://prow.example/run/command-error-1",
				SignatureID: "sig-command-error-1",
			}: "Command Error: exit status 2\nDeserializaion Error: no output from command",
		},
		reviewSignalIndex{},
		time.Date(2026, 3, 16, 16, 30, 0, 0, time.UTC),
		0,
		4,
		0,
	)
	if len(rows) != 1 {
		t.Fatalf("expected one quality row, got %d", len(rows))
	}

	row := rows[0]
	for _, code := range row.IssueCodes {
		if code == "source_deserialization_no_output" {
			t.Fatalf("did not expect source_deserialization_no_output when command error context exists, got %v", row.IssueCodes)
		}
	}
	if row.QualityScore != 0 {
		t.Fatalf("expected command-error-backed row score=0, got %d", row.QualityScore)
	}
	if row.isFlagged() {
		t.Fatalf("expected command-error-backed row to not be flagged")
	}
}

func TestBuildQualitySignatureRowsFlagsGenericFailurePhrase(t *testing.T) {
	t.Parallel()

	cluster := testCluster{
		Environment:             "stg",
		Phase1ClusterID:         "cluster-generic-failure",
		Lane:                    "e2e",
		JobName:                 "periodic-stage-e2e-parallel",
		TestName:                "Customer should be able to list available HCP OpenShift versions and validate response content",
		TestSuite:               "stage/parallel",
		CanonicalEvidencePhrase: "failure",
		SupportCount:            2,
		References: []reference{
			{
				RunURL:      "https://prow.example/run/generic-failure-1",
				OccurredAt:  "2026-03-07T12:00:00Z",
				SignatureID: "sig-generic-failure-1",
			},
		},
		MemberSignatureIDs: []string{"sig-generic-failure-1"},
	}

	rows := buildQualitySignatureRows(
		[]testCluster{cluster},
		map[testKey]testMetadata{},
		map[testKeyNoSuite]testMetadata{},
		map[referenceKey]string{
			{
				RunURL:      "https://prow.example/run/generic-failure-1",
				SignatureID: "sig-generic-failure-1",
			}: "no better error context available",
		},
		reviewSignalIndex{},
		time.Date(2026, 3, 7, 16, 30, 0, 0, time.UTC),
		0,
		4,
		0,
	)
	if len(rows) != 1 {
		t.Fatalf("expected one quality row, got %d", len(rows))
	}

	row := rows[0]
	hasIssueCode := false
	for _, code := range row.IssueCodes {
		if code == "generic_failure_phrase" {
			hasIssueCode = true
			break
		}
	}
	if !hasIssueCode {
		t.Fatalf("expected generic_failure_phrase issue code, got %v", row.IssueCodes)
	}
	if row.QualityScore < 5 {
		t.Fatalf("expected generic failure issue to score at least 5, got %d", row.QualityScore)
	}
	if !row.isFlagged() {
		t.Fatalf("expected generic failure row to be flagged")
	}
}

func TestResolveInspectorGitHubPRURLFromProwRunFallbackToDefaultRepo(t *testing.T) {
	t.Parallel()

	got, ok := resolveInspectorGitHubPRURLFromProwRun("https://prow.example/run/unknown-format", 4313)
	if !ok {
		t.Fatalf("expected resolver fallback to succeed")
	}
	if got != "https://github.com/Azure/ARO-HCP/pull/4313" {
		t.Fatalf("unexpected fallback URL: %q", got)
	}
}
