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

func TestBuildMarkdownHighlightsSignalsAndWarnings(t *testing.T) {
	t.Parallel()

	postGoodCluster := testCluster{
		Phase1ClusterID:         "cluster-a",
		Lane:                    "e2e",
		JobName:                 "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		TestName:                "Customer should be able to create an HCP cluster",
		TestSuite:               "rp-api-compat-all/parallel",
		CanonicalEvidencePhrase: "msg: \"failed to create route: the server is currently unable to handle the request\",",
		SupportCount:            2,
		PostGoodCommitCount:     2,
		MemberSignatureIDs:      []string{"sig-a", "sig-b"},
		References: []reference{
			{
				RunURL:      "https://prow.example/run/postgood-1",
				OccurredAt:  "2026-03-03T15:33:10Z",
				PRNumber:    4212,
				SignatureID: "sig-a",
			},
			{
				RunURL:      "https://prow.example/run/postgood-2",
				OccurredAt:  "2026-03-02T15:33:10Z",
				PRNumber:    4252,
				SignatureID: "sig-b",
			},
		},
	}

	badPRCluster := testCluster{
		Phase1ClusterID:         "cluster-b",
		Lane:                    "e2e",
		JobName:                 "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		TestName:                "Customer should be able to create an HCP cluster and manage pull secrets",
		TestSuite:               "rp-api-compat-all/parallel",
		CanonicalEvidencePhrase: "msg: \"failed waiting for cluster=\\\"<id>\\\" in resourcegroup=\\\"<id>\\\" to finish creating, caused by: timeout '45.000000' minutes exceeded during CreateHCPClusterFromParam\"",
		SupportCount:            11,
		PostGoodCommitCount:     0,
		MemberSignatureIDs:      []string{"sig-c", "sig-d"},
		References: []reference{
			{
				RunURL:      "https://prow.example/run/badpr-1",
				OccurredAt:  "2026-03-02T07:10:51Z",
				PRNumber:    4249,
				SignatureID: "sig-c",
			},
			{
				RunURL:      "https://prow.example/run/badpr-2",
				OccurredAt:  "2026-03-02T02:51:46Z",
				PRNumber:    4249,
				SignatureID: "sig-d",
			},
		},
	}

	metadataByFull := map[testKey]testMetadata{
		toTestKey(postGoodCluster.Lane, postGoodCluster.JobName, postGoodCluster.TestName, postGoodCluster.TestSuite): {
			PassRate: float64Ptr(96.25),
			Runs:     160,
			Tags:     []string{"required"},
		},
		toTestKey(badPRCluster.Lane, badPRCluster.JobName, badPRCluster.TestName, badPRCluster.TestSuite): {
			PassRate: float64Ptr(81.00),
			Runs:     90,
		},
	}

	reviewIndex := buildReviewSignalIndex([]reviewItem{
		{
			Reason:                 "insufficient_inner_error",
			SourcePhase1ClusterIDs: []string{"cluster-b"},
		},
		{
			Reason:             "low_confidence_evidence",
			MemberSignatureIDs: []string{"sig-a"},
		},
	})

	report := buildMarkdown(
		[]testCluster{postGoodCluster, badPRCluster},
		metadataByFull,
		map[testKeyNoSuite]testMetadata{},
		map[referenceKey]string{
			{RunURL: "https://prow.example/run/badpr-1", SignatureID: "sig-c"}: "full badpr error sample 1",
			{RunURL: "https://prow.example/run/badpr-2", SignatureID: "sig-d"}: "full badpr error sample 2",
		},
		reviewIndex,
		"data/views/agent/test_clusters.ndjson",
		"data/raw/failures.ndjson",
		time.Date(2026, 3, 4, 16, 30, 0, 0, time.UTC),
		0,
		4,
		10,
	)

	required := []string{
		"# CI Test Failure Summary",
		"Filtered to tests with at least **10** observed runs.",
		"## 1. Customer should be able to create an HCP cluster and manage pull secrets",
		"## 2. Customer should be able to create an HCP cluster",
		"Risk classification: **likely PR-specific (single PR)**",
		"Risk classification: **systemic signal (post-good-commit-runs > 0)**",
		"Current success rate: **81.00%** over **90** runs",
		"Current success rate: **96.25%** over **160** runs",
		"Confidence: **needs review** (`insufficient_inner_error`)",
		"Confidence: **needs review** (`low_confidence_evidence`)",
		"WARNING: signature post-good-commit-runs=0 and all observed failures map to PR #4249 (2/11). Strong bad-PR signal.",
		"SIGNAL: signature has post-good-commit-runs evidence (**2/2** failures). Treat as likely systemic or pre-existing instability.",
		"Daily density (last 7d, oldest->newest",
		"Full error samples (for eyeballing report accuracy)",
		"full badpr error sample 1",
		"full badpr error sample 2",
	}
	for _, expected := range required {
		if !strings.Contains(report, expected) {
			t.Fatalf("expected report to contain %q", expected)
		}
	}

	disallowed := []string{
		"## Quick Index",
		"## At A Glance",
		"## Triage Queue",
		"## Top Signature Hotspots",
	}
	for _, section := range disallowed {
		if strings.Contains(report, section) {
			t.Fatalf("did not expect report section %q", section)
		}
	}
}

func TestBuildMarkdownSortsTestsBySuccessRate(t *testing.T) {
	t.Parallel()

	highPassHighFailures := testCluster{
		Phase1ClusterID:         "cluster-high-pass",
		Lane:                    "e2e",
		JobName:                 "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		TestName:                "high pass-rate test",
		TestSuite:               "rp-api-compat-all/parallel",
		CanonicalEvidencePhrase: "high pass sample",
		SupportCount:            20,
		References: []reference{
			{RunURL: "https://prow.example/run/high-1", OccurredAt: "2026-03-03T10:00:00Z", SignatureID: "sig-high"},
		},
	}
	lowPassLowFailures := testCluster{
		Phase1ClusterID:         "cluster-low-pass",
		Lane:                    "e2e",
		JobName:                 "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		TestName:                "low pass-rate test",
		TestSuite:               "rp-api-compat-all/parallel",
		CanonicalEvidencePhrase: "low pass sample",
		SupportCount:            2,
		References: []reference{
			{RunURL: "https://prow.example/run/low-1", OccurredAt: "2026-03-03T09:00:00Z", SignatureID: "sig-low"},
		},
	}

	metadataByFull := map[testKey]testMetadata{
		toTestKey(highPassHighFailures.Lane, highPassHighFailures.JobName, highPassHighFailures.TestName, highPassHighFailures.TestSuite): {
			PassRate: float64Ptr(99.0),
			Runs:     200,
		},
		toTestKey(lowPassLowFailures.Lane, lowPassLowFailures.JobName, lowPassLowFailures.TestName, lowPassLowFailures.TestSuite): {
			PassRate: float64Ptr(75.0),
			Runs:     100,
		},
	}

	report := buildMarkdown(
		[]testCluster{highPassHighFailures, lowPassLowFailures},
		metadataByFull,
		map[testKeyNoSuite]testMetadata{},
		map[referenceKey]string{},
		reviewSignalIndex{},
		"data/views/agent/test_clusters.ndjson",
		"data/raw/failures.ndjson",
		time.Date(2026, 3, 4, 16, 30, 0, 0, time.UTC),
		0,
		4,
		10,
	)

	first := strings.Index(report, "## 1. low pass-rate test")
	second := strings.Index(report, "## 2. high pass-rate test")
	if first < 0 || second < 0 {
		t.Fatalf("expected both tests to be present in ordered headings; report=%q", report)
	}
	if first > second {
		t.Fatalf("expected lower success-rate test first; report=%q", report)
	}
}

func TestBuildMarkdownFiltersTestsByMinRuns(t *testing.T) {
	t.Parallel()

	eligible := testCluster{
		Phase1ClusterID:         "cluster-eligible",
		Lane:                    "e2e",
		JobName:                 "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		TestName:                "eligible test",
		TestSuite:               "rp-api-compat-all/parallel",
		CanonicalEvidencePhrase: "eligible sample",
		SupportCount:            3,
		References: []reference{
			{RunURL: "https://prow.example/run/eligible", OccurredAt: "2026-03-03T10:00:00Z", SignatureID: "sig-e"},
		},
	}
	ineligible := testCluster{
		Phase1ClusterID:         "cluster-ineligible",
		Lane:                    "e2e",
		JobName:                 "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
		TestName:                "ineligible test",
		TestSuite:               "rp-api-compat-all/parallel",
		CanonicalEvidencePhrase: "ineligible sample",
		SupportCount:            9,
		References: []reference{
			{RunURL: "https://prow.example/run/ineligible", OccurredAt: "2026-03-03T11:00:00Z", SignatureID: "sig-i"},
		},
	}

	metadataByFull := map[testKey]testMetadata{
		toTestKey(eligible.Lane, eligible.JobName, eligible.TestName, eligible.TestSuite): {
			PassRate: float64Ptr(90.0),
			Runs:     10,
		},
		toTestKey(ineligible.Lane, ineligible.JobName, ineligible.TestName, ineligible.TestSuite): {
			PassRate: float64Ptr(50.0),
			Runs:     9,
		},
	}

	report := buildMarkdown(
		[]testCluster{eligible, ineligible},
		metadataByFull,
		map[testKeyNoSuite]testMetadata{},
		map[referenceKey]string{},
		reviewSignalIndex{},
		"data/views/agent/test_clusters.ndjson",
		"data/raw/failures.ndjson",
		time.Date(2026, 3, 4, 16, 30, 0, 0, time.UTC),
		0,
		4,
		10,
	)

	if !strings.Contains(report, "## 1. eligible test") {
		t.Fatalf("expected eligible test in report, got: %q", report)
	}
	if strings.Contains(report, "ineligible test") {
		t.Fatalf("did not expect ineligible test in report, got: %q", report)
	}
}

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

	outputPath := filepath.Join(t.TempDir(), "test-failure-summary.md")
	opts := DefaultOptions()
	opts.OutputPath = outputPath
	opts.SplitByEnvironment = true
	opts.Environments = []string{"dev", "int"}

	if err := Generate(ctx, store, opts); err != nil {
		t.Fatalf("generate test summary: %v", err)
	}

	devPath := filepath.Join(filepath.Dir(outputPath), "test-failure-summary.dev.md")
	intPath := filepath.Join(filepath.Dir(outputPath), "test-failure-summary.int.md")
	devReport, err := os.ReadFile(devPath)
	if err != nil {
		t.Fatalf("read dev report: %v", err)
	}
	intReport, err := os.ReadFile(intPath)
	if err != nil {
		t.Fatalf("read int report: %v", err)
	}
	if !strings.Contains(string(devReport), "## 1. dev test") {
		t.Fatalf("expected dev report to include dev test: %q", string(devReport))
	}
	if strings.Contains(string(devReport), "## 1. int test") || strings.Contains(string(devReport), "int test") {
		t.Fatalf("did not expect dev report to include int test: %q", string(devReport))
	}
	if !strings.Contains(string(intReport), "## 1. int test") {
		t.Fatalf("expected int report to include int test: %q", string(intReport))
	}
	if strings.Contains(string(intReport), "## 1. dev test") || strings.Contains(string(intReport), "dev test") {
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

	outputPath := filepath.Join(t.TempDir(), "test-failure-summary.md")
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
	if !strings.Contains(text, "## 1. "+testName) {
		t.Fatalf("expected test to survive min-runs filter via sippy metadata, got report=%q", text)
	}
	if !strings.Contains(text, "Current success rate: **92.50%** over **25** runs") {
		t.Fatalf("expected report to include sippy runs/passrate, got report=%q", text)
	}
}

func TestValidateOptionsRejectsUnknownFormat(t *testing.T) {
	t.Parallel()

	_, err := validateOptions(Options{
		OutputPath: "data/reports/test-failure-summary.md",
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
					RunURL:      "https://prow.example/run/suspicious-1",
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
					RunURL:      "https://prow.example/run/suspicious-2",
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
			RunURL:      "https://prow.example/run/suspicious-1",
			SignatureID: "sig-suspicious-1",
			RawText:     "failed waiting for cluster create with context deadline exceeded",
		},
		{
			Environment: "dev",
			RowID:       "row-suspicious-2",
			RunURL:      "https://prow.example/run/suspicious-2",
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
		"Suspicious Signatures",
		"Error Spread (last 7 days)",
		"filter-env",
		"Show 1 full errors",
		"&lt;context.deadlineExceededError&gt;{},",
		"context type stub leaked",
		"contains empty ErrorCode",
	}
	for _, snippet := range requiredReportSnippets {
		if !strings.Contains(report, snippet) {
			t.Fatalf("expected html quality report to contain %q", snippet)
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
