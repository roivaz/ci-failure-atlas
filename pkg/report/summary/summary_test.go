package summary

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/report/triagehtml"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

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

	outputPath := filepath.Join(t.TempDir(), "global-signature-triage.html")
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
	if !strings.Contains(content, "CI Global Signature Triage Report") {
		t.Fatalf("expected global triage HTML header in output: %q", content)
	}
	if !strings.Contains(content, "<th>Signature</th>") {
		t.Fatalf("expected rendered triage table in output: %q", content)
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

	outputPath := filepath.Join(t.TempDir(), "global-signature-triage.html")
	opts := DefaultOptions()
	opts.OutputPath = outputPath
	opts.SplitByEnvironment = true
	opts.Environments = []string{"dev", "int"}

	if err := Generate(ctx, store, opts); err != nil {
		t.Fatalf("generate summary: %v", err)
	}

	devPath := filepath.Join(filepath.Dir(outputPath), "global-signature-triage.dev.html")
	intPath := filepath.Join(filepath.Dir(outputPath), "global-signature-triage.int.html")
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

func TestValidateOptionsRejectsUnknownFormat(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.Format = "pdf"
	if _, err := validateOptions(opts); err == nil {
		t.Fatalf("expected unknown format to fail validation")
	}
}

func TestValidateOptionsRejectsPartialWindow(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.WindowStart = "2026-03-01"
	if _, err := validateOptions(opts); err == nil {
		t.Fatalf("expected partial workflow window to fail validation")
	}
}

func TestGenerateWritesHTMLGlobalTriageReport(t *testing.T) {
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
			Phase2ClusterID:         "g-dev-shared",
			CanonicalEvidencePhrase: "<context.deadlineExceededError>{}",
			SearchQueryPhrase:       "deadline exceeded",
			SupportCount:            6,
			PostGoodCommitCount:     2,
			MemberSignatureIDs:      []string{"sig-dev-1"},
			ContributingTests: []semanticcontracts.ContributingTestRecord{
				{Lane: "e2e", JobName: "job-dev", TestName: "test-dev-a", SupportCount: 4},
			},
			References: []semanticcontracts.ReferenceRecord{
				{
					RunURL:      "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029603943687917568",
					OccurredAt:  "2026-03-09T11:00:00Z",
					SignatureID: "sig-dev-1",
					PRNumber:    4313,
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "int",
			Phase2ClusterID:         "g-int-shared",
			CanonicalEvidencePhrase: "<context.deadlineExceededError>{}",
			SearchQueryPhrase:       "deadline exceeded",
			SupportCount:            4,
			PostGoodCommitCount:     1,
			MemberSignatureIDs:      []string{"sig-int-1"},
			ContributingTests: []semanticcontracts.ContributingTestRecord{
				{Lane: "e2e", JobName: "job-int", TestName: "test-int-a", SupportCount: 3},
			},
			References: []semanticcontracts.ReferenceRecord{
				{RunURL: "https://prow.example/run/int-1", OccurredAt: "2026-03-09T11:30:00Z", SignatureID: "sig-int-1"},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "prod",
			Phase2ClusterID:         "g-prod-unique",
			CanonicalEvidencePhrase: "failed waiting for cluster operators",
			SearchQueryPhrase:       "cluster operators not available",
			SupportCount:            3,
			PostGoodCommitCount:     0,
			MemberSignatureIDs:      []string{"sig-prod-1"},
			ContributingTests: []semanticcontracts.ContributingTestRecord{
				{Lane: "e2e", JobName: "job-prod", TestName: "test-prod-a", SupportCount: 2},
			},
			References: []semanticcontracts.ReferenceRecord{
				{RunURL: "https://prow.example/run/prod-1", OccurredAt: "2026-03-09T12:00:00Z", SignatureID: "sig-prod-1"},
			},
		},
	}); err != nil {
		t.Fatalf("seed global clusters: %v", err)
	}
	if err := store.UpsertRawFailures(ctx, []storecontracts.RawFailureRecord{
		{
			Environment: "dev",
			RowID:       "raw-dev-1",
			RunURL:      "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029603943687917568",
			SignatureID: "sig-dev-1",
			RawText:     "raw dev timeout sample",
		},
		{
			Environment: "int",
			RowID:       "raw-int-1",
			RunURL:      "https://prow.example/run/int-1",
			SignatureID: "sig-int-1",
			RawText:     "raw int timeout sample",
		},
		{
			Environment: "prod",
			RowID:       "raw-prod-1",
			RunURL:      "https://prow.example/run/prod-1",
			SignatureID: "sig-prod-1",
			RawText:     "raw prod timeout sample",
		},
	}); err != nil {
		t.Fatalf("seed raw failures: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "triage-summary.html")
	opts := DefaultOptions()
	opts.OutputPath = outputPath
	opts.Format = "html"
	opts.WindowStart = "2026-03-01"
	opts.WindowEnd = "2026-03-08"
	opts.Top = 10
	opts.MinPercent = 0
	opts.Chrome = triagehtml.ReportChromeOptions{
		CurrentWeek:  "2026-03-01",
		CurrentView:  triagehtml.ReportViewGlobal,
		PreviousWeek: "2026-02-22",
		PreviousHref: "../2026-02-22/global-signature-triage.html",
		NextWeek:     "2026-03-08",
		NextHref:     "../2026-03-08/global-signature-triage.html",
		WeeklyHref:   "weekly-metrics.html",
		GlobalHref:   "global-signature-triage.html",
		ArchiveHref:  "../archive/",
	}

	if err := Generate(ctx, store, opts); err != nil {
		t.Fatalf("generate summary: %v", err)
	}

	reportBytes, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read generated summary: %v", err)
	}
	report := string(reportBytes)
	requiredSnippets := []string{
		"CI Global Signature Triage Report",
		"id=\"theme-toggle\"",
		"class=\"report-chrome\"",
		"../2026-02-22/global-signature-triage.html",
		"../2026-03-08/global-signature-triage.html",
		"href=\"weekly-metrics.html\"",
		"href=\"../archive/\"",
		"Weekly Report",
		"Triage Report",
		"Window: <strong>2026-03-01</strong> to <strong>2026-03-07</strong> (7 days)",
		"Environment: DEV",
		"Environment: INT",
		"Environment: PROD",
		"<section id=\"env-dev\" class=\"section\">",
		"<section id=\"env-int\" class=\"section\">",
		"<section id=\"env-prod\" class=\"section\">",
		"data-sort-key=\"count\"",
		"data-sort-key=\"after_last_push\"",
		"data-sort-key=\"jobs_affected\"",
		"data-sort-key=\"flake_score\"",
		"<th>Seen in",
		"title=\"Job run occurred after last push of a PR that merges.\"",
		"title=\"Unique job runs affected by this signature in the selected window.\"",
		"title=\"Heuristic score for unresolved recurrent flakes (0-14). Higher means more likely ongoing flake; likely bad-PR patterns reduce this score.\"",
		"title=\"Other environments where the same canonical signature phrase appears.\"",
		"<th>Trend</th>",
		"2026-03-01..2026-03-07",
		"Quality score",
		"bad PR score: 1/3 (post-good=0)",
		"Full failure examples",
		"Full failure examples (1)",
		"Contributing tests (1)",
		"<th>Lane</th><th>Job</th><th>Test</th><th>Support</th>",
		"Affected runs (1)",
		"Associated PR",
		"https://github.com/Azure/ARO-HCP/pull/4313",
		"prow job",
		"raw dev timeout sample",
		"full signature:",
		"context type stub leaked",
		">INT<",
	}
	for _, snippet := range requiredSnippets {
		if !strings.Contains(report, snippet) {
			t.Fatalf("expected HTML report to include %q", snippet)
		}
	}
	headerStart := strings.Index(report, "<thead><tr>")
	headerEnd := strings.Index(report, "</tr></thead>")
	if headerStart < 0 || headerEnd < 0 || headerEnd <= headerStart {
		t.Fatalf("expected global triage table header row to be present for order verification")
	}
	headerRow := report[headerStart:headerEnd]
	signatureHeader := strings.Index(headerRow, "<th>Signature</th>")
	countHeader := strings.Index(headerRow, "data-sort-key=\"count\"")
	afterLastPushHeader := strings.Index(headerRow, "data-sort-key=\"after_last_push\"")
	jobsAffectedHeader := strings.Index(headerRow, "data-sort-key=\"jobs_affected\"")
	flakeScoreHeader := strings.Index(headerRow, "data-sort-key=\"flake_score\"")
	shareHeader := strings.Index(headerRow, "data-sort-key=\"share\"")
	trendHeader := strings.Index(headerRow, "<th>Trend</th>")
	seenInHeader := strings.Index(headerRow, "<th>Seen in")
	if signatureHeader < 0 || countHeader < 0 || afterLastPushHeader < 0 || jobsAffectedHeader < 0 || flakeScoreHeader < 0 || shareHeader < 0 || trendHeader < 0 || seenInHeader < 0 {
		t.Fatalf("expected global triage headers to be present for order verification")
	}
	if !(signatureHeader < countHeader && countHeader < afterLastPushHeader && afterLastPushHeader < jobsAffectedHeader && jobsAffectedHeader < flakeScoreHeader && flakeScoreHeader < shareHeader && shareHeader < trendHeader && trendHeader < seenInHeader) {
		t.Fatalf("expected global triage column order Signature, Count, After last push, Jobs affected, Flake score, Share, Trend, Seen in")
	}
	if strings.Contains(report, "<th>Latest runs</th>") {
		t.Fatalf("expected HTML report to not include latest runs main column")
	}
	if strings.Contains(report, "<th>Contributing tests</th>") {
		t.Fatalf("expected HTML report to not include contributing tests main column")
	}
	if strings.Contains(report, "<span class=\"bad-pr-flag\"") {
		t.Fatalf("expected HTML report to not show bad-pr icon for score 1/3 rows")
	}
}

func TestGlobalQualityIssueCodesFlagsSourceDeserializationNoOutput(t *testing.T) {
	t.Parallel()

	phrase := "Deserializaion Error: no output from command"
	codes := globalQualityIssueCodes(phrase)

	found := false
	for _, code := range codes {
		if code == "source_deserialization_no_output" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected source_deserialization_no_output issue code, got %v", codes)
	}

	score := globalQualityScore(codes)
	if score < 9 {
		t.Fatalf("expected source deserialization issue to be high severity (>=9), got %d", score)
	}
}

func TestGlobalQualityIssueCodesFlagsGenericFailurePhrase(t *testing.T) {
	t.Parallel()

	phrase := "failure"
	codes := globalQualityIssueCodes(phrase)

	found := false
	for _, code := range codes {
		if code == "generic_failure_phrase" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected generic_failure_phrase issue code, got %v", codes)
	}

	score := globalQualityScore(codes)
	if score < 5 {
		t.Fatalf("expected generic failure phrase score >=5, got %d", score)
	}
}

func TestResolveGitHubPRURLFromProwRunFallbackToDefaultRepo(t *testing.T) {
	t.Parallel()

	got, ok := resolveGitHubPRURLFromProwRun("https://prow.example/run/unknown-format", 4249)
	if !ok {
		t.Fatalf("expected resolver fallback to succeed")
	}
	if got != "https://github.com/Azure/ARO-HCP/pull/4249" {
		t.Fatalf("unexpected fallback URL: %q", got)
	}
}
