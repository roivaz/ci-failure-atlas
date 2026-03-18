package site

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestBuildGeneratesIndexesAndSelectsLatestWeek(t *testing.T) {
	t.Parallel()

	reportsRoot := filepath.Join(t.TempDir(), "reports")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-01", weeklyReportFile), "weekly-1")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-01", globalReportFile), "global-1")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-01", "semantic-quality.html"), "quality-ignored")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-08", weeklyReportFile), "weekly-2")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-08", globalReportFile), "global-2")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-09", weeklyReportFile), "incomplete-week")

	result, err := Build(context.Background(), BuildOptions{
		DataDirectory: t.TempDir(),
		SiteRoot:      reportsRoot,
		FromExisting:  true,
	})
	if err != nil {
		t.Fatalf("build site indexes: %v", err)
	}
	if result.LatestWeek != "2026-03-08" {
		t.Fatalf("unexpected latest week: got %q want %q", result.LatestWeek, "2026-03-08")
	}
	if len(result.Weeks) != 2 {
		t.Fatalf("unexpected week count: got %d want %d", len(result.Weeks), 2)
	}

	weekIndexBytes, err := os.ReadFile(filepath.Join(reportsRoot, "2026-03-08", indexFileName))
	if err != nil {
		t.Fatalf("read week index: %v", err)
	}
	weekIndex := string(weekIndexBytes)
	for _, expected := range []string{
		"Weekly CI status",
		"Global signature triage",
		weeklyReportFile,
		globalReportFile,
		"../archive/",
		"id=\"theme-toggle\"",
	} {
		if !strings.Contains(weekIndex, expected) {
			t.Fatalf("expected week index to contain %q", expected)
		}
	}
	if strings.Contains(weekIndex, "semantic-quality.html") {
		t.Fatalf("did not expect quality report link in week index")
	}

	rootIndexBytes, err := os.ReadFile(filepath.Join(reportsRoot, indexFileName))
	if err != nil {
		t.Fatalf("read root index: %v", err)
	}
	rootIndex := string(rootIndexBytes)
	for _, expected := range []string{
		"http-equiv=\"refresh\"",
		"latest/weekly-metrics.html",
		"archive/",
		"2026-03-08",
	} {
		if !strings.Contains(rootIndex, expected) {
			t.Fatalf("expected root index to contain %q", expected)
		}
	}
	if strings.Contains(rootIndex, "id=\"theme-toggle\"") {
		t.Fatalf("did not expect root redirect page to render theme toggle")
	}

	archiveIndexBytes, err := os.ReadFile(filepath.Join(reportsRoot, "archive", indexFileName))
	if err != nil {
		t.Fatalf("read archive index: %v", err)
	}
	archiveIndex := string(archiveIndexBytes)
	for _, expected := range []string{
		"CI Reports Archive",
		"../latest/weekly-metrics.html",
		"from 2026-03-08",
		"../2026-03-08/",
		"../2026-03-01/",
		"id=\"theme-toggle\"",
	} {
		if !strings.Contains(archiveIndex, expected) {
			t.Fatalf("expected archive index to contain %q", expected)
		}
	}

	latestIndexBytes, err := os.ReadFile(filepath.Join(reportsRoot, "latest", indexFileName))
	if err != nil {
		t.Fatalf("read latest index: %v", err)
	}
	latestIndex := string(latestIndexBytes)
	for _, expected := range []string{
		"Latest week directory: <strong>2026-03-08</strong>",
		weeklyReportFile,
		globalReportFile,
		"id=\"theme-toggle\"",
	} {
		if !strings.Contains(latestIndex, expected) {
			t.Fatalf("expected latest index to contain %q", expected)
		}
	}

	latestWeeklyBytes, err := os.ReadFile(filepath.Join(reportsRoot, "latest", weeklyReportFile))
	if err != nil {
		t.Fatalf("read latest weekly report: %v", err)
	}
	if string(latestWeeklyBytes) != "weekly-2" {
		t.Fatalf("unexpected latest weekly report content: got %q want %q", string(latestWeeklyBytes), "weekly-2")
	}
	latestGlobalBytes, err := os.ReadFile(filepath.Join(reportsRoot, "latest", globalReportFile))
	if err != nil {
		t.Fatalf("read latest global report: %v", err)
	}
	if string(latestGlobalBytes) != "global-2" {
		t.Fatalf("unexpected latest global report content: got %q want %q", string(latestGlobalBytes), "global-2")
	}
}

func TestBuildFromExistingRebuildsReportsFromSemanticSnapshots(t *testing.T) {
	t.Parallel()

	dataDirectory := filepath.Join(t.TempDir(), "data")
	siteRoot := filepath.Join(t.TempDir(), "site")
	for _, week := range []string{"2026-03-01", "2026-03-08"} {
		if err := os.MkdirAll(filepath.Join(dataDirectory, "semantic", week), 0o755); err != nil {
			t.Fatalf("create semantic week directory %q: %v", week, err)
		}
	}
	mustWriteFile(t, filepath.Join(siteRoot, "2026-02-22", weeklyReportFile), "stale-older-weekly")
	mustWriteFile(t, filepath.Join(siteRoot, "2026-02-22", globalReportFile), "stale-older-global")
	mustWriteFile(t, filepath.Join(siteRoot, "2026-03-01", weeklyReportFile), "stale-current-weekly")
	mustWriteFile(t, filepath.Join(siteRoot, "2026-03-01", globalReportFile), "stale-current-global")

	result, err := Build(context.Background(), BuildOptions{
		DataDirectory: dataDirectory,
		SiteRoot:      siteRoot,
		FromExisting:  true,
	})
	if err != nil {
		t.Fatalf("build site from semantic snapshots: %v", err)
	}
	if result.LatestWeek != "2026-03-08" {
		t.Fatalf("unexpected latest week: got %q want %q", result.LatestWeek, "2026-03-08")
	}
	if len(result.Weeks) != 2 {
		t.Fatalf("unexpected week count: got %d want 2", len(result.Weeks))
	}
	if _, err := os.Stat(filepath.Join(siteRoot, "2026-02-22")); !os.IsNotExist(err) {
		t.Fatalf("expected stale week directory to be removed, stat err=%v", err)
	}

	weeklyBytes, err := os.ReadFile(filepath.Join(siteRoot, "2026-03-08", weeklyReportFile))
	if err != nil {
		t.Fatalf("read rebuilt weekly report: %v", err)
	}
	weeklyHTML := string(weeklyBytes)
	for _, expected := range []string{
		"CI Weekly Report",
		"class=\"report-chrome\"",
		"Weekly Report",
		"Triage Report",
		"href=\"../2026-03-01/weekly-metrics.html\"",
	} {
		if !strings.Contains(weeklyHTML, expected) {
			t.Fatalf("expected rebuilt weekly report to contain %q", expected)
		}
	}
	if strings.Contains(weeklyHTML, "stale-current-weekly") {
		t.Fatalf("expected rebuilt weekly report content, not stale placeholder")
	}

	globalBytes, err := os.ReadFile(filepath.Join(siteRoot, "2026-03-08", globalReportFile))
	if err != nil {
		t.Fatalf("read rebuilt triage report: %v", err)
	}
	globalHTML := string(globalBytes)
	for _, expected := range []string{
		"CI Global Signature Triage Report",
		"class=\"report-chrome\"",
		"Weekly Report",
		"Triage Report",
		"href=\"../2026-03-01/global-signature-triage.html\"",
	} {
		if !strings.Contains(globalHTML, expected) {
			t.Fatalf("expected rebuilt triage report to contain %q", expected)
		}
	}
	if strings.Contains(globalHTML, "stale-current-global") {
		t.Fatalf("expected rebuilt triage report content, not stale placeholder")
	}
}

func TestBuildFromExistingAppliesPhase3MaterializedViewToGlobalReport(t *testing.T) {
	t.Parallel()

	dataDirectory := filepath.Join(t.TempDir(), "data")
	siteRoot := filepath.Join(t.TempDir(), "site")
	semanticWeek := "2026-03-08"
	store, err := ndjson.NewWithOptions(dataDirectory, ndjson.Options{
		SemanticSubdirectory: semanticWeek,
	})
	if err != nil {
		t.Fatalf("create semantic store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertGlobalClusters(context.Background(), []semanticcontracts.GlobalClusterRecord{
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "g-dev-a",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            2,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-a",
					RunURL:      "https://prow.example/dev/run-a",
					OccurredAt:  "2026-03-03T12:00:00Z",
					SignatureID: "sig-a",
				},
			},
		},
		{
			SchemaVersion:           semanticcontracts.SchemaVersionV1,
			Environment:             "dev",
			Phase2ClusterID:         "g-dev-b",
			CanonicalEvidencePhrase: "context deadline exceeded",
			SearchQueryPhrase:       "context deadline exceeded",
			SupportCount:            3,
			References: []semanticcontracts.ReferenceRecord{
				{
					RowID:       "row-b",
					RunURL:      "https://prow.example/dev/run-b",
					OccurredAt:  "2026-03-03T12:10:00Z",
					SignatureID: "sig-b",
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed global clusters: %v", err)
	}
	if err := store.UpsertPhase3Links(context.Background(), []semanticcontracts.Phase3LinkRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-shared",
			Environment:   "dev",
			RunURL:        "https://prow.example/dev/run-a",
			RowID:         "row-a",
		},
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       "p3c-shared",
			Environment:   "dev",
			RunURL:        "https://prow.example/dev/run-b",
			RowID:         "row-b",
		},
	}); err != nil {
		t.Fatalf("seed phase3 links: %v", err)
	}

	result, err := Build(context.Background(), BuildOptions{
		DataDirectory: dataDirectory,
		SiteRoot:      siteRoot,
		FromExisting:  true,
	})
	if err != nil {
		t.Fatalf("build site from semantic snapshots: %v", err)
	}
	if result.LatestWeek != semanticWeek {
		t.Fatalf("unexpected latest week: got %q want %q", result.LatestWeek, semanticWeek)
	}

	globalBytes, err := os.ReadFile(filepath.Join(siteRoot, semanticWeek, globalReportFile))
	if err != nil {
		t.Fatalf("read rebuilt triage report: %v", err)
	}
	globalHTML := string(globalBytes)
	if !strings.Contains(globalHTML, `data-sort-cluster="p3c-shared"`) {
		t.Fatalf("expected triage report to include collapsed phase3 cluster id: %q", globalHTML)
	}
	if !strings.Contains(globalHTML, "Linked signatures (2)") {
		t.Fatalf("expected triage report to include linked signatures expander for collapsed rows: %q", globalHTML)
	}
	if !strings.Contains(globalHTML, "context deadline exceeded") || !strings.Contains(globalHTML, "jobs affected: 1") {
		t.Fatalf("expected triage report linked children to include jobs-affected details: %q", globalHTML)
	}
}

func TestPushUploadsWeeksAndLatestFromMostRecentDirectory(t *testing.T) {
	t.Parallel()

	reportsRoot := filepath.Join(t.TempDir(), "reports")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-01", weeklyReportFile), "weekly-1")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-01", globalReportFile), "global-1")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-01", "semantic-quality.html"), "quality-ignored")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-08", weeklyReportFile), "weekly-2")
	mustWriteFile(t, filepath.Join(reportsRoot, "2026-03-08", globalReportFile), "global-2")

	recorder := &uploadRecorder{}
	result, err := Push(context.Background(), PushOptions{
		SiteRoot:       reportsRoot,
		StorageAccount: "cihealthreports",
		AuthMode:       "login",
		ContainerName:  "$web",
		Uploader:       recorder,
	})
	if err != nil {
		t.Fatalf("push site artifacts: %v", err)
	}
	if result.LatestWeek != "2026-03-08" {
		t.Fatalf("unexpected latest week in push result: got %q want %q", result.LatestWeek, "2026-03-08")
	}

	targets := recorder.targets()
	requiredTargets := []string{
		"2026-03-08/weekly-metrics.html",
		"2026-03-08/global-signature-triage.html",
		"2026-03-08/index.html",
		"2026-03-01/weekly-metrics.html",
		"2026-03-01/global-signature-triage.html",
		"2026-03-01/index.html",
		"latest/weekly-metrics.html",
		"latest/global-signature-triage.html",
		"latest/index.html",
		"archive/index.html",
		"index.html",
	}
	for _, target := range requiredTargets {
		if !containsString(targets, target) {
			t.Fatalf("expected uploaded targets to include %q; got %#v", target, targets)
		}
	}
	for _, target := range targets {
		if strings.Contains(target, "semantic-quality") {
			t.Fatalf("did not expect quality report upload target, got %q", target)
		}
	}
}

func TestNormalizeBuildOptionsDefaults(t *testing.T) {
	t.Parallel()

	normalized, err := normalizeBuildOptions(BuildOptions{})
	if err != nil {
		t.Fatalf("normalizeBuildOptions returned error: %v", err)
	}
	if normalized.DataDirectory != "data" {
		t.Fatalf("expected default data directory %q, got %q", "data", normalized.DataDirectory)
	}
	if normalized.SiteRoot != "site" {
		t.Fatalf("expected default site root %q, got %q", "site", normalized.SiteRoot)
	}
	if normalized.HistoryWeeks != 4 {
		t.Fatalf("expected default weeks 4, got %d", normalized.HistoryWeeks)
	}
	if len(normalized.SourceEnvironments) != 4 {
		t.Fatalf("expected 4 default source environments, got %d", len(normalized.SourceEnvironments))
	}
}

func TestWeekStartsToGenerateReturnsOldestToNewest(t *testing.T) {
	t.Parallel()

	current := time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC)
	got := weekStartsToGenerate(current, 4)
	if len(got) != 4 {
		t.Fatalf("expected 4 week starts, got %d", len(got))
	}
	want := []string{"2026-02-15", "2026-02-22", "2026-03-01", "2026-03-08"}
	for i, expected := range want {
		if got[i].Format("2006-01-02") != expected {
			t.Fatalf("unexpected week at index %d: got %s want %s", i, got[i].Format("2006-01-02"), expected)
		}
	}
}

type uploadRecorder struct {
	requests []BlobUploadRequest
}

func (r *uploadRecorder) Upload(_ context.Context, req BlobUploadRequest) error {
	r.requests = append(r.requests, req)
	return nil
}

func (r *uploadRecorder) targets() []string {
	out := make([]string, 0, len(r.requests))
	for _, req := range r.requests {
		out = append(out, req.TargetPath)
	}
	return out
}

func mustWriteFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create directory for %q: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file %q: %v", path, err)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
