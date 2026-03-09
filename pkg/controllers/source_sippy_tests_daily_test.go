package controllers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	sippysource "ci-failure-atlas/pkg/source/sippy"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestSourceSippyTestsDailyRunOnceStoresDailyRowsAndSkipsWhenPresent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	fakeClient := &fakeSippyTestsDailyClient{
		tests: []sippysource.TestSummary{
			{
				Name:                   "api-compat-test-a",
				SuiteName:              "rp-api-compat-all/parallel",
				CurrentPassPercentage:  98.2,
				CurrentRuns:            120,
				PreviousPassPercentage: 96.7,
				PreviousRuns:           110,
				NetImprovement:         1.5,
			},
			{
				Name:                   "Run pipeline step Microsoft.Azure.ARO.HCP.Region/regional/infra",
				SuiteName:              "step graph",
				CurrentPassPercentage:  87.0,
				CurrentRuns:            40,
				PreviousPassPercentage: 90.0,
				PreviousRuns:           55,
				NetImprovement:         -3.0,
			},
			{
				Name:        "Run pipeline step some-other-service/build-image",
				SuiteName:   "step graph", // should be filtered by test name regex
				CurrentRuns: 5,
			},
			{
				Name: "   ", // skipped on normalization
			},
		},
	}

	controller, err := newSourceSippyTestsDailyController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptions(t, []string{"dev"}),
	}, fakeClient)
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	const key = "dev|2026-03-10"
	if err := controller.RunOnce(ctx, key); err != nil {
		t.Fatalf("run once first time: %v", err)
	}
	if len(fakeClient.testCalls) != 4 {
		t.Fatalf("expected 4 ListTests calls after first run (2 periods x 2 filters), got %d", len(fakeClient.testCalls))
	}
	if fakeClient.testCalls[0].Release != "Presubmits" {
		t.Fatalf("unexpected release in ListTests call: got=%q want=%q", fakeClient.testCalls[0].Release, "Presubmits")
	}
	if strings.TrimSpace(fakeClient.testCalls[0].Filter) == "" || strings.TrimSpace(fakeClient.testCalls[1].Filter) == "" || strings.TrimSpace(fakeClient.testCalls[2].Filter) == "" || strings.TrimSpace(fakeClient.testCalls[3].Filter) == "" {
		t.Fatalf("expected server-side filter JSON to be set on all ListTests calls")
	}
	periodCounts := map[string]int{}
	filterSuites := map[string]struct{}{}
	filterNameContainsValues := map[string]struct{}{}
	for _, call := range fakeClient.testCalls {
		periodCounts[call.Period]++
		var parsed sippyFilterModel
		if err := json.Unmarshal([]byte(call.Filter), &parsed); err != nil {
			t.Fatalf("decode ListTests filter JSON %q: %v", call.Filter, err)
		}
		for _, item := range parsed.Items {
			if item.ColumnField == "suite_name" {
				filterSuites[item.Value] = struct{}{}
			}
			if item.ColumnField == "name" && item.OperatorValue == "contains" {
				filterNameContainsValues[item.Value] = struct{}{}
			}
		}
	}
	if periodCounts[sourceSippyTestsDailyDefaultPeriod] != 2 || periodCounts[sourceSippyTestsDailyTwoDayPeriod] != 2 {
		t.Fatalf("unexpected period call distribution: %+v", periodCounts)
	}
	if _, ok := filterSuites["rp-api-compat-all/parallel"]; !ok {
		t.Fatalf("expected suite filter for rp-api-compat-all/parallel")
	}
	if _, ok := filterSuites["step graph"]; !ok {
		t.Fatalf("expected suite filter for step graph")
	}
	if _, ok := filterNameContainsValues["Microsoft.Azure.ARO.HCP"]; !ok {
		t.Fatalf("expected name contains filter for Microsoft.Azure.ARO.HCP")
	}

	rows, err := store.ListTestMetadataDailyByDate(ctx, "dev", "2026-03-10")
	if err != nil {
		t.Fatalf("list test metadata daily rows: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("unexpected stored rows length: got=%d want=4", len(rows))
	}
	if rows[0].Environment != "dev" || rows[0].Date != "2026-03-10" || rows[0].Release != "Presubmits" {
		t.Fatalf("unexpected first stored row: %+v", rows[0])
	}

	if err := controller.RunOnce(ctx, key); err != nil {
		t.Fatalf("run once second time: %v", err)
	}
	if len(fakeClient.testCalls) != 4 {
		t.Fatalf("expected no additional ListTests calls when datapoint already exists, got %d", len(fakeClient.testCalls))
	}
}

func TestSourceSippyTestsDailySyncOnceFiltersConfiguredEnvironments(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := ndjson.New(t.TempDir())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	fakeClient := &fakeSippyTestsDailyClient{
		tests: []sippysource.TestSummary{
			{
				Name:                   "int-test-a",
				SuiteName:              "integration/parallel",
				CurrentPassPercentage:  95.0,
				CurrentRuns:            60,
				PreviousPassPercentage: 92.0,
				PreviousRuns:           58,
				NetImprovement:         3.0,
			},
		},
	}

	controller, err := newSourceSippyTestsDailyController(logr.Discard(), Dependencies{
		Store:  store,
		Source: mustCompleteSourceOptions(t, []string{"int"}),
	}, fakeClient)
	if err != nil {
		t.Fatalf("create controller: %v", err)
	}

	keys, err := controller.listKeys(ctx)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("unexpected key count: got=%d want=1 keys=%v", len(keys), keys)
	}
	_, keyDate, err := parseSippyTestsDailyKey(keys[0])
	if err != nil {
		t.Fatalf("parse key %q: %v", keys[0], err)
	}

	if err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("sync once: %v", err)
	}
	if len(fakeClient.testCalls) != 2 {
		t.Fatalf("expected two ListTests calls for one configured environment (default + twoDay), got %d", len(fakeClient.testCalls))
	}
	if fakeClient.testCalls[0].Release != "INT" {
		t.Fatalf("unexpected int release mapping used in ListTests call: got=%q want=%q", fakeClient.testCalls[0].Release, "INT")
	}

	intRows, err := store.ListTestMetadataDailyByDate(ctx, "int", keyDate)
	if err != nil {
		t.Fatalf("list int rows: %v", err)
	}
	if len(intRows) != 2 {
		t.Fatalf("unexpected int row count: got=%d want=2", len(intRows))
	}

	devRows, err := store.ListTestMetadataDailyByDate(ctx, "dev", keyDate)
	if err != nil {
		t.Fatalf("list dev rows: %v", err)
	}
	if len(devRows) != 0 {
		t.Fatalf("expected no dev rows for int-only sync, got=%d", len(devRows))
	}
}

type fakeSippyTestsDailyClient struct {
	tests    []sippysource.TestSummary
	testsErr error

	testCalls []sippysource.ListTestsOptions
}

func (f *fakeSippyTestsDailyClient) ListPullRequests(_ context.Context, _ sippysource.ListPullRequestsOptions) ([]sippysource.PullRequest, error) {
	return nil, nil
}

func (f *fakeSippyTestsDailyClient) ListJobRuns(_ context.Context, _ sippysource.ListJobRunsOptions) ([]sippysource.JobRun, error) {
	return nil, nil
}

func (f *fakeSippyTestsDailyClient) ListTests(_ context.Context, opts sippysource.ListTestsOptions) ([]sippysource.TestSummary, error) {
	f.testCalls = append(f.testCalls, opts)
	if f.testsErr != nil {
		return nil, f.testsErr
	}
	out := make([]sippysource.TestSummary, len(f.tests))
	copy(out, f.tests)
	return out, nil
}

func TestParseSippyTestsDailyKeyDefaultsDateWhenMissing(t *testing.T) {
	t.Parallel()

	env, date, err := parseSippyTestsDailyKey("dev")
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	if env != "dev" {
		t.Fatalf("unexpected env: got=%q want=dev", env)
	}
	if _, err := time.Parse("2006-01-02", date); err != nil {
		t.Fatalf("expected parsed date in YYYY-MM-DD format, got=%q err=%v", date, err)
	}
}
