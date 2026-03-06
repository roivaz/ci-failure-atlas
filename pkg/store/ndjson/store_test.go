package ndjson

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"ci-failure-atlas/pkg/store/contracts"
)

func TestUpsertRunsAndListRunKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = store.UpsertRuns(ctx, []contracts.RunRecord{
		{Environment: "dev", RunURL: " https://run-b ", JobName: "job-b", PRNumber: 2, PRSHA: "sha-b", OccurredAt: "2026-03-05T10:00:00Z"},
		{Environment: "dev", RunURL: "https://run-a", JobName: "job-a", PRNumber: 1, PRSHA: "sha-a", OccurredAt: "2026-03-05T09:00:00Z"},
		{Environment: "int", RunURL: "https://run-a", JobName: "job-a-int", PRNumber: 3, PRSHA: "sha-int-a", OccurredAt: "2026-03-05T08:00:00Z"},
	})
	if err != nil {
		t.Fatalf("upsert initial runs: %v", err)
	}

	err = store.UpsertRuns(ctx, []contracts.RunRecord{
		{Environment: "dev", RunURL: "https://run-a", JobName: "job-a-updated", PRNumber: 1, PRSHA: "sha-a", FinalMergedSHA: "sha-a", MergedPR: true, PostGoodCommit: true, OccurredAt: "2026-03-05T11:00:00Z"},
	})
	if err != nil {
		t.Fatalf("upsert updated run: %v", err)
	}

	keys, err := store.ListRunKeys(ctx)
	if err != nil {
		t.Fatalf("list run keys: %v", err)
	}
	wantKeys := []string{"dev|https://run-a", "dev|https://run-b", "int|https://run-a"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("run keys mismatch: got=%v want=%v", keys, wantKeys)
	}

	run, found, err := store.GetRun(ctx, "dev", "https://run-a")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if !found {
		t.Fatalf("expected run to be found")
	}
	if run.JobName != "job-a-updated" || run.OccurredAt != "2026-03-05T11:00:00Z" {
		t.Fatalf("unexpected run record: %+v", run)
	}
	if !run.MergedPR || !run.PostGoodCommit || run.FinalMergedSHA != "sha-a" || run.PRSHA != "sha-a" || run.PRNumber != 1 {
		t.Fatalf("unexpected run merge metadata: %+v", run)
	}

	_, found, err = store.GetRun(ctx, "dev", "https://run-missing")
	if err != nil {
		t.Fatalf("get missing run: %v", err)
	}
	if found {
		t.Fatalf("expected missing run to not be found")
	}

	rows, err := readNDJSON[contracts.RunRecord](filepath.Join(store.dataDirectory, factsDirectory, runsFilename))
	if err != nil {
		t.Fatalf("read runs file: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("unexpected run row count: got=%d want=3", len(rows))
	}
	if rows[0].Environment != "dev" || rows[0].RunURL != "https://run-a" || rows[0].JobName != "job-a-updated" {
		t.Fatalf("unexpected first run row: %+v", rows[0])
	}
}

func TestUpsertArtifactFailuresAndListRunKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = store.UpsertArtifactFailures(ctx, []contracts.ArtifactFailureRecord{
		{
			Environment:   "dev",
			ArtifactRowID: "dev-run1-case1",
			RunURL:        "https://run-1",
			TestName:      "test-a",
			TestSuite:     "suite-a",
			SignatureID:   "sig-1",
			FailureText:   "first text",
		},
		{
			Environment:   "int",
			ArtifactRowID: "int-run1-case1",
			RunURL:        "https://run-1",
			TestName:      "test-a",
			TestSuite:     "suite-a",
			SignatureID:   "sig-1",
			FailureText:   "first text int",
		},
		{
			Environment:   "dev",
			ArtifactRowID: "dev-run2-case1",
			RunURL:        "https://run-2",
			TestName:      "test-b",
			TestSuite:     "suite-b",
			SignatureID:   "sig-2",
			FailureText:   "other text",
		},
		{
			Environment:   "dev",
			ArtifactRowID: "dev-run1-case2",
			RunURL:        "https://run-1",
			TestName:      "test-c",
			TestSuite:     "suite-c",
			SignatureID:   "sig-1",
			FailureText:   "another row same signature",
		},
	})
	if err != nil {
		t.Fatalf("upsert initial artifact failures: %v", err)
	}

	err = store.UpsertArtifactFailures(ctx, []contracts.ArtifactFailureRecord{
		{
			Environment:   "dev",
			ArtifactRowID: "dev-run1-case1",
			RunURL:        "https://run-1",
			TestName:      "test-a",
			TestSuite:     "suite-a",
			SignatureID:   "sig-1",
			FailureText:   "updated text",
		},
	})
	if err != nil {
		t.Fatalf("upsert updated artifact failure: %v", err)
	}

	keys, err := store.ListArtifactRunKeys(ctx)
	if err != nil {
		t.Fatalf("list artifact run keys: %v", err)
	}
	wantKeys := []string{"dev|https://run-1", "dev|https://run-2", "int|https://run-1"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("artifact run keys mismatch: got=%v want=%v", keys, wantKeys)
	}

	devRunRows, err := store.ListArtifactFailuresByRun(ctx, "dev", "https://run-1")
	if err != nil {
		t.Fatalf("list artifact failures by run: %v", err)
	}
	if len(devRunRows) != 2 {
		t.Fatalf("unexpected artifact rows for dev/run-1: got=%d want=2", len(devRunRows))
	}
	if devRunRows[0].ArtifactRowID != "dev-run1-case1" || devRunRows[1].ArtifactRowID != "dev-run1-case2" {
		t.Fatalf("unexpected artifact rows ordering/content for dev/run-1: %+v", devRunRows)
	}

	rows, err := readNDJSON[contracts.ArtifactFailureRecord](filepath.Join(store.dataDirectory, factsDirectory, artifactFailuresFilename))
	if err != nil {
		t.Fatalf("read artifact failures file: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("unexpected artifact row count: got=%d want=4", len(rows))
	}
	var updatedFound bool
	var sameSignatureDistinctRows int
	for _, row := range rows {
		if row.Environment == "dev" && row.ArtifactRowID == "dev-run1-case1" {
			updatedFound = true
			if row.FailureText != "updated text" {
				t.Fatalf("expected updated failure text for dev-run1-case1, got=%q", row.FailureText)
			}
		}
		if row.Environment == "dev" && row.RunURL == "https://run-1" && row.SignatureID == "sig-1" {
			sameSignatureDistinctRows++
			if row.ArtifactRowID == "" {
				t.Fatalf("expected artifact_row_id for dev/run-1/sig-1 rows")
			}
		}
	}
	if !updatedFound {
		t.Fatalf("updated artifact failure for dev-run1-case1 not found")
	}
	if sameSignatureDistinctRows != 2 {
		t.Fatalf("expected 2 distinct dev/run-1 rows with sig-1, got=%d", sameSignatureDistinctRows)
	}
}

func TestUpsertRawFailuresAndListRunKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{Environment: "dev", RowID: "row-2", RunURL: "https://run-b", TestName: "test-b", TestSuite: "suite-dev", MergedPR: false, PostGoodCommitFailures: 0, SignatureID: "sig-b", OccurredAt: "2026-03-05T10:00:00Z", RawText: "raw-b", NormalizedText: "norm-b"},
		{Environment: "dev", RowID: "row-1", RunURL: "https://run-a", TestName: "test-a", TestSuite: "suite-dev", MergedPR: false, PostGoodCommitFailures: 0, SignatureID: "sig-a", OccurredAt: "2026-03-05T09:00:00Z", RawText: "raw-a", NormalizedText: "norm-a"},
		{Environment: "int", RowID: "row-1", RunURL: "https://run-a", TestName: "test-a-int", TestSuite: "suite-int", MergedPR: true, PostGoodCommitFailures: 1, SignatureID: "sig-a-int", OccurredAt: "2026-03-05T07:00:00Z", RawText: "raw-a-int", NormalizedText: "norm-a-int"},
	})
	if err != nil {
		t.Fatalf("upsert initial raw failures: %v", err)
	}

	err = store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{Environment: "dev", RowID: "row-1", RunURL: "https://run-a", TestName: "test-a-updated", TestSuite: "suite-dev-updated", MergedPR: true, PostGoodCommitFailures: 1, SignatureID: "sig-a", OccurredAt: "2026-03-05T11:00:00Z", RawText: "raw-a-updated", NormalizedText: "norm-a-updated"},
	})
	if err != nil {
		t.Fatalf("upsert updated raw failure: %v", err)
	}

	keys, err := store.ListRawFailureRunKeys(ctx)
	if err != nil {
		t.Fatalf("list raw failure run keys: %v", err)
	}
	wantKeys := []string{"dev|https://run-a", "dev|https://run-b", "int|https://run-a"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("raw failure run keys mismatch: got=%v want=%v", keys, wantKeys)
	}

	devRowsByDate, err := store.ListRawFailuresByDate(ctx, "dev", "2026-03-05")
	if err != nil {
		t.Fatalf("list raw failures by date: %v", err)
	}
	if len(devRowsByDate) != 2 {
		t.Fatalf("unexpected dev raw failures by date count: got=%d want=2", len(devRowsByDate))
	}
	if devRowsByDate[0].RowID != "row-2" || devRowsByDate[1].RowID != "row-1" {
		t.Fatalf("unexpected dev raw failures by date ordering/content: %+v", devRowsByDate)
	}

	devRowsByRun, err := store.ListRawFailuresByRun(ctx, "dev", "https://run-a")
	if err != nil {
		t.Fatalf("list raw failures by run: %v", err)
	}
	if len(devRowsByRun) != 1 {
		t.Fatalf("unexpected dev raw failures by run count: got=%d want=1", len(devRowsByRun))
	}
	if devRowsByRun[0].RowID != "row-1" || devRowsByRun[0].NormalizedText != "norm-a-updated" {
		t.Fatalf("unexpected dev raw failure row for run-a: %+v", devRowsByRun[0])
	}
	if !devRowsByRun[0].MergedPR || devRowsByRun[0].PostGoodCommitFailures != 1 {
		t.Fatalf("expected updated merge metadata on dev/run-a row, got=%+v", devRowsByRun[0])
	}

	rows, err := readNDJSON[contracts.RawFailureRecord](filepath.Join(store.dataDirectory, factsDirectory, rawFailuresFilename))
	if err != nil {
		t.Fatalf("read raw failures file: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("unexpected raw failure row count: got=%d want=3", len(rows))
	}
	var updatedFound bool
	for _, row := range rows {
		if row.Environment == "dev" && row.RowID == "row-1" {
			updatedFound = true
			if row.NormalizedText != "norm-a-updated" {
				t.Fatalf("expected updated normalized text for dev/row-1, got=%q", row.NormalizedText)
			}
			if row.TestName != "test-a-updated" || row.TestSuite != "suite-dev-updated" {
				t.Fatalf("expected updated test metadata for dev/row-1, got=%+v", row)
			}
			if !row.MergedPR || row.PostGoodCommitFailures != 1 {
				t.Fatalf("expected updated merge metadata for dev/row-1, got=%+v", row)
			}
		}
	}
	if !updatedFound {
		t.Fatalf("updated raw failure for dev/row-1 not found")
	}
}

func TestUpsertMetricsDailyAndListDates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = store.UpsertMetricsDaily(ctx, []contracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-05", Metric: "top_n_share", Value: 0.5},
		{Environment: "dev", Date: "2026-03-04", Metric: "novelty_rate", Value: 0.1},
		{Environment: "int", Date: "2026-03-05", Metric: "top_n_share", Value: 0.2},
	})
	if err != nil {
		t.Fatalf("upsert initial metrics: %v", err)
	}

	err = store.UpsertMetricsDaily(ctx, []contracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-05", Metric: "top_n_share", Value: 0.7},
	})
	if err != nil {
		t.Fatalf("upsert updated metric: %v", err)
	}

	dates, err := store.ListMetricDates(ctx)
	if err != nil {
		t.Fatalf("list metric dates: %v", err)
	}
	wantDates := []string{"2026-03-04", "2026-03-05"}
	if !reflect.DeepEqual(dates, wantDates) {
		t.Fatalf("metric dates mismatch: got=%v want=%v", dates, wantDates)
	}

	devRowsByDate, err := store.ListMetricsDailyByDate(ctx, "dev", "2026-03-05")
	if err != nil {
		t.Fatalf("list metrics by date: %v", err)
	}
	if len(devRowsByDate) != 1 {
		t.Fatalf("unexpected dev metrics by date row count: got=%d want=1", len(devRowsByDate))
	}
	if devRowsByDate[0].Metric != "top_n_share" || devRowsByDate[0].Value != 0.7 {
		t.Fatalf("unexpected dev metric row for 2026-03-05: %+v", devRowsByDate[0])
	}

	rows, err := readNDJSON[contracts.MetricDailyRecord](filepath.Join(store.dataDirectory, factsDirectory, metricsDailyFilename))
	if err != nil {
		t.Fatalf("read metrics file: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("unexpected metric row count: got=%d want=3", len(rows))
	}
	var devUpdatedFound bool
	var intValuePreserved bool
	for _, row := range rows {
		if row.Environment == "dev" && row.Date == "2026-03-05" && row.Metric == "top_n_share" {
			devUpdatedFound = true
			if row.Value != 0.7 {
				t.Fatalf("expected updated dev metric value 0.7, got=%v", row.Value)
			}
		}
		if row.Environment == "int" && row.Date == "2026-03-05" && row.Metric == "top_n_share" {
			intValuePreserved = true
			if row.Value != 0.2 {
				t.Fatalf("expected int metric value 0.2, got=%v", row.Value)
			}
		}
	}
	if !devUpdatedFound {
		t.Fatalf("updated metric for dev/2026-03-05/top_n_share not found")
	}
	if !intValuePreserved {
		t.Fatalf("metric for int/2026-03-05/top_n_share not found")
	}
}

func TestUpsertMetricsDailyReplacesTouchedDateEnvironmentMetricSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = store.UpsertMetricsDaily(ctx, []contracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-05", Metric: "total_runs", Value: 100},
		{Environment: "dev", Date: "2026-03-05", Metric: "unique_failure_signatures", Value: 10},
		{Environment: "int", Date: "2026-03-05", Metric: "total_runs", Value: 40},
	})
	if err != nil {
		t.Fatalf("upsert initial metrics: %v", err)
	}

	err = store.UpsertMetricsDaily(ctx, []contracts.MetricDailyRecord{
		{Environment: "dev", Date: "2026-03-05", Metric: "total_runs", Value: 120},
		{Environment: "dev", Date: "2026-03-05", Metric: "failed_runs", Value: 15},
	})
	if err != nil {
		t.Fatalf("upsert replacement metric set: %v", err)
	}

	rows, err := readNDJSON[contracts.MetricDailyRecord](filepath.Join(store.dataDirectory, factsDirectory, metricsDailyFilename))
	if err != nil {
		t.Fatalf("read metrics file: %v", err)
	}

	devMetrics := map[string]float64{}
	intMetrics := map[string]float64{}
	for _, row := range rows {
		switch row.Environment {
		case "dev":
			if row.Date == "2026-03-05" {
				devMetrics[row.Metric] = row.Value
			}
		case "int":
			if row.Date == "2026-03-05" {
				intMetrics[row.Metric] = row.Value
			}
		}
	}

	if len(devMetrics) != 2 {
		t.Fatalf("expected exactly 2 dev metrics for touched date, got=%d values=%v", len(devMetrics), devMetrics)
	}
	if _, exists := devMetrics["unique_failure_signatures"]; exists {
		t.Fatalf("expected deprecated metric to be removed after touched date replacement")
	}
	if devMetrics["total_runs"] != 120 || devMetrics["failed_runs"] != 15 {
		t.Fatalf("unexpected dev metric values after replacement: %v", devMetrics)
	}
	if intMetrics["total_runs"] != 40 {
		t.Fatalf("expected untouched int metric to be preserved, got=%v", intMetrics)
	}
}

func TestUpsertRunCountsHourlyAndListHours(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = store.UpsertRunCountsHourly(ctx, []contracts.RunCountHourlyRecord{
		{Environment: "dev", Hour: "2026-03-05T10:00:00Z", TotalRuns: 7, FailedRuns: 2, SuccessfulRuns: 5},
		{Environment: "dev", Hour: "2026-03-05T09:00:00Z", TotalRuns: 5, FailedRuns: 1, SuccessfulRuns: 4},
		{Environment: "int", Hour: "2026-03-05T10:00:00Z", TotalRuns: 3, FailedRuns: 0, SuccessfulRuns: 3},
	})
	if err != nil {
		t.Fatalf("upsert initial run counts hourly: %v", err)
	}

	err = store.UpsertRunCountsHourly(ctx, []contracts.RunCountHourlyRecord{
		{Environment: "dev", Hour: "2026-03-05T10:10:00Z", TotalRuns: 8, FailedRuns: 3, SuccessfulRuns: 5},
	})
	if err != nil {
		t.Fatalf("upsert updated run count hourly: %v", err)
	}

	hours, err := store.ListRunCountHourlyHours(ctx)
	if err != nil {
		t.Fatalf("list run count hours: %v", err)
	}
	wantHours := []string{"2026-03-05T09:00:00Z", "2026-03-05T10:00:00Z"}
	if !reflect.DeepEqual(hours, wantHours) {
		t.Fatalf("hour list mismatch: got=%v want=%v", hours, wantHours)
	}

	devRowsByDate, err := store.ListRunCountsHourlyByDate(ctx, "dev", "2026-03-05")
	if err != nil {
		t.Fatalf("list run counts hourly by date: %v", err)
	}
	if len(devRowsByDate) != 2 {
		t.Fatalf("unexpected dev run-count rows by date count: got=%d want=2", len(devRowsByDate))
	}
	if devRowsByDate[0].Hour != "2026-03-05T09:00:00Z" || devRowsByDate[1].Hour != "2026-03-05T10:00:00Z" {
		t.Fatalf("unexpected dev run-count rows by date ordering/content: %+v", devRowsByDate)
	}

	rows, err := readNDJSON[contracts.RunCountHourlyRecord](filepath.Join(store.dataDirectory, factsDirectory, runCountsHourlyFilename))
	if err != nil {
		t.Fatalf("read run counts hourly file: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("unexpected run count hourly row count: got=%d want=3", len(rows))
	}

	var devUpdatedFound bool
	for _, row := range rows {
		if row.Environment == "dev" && row.Hour == "2026-03-05T10:00:00Z" {
			devUpdatedFound = true
			if row.TotalRuns != 8 || row.FailedRuns != 3 || row.SuccessfulRuns != 5 {
				t.Fatalf("unexpected updated dev run count row: %+v", row)
			}
		}
	}
	if !devUpdatedFound {
		t.Fatalf("updated run count row for dev/2026-03-05T10:00:00Z not found")
	}
}

func TestUpsertAndGetCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = store.UpsertCheckpoints(ctx, []contracts.CheckpointRecord{
		{Name: "source.sippy.runs", Value: "cursor-1"},
	})
	if err != nil {
		t.Fatalf("upsert initial checkpoint: %v", err)
	}

	initial, found, err := store.GetCheckpoint(ctx, "source.sippy.runs")
	if err != nil {
		t.Fatalf("get initial checkpoint: %v", err)
	}
	if !found {
		t.Fatalf("expected checkpoint to be found")
	}
	if initial.UpdatedAt == "" {
		t.Fatalf("expected checkpoint updated_at to be set")
	}

	err = store.UpsertCheckpoints(ctx, []contracts.CheckpointRecord{
		{Name: "source.sippy.runs", Value: "cursor-2", UpdatedAt: "2026-03-06T00:00:00Z"},
	})
	if err != nil {
		t.Fatalf("upsert updated checkpoint: %v", err)
	}

	updated, found, err := store.GetCheckpoint(ctx, "source.sippy.runs")
	if err != nil {
		t.Fatalf("get updated checkpoint: %v", err)
	}
	if !found {
		t.Fatalf("expected checkpoint to be found")
	}
	if updated.Value != "cursor-2" || updated.UpdatedAt != "2026-03-06T00:00:00Z" {
		t.Fatalf("unexpected checkpoint record: %+v", updated)
	}

	_, found, err = store.GetCheckpoint(ctx, "missing")
	if err != nil {
		t.Fatalf("get missing checkpoint: %v", err)
	}
	if found {
		t.Fatalf("expected missing checkpoint to not be found")
	}
}

func TestAppendAndListDeadLetters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = store.AppendDeadLetters(ctx, []contracts.DeadLetterRecord{
		{Controller: "source.sippy.runs", Key: "run-1", Error: "err1", FailedAt: "2026-03-05T01:00:00Z"},
		{Controller: "source.prow.failures", Key: "run-2", Error: "err2", FailedAt: "2026-03-05T03:00:00Z"},
		{Controller: "facts.raw-failures", Key: "run-3", Error: "err3", FailedAt: "2026-03-05T02:00:00Z"},
	})
	if err != nil {
		t.Fatalf("append dead letters: %v", err)
	}

	latestTwo, err := store.ListDeadLetters(ctx, 2)
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	if len(latestTwo) != 2 {
		t.Fatalf("unexpected dead letter list size: got=%d want=2", len(latestTwo))
	}
	if latestTwo[0].FailedAt != "2026-03-05T03:00:00Z" || latestTwo[1].FailedAt != "2026-03-05T02:00:00Z" {
		t.Fatalf("unexpected dead letter ordering: %+v", latestTwo)
	}
}

func TestUpsertRequiresEnvironment(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	if err := store.UpsertRuns(ctx, []contracts.RunRecord{
		{RunURL: "https://run-a", JobName: "job-a"},
	}); err == nil {
		t.Fatalf("expected UpsertRuns to fail without environment")
	}

	if err := store.UpsertArtifactFailures(ctx, []contracts.ArtifactFailureRecord{
		{RunURL: "https://run-a", SignatureID: "sig-a", FailureText: "text"},
	}); err == nil {
		t.Fatalf("expected UpsertArtifactFailures to fail without environment")
	}

	if err := store.UpsertArtifactFailures(ctx, []contracts.ArtifactFailureRecord{
		{Environment: "dev", RunURL: "https://run-a", SignatureID: "sig-a", FailureText: "text"},
	}); err == nil {
		t.Fatalf("expected UpsertArtifactFailures to fail without artifact_row_id")
	}

	if err := store.UpsertRawFailures(ctx, []contracts.RawFailureRecord{
		{RowID: "row-a", RunURL: "https://run-a", SignatureID: "sig-a"},
	}); err == nil {
		t.Fatalf("expected UpsertRawFailures to fail without environment")
	}

	if err := store.UpsertRunCountsHourly(ctx, []contracts.RunCountHourlyRecord{
		{Hour: "2026-03-05T10:00:00Z", TotalRuns: 1, FailedRuns: 1, SuccessfulRuns: 0},
	}); err == nil {
		t.Fatalf("expected UpsertRunCountsHourly to fail without environment")
	}

	if err := store.UpsertRunCountsHourly(ctx, []contracts.RunCountHourlyRecord{
		{Environment: "dev", Hour: "2026-03-05T10:00:00Z", TotalRuns: 1, FailedRuns: 0, SuccessfulRuns: 0},
	}); err == nil {
		t.Fatalf("expected UpsertRunCountsHourly to fail on inconsistent counters")
	}

	if _, _, err := store.GetRun(ctx, "", "https://run-a"); err == nil {
		t.Fatalf("expected GetRun to fail without environment")
	}

	if _, err := store.ListArtifactFailuresByRun(ctx, "", "https://run-a"); err == nil {
		t.Fatalf("expected ListArtifactFailuresByRun to fail without environment")
	}

	if _, err := store.ListRunCountsHourlyByDate(ctx, "", "2026-03-05"); err == nil {
		t.Fatalf("expected ListRunCountsHourlyByDate to fail without environment")
	}

	if _, err := store.ListRunCountsHourlyByDate(ctx, "dev", "20260305"); err == nil {
		t.Fatalf("expected ListRunCountsHourlyByDate to fail with invalid date")
	}

	if _, err := store.ListRawFailuresByDate(ctx, "", "2026-03-05"); err == nil {
		t.Fatalf("expected ListRawFailuresByDate to fail without environment")
	}

	if _, err := store.ListRawFailuresByRun(ctx, "", "https://run-a"); err == nil {
		t.Fatalf("expected ListRawFailuresByRun to fail without environment")
	}

	if _, err := store.ListRawFailuresByDate(ctx, "dev", "20260305"); err == nil {
		t.Fatalf("expected ListRawFailuresByDate to fail with invalid date")
	}

	if _, err := store.ListMetricsDailyByDate(ctx, "", "2026-03-05"); err == nil {
		t.Fatalf("expected ListMetricsDailyByDate to fail without environment")
	}

	if _, err := store.ListMetricsDailyByDate(ctx, "dev", "20260305"); err == nil {
		t.Fatalf("expected ListMetricsDailyByDate to fail with invalid date")
	}
}
