package ui

import (
	"strings"
	"testing"
)

func TestBadPRScoreAndReasonsGateOnPostGood(t *testing.T) {
	t.Parallel()

	score, reasons := BadPRScoreAndReasons(FailurePatternRow{
		Environment:        "dev",
		AfterLastPushCount: 2,
		AlsoIn:             nil,
		AffectedRuns: []RunReference{
			{RunURL: "https://prow.example/run/1", PRNumber: 4313, OccurredAt: "2026-03-07T10:00:00Z"},
		},
	})
	if score != 0 {
		t.Fatalf("expected score 0 when post-good is positive, got %d", score)
	}
	if len(reasons) != 0 {
		t.Fatalf("expected no reasons when post-good is positive, got %v", reasons)
	}
}

func TestBadPRScoreAndReasonsIncludesAllSignals(t *testing.T) {
	t.Parallel()

	score, reasons := BadPRScoreAndReasons(FailurePatternRow{
		Environment:        "dev",
		AfterLastPushCount: 0,
		AlsoIn:             nil,
		AffectedRuns: []RunReference{
			{RunURL: "https://prow.example/run/1", PRNumber: 4313, OccurredAt: "2026-03-07T10:00:00Z"},
			{RunURL: "https://prow.example/run/2", PRNumber: 4313, OccurredAt: "2026-03-07T11:00:00Z"},
		},
	})
	if score != 3 {
		t.Fatalf("expected score 3, got %d", score)
	}
	expected := []string{"post-good=0", "only seen in DEV", "only seen in one PR"}
	for _, want := range expected {
		found := false
		for _, got := range reasons {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected reasons to include %q, got %v", want, reasons)
		}
	}
}

func TestSortRowsByBadPRScore(t *testing.T) {
	t.Parallel()

	rows := []FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "score-three",
			Occurrences:        100,
			AfterLastPushCount: 0,
			AffectedRuns: []RunReference{
				{RunURL: "https://prow.example/run/1", PRNumber: 10, OccurredAt: "2026-03-07T10:00:00Z"},
			},
		},
		{
			Environment:        "dev",
			FailurePattern:     "score-zero",
			Occurrences:        1,
			AfterLastPushCount: 1,
			AffectedRuns: []RunReference{
				{RunURL: "https://prow.example/run/2", PRNumber: 20, OccurredAt: "2026-03-07T09:00:00Z"},
			},
		},
		{
			Environment:        "int",
			FailurePattern:     "score-one",
			Occurrences:        50,
			AfterLastPushCount: 0,
			AffectedRuns: []RunReference{
				{RunURL: "https://prow.example/run/3", PRNumber: 0, OccurredAt: "2026-03-07T08:00:00Z"},
			},
		},
	}

	SortRowsByBadPRScore(rows)

	if rows[0].FailurePattern != "score-zero" {
		t.Fatalf("expected score-zero first, got %q", rows[0].FailurePattern)
	}
	if rows[1].FailurePattern != "score-one" {
		t.Fatalf("expected score-one second, got %q", rows[1].FailurePattern)
	}
	if rows[2].FailurePattern != "score-three" {
		t.Fatalf("expected score-three last, got %q", rows[2].FailurePattern)
	}
}

func TestFlakeScoreAndReasonsIncludesRequestedSignals(t *testing.T) {
	t.Parallel()

	score, reasons := FlakeScoreAndReasons(FailurePatternRow{
		Environment:        "dev",
		AfterLastPushCount: 5,
		TrendCounts:        []int{1, 1, 1, 1, 1, 1, 1},
		TrendRange:         "2026-03-01..2026-03-07",
		PriorWeeksPresent:  2,
		AffectedRuns: []RunReference{
			{RunURL: "https://prow.example/run/1", PRNumber: 4201, OccurredAt: "2026-03-06T02:00:00Z"},
			{RunURL: "https://prow.example/run/2", PRNumber: 4202, OccurredAt: "2026-03-07T12:00:00Z"},
			{RunURL: "https://prow.example/run/3", PRNumber: 4203, OccurredAt: "2026-03-05T12:00:00Z"},
			{RunURL: "https://prow.example/run/4", PRNumber: 4204, OccurredAt: "2026-03-04T12:00:00Z"},
			{RunURL: "https://prow.example/run/5", PRNumber: 4205, OccurredAt: "2026-03-03T12:00:00Z"},
			{RunURL: "https://prow.example/run/6", PRNumber: 4206, OccurredAt: "2026-03-02T12:00:00Z"},
		},
	})
	if score <= 0 {
		t.Fatalf("expected positive flake score, got %d", score)
	}
	for _, expected := range []string{
		"jobs affected",
		"after last push",
		"daily spread",
		"recent occurrence",
		"present in 2 prior weeks",
	} {
		found := false
		for _, reason := range reasons {
			if strings.Contains(reason, expected) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected reason containing %q, got %v", expected, reasons)
		}
	}
}

func TestFlakeScoreAndReasonsAggregatedRowsUseLinkedPostGoodAndScoringAffectedRuns(t *testing.T) {
	t.Parallel()

	score, reasons := FlakeScoreAndReasons(FailurePatternRow{
		Environment:        "dev",
		AfterLastPushCount: 0,
		TrendCounts:        []int{1, 1, 1, 1, 1, 1, 1},
		TrendRange:         "2026-03-01..2026-03-07",
		AffectedRuns: []RunReference{
			{RunURL: "https://prow.example/run/stale", PRNumber: 4100, OccurredAt: "2026-03-01T00:00:00Z"},
		},
		ScoringReferences: []RunReference{
			{RunURL: "https://prow.example/run/recent", PRNumber: 4101, OccurredAt: "2026-03-07T23:30:00Z"},
			{RunURL: "https://prow.example/run/older", PRNumber: 4102, OccurredAt: "2026-03-04T12:00:00Z"},
		},
		LinkedPatterns: []FailurePatternRow{
			{
				AfterLastPushCount: 1,
				AffectedRuns:       []RunReference{{RunURL: "https://prow.example/child/1", OccurredAt: "2026-03-06T12:00:00Z"}},
			},
			{
				AfterLastPushCount: 2,
				AffectedRuns:       []RunReference{{RunURL: "https://prow.example/child/2", OccurredAt: "2026-03-07T12:00:00Z"}},
			},
		},
	})
	if score <= 0 {
		t.Fatalf("expected positive flake score, got %d", score)
	}
	expectedSnippets := []string{
		"after last push",
		"recent occurrence",
	}
	for _, snippet := range expectedSnippets {
		found := false
		for _, reason := range reasons {
			if strings.Contains(reason, snippet) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected reason containing %q, got %v", snippet, reasons)
		}
	}
}

func TestRenderTableShowsBadPRIndicator(t *testing.T) {
	t.Parallel()

	html := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "deadline exceeded",
			Occurrences:        3,
			OccurrenceShare:    50,
			AfterLastPushCount: 0,
			AffectedRuns: []RunReference{
				{
					RunURL:     "https://prow.example/run/1",
					PRNumber:   4313,
					OccurredAt: "2026-03-07T10:00:00Z",
				},
			},
		},
	}, TableOptions{})

	if !strings.Contains(html, "<span class=\"bad-pr-flag\"") {
		t.Fatalf("expected bad-pr flag icon in rendered table")
	}
	if !strings.Contains(html, "This failure pattern appears to be caused by the PR under test") {
		t.Fatalf("expected tooltip to describe PR-caused failure, got %q", html)
	}
	if !strings.Contains(html, "no runs after the PR&#39;s last push") && !strings.Contains(html, "no runs after the PR's last push") {
		t.Fatalf("expected tooltip to include translated reason, got %q", html)
	}
	if strings.Contains(html, "bad PR score:") {
		t.Fatalf("expected bad-pr score details to be removed from expanded view, got %q", html)
	}
}

func TestRenderTableDoesNotShowBadPRIndicatorForScoreTwo(t *testing.T) {
	t.Parallel()

	html := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "provision flake",
			Occurrences:        4,
			OccurrenceShare:    25,
			AfterLastPushCount: 0,
			AffectedRuns: []RunReference{
				{
					RunURL:     "https://prow.example/run/1",
					PRNumber:   4313,
					OccurredAt: "2026-03-07T10:00:00Z",
				},
				{
					RunURL:     "https://prow.example/run/2",
					PRNumber:   0,
					OccurredAt: "2026-03-07T09:00:00Z",
				},
			},
		},
	}, TableOptions{})

	if strings.Contains(html, "<span class=\"bad-pr-flag\"") {
		t.Fatalf("expected no bad-pr icon for score 2/3")
	}
	if strings.Contains(html, "bad PR score:") {
		t.Fatalf("expected bad-pr score details to be removed from expanded view, got %q", html)
	}
}

func TestRenderTableUsesSharedHeaderLabelsAndOrder(t *testing.T) {
	t.Parallel()

	html := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "deadline exceeded",
			Occurrences:        3,
			OccurrenceShare:    50,
			AfterLastPushCount: 1,
			TrendCounts:        []int{0, 2, 1, 3, 0, 4, 2},
			TrendRange:         "2026-03-01..2026-03-07",
		},
	}, TableOptions{IncludeTrend: true})

	required := []string{
		"Failed at",
		"data-sort-key=\"jobs_affected\"",
		"data-sort-key=\"impact\"",
		"data-sort-key=\"flake_score\"",
		"<th>Trend</th>",
		"<th>Also in",
		"title=\"Percentage of all job runs in this environment affected by this failure pattern during the selected window.\"",
		"title=\"Other environments where the same failure pattern was also detected during the selected window.\"",
		"<svg class=\"trend-svg\"",
		"Mar 1: 0 · Mar 2: 2 · Mar 3: 1 · Mar 4: 3 · Mar 5: 0 · Mar 6: 4 · Mar 7: 2",
	}
	for _, snippet := range required {
		if !strings.Contains(html, snippet) {
			t.Fatalf("expected rendered table to contain %q", snippet)
		}
	}

	headerStart := strings.Index(html, "<thead><tr>")
	headerEnd := strings.Index(html, "</tr></thead>")
	if headerStart < 0 || headerEnd < 0 || headerEnd <= headerStart {
		t.Fatalf("expected failure-pattern table header row to be present")
	}
	headerRow := html[headerStart:headerEnd]
	signatureHeader := strings.Index(headerRow, "Failure pattern")
	laneHeader := strings.Index(headerRow, "Failed at")
	jobsAffectedHeader := strings.Index(headerRow, "data-sort-key=\"jobs_affected\"")
	impactHeader := strings.Index(headerRow, "data-sort-key=\"impact\"")
	flakeScoreHeader := strings.Index(headerRow, "data-sort-key=\"flake_score\"")
	trendHeader := strings.Index(headerRow, "<th>Trend</th>")
	seenInHeader := strings.Index(headerRow, "<th>Also in")
	if !(signatureHeader < laneHeader && laneHeader < jobsAffectedHeader && jobsAffectedHeader < impactHeader && impactHeader < flakeScoreHeader && flakeScoreHeader < trendHeader && trendHeader < seenInHeader) {
		t.Fatalf("unexpected shared header order in rendered table")
	}
	if strings.Contains(html, "data-sort-key=\"count\"") || strings.Contains(html, "data-sort-key=\"after_last_push\"") || strings.Contains(html, "data-sort-key=\"share\"") {
		t.Fatalf("expected count/after-last-push/share columns to be hidden by default")
	}
}

func TestRenderTableIncludesClientSortingAndVisibilityConfiguration(t *testing.T) {
	t.Parallel()

	html := RenderTable([]FailurePatternRow{
		{FailurePattern: "one", Occurrences: 3, OccurrenceShare: 50, AfterLastPushCount: 1},
		{FailurePattern: "two", Occurrences: 2, OccurrenceShare: 30, AfterLastPushCount: 0},
		{FailurePattern: "three", Occurrences: 1, OccurrenceShare: 20, AfterLastPushCount: 0},
	}, TableOptions{
		LoadedRowsLimit:    2,
		InitialVisibleRows: 1,
	})

	for _, snippet := range []string{
		"data-sortable=\"true\"",
		"data-sort-key=\"impact\"",
		"data-sort-dir=\"desc\"",
		"data-initial-visible=\"1\"",
		"data-row-id=\"failure-pattern-row-0\"",
		"data-parent-row-id=\"failure-pattern-row-0\"",
		"button.failure-patterns-sort-button",
	} {
		if !strings.Contains(html, snippet) {
			t.Fatalf("expected rendered table to contain %q", snippet)
		}
	}
	if strings.Contains(html, "three") {
		t.Fatalf("expected loaded rows limit to omit third row")
	}
}

func TestRenderTableQualityColumnsHiddenByDefault(t *testing.T) {
	t.Parallel()

	html := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "deadline exceeded",
			Occurrences:        3,
			OccurrenceShare:    50,
			AfterLastPushCount: 1,
			QualityScore:       9,
			QualityNoteLabels:  []string{"context type stub leaked"},
			ReviewNoteLabels:   []string{"low_confidence_evidence"},
		},
	}, TableOptions{})

	for _, snippet := range []string{
		"<th>Quality score</th>",
		"<th>Quality flags</th>",
		"<th>Review flags</th>",
		"badge-quality",
		"badge-review",
	} {
		if strings.Contains(html, snippet) {
			t.Fatalf("expected rendered table to hide quality/review columns by default; found %q", snippet)
		}
	}
}

func TestRenderTableShowsQualityAndReviewColumnsWhenEnabled(t *testing.T) {
	t.Parallel()

	html := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "deadline exceeded",
			Occurrences:        3,
			OccurrenceShare:    50,
			AfterLastPushCount: 1,
			QualityScore:       9,
			QualityNoteLabels:  []string{"context type stub leaked"},
			ReviewNoteLabels:   []string{"low_confidence_evidence"},
		},
	}, TableOptions{
		ShowQualityScore: true,
		ShowQualityFlags: true,
		ShowReviewFlags:  true,
	})

	for _, snippet := range []string{
		"<th>Quality score</th>",
		"<th>Quality flags</th>",
		"<th>Review flags</th>",
		"context type stub leaked",
		"low_confidence_evidence",
		"badge-quality",
		"badge-review",
	} {
		if !strings.Contains(html, snippet) {
			t.Fatalf("expected rendered table to contain %q", snippet)
		}
	}
}

func TestRenderTableAllowsUnlimitedLoadedRows(t *testing.T) {
	t.Parallel()

	html := RenderTable([]FailurePatternRow{
		{FailurePattern: "one", Occurrences: 3, OccurrenceShare: 50, AfterLastPushCount: 1},
		{FailurePattern: "two", Occurrences: 2, OccurrenceShare: 30, AfterLastPushCount: 0},
		{FailurePattern: "three", Occurrences: 1, OccurrenceShare: 20, AfterLastPushCount: 0},
	}, TableOptions{
		LoadedRowsLimit:    -1,
		InitialVisibleRows: -1,
	})

	for _, phrase := range []string{"one", "two", "three"} {
		if !strings.Contains(html, phrase) {
			t.Fatalf("expected rendered table to include row %q", phrase)
		}
	}
}

func TestReportChromeHTMLFallsBackToThemeToggleWhenUnset(t *testing.T) {
	t.Parallel()

	rendered := ReportChromeHTML(ReportChromeOptions{})
	if !strings.Contains(rendered, "theme-toggle-wrap") {
		t.Fatalf("expected fallback theme toggle wrapper, got %q", rendered)
	}
	if strings.Contains(rendered, "report-chrome") {
		t.Fatalf("did not expect report chrome container when options are empty")
	}
}

func TestReportChromeHTMLRendersNavigationAndThemeToggleButton(t *testing.T) {
	t.Parallel()

	rendered := ReportChromeHTML(ReportChromeOptions{
		WindowLabel:         "2026-03-08 to 2026-03-14 UTC",
		CurrentView:         ReportViewReport,
		PreviousWeek:        "2026-03-01",
		PreviousHref:        "../2026-03-01/weekly-metrics.html",
		NextWeek:            "",
		NextHref:            "",
		RollingHref:         "rolling.html",
		ReportHref:          "weekly-metrics.html",
		FailurePatternsHref: "failure-patterns-report.html",
		RunLogHref:          "run-log.html",
		ArchiveHref:         "../archive/",
		WindowStartDate:     "2026-03-08",
		WindowEndDate:       "2026-03-14",
	})
	for _, snippet := range []string{
		"class=\"report-chrome\"",
		"href=\"../2026-03-01/weekly-metrics.html\"",
		"2026-03-08 to 2026-03-14 UTC",
		"href=\"rolling.html\"",
		"class=\"report-view-link active\" href=\"weekly-metrics.html\"",
		"href=\"failure-patterns-report.html\"",
		"href=\"run-log.html\"",
		">Last 7 Days</a>",
		">Report</a>",
		">Failure Patterns</a>",
		">Run Log</a>",
		"href=\"../archive/\"",
		"class=\"report-nav-btn disabled\"",
		"id=\"theme-toggle\"",
	} {
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected rendered report chrome to contain %q", snippet)
		}
	}
}

func TestRenderTableShowsManualIssueAndSelectionWhenEnabled(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "deadline exceeded",
			FailurePatternID:   "cluster-1",
			Occurrences:        3,
			OccurrenceShare:    50,
			AfterLastPushCount: 1,
			ManualIssueID:      "p3c-abc123",
		},
		{
			Environment:        "int",
			FailurePattern:     "network timeout",
			FailurePatternID:   "cluster-2",
			Occurrences:        2,
			OccurrenceShare:    50,
			AfterLastPushCount: 0,
			ManualIssueID:      "",
		},
	}, TableOptions{
		ShowManualIssue:      true,
		IncludeSelection:     true,
		SelectionInputName:   "cluster_id",
		InitialSortKey:       "manual_cluster",
		InitialSortDirection: "asc",
	})

	for _, snippet := range []string{
		"<th class=\"failure-patterns-select-col\">Select</th>",
		"data-sort-key=\"manual_cluster\"",
		"Linked group ID",
		"name=\"cluster_id\" value=\"cluster-1\"",
		">p3c-abc123</td>",
		"data-sort-key=\"manual_cluster\" data-sort-dir=\"asc\"",
	} {
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected rendered table to contain %q", snippet)
		}
	}
}

func TestRenderTableUsesCustomSelectionValueWhenProvided(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]FailurePatternRow{
		{
			Environment:      "dev",
			FailurePattern:   "deadline exceeded",
			FailurePatternID: "p3c-abc123",
			SelectionValue:   "dev|p3c-abc123",
			Occurrences:      3,
			OccurrenceShare:  100,
		},
	}, TableOptions{
		IncludeSelection:   true,
		SelectionInputName: "cluster_id",
	})

	if !strings.Contains(rendered, "name=\"cluster_id\" value=\"dev|p3c-abc123\"") {
		t.Fatalf("expected custom row selection value in rendered checkbox: %q", rendered)
	}
}

func TestRenderTableLinkedRowsRenderChildExpandersInDetailRow(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "aggregate phrase",
			FailurePatternID:   "p3c-aggregate",
			Occurrences:        9,
			OccurrenceShare:    100,
			AfterLastPushCount: 99,
			LinkedPatterns: []FailurePatternRow{
				{
					Environment:        "dev",
					FailurePattern:     "child phrase one",
					FailurePatternID:   "phase2-dev-1",
					SelectionValue:     "dev|phase2-dev-1",
					SearchQuery:        "child one query",
					Occurrences:        4,
					OccurrenceShare:    44.44,
					AfterLastPushCount: 1,
					QualityScore:       5,
					QualityNoteLabels:  []string{"generic fallback phrase"},
					ReviewNoteLabels:   []string{"low-confidence-source"},
					FullErrorSamples:   []string{"child one full error"},
					ContributingTests:  []ContributingTest{{FailedAt: "e2e", JobName: "job-one", TestName: "test-one", Occurrences: 4}},
					AffectedRuns:       []RunReference{{RunURL: "https://prow.example/run/1", OccurredAt: "2026-03-15T10:00:00Z"}},
				},
				{
					Environment:        "dev",
					FailurePattern:     "child phrase two",
					FailurePatternID:   "phase2-dev-2",
					SearchQuery:        "child two query",
					Occurrences:        5,
					OccurrenceShare:    55.56,
					AfterLastPushCount: 1,
					QualityScore:       1,
					FullErrorSamples:   []string{"child two full error"},
					AffectedRuns:       []RunReference{{RunURL: "https://prow.example/run/2", OccurredAt: "2026-03-15T11:00:00Z"}},
				},
			},
		},
	}, TableOptions{
		ShowQualityScore:      true,
		ShowQualityFlags:      true,
		ShowReviewFlags:       true,
		ShowLinkedChildRemove: true,
	})

	for _, snippet := range []string{
		"Linked failure patterns (2)",
		"child phrase one",
		"child phrase two",
		"name=\"unlink_child\" value=\"dev|phase2-dev-1\"",
		"runs affected: 1",
		"Flake signal:",
		"Full failure examples (1)",
		"Affected runs (1)",
	} {
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected linked-child detail rendering to contain %q", snippet)
		}
	}
	if !strings.Contains(rendered, `data-sort-jobs="2"`) {
		t.Fatalf("expected aggregate row jobs affected to sum linked children, got %q", rendered)
	}
	if !strings.Contains(rendered, `data-sort-post-good="2"`) {
		t.Fatalf("expected aggregate row after-last-push to sum linked children, got %q", rendered)
	}
}

func TestRenderTableLinkedRowsDeduplicateAggregateJobsAffectedByRunURL(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]FailurePatternRow{
		{
			Environment:      "dev",
			FailurePattern:   "aggregate phrase",
			FailurePatternID: "p3c-aggregate",
			Occurrences:      3,
			OccurrenceShare:  100,
			LinkedPatterns: []FailurePatternRow{
				{
					Environment:      "dev",
					FailurePattern:   "child phrase one",
					FailurePatternID: "phase2-dev-1",
					AffectedRuns: []RunReference{
						{RunURL: "https://prow.example/run/shared", OccurredAt: "2026-03-15T10:00:00Z"},
						{RunURL: "https://prow.example/run/shared", OccurredAt: "2026-03-15T10:05:00Z"},
					},
				},
				{
					Environment:      "dev",
					FailurePattern:   "child phrase two",
					FailurePatternID: "phase2-dev-2",
					AffectedRuns: []RunReference{
						{RunURL: "https://prow.example/run/shared", OccurredAt: "2026-03-15T11:00:00Z"},
					},
				},
			},
		},
	}, TableOptions{})

	if !strings.Contains(rendered, `data-sort-jobs="1"`) {
		t.Fatalf("expected aggregate row jobs affected to deduplicate shared runs, got %q", rendered)
	}
	if !strings.Contains(rendered, `data-sort-impact="100.000000"`) {
		t.Fatalf("expected aggregate row impact to use deduplicated shared runs, got %q", rendered)
	}
}

func TestRenderTableHidesCountAfterShareByDefaultAndShowsImpact(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "deadline exceeded",
			FailurePatternID:   "cluster-1",
			Occurrences:        3,
			OccurrenceShare:    75,
			AfterLastPushCount: 1,
			AffectedRuns: []RunReference{
				{RunURL: "https://prow.example/run/1", OccurredAt: "2026-03-15T10:00:00Z"},
				{RunURL: "https://prow.example/run/2", OccurredAt: "2026-03-15T11:00:00Z"},
			},
		},
		{
			Environment:        "dev",
			FailurePattern:     "api timeout",
			FailurePatternID:   "cluster-2",
			Occurrences:        1,
			OccurrenceShare:    25,
			AfterLastPushCount: 0,
			AffectedRuns: []RunReference{
				{RunURL: "https://prow.example/run/3", OccurredAt: "2026-03-15T12:00:00Z"},
			},
		},
	}, TableOptions{})

	required := []string{
		"data-sort-key=\"jobs_affected\"",
		"data-sort-key=\"impact\"",
		"data-sort-key=\"flake_score\"",
	}
	for _, snippet := range required {
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected rendered table to contain %q", snippet)
		}
	}
	if strings.Contains(rendered, "data-sort-key=\"count\"") || strings.Contains(rendered, "data-sort-key=\"after_last_push\"") || strings.Contains(rendered, "data-sort-key=\"share\"") {
		t.Fatalf("expected count/after-last-push/share columns hidden by default: %q", rendered)
	}
	if !strings.Contains(rendered, "job runs affected") {
		t.Fatalf("expected impact cell tooltip in rendered table: %q", rendered)
	}
}

func TestRenderTableDerivesFailedAtFilterFromContributingTests(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "deadline exceeded",
			FailurePatternID:   "cluster-1",
			Occurrences:        1,
			OccurrenceShare:    100,
			AfterLastPushCount: 0,
			ContributingTests: []ContributingTest{
				{FailedAt: "e2e", JobName: "job-1", TestName: "test-1", Occurrences: 1},
			},
			AffectedRuns: []RunReference{
				{RunURL: "https://prow.example/run/1", OccurredAt: "2026-03-15T10:00:00Z"},
			},
		},
	}, TableOptions{})

	if !strings.Contains(rendered, `data-filter-lane="e2e"`) {
		t.Fatalf("expected lane filter to be derived from contributing tests, got %q", rendered)
	}
}

func TestRenderTableUsesUnknownFailedAtWhenFailedAtDataMissing(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "api timeout",
			FailurePatternID:   "cluster-2",
			Occurrences:        1,
			OccurrenceShare:    100,
			AfterLastPushCount: 0,
			AffectedRuns: []RunReference{
				{RunURL: "https://prow.example/run/2", OccurredAt: "2026-03-15T11:00:00Z"},
			},
		},
	}, TableOptions{})

	if !strings.Contains(rendered, `data-filter-lane="unknown"`) {
		t.Fatalf("expected lane filter fallback to unknown, got %q", rendered)
	}
}

func TestRenderContributingTestsDetailsUsesFailedAtHeaderAndProwJobLinks(t *testing.T) {
	t.Parallel()

	rendered := renderContributingTestsDetails([]ContributingTest{
		{
			FailedAt:    "e2e",
			JobName:     "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			TestName:    "TestClusterCreate",
			Occurrences: 3,
		},
	}, "")

	if !strings.Contains(rendered, "<th>Failed At</th>") {
		t.Fatalf("expected contributing tests header to use Failed At, got %q", rendered)
	}
	if strings.Contains(rendered, "<th>Lane</th>") {
		t.Fatalf("expected legacy Lane header to be removed, got %q", rendered)
	}
	if !strings.Contains(rendered, `href="https://prow.ci.openshift.org/?job=pull-ci-Azure-ARO-HCP-main-e2e-parallel"`) {
		t.Fatalf("expected contributing job name to link to prow job history, got %q", rendered)
	}
	if !strings.Contains(rendered, ">pull-ci-Azure-ARO-HCP-main-e2e-parallel</a>") {
		t.Fatalf("expected contributing job link label to preserve the job name, got %q", rendered)
	}
}

func TestRenderFullErrorDetailsKeepsFullSamples(t *testing.T) {
	t.Parallel()

	sample := strings.Repeat("0123456789abcdef", 24) + "::tail-marker"
	rendered := renderFullErrorDetails([]string{sample}, "")

	if !strings.Contains(rendered, `class="full-errors-list"`) {
		t.Fatalf("expected full error details to render in the dedicated container, got %q", rendered)
	}
	if !strings.Contains(rendered, sample) {
		t.Fatalf("expected full error sample to remain untruncated, got %q", rendered)
	}
}

func TestStylesCSSExpandsOpenFullErrors(t *testing.T) {
	t.Parallel()

	styles := StylesCSS()
	for _, snippet := range []string{
		`details.full-errors-toggle[open] { flex: 1 1 100%; min-width: 0; }`,
		`.failure-patterns-errors-row .full-errors-list { margin-top: 6px; max-width: 100%; }`,
		`pre { white-space: pre-wrap; word-break: break-word; overflow-x: auto; max-width: 100%;`,
	} {
		if !strings.Contains(styles, snippet) {
			t.Fatalf("expected styles to contain %q, got %q", snippet, styles)
		}
	}
}
