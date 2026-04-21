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

func TestClassifyFailurePatternRegression(t *testing.T) {
	t.Parallel()

	category, reasons := ClassifyFailurePattern(FailurePatternRow{
		Environment:        "dev",
		AfterLastPushCount: 0,
		PriorWeeksPresent:  0,
		AffectedRuns: []RunReference{
			{RunURL: "https://prow.example/run/1", PRNumber: 4313, OccurredAt: "2026-03-07T10:00:00Z"},
		},
	})
	if category != "regression" {
		t.Fatalf("expected regression, got %q", category)
	}
	if len(reasons) == 0 {
		t.Fatalf("expected reasons for regression")
	}
}

func TestClassifyFailurePatternFlake(t *testing.T) {
	t.Parallel()

	category, reasons := ClassifyFailurePattern(FailurePatternRow{
		Environment:        "dev",
		AfterLastPushCount: 5,
		PriorWeeksPresent:  3,
		TrendCounts:        []int{1, 0, 1, 0, 1, 0, 1},
		AffectedRuns: []RunReference{
			{RunURL: "https://prow.example/run/1", PRNumber: 4201, OccurredAt: "2026-03-07T10:00:00Z"},
			{RunURL: "https://prow.example/run/2", PRNumber: 4202, OccurredAt: "2026-03-06T10:00:00Z"},
		},
	})
	if category != "flake" {
		t.Fatalf("expected flake, got %q", category)
	}
	if len(reasons) == 0 {
		t.Fatalf("expected reasons for flake classification")
	}
}

func TestClassifyFailurePatternNoise(t *testing.T) {
	t.Parallel()

	category, _ := ClassifyFailurePattern(FailurePatternRow{
		Environment:        "dev",
		FailurePattern:     "failure",
		AfterLastPushCount: 1,
		PriorWeeksPresent:  0,
		AffectedRuns: []RunReference{
			{RunURL: "https://prow.example/run/1", PRNumber: 4201, OccurredAt: "2026-03-07T10:00:00Z"},
			{RunURL: "https://prow.example/run/2", PRNumber: 4202, OccurredAt: "2026-03-06T10:00:00Z"},
		},
	})
	if category != "noise" {
		t.Fatalf("expected noise for generic phrase, got %q", category)
	}
}

func TestClassifyFailurePatternIndeterminate(t *testing.T) {
	t.Parallel()

	category, reasons := ClassifyFailurePattern(FailurePatternRow{
		Environment:        "dev",
		FailurePattern:     "some specific error: context deadline exceeded in foo bar",
		AfterLastPushCount: 2,
		PriorWeeksPresent:  1,
		TrendCounts:        []int{1, 0, 0, 0, 0, 0, 0},
		AffectedRuns: []RunReference{
			{RunURL: "https://prow.example/run/1", PRNumber: 4201, OccurredAt: "2026-03-07T10:00:00Z"},
			{RunURL: "https://prow.example/run/2", PRNumber: 4202, OccurredAt: "2026-03-06T10:00:00Z"},
		},
	})
	if category != "indeterminate" {
		t.Fatalf("expected indeterminate, got %q", category)
	}
	if len(reasons) == 0 {
		t.Fatalf("expected reasons for indeterminate classification")
	}
}

func TestClassifyFailurePatternRegressionNotTriggeredWithPriorHistory(t *testing.T) {
	t.Parallel()

	category, _ := ClassifyFailurePattern(FailurePatternRow{
		Environment:        "dev",
		FailurePattern:     "context deadline exceeded in provisioning step",
		AfterLastPushCount: 0,
		PriorWeeksPresent:  2,
		AffectedRuns: []RunReference{
			{RunURL: "https://prow.example/run/1", PRNumber: 4313, OccurredAt: "2026-03-07T10:00:00Z"},
		},
	})
	if category == "regression" {
		t.Fatalf("expected non-regression category when PriorWeeksPresent > 0, got regression")
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
	if !strings.Contains(html, "Likely regression") {
		t.Fatalf("expected tooltip to describe regression classification, got %q", html)
	}
}

func TestRenderTableDoesNotShowRegressionIndicatorWhenPatternHasPriorHistory(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]FailurePatternRow{
		{
			Environment:        "dev",
			FailurePattern:     "provision flake with some unique text here",
			Occurrences:        4,
			OccurrenceShare:    25,
			AfterLastPushCount: 0,
			PriorWeeksPresent:  2,
			AffectedRuns: []RunReference{
				{
					RunURL:     "https://prow.example/run/1",
					PRNumber:   4313,
					OccurredAt: "2026-03-07T10:00:00Z",
				},
			},
		},
	}, TableOptions{})

	if strings.Contains(rendered, "<span class=\"bad-pr-flag\"") {
		t.Fatalf("expected no regression indicator for pattern with prior history")
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
		"data-sort-key=\"category\"",
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
	categoryHeader := strings.Index(headerRow, "data-sort-key=\"category\"")
	trendHeader := strings.Index(headerRow, "<th>Trend</th>")
	seenInHeader := strings.Index(headerRow, "<th>Also in")
	if !(signatureHeader < laneHeader && laneHeader < jobsAffectedHeader && jobsAffectedHeader < impactHeader && impactHeader < categoryHeader && categoryHeader < trendHeader && trendHeader < seenInHeader) {
		t.Fatalf("unexpected shared header order in rendered table")
	}
	if strings.Contains(html, "data-sort-key=\"count\"") || strings.Contains(html, "data-sort-key=\"after_last_push\"") || strings.Contains(html, "data-sort-key=\"share\"") {
		t.Fatalf("expected count/after-last-push/share columns to be hidden by default")
	}
}

func TestRenderTableIncludesClientSortingAndVisibilityConfiguration(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]FailurePatternRow{
		{FailurePattern: "context deadline exceeded in provisioning step alpha", Occurrences: 3, OccurrenceShare: 50, AfterLastPushCount: 1},
		{FailurePattern: "context deadline exceeded in provisioning step beta", Occurrences: 2, OccurrenceShare: 30, AfterLastPushCount: 1},
		{FailurePattern: "context deadline exceeded in provisioning step gamma", Occurrences: 1, OccurrenceShare: 20, AfterLastPushCount: 1},
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
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected rendered table to contain %q", snippet)
		}
	}
	if strings.Contains(rendered, "gamma") {
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

func TestReportChromeHTMLRendersTwoTierNavigationAndContextControls(t *testing.T) {
	t.Parallel()

	rendered := ReportChromeHTML(ReportChromeOptions{
		CurrentView:         ReportViewFailurePatterns,
		OverviewHref:        "/",
		FailurePatternsHref: "/failure-patterns",
		RunLogHref:          "/run-log",
		FilterFormAction:    "/failure-patterns",
		TimeSelector: TimeSelectorOptions{
			Mode:         TimeSelectorModeWeekly,
			Label:        "Weekly: Mar 9 - Mar 15",
			PreviousHref: "/failure-patterns?end_date=2026-03-08&env=dev&start_date=2026-03-02",
			MenuLinks: []ChromeLink{
				{Label: "Last 7 Days", Href: "/failure-patterns?end_date=2026-03-15&env=dev&mode=rolling&start_date=2026-03-09"},
				{Label: "Weekly: Mar 9 - Mar 15", Href: "/failure-patterns?end_date=2026-03-15&env=dev&start_date=2026-03-09", Active: true},
			},
			ShowRangeInputs: true,
			RangeStartDate:  "2026-03-09",
			RangeEndDate:    "2026-03-15",
		},
		Environment: EnvironmentControlOptions{
			Value: "dev",
		},
		JSONAPIHref: "/api/failure-patterns/window?end_date=2026-03-15&env=dev&start_date=2026-03-09",
		ResetHref:   "/failure-patterns",
		ShowApply:   true,
	})
	for _, snippet := range []string{
		"class=\"report-shell\"",
		">CIHealth</a>",
		"class=\"report-route-link\" href=\"/\">Overview</a>",
		"class=\"report-route-link active\" href=\"/failure-patterns\">Failure Patterns</a>",
		"href=\"/run-log\">Run Log</a>",
		"Theme: Auto",
		">Time:</span>",
		"Weekly: Mar 9 - Mar 15",
		"href=\"/failure-patterns?end_date=2026-03-08&amp;env=dev&amp;start_date=2026-03-02\"",
		"class=\"report-context-nav-btn disabled\"",
		"class=\"time-selector-option active\" href=\"/failure-patterns?end_date=2026-03-15&amp;env=dev&amp;start_date=2026-03-09\"",
		"name=\"start_date\" value=\"2026-03-09\"",
		"name=\"end_date\" value=\"2026-03-15\"",
		"name=\"env\"",
		"option value=\"dev\" selected=\"selected\">DEV</option>",
		">Failure Patterns</a>",
		">View JSON API</a>",
		">Reset</a>",
		">Apply</button>",
		"id=\"theme-toggle\"",
	} {
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected rendered report chrome to contain %q", snippet)
		}
	}
	resetIndex := strings.Index(rendered, ">Reset</a>")
	applyIndex := strings.Index(rendered, ">Apply</button>")
	jsonIndex := strings.Index(rendered, ">View JSON API</a>")
	if resetIndex == -1 || applyIndex == -1 || jsonIndex == -1 {
		t.Fatalf("expected reset/apply/json controls in rendered chrome: %q", rendered)
	}
	if resetIndex > jsonIndex || applyIndex > jsonIndex {
		t.Fatalf("expected reset/apply controls before the JSON API action: %q", rendered)
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
		"Signal:",
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
		"data-sort-key=\"category\"",
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
