package triagehtml

import (
	"strings"
	"testing"
)

func TestBadPRScoreAndReasonsGateOnPostGood(t *testing.T) {
	t.Parallel()

	score, reasons := BadPRScoreAndReasons(SignatureRow{
		Environment:   "dev",
		PostGoodCount: 2,
		AlsoSeenIn:    nil,
		References: []RunReference{
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

	score, reasons := BadPRScoreAndReasons(SignatureRow{
		Environment:   "dev",
		PostGoodCount: 0,
		AlsoSeenIn:    nil,
		References: []RunReference{
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

	rows := []SignatureRow{
		{
			Environment:   "dev",
			Phrase:        "score-three",
			SupportCount:  100,
			PostGoodCount: 0,
			References: []RunReference{
				{RunURL: "https://prow.example/run/1", PRNumber: 10, OccurredAt: "2026-03-07T10:00:00Z"},
			},
		},
		{
			Environment:   "dev",
			Phrase:        "score-zero",
			SupportCount:  1,
			PostGoodCount: 1,
			References: []RunReference{
				{RunURL: "https://prow.example/run/2", PRNumber: 20, OccurredAt: "2026-03-07T09:00:00Z"},
			},
		},
		{
			Environment:   "int",
			Phrase:        "score-one",
			SupportCount:  50,
			PostGoodCount: 0,
			References: []RunReference{
				{RunURL: "https://prow.example/run/3", PRNumber: 0, OccurredAt: "2026-03-07T08:00:00Z"},
			},
		},
	}

	SortRowsByBadPRScore(rows)

	if rows[0].Phrase != "score-zero" {
		t.Fatalf("expected score-zero first, got %q", rows[0].Phrase)
	}
	if rows[1].Phrase != "score-one" {
		t.Fatalf("expected score-one second, got %q", rows[1].Phrase)
	}
	if rows[2].Phrase != "score-three" {
		t.Fatalf("expected score-three last, got %q", rows[2].Phrase)
	}
}

func TestFlakeScoreAndReasonsIncludesRequestedSignals(t *testing.T) {
	t.Parallel()

	score, reasons := FlakeScoreAndReasons(SignatureRow{
		Environment:       "dev",
		PostGoodCount:     5,
		TrendCounts:       []int{1, 1, 1, 1, 1, 1, 1},
		TrendRange:        "2026-03-01..2026-03-07",
		PriorWeeksPresent: 2,
		References: []RunReference{
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

func TestFlakeScoreAndReasonsAggregatedRowsUseLinkedPostGoodAndScoringReferences(t *testing.T) {
	t.Parallel()

	score, reasons := FlakeScoreAndReasons(SignatureRow{
		Environment:   "dev",
		PostGoodCount: 0,
		TrendCounts:   []int{1, 1, 1, 1, 1, 1, 1},
		TrendRange:    "2026-03-01..2026-03-07",
		References: []RunReference{
			{RunURL: "https://prow.example/run/stale", PRNumber: 4100, OccurredAt: "2026-03-01T00:00:00Z"},
		},
		ScoringReferences: []RunReference{
			{RunURL: "https://prow.example/run/recent", PRNumber: 4101, OccurredAt: "2026-03-07T23:30:00Z"},
			{RunURL: "https://prow.example/run/older", PRNumber: 4102, OccurredAt: "2026-03-04T12:00:00Z"},
		},
		LinkedChildren: []SignatureRow{
			{
				PostGoodCount: 1,
				References:    []RunReference{{RunURL: "https://prow.example/child/1", OccurredAt: "2026-03-06T12:00:00Z"}},
			},
			{
				PostGoodCount: 2,
				References:    []RunReference{{RunURL: "https://prow.example/child/2", OccurredAt: "2026-03-07T12:00:00Z"}},
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

	html := RenderTable([]SignatureRow{
		{
			Environment:   "dev",
			Phrase:        "deadline exceeded",
			SupportCount:  3,
			SupportShare:  50,
			PostGoodCount: 0,
			References: []RunReference{
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

	html := RenderTable([]SignatureRow{
		{
			Environment:   "dev",
			Phrase:        "provision flake",
			SupportCount:  4,
			SupportShare:  25,
			PostGoodCount: 0,
			References: []RunReference{
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

	html := RenderTable([]SignatureRow{
		{
			Environment:   "dev",
			Phrase:        "deadline exceeded",
			SupportCount:  3,
			SupportShare:  50,
			PostGoodCount: 1,
			TrendCounts:   []int{0, 2, 1, 3, 0, 4, 2},
			TrendRange:    "2026-03-01..2026-03-07",
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
		t.Fatalf("expected triage table header row to be present")
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

	html := RenderTable([]SignatureRow{
		{Phrase: "one", SupportCount: 3, SupportShare: 50, PostGoodCount: 1},
		{Phrase: "two", SupportCount: 2, SupportShare: 30, PostGoodCount: 0},
		{Phrase: "three", SupportCount: 1, SupportShare: 20, PostGoodCount: 0},
	}, TableOptions{
		LoadedRowsLimit:    2,
		InitialVisibleRows: 1,
	})

	for _, snippet := range []string{
		"data-sortable=\"true\"",
		"data-sort-key=\"impact\"",
		"data-sort-dir=\"desc\"",
		"data-initial-visible=\"1\"",
		"data-row-id=\"triage-row-0\"",
		"data-parent-row-id=\"triage-row-0\"",
		"button.triage-sort-button",
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

	html := RenderTable([]SignatureRow{
		{
			Environment:       "dev",
			Phrase:            "deadline exceeded",
			SupportCount:      3,
			SupportShare:      50,
			PostGoodCount:     1,
			QualityScore:      9,
			QualityNoteLabels: []string{"context type stub leaked"},
			ReviewNoteLabels:  []string{"low_confidence_evidence"},
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

	html := RenderTable([]SignatureRow{
		{
			Environment:       "dev",
			Phrase:            "deadline exceeded",
			SupportCount:      3,
			SupportShare:      50,
			PostGoodCount:     1,
			QualityScore:      9,
			QualityNoteLabels: []string{"context type stub leaked"},
			ReviewNoteLabels:  []string{"low_confidence_evidence"},
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

	html := RenderTable([]SignatureRow{
		{Phrase: "one", SupportCount: 3, SupportShare: 50, PostGoodCount: 1},
		{Phrase: "two", SupportCount: 2, SupportShare: 30, PostGoodCount: 0},
		{Phrase: "three", SupportCount: 1, SupportShare: 20, PostGoodCount: 0},
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
		CurrentWeek:  "2026-03-08",
		CurrentView:  ReportViewReport,
		PreviousWeek: "2026-03-01",
		PreviousHref: "../2026-03-01/weekly-metrics.html",
		NextWeek:     "",
		NextHref:     "",
		RollingHref:  "rolling.html",
		ReportHref:   "weekly-metrics.html",
		TriageHref:   "triage-report.html",
		RunsHref:     "runs.html",
		ArchiveHref:  "../archive/",
	})
	for _, snippet := range []string{
		"class=\"report-chrome\"",
		"href=\"../2026-03-01/weekly-metrics.html\"",
		"Week 2026-03-08 (UTC)",
		"href=\"rolling.html\"",
		"class=\"report-view-link active\" href=\"weekly-metrics.html\"",
		"href=\"triage-report.html\"",
		"href=\"runs.html\"",
		">Last 7 Days</a>",
		">Weekly Report</a>",
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

	rendered := RenderTable([]SignatureRow{
		{
			Environment:   "dev",
			Phrase:        "deadline exceeded",
			ClusterID:     "cluster-1",
			SupportCount:  3,
			SupportShare:  50,
			PostGoodCount: 1,
			ManualIssueID: "p3c-abc123",
		},
		{
			Environment:   "int",
			Phrase:        "network timeout",
			ClusterID:     "cluster-2",
			SupportCount:  2,
			SupportShare:  50,
			PostGoodCount: 0,
			ManualIssueID: "",
		},
	}, TableOptions{
		ShowManualIssue:      true,
		IncludeSelection:     true,
		SelectionInputName:   "cluster_id",
		InitialSortKey:       "manual_cluster",
		InitialSortDirection: "asc",
	})

	for _, snippet := range []string{
		"<th class=\"triage-select-col\">Select</th>",
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

	rendered := RenderTable([]SignatureRow{
		{
			Environment:    "dev",
			Phrase:         "deadline exceeded",
			ClusterID:      "p3c-abc123",
			SelectionValue: "dev|p3c-abc123",
			SupportCount:   3,
			SupportShare:   100,
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

	rendered := RenderTable([]SignatureRow{
		{
			Environment:   "dev",
			Phrase:        "aggregate phrase",
			ClusterID:     "p3c-aggregate",
			SupportCount:  9,
			SupportShare:  100,
			PostGoodCount: 99,
			LinkedChildren: []SignatureRow{
				{
					Environment:       "dev",
					Phrase:            "child phrase one",
					ClusterID:         "phase2-dev-1",
					SelectionValue:    "dev|phase2-dev-1",
					SearchQuery:       "child one query",
					SupportCount:      4,
					SupportShare:      44.44,
					PostGoodCount:     1,
					QualityScore:      5,
					QualityNoteLabels: []string{"generic fallback phrase"},
					ReviewNoteLabels:  []string{"low-confidence-source"},
					FullErrorSamples:  []string{"child one full error"},
					ContributingTests: []ContributingTest{{Lane: "e2e", JobName: "job-one", TestName: "test-one", SupportCount: 4}},
					References:        []RunReference{{RunURL: "https://prow.example/run/1", OccurredAt: "2026-03-15T10:00:00Z"}},
				},
				{
					Environment:      "dev",
					Phrase:           "child phrase two",
					ClusterID:        "phase2-dev-2",
					SearchQuery:      "child two query",
					SupportCount:     5,
					SupportShare:     55.56,
					PostGoodCount:    1,
					QualityScore:     1,
					FullErrorSamples: []string{"child two full error"},
					References:       []RunReference{{RunURL: "https://prow.example/run/2", OccurredAt: "2026-03-15T11:00:00Z"}},
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

	rendered := RenderTable([]SignatureRow{
		{
			Environment:  "dev",
			Phrase:       "aggregate phrase",
			ClusterID:    "p3c-aggregate",
			SupportCount: 3,
			SupportShare: 100,
			LinkedChildren: []SignatureRow{
				{
					Environment: "dev",
					Phrase:      "child phrase one",
					ClusterID:   "phase2-dev-1",
					References: []RunReference{
						{RunURL: "https://prow.example/run/shared", OccurredAt: "2026-03-15T10:00:00Z"},
						{RunURL: "https://prow.example/run/shared", OccurredAt: "2026-03-15T10:05:00Z"},
					},
				},
				{
					Environment: "dev",
					Phrase:      "child phrase two",
					ClusterID:   "phase2-dev-2",
					References: []RunReference{
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

	rendered := RenderTable([]SignatureRow{
		{
			Environment:   "dev",
			Phrase:        "deadline exceeded",
			ClusterID:     "cluster-1",
			SupportCount:  3,
			SupportShare:  75,
			PostGoodCount: 1,
			References: []RunReference{
				{RunURL: "https://prow.example/run/1", OccurredAt: "2026-03-15T10:00:00Z"},
				{RunURL: "https://prow.example/run/2", OccurredAt: "2026-03-15T11:00:00Z"},
			},
		},
		{
			Environment:   "dev",
			Phrase:        "api timeout",
			ClusterID:     "cluster-2",
			SupportCount:  1,
			SupportShare:  25,
			PostGoodCount: 0,
			References: []RunReference{
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

func TestRenderTableDerivesLaneFilterFromContributingTests(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]SignatureRow{
		{
			Environment:   "dev",
			Phrase:        "deadline exceeded",
			ClusterID:     "cluster-1",
			SupportCount:  1,
			SupportShare:  100,
			PostGoodCount: 0,
			ContributingTests: []ContributingTest{
				{Lane: "e2e", JobName: "job-1", TestName: "test-1", SupportCount: 1},
			},
			References: []RunReference{
				{RunURL: "https://prow.example/run/1", OccurredAt: "2026-03-15T10:00:00Z"},
			},
		},
	}, TableOptions{})

	if !strings.Contains(rendered, `data-filter-lane="e2e"`) {
		t.Fatalf("expected lane filter to be derived from contributing tests, got %q", rendered)
	}
}

func TestRenderTableUsesUnknownLaneWhenLaneDataMissing(t *testing.T) {
	t.Parallel()

	rendered := RenderTable([]SignatureRow{
		{
			Environment:   "dev",
			Phrase:        "api timeout",
			ClusterID:     "cluster-2",
			SupportCount:  1,
			SupportShare:  100,
			PostGoodCount: 0,
			References: []RunReference{
				{RunURL: "https://prow.example/run/2", OccurredAt: "2026-03-15T11:00:00Z"},
			},
		},
	}, TableOptions{})

	if !strings.Contains(rendered, `data-filter-lane="unknown"`) {
		t.Fatalf("expected lane filter fallback to unknown, got %q", rendered)
	}
}
