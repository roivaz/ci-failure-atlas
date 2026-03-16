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
	if !strings.Contains(html, "Likely bad PR signal (score 3/3): post-good=0; only seen in DEV; only seen in one PR") {
		t.Fatalf("expected tooltip to include score and reasons, got %q", html)
	}
	if !strings.Contains(html, "bad PR score: 3/3 (post-good=0; only seen in DEV; only seen in one PR)") {
		t.Fatalf("expected expanded details to include bad-pr score, got %q", html)
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
	if !strings.Contains(html, "bad PR score: 2/3 (post-good=0; only seen in DEV)") {
		t.Fatalf("expected expanded details to include score 2/3, got %q", html)
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
		"data-sort-key=\"count\"",
		"data-sort-key=\"after_last_push\"",
		"data-sort-key=\"jobs_affected\"",
		"data-sort-key=\"flake_score\"",
		"data-sort-key=\"share\"",
		"<th>Trend</th>",
		"<th>Seen in",
		"title=\"Job run occurred after last push of a PR that merges.\"",
		"title=\"Other environments where the same canonical signature phrase appears.\"",
		"<svg class=\"trend-svg\"",
		"2026-03-01..2026-03-07",
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
	signatureHeader := strings.Index(headerRow, "<th>Signature</th>")
	countHeader := strings.Index(headerRow, "data-sort-key=\"count\"")
	afterLastPushHeader := strings.Index(headerRow, "data-sort-key=\"after_last_push\"")
	jobsAffectedHeader := strings.Index(headerRow, "data-sort-key=\"jobs_affected\"")
	flakeScoreHeader := strings.Index(headerRow, "data-sort-key=\"flake_score\"")
	shareHeader := strings.Index(headerRow, "data-sort-key=\"share\"")
	trendHeader := strings.Index(headerRow, "<th>Trend</th>")
	seenInHeader := strings.Index(headerRow, "<th>Seen in")
	if !(signatureHeader < countHeader && countHeader < afterLastPushHeader && afterLastPushHeader < jobsAffectedHeader && jobsAffectedHeader < flakeScoreHeader && flakeScoreHeader < shareHeader && shareHeader < trendHeader && trendHeader < seenInHeader) {
		t.Fatalf("unexpected shared header order in rendered table")
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
		"data-sort-key=\"flake_score\"",
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
		CurrentView:  ReportViewWeekly,
		PreviousWeek: "2026-03-01",
		PreviousHref: "../2026-03-01/weekly-metrics.html",
		NextWeek:     "",
		NextHref:     "",
		WeeklyHref:   "weekly-metrics.html",
		GlobalHref:   "global-signature-triage.html",
		ArchiveHref:  "../archive/",
	})
	for _, snippet := range []string{
		"class=\"report-chrome\"",
		"href=\"../2026-03-01/weekly-metrics.html\"",
		"Week 2026-03-08",
		"class=\"report-view-link active\" href=\"weekly-metrics.html\"",
		"href=\"global-signature-triage.html\"",
		">Weekly Report</a>",
		">Triage Report</a>",
		"href=\"../archive/\"",
		"class=\"report-nav-btn disabled\"",
		"id=\"theme-toggle\"",
	} {
		if !strings.Contains(rendered, snippet) {
			t.Fatalf("expected rendered report chrome to contain %q", snippet)
		}
	}
}
