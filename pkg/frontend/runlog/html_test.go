package runlog

import (
	"strings"
	"testing"

	frontservice "ci-failure-atlas/pkg/frontend/readmodel"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

func TestDayRunHistoryFailureDetailsHTMLSkipsNonArtifactBackedFailures(t *testing.T) {
	t.Parallel()

	rendered := runLogDayFailureDetailsHTML(frontservice.JobHistoryRunRow{
		FailureRows: []frontservice.JobHistoryFailureRow{
			{
				FailureText:       "job failed and CFA synthesized a non-artifact-backed row",
				NonArtifactBacked: true,
			},
		},
	})
	if rendered != "" {
		t.Fatalf("expected no expander for non-artifact-backed-only row, got %q", rendered)
	}
}

func TestDayRunHistoryFailureDetailsHTMLRendersArtifactBackedFailures(t *testing.T) {
	t.Parallel()

	rendered := runLogDayFailureDetailsHTML(frontservice.JobHistoryRunRow{
		FailureRows: []frontservice.JobHistoryFailureRow{
			{
				FailureText:       "real junit-backed failure text",
				NonArtifactBacked: false,
			},
		},
	})
	if !strings.Contains(rendered, "Failure details (1)") {
		t.Fatalf("expected expander for artifact-backed row, got %q", rendered)
	}
}

func TestDayRunHistoryPRHTMLShowsRegressionIconForLikelyBadPR(t *testing.T) {
	t.Parallel()

	rendered := runLogDayPRHTML(frontservice.JobHistoryRunRow{
		Run: storecontracts.RunRecord{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/run-1",
			PRNumber:    123,
			PRState:     "open",
			MergedPR:    false,
			Failed:      true,
		},
		SemanticRollups: frontservice.JobHistorySemanticRollups{
			ClusteredRows: 1,
		},
		BadPRScore:   3,
		BadPRReasons: []string{"post-good=0", "only seen in DEV", "only seen in one PR"},
	})
	if !strings.Contains(rendered, `class="signal-icon signal-regression"`) {
		t.Fatalf("expected regression signal icon in PR cell, got %q", rendered)
	}
	if !strings.Contains(rendered, "Likely regression") {
		t.Fatalf("expected Likely regression tooltip, got %q", rendered)
	}
	if !strings.Contains(rendered, "#123 (open)") {
		t.Fatalf("expected open PR label in PR cell, got %q", rendered)
	}
}

func TestDayRunHistoryPRHTMLDoesNotUseRunLocalBadPRApproximation(t *testing.T) {
	t.Parallel()

	rendered := runLogDayPRHTML(frontservice.JobHistoryRunRow{
		Run: storecontracts.RunRecord{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/run-1b",
			PRNumber:    123,
			PRState:     "open",
			MergedPR:    false,
			Failed:      true,
		},
		SemanticRollups: frontservice.JobHistorySemanticRollups{
			ClusteredRows: 1,
		},
	})
	if strings.Contains(rendered, `class="signal-icon signal-regression"`) {
		t.Fatalf("did not expect regression icon without weekly signature score, got %q", rendered)
	}
}

func TestDayRunHistoryPRHTMLUsesMergedStateWhenMergedPR(t *testing.T) {
	t.Parallel()

	rendered := runLogDayPRHTML(frontservice.JobHistoryRunRow{
		Run: storecontracts.RunRecord{
			Environment:    "dev",
			RunURL:         "https://prow.example.com/view/run-2",
			PRNumber:       456,
			PRState:        "closed",
			MergedPR:       true,
			PostGoodCommit: true,
		},
	})
	if !strings.Contains(rendered, "#456 (merged)") {
		t.Fatalf("expected merged PR label in PR cell, got %q", rendered)
	}
	if strings.Contains(rendered, "#456 (closed)") {
		t.Fatalf("did not expect closed label for merged PR, got %q", rendered)
	}
}

func TestDayRunHistoryPRHTMLUsesClosedStateWhenNotMerged(t *testing.T) {
	t.Parallel()

	rendered := runLogDayPRHTML(frontservice.JobHistoryRunRow{
		Run: storecontracts.RunRecord{
			Environment:    "int",
			RunURL:         "https://prow.example.com/view/run-3",
			PRNumber:       789,
			PRState:        "closed",
			MergedPR:       false,
			PostGoodCommit: true,
		},
	})
	if !strings.Contains(rendered, "#789 (closed)") {
		t.Fatalf("expected closed PR label in PR cell, got %q", rendered)
	}
}

func TestDayRunHistoryPRHTMLDoesNotShowSignalIconForPassedRun(t *testing.T) {
	t.Parallel()

	rendered := runLogDayPRHTML(frontservice.JobHistoryRunRow{
		Run: storecontracts.RunRecord{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/run-4",
			PRNumber:    321,
			PRState:     "open",
			Failed:      false,
		},
		SemanticRollups: frontservice.JobHistorySemanticRollups{
			ClusteredRows: 1,
		},
		BadPRScore:   3,
		BadPRReasons: []string{"post-good=0", "only seen in DEV", "only seen in one PR"},
	})
	if strings.Contains(rendered, `class="signal-icon`) {
		t.Fatalf("did not expect signal icon for passed run, got %q", rendered)
	}
}

func TestDayRunHistoryPRHTMLDoesNotShowSignalIconForUnmatchedFailure(t *testing.T) {
	t.Parallel()

	rendered := runLogDayPRHTML(frontservice.JobHistoryRunRow{
		Run: storecontracts.RunRecord{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/run-5",
			PRNumber:    654,
			PRState:     "open",
			Failed:      true,
		},
		SemanticRollups: frontservice.JobHistorySemanticRollups{
			ClusteredRows: 0,
			UnmatchedRows: 1,
		},
		BadPRScore:   3,
		BadPRReasons: []string{"post-good=0", "only seen in DEV", "only seen in one PR"},
	})
	if strings.Contains(rendered, `class="signal-icon`) {
		t.Fatalf("did not expect signal icon for unmatched-only failure, got %q", rendered)
	}
}

func TestDayRunHistoryPRHTMLShowsNewPatternIcon(t *testing.T) {
	t.Parallel()

	rendered := runLogDayPRHTML(frontservice.JobHistoryRunRow{
		Run: storecontracts.RunRecord{
			Environment: "dev",
			RunURL:      "https://prow.example.com/view/run-6",
			PRNumber:    999,
			PRState:     "open",
			Failed:      true,
		},
		SemanticRollups: frontservice.JobHistorySemanticRollups{
			ClusteredRows: 1,
		},
		FailureRows: []frontservice.JobHistoryFailureRow{
			{
				SemanticAttachment: frontservice.JobHistorySemanticAttachment{
					Status:    "clustered",
					ClusterID: "fp-1",
				},
				PriorWeeksPresent: 0,
			},
		},
	})
	if !strings.Contains(rendered, `class="signal-icon signal-new"`) {
		t.Fatalf("expected new-pattern star icon in PR cell, got %q", rendered)
	}
	if !strings.Contains(rendered, "New failure pattern") {
		t.Fatalf("expected New failure pattern tooltip, got %q", rendered)
	}
}
