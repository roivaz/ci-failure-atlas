package readmodel

type RunReference struct {
	RunURL      string
	OccurredAt  string
	SignatureID string
	PRNumber    int
}

type ContributingTest struct {
	FailedAt    string
	JobName     string
	TestName    string
	Occurrences int
}

type FailurePatternRow struct {
	Environment         string
	FailedAt            string
	JobName             string
	TestName            string
	TestSuite           string
	FailurePattern      string
	FailurePatternID    string
	SearchQuery         string
	SearchIndex         string
	Occurrences         int
	TrendSparkline      string
	TrendCounts         []int
	TrendRange          string
	OccurrenceShare     float64
	AfterLastPushCount  int
	AlsoIn              []string
	QualityScore        int
	QualityNoteLabels   []string
	ReviewNoteLabels    []string
	ContributingTests   []ContributingTest
	FullErrorSamples    []string
	AffectedRuns        []RunReference
	ScoringReferences   []RunReference
	PriorWeeksPresent   int
	PriorWeekStarts     []string
	PriorRunsAffected   int
	PriorLastSeenAt     string
	ManualIssueID       string
	ManualIssueConflict bool
	SelectionValue      string
	LinkedPatterns      []FailurePatternRow
}
