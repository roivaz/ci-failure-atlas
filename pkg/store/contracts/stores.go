package contracts

import (
	"context"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

type RunRecord struct {
	Environment    string `json:"environment"`
	RunURL         string `json:"run_url"`
	JobName        string `json:"job_name"`
	PRNumber       int    `json:"pr_number"`
	PRState        string `json:"pr_state"`
	PRSHA          string `json:"pr_sha"`
	FinalMergedSHA string `json:"final_merged_sha"`
	MergedPR       bool   `json:"merged_pr"`
	PostGoodCommit bool   `json:"post_good_commit"`
	Failed         bool   `json:"failed"`
	OccurredAt     string `json:"occurred_at"`
}

type PullRequestRecord struct {
	PRNumber       int    `json:"pr_number"`
	State          string `json:"state"`
	Merged         bool   `json:"merged"`
	HeadSHA        string `json:"head_sha"`
	MergeCommitSHA string `json:"merge_commit_sha"`
	MergedAt       string `json:"merged_at"`
	ClosedAt       string `json:"closed_at"`
	UpdatedAt      string `json:"updated_at"`
	LastCheckedAt  string `json:"last_checked_at"`
}

type ArtifactFailureRecord struct {
	Environment string `json:"environment"`
	// ArtifactRowID is the deterministic failure occurrence identifier.
	// It should be stable for the exact testcase failure row within one run.
	ArtifactRowID string `json:"artifact_row_id"`
	RunURL        string `json:"run_url"`
	TestName      string `json:"test_name"`
	TestSuite     string `json:"test_suite"`
	// SignatureID is the deterministic failure fingerprint:
	// sha256(normalized failure text).
	SignatureID string `json:"signature_id"`
	FailureText string `json:"failure_text"`
}

type RawFailureRecord struct {
	Environment string `json:"environment"`
	RowID       string `json:"row_id"`
	RunURL      string `json:"run_url"`
	// NonArtifactBacked marks synthetic rows generated from failed runs when no
	// artifact-backed JUnit failure rows are available.
	NonArtifactBacked bool   `json:"non_artifact_backed"`
	TestName          string `json:"test_name"`
	TestSuite         string `json:"test_suite"`
	// SignatureID is the deterministic failure fingerprint:
	// sha256(normalized failure text).
	SignatureID    string `json:"signature_id"`
	OccurredAt     string `json:"occurred_at"`
	RawText        string `json:"raw_text"`
	NormalizedText string `json:"normalized_text"`
}

type MetricDailyRecord struct {
	Environment string  `json:"environment"`
	Date        string  `json:"date"`
	Metric      string  `json:"metric"`
	Value       float64 `json:"value"`
}

type TestMetadataDailyRecord struct {
	Environment            string  `json:"environment"`
	Date                   string  `json:"date"`
	Release                string  `json:"release"`
	Period                 string  `json:"period"`
	TestName               string  `json:"test_name"`
	TestSuite              string  `json:"test_suite"`
	CurrentPassPercentage  float64 `json:"current_pass_percentage"`
	CurrentRuns            int     `json:"current_runs"`
	PreviousPassPercentage float64 `json:"previous_pass_percentage"`
	PreviousRuns           int     `json:"previous_runs"`
	NetImprovement         float64 `json:"net_improvement"`
	IngestedAt             string  `json:"ingested_at"`
}

type CheckpointRecord struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	UpdatedAt string `json:"updated_at"`
}

type DeadLetterRecord struct {
	Controller string `json:"controller"`
	Key        string `json:"key"`
	Error      string `json:"error"`
	FailedAt   string `json:"failed_at"`
}

type RunStore interface {
	UpsertRuns(ctx context.Context, runs []RunRecord) error
	ListRunKeys(ctx context.Context) ([]string, error)
	ListRunDates(ctx context.Context) ([]string, error)
	ListRunsByDate(ctx context.Context, environment string, date string) ([]RunRecord, error)
	GetRun(ctx context.Context, environment string, runURL string) (RunRecord, bool, error)
}

type PullRequestStore interface {
	UpsertPullRequests(ctx context.Context, rows []PullRequestRecord) error
	ListPullRequests(ctx context.Context) ([]PullRequestRecord, error)
	GetPullRequest(ctx context.Context, prNumber int) (PullRequestRecord, bool, error)
}

type ArtifactFailureStore interface {
	UpsertArtifactFailures(ctx context.Context, rows []ArtifactFailureRecord) error
	ListArtifactRunKeys(ctx context.Context) ([]string, error)
	ListArtifactFailuresByRun(ctx context.Context, environment string, runURL string) ([]ArtifactFailureRecord, error)
}

type RawFailureStore interface {
	UpsertRawFailures(ctx context.Context, rows []RawFailureRecord) error
	ListRawFailureRunKeys(ctx context.Context) ([]string, error)
	ListRawFailuresByRun(ctx context.Context, environment string, runURL string) ([]RawFailureRecord, error)
	ListRawFailuresByDate(ctx context.Context, environment string, date string) ([]RawFailureRecord, error)
}

type MetricsStore interface {
	UpsertMetricsDaily(ctx context.Context, rows []MetricDailyRecord) error
	ListMetricsDailyByDate(ctx context.Context, environment string, date string) ([]MetricDailyRecord, error)
	ListMetricDates(ctx context.Context) ([]string, error)
}

type TestMetadataDailyStore interface {
	UpsertTestMetadataDaily(ctx context.Context, rows []TestMetadataDailyRecord) error
	ListTestMetadataDailyByDate(ctx context.Context, environment string, date string) ([]TestMetadataDailyRecord, error)
}

type CheckpointStore interface {
	UpsertCheckpoints(ctx context.Context, rows []CheckpointRecord) error
	GetCheckpoint(ctx context.Context, name string) (CheckpointRecord, bool, error)
}

type DeadLetterStore interface {
	AppendDeadLetters(ctx context.Context, rows []DeadLetterRecord) error
	ListDeadLetters(ctx context.Context, limit int) ([]DeadLetterRecord, error)
}

type SemanticStore interface {
	UpsertPhase1Workset(ctx context.Context, rows []semanticcontracts.Phase1WorksetRecord) error
	ListPhase1Workset(ctx context.Context) ([]semanticcontracts.Phase1WorksetRecord, error)

	UpsertPhase1Normalized(ctx context.Context, rows []semanticcontracts.Phase1NormalizedRecord) error
	ListPhase1Normalized(ctx context.Context) ([]semanticcontracts.Phase1NormalizedRecord, error)

	UpsertPhase1Assignments(ctx context.Context, rows []semanticcontracts.Phase1AssignmentRecord) error
	ListPhase1Assignments(ctx context.Context) ([]semanticcontracts.Phase1AssignmentRecord, error)

	UpsertTestClusters(ctx context.Context, rows []semanticcontracts.TestClusterRecord) error
	ListTestClusters(ctx context.Context) ([]semanticcontracts.TestClusterRecord, error)

	UpsertGlobalClusters(ctx context.Context, rows []semanticcontracts.GlobalClusterRecord) error
	ListGlobalClusters(ctx context.Context) ([]semanticcontracts.GlobalClusterRecord, error)

	UpsertPhase3GlobalClusters(ctx context.Context, rows []semanticcontracts.GlobalClusterRecord) error
	ListPhase3GlobalClusters(ctx context.Context) ([]semanticcontracts.GlobalClusterRecord, error)

	UpsertReviewQueue(ctx context.Context, rows []semanticcontracts.ReviewItemRecord) error
	ListReviewQueue(ctx context.Context) ([]semanticcontracts.ReviewItemRecord, error)

	UpsertPhase3Issues(ctx context.Context, rows []semanticcontracts.Phase3IssueRecord) error
	ListPhase3Issues(ctx context.Context) ([]semanticcontracts.Phase3IssueRecord, error)

	UpsertPhase3Links(ctx context.Context, rows []semanticcontracts.Phase3LinkRecord) error
	DeletePhase3Links(ctx context.Context, rows []semanticcontracts.Phase3LinkRecord) error
	ListPhase3Links(ctx context.Context) ([]semanticcontracts.Phase3LinkRecord, error)

	AppendPhase3Events(ctx context.Context, rows []semanticcontracts.Phase3EventRecord) error
	ListPhase3Events(ctx context.Context, limit int) ([]semanticcontracts.Phase3EventRecord, error)
}

type Store interface {
	RunStore
	PullRequestStore
	ArtifactFailureStore
	RawFailureStore
	MetricsStore
	TestMetadataDailyStore
	CheckpointStore
	DeadLetterStore
	SemanticStore

	Close() error
}
