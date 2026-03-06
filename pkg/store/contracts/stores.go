package contracts

import "context"

type RunRecord struct {
	Environment    string `json:"environment"`
	RunURL         string `json:"run_url"`
	JobName        string `json:"job_name"`
	PRNumber       int    `json:"pr_number"`
	PRSHA          string `json:"pr_sha"`
	FinalMergedSHA string `json:"final_merged_sha"`
	MergedPR       bool   `json:"merged_pr"`
	PostGoodCommit bool   `json:"post_good_commit"`
	OccurredAt     string `json:"occurred_at"`
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
	TestName    string `json:"test_name"`
	TestSuite   string `json:"test_suite"`
	MergedPR    bool   `json:"merged_pr"`
	// PostGoodCommitFailures is a row-level contribution to the aggregate
	// post-good-commit failure count. It is either 0 or 1 in v1.
	PostGoodCommitFailures int `json:"post_good_commit_failures"`
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

type RunCountHourlyRecord struct {
	Environment    string `json:"environment"`
	Hour           string `json:"hour"`
	TotalRuns      int    `json:"total_runs"`
	FailedRuns     int    `json:"failed_runs"`
	SuccessfulRuns int    `json:"successful_runs"`
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
	GetRun(ctx context.Context, environment string, runURL string) (RunRecord, bool, error)
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

type RunCountHourlyStore interface {
	UpsertRunCountsHourly(ctx context.Context, rows []RunCountHourlyRecord) error
	ListRunCountHourlyHours(ctx context.Context) ([]string, error)
	ListRunCountsHourlyByDate(ctx context.Context, environment string, date string) ([]RunCountHourlyRecord, error)
}

type CheckpointStore interface {
	UpsertCheckpoints(ctx context.Context, rows []CheckpointRecord) error
	GetCheckpoint(ctx context.Context, name string) (CheckpointRecord, bool, error)
}

type DeadLetterStore interface {
	AppendDeadLetters(ctx context.Context, rows []DeadLetterRecord) error
	ListDeadLetters(ctx context.Context, limit int) ([]DeadLetterRecord, error)
}

type Store interface {
	RunStore
	ArtifactFailureStore
	RawFailureStore
	MetricsStore
	RunCountHourlyStore
	CheckpointStore
	DeadLetterStore

	Close() error
}
