package postgres

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	SemanticSubdirectory string
}

type Store struct {
	pool                 *pgxpool.Pool
	semanticSubdirectory string
}

var _ storecontracts.Store = (*Store)(nil)

func New(pool *pgxpool.Pool, opts Options) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres pool is required")
	}
	semanticSubdirectory, err := normalizeSemanticSubdirectory(opts.SemanticSubdirectory)
	if err != nil {
		return nil, fmt.Errorf("invalid semantic subdirectory: %w", err)
	}
	return &Store{
		pool:                 pool,
		semanticSubdirectory: semanticSubdirectory,
	}, nil
}

func (s *Store) Close() error {
	// The pool lifecycle is owned by postgres options/setup callers.
	// A Store is a lightweight scoped view (semantic namespace) over that
	// shared pool, so closing a store must not close the shared pool.
	return nil
}

func (s *Store) UpsertRuns(ctx context.Context, rows []storecontracts.RunRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertRunsImpl(ctx, rows)
}

func (s *Store) ListRuns(ctx context.Context) ([]storecontracts.RunRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listRunsImpl(ctx)
}

func (s *Store) ListRunKeys(ctx context.Context) ([]string, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listRunKeysImpl(ctx)
}

func (s *Store) ListRunDates(ctx context.Context) ([]string, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listRunDatesImpl(ctx)
}

func (s *Store) ListRunsByDate(ctx context.Context, environment string, date string) ([]storecontracts.RunRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listRunsByDateImpl(ctx, environment, date)
}

func (s *Store) GetRun(ctx context.Context, environment string, runURL string) (storecontracts.RunRecord, bool, error) {
	if err := requireContext(ctx); err != nil {
		return storecontracts.RunRecord{}, false, err
	}
	return s.getRunImpl(ctx, environment, runURL)
}

func (s *Store) UpsertPullRequests(ctx context.Context, rows []storecontracts.PullRequestRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertPullRequestsImpl(ctx, rows)
}

func (s *Store) ListPullRequests(ctx context.Context) ([]storecontracts.PullRequestRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listPullRequestsImpl(ctx)
}

func (s *Store) GetPullRequest(ctx context.Context, prNumber int) (storecontracts.PullRequestRecord, bool, error) {
	if err := requireContext(ctx); err != nil {
		return storecontracts.PullRequestRecord{}, false, err
	}
	return s.getPullRequestImpl(ctx, prNumber)
}

func (s *Store) UpsertArtifactFailures(ctx context.Context, rows []storecontracts.ArtifactFailureRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertArtifactFailuresImpl(ctx, rows)
}

func (s *Store) ListArtifactRunKeys(ctx context.Context) ([]string, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listArtifactRunKeysImpl(ctx)
}

func (s *Store) ListArtifactFailuresByRun(ctx context.Context, environment string, runURL string) ([]storecontracts.ArtifactFailureRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listArtifactFailuresByRunImpl(ctx, environment, runURL)
}

func (s *Store) UpsertRawFailures(ctx context.Context, rows []storecontracts.RawFailureRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertRawFailuresImpl(ctx, rows)
}

func (s *Store) ListRawFailures(ctx context.Context) ([]storecontracts.RawFailureRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listRawFailuresImpl(ctx)
}

func (s *Store) ListRawFailureRunKeys(ctx context.Context) ([]string, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listRawFailureRunKeysImpl(ctx)
}

func (s *Store) ListRawFailuresByRun(ctx context.Context, environment string, runURL string) ([]storecontracts.RawFailureRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listRawFailuresByRunImpl(ctx, environment, runURL)
}

func (s *Store) ListRawFailuresByDate(ctx context.Context, environment string, date string) ([]storecontracts.RawFailureRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listRawFailuresByDateImpl(ctx, environment, date)
}

func (s *Store) UpsertMetricsDaily(ctx context.Context, rows []storecontracts.MetricDailyRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertMetricsDailyImpl(ctx, rows)
}

func (s *Store) ListMetricsDaily(ctx context.Context) ([]storecontracts.MetricDailyRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listMetricsDailyImpl(ctx)
}

func (s *Store) ListMetricsDailyByDate(ctx context.Context, environment string, date string) ([]storecontracts.MetricDailyRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listMetricsDailyByDateImpl(ctx, environment, date)
}

func (s *Store) ListMetricDates(ctx context.Context) ([]string, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listMetricDatesImpl(ctx)
}

func (s *Store) UpsertTestMetadataDaily(ctx context.Context, rows []storecontracts.TestMetadataDailyRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertTestMetadataDailyImpl(ctx, rows)
}

func (s *Store) ListTestMetadataDailyByDate(ctx context.Context, environment string, date string) ([]storecontracts.TestMetadataDailyRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listTestMetadataDailyByDateImpl(ctx, environment, date)
}

func (s *Store) UpsertCheckpoints(ctx context.Context, rows []storecontracts.CheckpointRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertCheckpointsImpl(ctx, rows)
}

func (s *Store) GetCheckpoint(ctx context.Context, name string) (storecontracts.CheckpointRecord, bool, error) {
	if err := requireContext(ctx); err != nil {
		return storecontracts.CheckpointRecord{}, false, err
	}
	return s.getCheckpointImpl(ctx, name)
}

func (s *Store) AppendDeadLetters(ctx context.Context, rows []storecontracts.DeadLetterRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.appendDeadLettersImpl(ctx, rows)
}

func (s *Store) ListDeadLetters(ctx context.Context, limit int) ([]storecontracts.DeadLetterRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listDeadLettersImpl(ctx, limit)
}

func (s *Store) UpsertPhase1Workset(ctx context.Context, rows []semanticcontracts.Phase1WorksetRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertPhase1WorksetImpl(ctx, rows)
}

func (s *Store) ListPhase1Workset(ctx context.Context) ([]semanticcontracts.Phase1WorksetRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listPhase1WorksetImpl(ctx)
}

func (s *Store) UpsertPhase1Normalized(ctx context.Context, _ []semanticcontracts.Phase1NormalizedRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.unimplemented("UpsertPhase1Normalized")
}

func (s *Store) ListPhase1Normalized(ctx context.Context) ([]semanticcontracts.Phase1NormalizedRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return nil, s.unimplemented("ListPhase1Normalized")
}

func (s *Store) UpsertPhase1Assignments(ctx context.Context, _ []semanticcontracts.Phase1AssignmentRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.unimplemented("UpsertPhase1Assignments")
}

func (s *Store) ListPhase1Assignments(ctx context.Context) ([]semanticcontracts.Phase1AssignmentRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return nil, s.unimplemented("ListPhase1Assignments")
}

func (s *Store) UpsertTestClusters(ctx context.Context, rows []semanticcontracts.TestClusterRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertTestClustersImpl(ctx, rows)
}

func (s *Store) ListTestClusters(ctx context.Context) ([]semanticcontracts.TestClusterRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listTestClustersImpl(ctx)
}

func (s *Store) UpsertGlobalClusters(ctx context.Context, rows []semanticcontracts.GlobalClusterRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertGlobalClustersImpl(ctx, rows)
}

func (s *Store) ListGlobalClusters(ctx context.Context) ([]semanticcontracts.GlobalClusterRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listGlobalClustersImpl(ctx)
}

func (s *Store) UpsertReviewQueue(ctx context.Context, rows []semanticcontracts.ReviewItemRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertReviewQueueImpl(ctx, rows)
}

func (s *Store) ListReviewQueue(ctx context.Context) ([]semanticcontracts.ReviewItemRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listReviewQueueImpl(ctx)
}

func (s *Store) UpsertPhase3Issues(ctx context.Context, rows []semanticcontracts.Phase3IssueRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertPhase3IssuesImpl(ctx, rows)
}

func (s *Store) ListPhase3Issues(ctx context.Context) ([]semanticcontracts.Phase3IssueRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listPhase3IssuesImpl(ctx)
}

func (s *Store) UpsertPhase3Links(ctx context.Context, rows []semanticcontracts.Phase3LinkRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.upsertPhase3LinksImpl(ctx, rows)
}

func (s *Store) DeletePhase3Links(ctx context.Context, rows []semanticcontracts.Phase3LinkRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.deletePhase3LinksImpl(ctx, rows)
}

func (s *Store) ListPhase3Links(ctx context.Context) ([]semanticcontracts.Phase3LinkRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listPhase3LinksImpl(ctx)
}

func (s *Store) AppendPhase3Events(ctx context.Context, rows []semanticcontracts.Phase3EventRecord) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return s.appendPhase3EventsImpl(ctx, rows)
}

func (s *Store) ListPhase3Events(ctx context.Context, limit int) ([]semanticcontracts.Phase3EventRecord, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	return s.listPhase3EventsImpl(ctx, limit)
}

func (s *Store) unimplemented(method string) error {
	return fmt.Errorf("%w: %s", ErrNotImplemented, method)
}

func requireContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	return ctx.Err()
}

func normalizeSemanticSubdirectory(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", nil
	}
	cleaned := filepath.Clean(trimmed)
	if cleaned == "." {
		return "", nil
	}
	if filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("must be a relative path")
	}
	for _, part := range strings.Split(cleaned, string(filepath.Separator)) {
		switch part {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("must not contain '..'")
		}
	}
	return cleaned, nil
}
