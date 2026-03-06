package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	factsnormalize "ci-failure-atlas/pkg/facts/normalize"
	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const factsRawFailuresReconcileInterval = 2 * time.Minute

type factsRawFailuresController struct {
	logger                  logr.Logger
	reconcileInterval       time.Duration
	queue                   workqueue.TypedRateLimitingInterface[string]
	activeWindow            time.Duration
	unresolvedPRRetryWindow time.Duration

	store contracts.Store
}

var _ Controller = (*factsRawFailuresController)(nil)

func NewFactsRawFailures(logger logr.Logger, deps Dependencies) (Controller, error) {
	return newFactsRawFailuresController(logger, deps)
}

func newFactsRawFailuresController(logger logr.Logger, deps Dependencies) (*factsRawFailuresController, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("facts.raw-failures: store dependency is required")
	}

	return &factsRawFailuresController{
		logger: logger.WithValues("controller", FactsRawFailuresControllerName),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: FactsRawFailuresControllerName,
			},
		),
		reconcileInterval:       factsRawFailuresReconcileInterval,
		activeWindow:            activeReconcileWindow(deps.Source),
		unresolvedPRRetryWindow: unresolvedPRRetryWindow(deps.Source),
		store:                   deps.Store,
	}, nil
}

func (c *factsRawFailuresController) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	if threadiness <= 0 {
		threadiness = 1
	}

	c.logger.Info("Starting controller.", "threads", threadiness)
	for i := 0; i < threadiness; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	go wait.JitterUntilWithContext(ctx, c.queueMetadata, c.reconcileInterval, 0.1, true)
	c.logger.Info("Started workers.")
	<-ctx.Done()
	c.logger.Info("Shutting down controller.")
}

func (c *factsRawFailuresController) RunOnce(ctx context.Context, key string) error {
	c.logger.Info("Reconciling one key.", "key", key)
	return c.processKey(ctx, key)
}

func (c *factsRawFailuresController) SyncOnce(ctx context.Context) error {
	keys, err := c.listKeys(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := c.processKey(ctx, key); err != nil {
			return fmt.Errorf("failed processing key %q: %w", key, err)
		}
	}
	c.logger.Info("Completed one full sync.", "keys", len(keys))
	return nil
}

func (c *factsRawFailuresController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *factsRawFailuresController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.processKey(ctx, key); err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, fmt.Errorf("failed processing key %q", key), "Error syncing; requeuing for later retry", "controller", FactsRawFailuresControllerName, "key", key)
	c.queue.AddRateLimited(key)
	return true
}

func (c *factsRawFailuresController) queueMetadata(ctx context.Context) {
	keys, err := c.listKeys(ctx)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed listing keys for periodic enqueue", "controller", FactsRawFailuresControllerName)
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.queue.Add(key)
	}
}

func (c *factsRawFailuresController) listKeys(ctx context.Context) ([]string, error) {
	keys, err := c.store.ListArtifactRunKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("list artifact run keys: %w", err)
	}

	now := time.Now().UTC()
	filtered := make([]string, 0, len(keys))
	for _, key := range keys {
		environment, runURL, err := splitEnvironmentRunKey(key)
		if err != nil {
			continue
		}
		run, found, err := c.store.GetRun(ctx, environment, runURL)
		if err != nil {
			return nil, fmt.Errorf("get run metadata for key %q: %w", key, err)
		}
		if !found {
			continue
		}
		if !isRunWithinActiveWindow(run, c.activeWindow, now) {
			continue
		}
		if run.PRNumber > 0 && !run.MergedPR && !isRunWithinUnresolvedRetryWindow(run, c.unresolvedPRRetryWindow, now) {
			continue
		}
		filtered = append(filtered, key)
	}
	return filtered, nil
}

func (c *factsRawFailuresController) processKey(ctx context.Context, key string) error {
	environment, runURL, err := splitEnvironmentRunKey(key)
	if err != nil {
		return err
	}

	runRecord, runMetadataFound, err := c.store.GetRun(ctx, environment, runURL)
	if err != nil {
		return fmt.Errorf("get run metadata for key %q: %w", key, err)
	}
	occurredAt := ""
	mergedPR := false
	postGoodCommit := false
	if runMetadataFound {
		occurredAt = strings.TrimSpace(runRecord.OccurredAt)
		mergedPR = runRecord.MergedPR
		postGoodCommit = runRecord.PostGoodCommit
	}

	existingRawRows, err := c.store.ListRawFailuresByRun(ctx, environment, runURL)
	if err != nil {
		return fmt.Errorf("list existing raw failures for key %q: %w", key, err)
	}
	if len(existingRawRows) > 0 {
		postGoodContribution := 0
		if postGoodCommit {
			postGoodContribution = 1
		}
		if !rawFailuresNeedRefresh(existingRawRows, mergedPR, postGoodContribution) {
			c.logger.V(1).Info("Skipping run; raw failures already materialized.", "key", key, "existing_rows", len(existingRawRows))
			return nil
		}
		c.logger.V(1).Info("Refreshing raw failures for run due PR-merge signal update.", "key", key, "existing_rows", len(existingRawRows), "merged_pr", mergedPR, "post_good_commit_failures", postGoodContribution)
	}

	artifactRows, err := c.store.ListArtifactFailuresByRun(ctx, environment, runURL)
	if err != nil {
		return fmt.Errorf("list artifact failures for key %q: %w", key, err)
	}
	if len(artifactRows) == 0 {
		c.logger.V(1).Info("No artifact failures to materialize.", "key", key)
		return nil
	}

	rawRows := buildRawFailureRecords(environment, runURL, occurredAt, mergedPR, postGoodCommit, artifactRows)
	if len(rawRows) == 0 {
		c.logger.V(1).Info("No raw failures produced from artifact rows.", "key", key)
		return nil
	}

	if err := c.store.UpsertRawFailures(ctx, rawRows); err != nil {
		return fmt.Errorf("upsert %d raw failure records for key %q: %w", len(rawRows), key, err)
	}

	c.logger.Info(
		"Materialized raw failures for run.",
		"key", key,
		"rows", len(rawRows),
		"run_metadata_found", runMetadataFound,
		"merged_pr", mergedPR,
		"post_good_commit_failures", rawRows[0].PostGoodCommitFailures,
	)
	return nil
}

func rawFailuresNeedRefresh(existingRows []contracts.RawFailureRecord, mergedPR bool, postGoodCommitFailures int) bool {
	for _, row := range existingRows {
		if row.MergedPR != mergedPR || row.PostGoodCommitFailures != postGoodCommitFailures {
			return true
		}
	}
	return false
}

func buildRawFailureRecords(environment, runURL, occurredAt string, mergedPR bool, postGoodCommit bool, artifactRows []contracts.ArtifactFailureRecord) []contracts.RawFailureRecord {
	normalizedEnvironment := normalizeEnvironment(environment)
	normalizedRunURL := strings.TrimSpace(runURL)
	normalizedOccurredAt := strings.TrimSpace(occurredAt)
	if normalizedEnvironment == "" || normalizedRunURL == "" || len(artifactRows) == 0 {
		return []contracts.RawFailureRecord{}
	}

	postGoodCommitFailures := 0
	if postGoodCommit {
		postGoodCommitFailures = 1
	}

	ordered := make([]contracts.ArtifactFailureRecord, 0, len(artifactRows))
	for _, row := range artifactRows {
		normalized := contracts.ArtifactFailureRecord{
			Environment:   normalizeEnvironment(row.Environment),
			ArtifactRowID: strings.TrimSpace(row.ArtifactRowID),
			RunURL:        strings.TrimSpace(row.RunURL),
			TestName:      strings.TrimSpace(row.TestName),
			TestSuite:     strings.TrimSpace(row.TestSuite),
			SignatureID:   strings.TrimSpace(row.SignatureID),
			FailureText:   strings.TrimSpace(row.FailureText),
		}
		if normalized.Environment != normalizedEnvironment || normalized.RunURL != normalizedRunURL {
			continue
		}
		if normalized.FailureText == "" {
			continue
		}
		ordered = append(ordered, normalized)
	}

	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].TestSuite != ordered[j].TestSuite {
			return ordered[i].TestSuite < ordered[j].TestSuite
		}
		if ordered[i].TestName != ordered[j].TestName {
			return ordered[i].TestName < ordered[j].TestName
		}
		if ordered[i].ArtifactRowID != ordered[j].ArtifactRowID {
			return ordered[i].ArtifactRowID < ordered[j].ArtifactRowID
		}
		if ordered[i].SignatureID != ordered[j].SignatureID {
			return ordered[i].SignatureID < ordered[j].SignatureID
		}
		return ordered[i].FailureText < ordered[j].FailureText
	})

	out := make([]contracts.RawFailureRecord, 0, len(ordered))
	for _, row := range ordered {
		normalizedText := factsnormalize.Text(row.FailureText)
		if normalizedText == "" {
			continue
		}

		rowID := strings.TrimSpace(row.ArtifactRowID)
		if rowID == "" {
			rowID = sha256Hex(strings.Join([]string{
				normalizedEnvironment,
				normalizedRunURL,
				row.TestSuite,
				row.TestName,
				normalizedText,
			}, "|"))
		}

		out = append(out, contracts.RawFailureRecord{
			Environment:            normalizedEnvironment,
			RowID:                  rowID,
			RunURL:                 normalizedRunURL,
			TestName:               row.TestName,
			TestSuite:              row.TestSuite,
			MergedPR:               mergedPR,
			PostGoodCommitFailures: postGoodCommitFailures,
			SignatureID:            sha256Hex(normalizedText),
			OccurredAt:             normalizedOccurredAt,
			RawText:                row.FailureText,
			NormalizedText:         normalizedText,
		})
	}

	return out
}
