package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/source/prowartifacts"
	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const sourceProwFailuresReconcileInterval = 2 * time.Minute

type sourceProwFailuresController struct {
	logger            logr.Logger
	reconcileInterval time.Duration
	queue             workqueue.TypedRateLimitingInterface[string]
	activeWindow      time.Duration

	store      contracts.Store
	prowClient prowartifacts.Client
}

var _ Controller = (*sourceProwFailuresController)(nil)

func NewSourceProwFailures(logger logr.Logger, deps Dependencies) (Controller, error) {
	return newSourceProwFailuresController(logger, deps, nil)
}

func newSourceProwFailuresController(logger logr.Logger, deps Dependencies, client prowartifacts.Client) (*sourceProwFailuresController, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("source.prow.failures: store dependency is required")
	}
	if deps.Source == nil {
		return nil, fmt.Errorf("source.prow.failures: source options dependency is required")
	}
	if strings.TrimSpace(deps.Source.ProwArtifactsBaseURL) == "" {
		return nil, fmt.Errorf("source.prow.failures: prow artifacts base URL is required")
	}
	if client == nil {
		client = prowartifacts.NewHTTPClient(deps.Source.ProwArtifactsBaseURL)
	}

	return &sourceProwFailuresController{
		logger: logger.WithValues("controller", SourceProwFailuresControllerName),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: SourceProwFailuresControllerName,
			},
		),
		reconcileInterval: sourceProwFailuresReconcileInterval,
		activeWindow:      activeReconcileWindow(deps.Source),
		store:             deps.Store,
		prowClient:        client,
	}, nil
}

func (c *sourceProwFailuresController) Run(ctx context.Context, threadiness int) {
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

func (c *sourceProwFailuresController) RunOnce(ctx context.Context, key string) error {
	c.logger.Info("Reconciling one key.", "key", key)
	return c.processKey(ctx, key)
}

func (c *sourceProwFailuresController) SyncOnce(ctx context.Context) error {
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

func (c *sourceProwFailuresController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *sourceProwFailuresController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.processKey(ctx, key); err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, fmt.Errorf("failed processing key %q", key), "Error syncing; requeuing for later retry", "controller", SourceProwFailuresControllerName, "key", key)
	c.queue.AddRateLimited(key)
	return true
}

func (c *sourceProwFailuresController) queueMetadata(ctx context.Context) {
	keys, err := c.listKeys(ctx)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed listing keys for periodic enqueue", "controller", SourceProwFailuresControllerName)
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.queue.Add(key)
	}
}

func (c *sourceProwFailuresController) listKeys(ctx context.Context) ([]string, error) {
	keys, err := c.store.ListRunKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("list run keys: %w", err)
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
		filtered = append(filtered, key)
	}
	return filtered, nil
}

func (c *sourceProwFailuresController) processKey(ctx context.Context, key string) error {
	environment, runURL, err := splitEnvironmentRunKey(key)
	if err != nil {
		return err
	}

	existingRows, err := c.store.ListArtifactFailuresByRun(ctx, environment, runURL)
	if err != nil {
		return fmt.Errorf("list existing artifact failures for run %q: %w", runURL, err)
	}
	if len(existingRows) > 0 {
		c.logger.V(1).Info("Skipping run; artifact failures already materialized.", "key", key, "existing_rows", len(existingRows))
		return nil
	}

	failures, err := c.prowClient.ListFailures(ctx, environment, runURL)
	if err != nil {
		return fmt.Errorf("list failures for run %q: %w", runURL, err)
	}

	records := buildArtifactFailureRecords(environment, runURL, failures)
	if len(records) == 0 {
		c.logger.V(1).Info("No junit failures extracted for run.", "key", key)
		return nil
	}

	if err := c.store.UpsertArtifactFailures(ctx, records); err != nil {
		return fmt.Errorf("upsert %d artifact failure records for key %q: %w", len(records), key, err)
	}

	c.logger.Info("Synced prow failures for run.", "key", key, "rows", len(records))
	return nil
}

func splitEnvironmentRunKey(key string) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(key), "|", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid key %q: expected environment|run_url", key)
	}
	environment := normalizeEnvironment(parts[0])
	runURL := strings.TrimSpace(parts[1])
	if environment == "" {
		return "", "", fmt.Errorf("invalid key %q: missing environment", key)
	}
	if runURL == "" {
		return "", "", fmt.Errorf("invalid key %q: missing run_url", key)
	}
	return environment, runURL, nil
}

func buildArtifactFailureRecords(environment, runURL string, failures []prowartifacts.Failure) []contracts.ArtifactFailureRecord {
	normalizedEnvironment := normalizeEnvironment(environment)
	normalizedRunURL := strings.TrimSpace(runURL)
	if normalizedEnvironment == "" || normalizedRunURL == "" || len(failures) == 0 {
		return []contracts.ArtifactFailureRecord{}
	}

	sortedFailures := make([]prowartifacts.Failure, 0, len(failures))
	for _, failure := range failures {
		if strings.TrimSpace(failure.FailureText) == "" {
			continue
		}
		sortedFailures = append(sortedFailures, failure)
	}
	sort.Slice(sortedFailures, func(i, j int) bool {
		if strings.TrimSpace(sortedFailures[i].ArtifactURL) != strings.TrimSpace(sortedFailures[j].ArtifactURL) {
			return strings.TrimSpace(sortedFailures[i].ArtifactURL) < strings.TrimSpace(sortedFailures[j].ArtifactURL)
		}
		if strings.TrimSpace(sortedFailures[i].TestSuite) != strings.TrimSpace(sortedFailures[j].TestSuite) {
			return strings.TrimSpace(sortedFailures[i].TestSuite) < strings.TrimSpace(sortedFailures[j].TestSuite)
		}
		if strings.TrimSpace(sortedFailures[i].TestName) != strings.TrimSpace(sortedFailures[j].TestName) {
			return strings.TrimSpace(sortedFailures[i].TestName) < strings.TrimSpace(sortedFailures[j].TestName)
		}
		return strings.TrimSpace(sortedFailures[i].FailureText) < strings.TrimSpace(sortedFailures[j].FailureText)
	})

	rows := make([]contracts.ArtifactFailureRecord, 0, len(sortedFailures))
	baseKeyCounter := map[string]int{}

	for _, failure := range sortedFailures {
		failureText := strings.TrimSpace(failure.FailureText)
		signatureID := signatureIDForFailureText(failureText)
		if signatureID == "" {
			continue
		}

		testName := strings.TrimSpace(failure.TestName)
		testSuite := strings.TrimSpace(failure.TestSuite)
		artifactURL := strings.TrimSpace(failure.ArtifactURL)

		baseKey := strings.Join([]string{
			normalizedEnvironment,
			normalizedRunURL,
			artifactURL,
			testSuite,
			testName,
			signatureID,
		}, "|")
		occurrenceIndex := baseKeyCounter[baseKey]
		baseKeyCounter[baseKey] = occurrenceIndex + 1

		artifactRowID := sha256Hex(fmt.Sprintf("%s|%d", baseKey, occurrenceIndex))

		rows = append(rows, contracts.ArtifactFailureRecord{
			Environment:   normalizedEnvironment,
			ArtifactRowID: artifactRowID,
			RunURL:        normalizedRunURL,
			TestName:      testName,
			TestSuite:     testSuite,
			SignatureID:   signatureID,
			FailureText:   failureText,
		})
	}

	return rows
}

func signatureIDForFailureText(failureText string) string {
	normalized := normalizeFailureTextForSignature(failureText)
	if normalized == "" {
		return ""
	}
	return sha256Hex(normalized)
}

func normalizeFailureTextForSignature(failureText string) string {
	normalized := strings.ToLower(strings.TrimSpace(failureText))
	if normalized == "" {
		return ""
	}
	return strings.Join(strings.Fields(normalized), " ")
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
