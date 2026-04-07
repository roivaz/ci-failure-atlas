package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	sourcenormalize "ci-failure-atlas/pkg/source/normalize"
	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const factsRawFailuresReconcileInterval = 2 * time.Minute

const (
	rawFailureUnknownPlaceholder = "unknown"
	rawFailureSyntheticText      = "non-artifact-backed failure (no junit artifacts)"
)

type factsRawFailuresController struct {
	logger            logr.Logger
	reconcileInterval time.Duration
	queue             workqueue.TypedRateLimitingInterface[string]
	activeWindow      time.Duration
	envSet            map[string]struct{}

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
	if deps.Source == nil {
		return nil, fmt.Errorf("facts.raw-failures: source options dependency is required")
	}
	if len(deps.Source.Environments) == 0 {
		return nil, fmt.Errorf("facts.raw-failures: no source environments configured")
	}

	envSet := make(map[string]struct{}, len(deps.Source.Environments))
	for _, env := range deps.Source.Environments {
		normalized := normalizeEnvironment(env)
		if normalized == "" {
			continue
		}
		envSet[normalized] = struct{}{}
	}
	if len(envSet) == 0 {
		return nil, fmt.Errorf("facts.raw-failures: no valid source environments configured")
	}

	return &factsRawFailuresController{
		logger: logger.WithValues("controller", FactsRawFailuresControllerName),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: FactsRawFailuresControllerName,
			},
		),
		reconcileInterval: factsRawFailuresReconcileInterval,
		activeWindow:      activeReconcileWindow(deps.Source),
		envSet:            envSet,
		store:             deps.Store,
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
	runKeys, err := c.store.ListRunKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("list run keys: %w", err)
	}
	artifactKeys, err := c.store.ListArtifactRunKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("list artifact run keys: %w", err)
	}

	keysSet := make(map[string]struct{}, len(runKeys)+len(artifactKeys))
	for _, key := range runKeys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		keysSet[trimmed] = struct{}{}
	}
	for _, key := range artifactKeys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		keysSet[trimmed] = struct{}{}
	}
	keys := make([]string, 0, len(keysSet))
	for key := range keysSet {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	now := time.Now().UTC()
	filtered := make([]string, 0, len(keys))
	for _, key := range keys {
		environment, runURL, err := splitEnvironmentRunKey(key)
		if err != nil {
			continue
		}
		if !c.isEnvironmentEnabled(environment) {
			continue
		}
		run, found, err := c.store.GetRun(ctx, environment, runURL)
		if err != nil {
			return nil, fmt.Errorf("get run metadata for key %q: %w", key, err)
		}
		if !found {
			// Keep artifact-only keys even if run metadata is missing; these can
			// happen during replay/migration scenarios.
			filtered = append(filtered, key)
			continue
		}
		if !isRunWithinActiveWindow(run, c.activeWindow, now) {
			continue
		}
		if !run.Failed {
			continue
		}
		filtered = append(filtered, key)
	}
	return filtered, nil
}

func (c *factsRawFailuresController) isEnvironmentEnabled(environment string) bool {
	normalized := normalizeEnvironment(environment)
	if normalized == "" {
		return false
	}
	_, enabled := c.envSet[normalized]
	return enabled
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
	if runMetadataFound {
		if !runRecord.Failed {
			c.logger.V(1).Info("Skipping run; run is not failed.", "key", key, "run_url", runURL)
			return nil
		}
		occurredAt = strings.TrimSpace(runRecord.OccurredAt)
	}

	artifactRows, err := c.store.ListArtifactFailuresByRun(ctx, environment, runURL)
	if err != nil {
		return fmt.Errorf("list artifact failures for key %q: %w", key, err)
	}
	expectedRows := expectedRawFailureRows(environment, runURL, occurredAt, artifactRows, runMetadataFound)
	if len(expectedRows) == 0 {
		c.logger.V(1).Info("No expected raw failures produced for run.", "key", key)
		return nil
	}

	existingRows, err := c.store.ListRawFailuresByRun(ctx, environment, runURL)
	if err != nil {
		return fmt.Errorf("list existing raw failures for key %q: %w", key, err)
	}
	if rawFailureRowsMatch(existingRows, expectedRows) {
		c.logger.V(1).Info(
			"Skipping run; raw failures already match expected state.",
			"key", key,
			"rows", len(expectedRows),
			"run_metadata_found", runMetadataFound,
			"artifact_rows", len(artifactRows),
		)
		return nil
	}

	if err := c.store.UpsertRawFailures(ctx, expectedRows); err != nil {
		return fmt.Errorf("upsert %d raw failure records for key %q: %w", len(expectedRows), key, err)
	}

	c.logger.Info(
		"Materialized raw failures for run from expected state.",
		"key", key,
		"rows", len(expectedRows),
		"run_metadata_found", runMetadataFound,
		"artifact_rows", len(artifactRows),
	)
	return nil
}

func expectedRawFailureRows(environment, runURL, occurredAt string, artifactRows []contracts.ArtifactFailureRecord, runMetadataFound bool) []contracts.RawFailureRecord {
	if len(artifactRows) > 0 {
		rows := buildRawFailureRecords(environment, runURL, occurredAt, artifactRows)
		if len(rows) > 0 {
			return rows
		}
	}
	if runMetadataFound {
		return []contracts.RawFailureRecord{
			buildSyntheticRawFailureRecord(environment, runURL, occurredAt),
		}
	}
	return []contracts.RawFailureRecord{}
}

func rawFailureRowsMatch(existingRows []contracts.RawFailureRecord, expectedRows []contracts.RawFailureRecord) bool {
	if len(existingRows) != len(expectedRows) {
		return false
	}
	if len(expectedRows) == 0 {
		return true
	}

	existingByKey := map[string]contracts.RawFailureRecord{}
	for _, row := range existingRows {
		normalized := normalizeRawFailureForComparison(row)
		if normalized.Environment == "" || normalized.RunURL == "" || normalized.RowID == "" {
			return false
		}
		key := normalized.Environment + "|" + normalized.RunURL + "|" + normalized.RowID
		if _, exists := existingByKey[key]; exists {
			return false
		}
		existingByKey[key] = normalized
	}

	for _, row := range expectedRows {
		normalized := normalizeRawFailureForComparison(row)
		if normalized.Environment == "" || normalized.RunURL == "" || normalized.RowID == "" {
			return false
		}
		key := normalized.Environment + "|" + normalized.RunURL + "|" + normalized.RowID
		existing, found := existingByKey[key]
		if !found {
			return false
		}
		if existing != normalized {
			return false
		}
	}
	return true
}

func normalizeRawFailureForComparison(row contracts.RawFailureRecord) contracts.RawFailureRecord {
	return contracts.RawFailureRecord{
		Environment:       normalizeEnvironment(row.Environment),
		RowID:             strings.TrimSpace(row.RowID),
		RunURL:            strings.TrimSpace(row.RunURL),
		NonArtifactBacked: row.NonArtifactBacked,
		TestName:          strings.TrimSpace(row.TestName),
		TestSuite:         strings.TrimSpace(row.TestSuite),
		SignatureID:       strings.TrimSpace(row.SignatureID),
		OccurredAt:        strings.TrimSpace(row.OccurredAt),
		RawText:           strings.TrimSpace(row.RawText),
		NormalizedText:    strings.TrimSpace(row.NormalizedText),
	}
}

func buildRawFailureRecords(environment, runURL, occurredAt string, artifactRows []contracts.ArtifactFailureRecord) []contracts.RawFailureRecord {
	normalizedEnvironment := normalizeEnvironment(environment)
	normalizedRunURL := strings.TrimSpace(runURL)
	normalizedOccurredAt := strings.TrimSpace(occurredAt)
	if normalizedEnvironment == "" || normalizedRunURL == "" || len(artifactRows) == 0 {
		return []contracts.RawFailureRecord{}
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
		normalizedText := sourcenormalize.Text(row.FailureText)
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
			Environment:       normalizedEnvironment,
			RowID:             rowID,
			RunURL:            normalizedRunURL,
			NonArtifactBacked: false,
			TestName:          row.TestName,
			TestSuite:         row.TestSuite,
			SignatureID:       sha256Hex(normalizedText),
			OccurredAt:        normalizedOccurredAt,
			RawText:           row.FailureText,
			NormalizedText:    normalizedText,
		})
	}

	return out
}

func buildSyntheticRawFailureRecord(environment, runURL, occurredAt string) contracts.RawFailureRecord {
	normalizedEnvironment := normalizeEnvironment(environment)
	normalizedRunURL := strings.TrimSpace(runURL)
	normalizedOccurredAt := strings.TrimSpace(occurredAt)
	normalizedText := sourcenormalize.Text(rawFailureSyntheticText)
	rowID := sha256Hex(strings.Join([]string{
		normalizedEnvironment,
		normalizedRunURL,
		"non_artifact_backed",
	}, "|"))
	return contracts.RawFailureRecord{
		Environment:       normalizedEnvironment,
		RowID:             rowID,
		RunURL:            normalizedRunURL,
		NonArtifactBacked: true,
		TestName:          rawFailureUnknownPlaceholder,
		TestSuite:         rawFailureUnknownPlaceholder,
		SignatureID:       sha256Hex(normalizedText),
		OccurredAt:        normalizedOccurredAt,
		RawText:           rawFailureSyntheticText,
		NormalizedText:    normalizedText,
	}
}
