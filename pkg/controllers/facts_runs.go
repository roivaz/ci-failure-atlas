package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const factsRunsReconcileInterval = 5 * time.Minute

type factsRunsController struct {
	logger            logr.Logger
	reconcileInterval time.Duration
	queue             workqueue.TypedRateLimitingInterface[string]
	activeWindow      time.Duration
	envSet            map[string]struct{}

	store contracts.Store
}

var _ Controller = (*factsRunsController)(nil)

func NewFactsRuns(logger logr.Logger, deps Dependencies) (Controller, error) {
	return newFactsRunsController(logger, deps)
}

func newFactsRunsController(logger logr.Logger, deps Dependencies) (*factsRunsController, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("facts.runs: store dependency is required")
	}
	if deps.Source == nil {
		return nil, fmt.Errorf("facts.runs: source options dependency is required")
	}
	if len(deps.Source.Environments) == 0 {
		return nil, fmt.Errorf("facts.runs: no source environments configured")
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
		return nil, fmt.Errorf("facts.runs: no valid source environments configured")
	}

	return &factsRunsController{
		logger: logger.WithValues("controller", FactsRunsControllerName),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: FactsRunsControllerName,
			},
		),
		reconcileInterval: factsRunsReconcileInterval,
		activeWindow:      activeReconcileWindow(deps.Source),
		envSet:            envSet,
		store:             deps.Store,
	}, nil
}

func (c *factsRunsController) Run(ctx context.Context, threadiness int) {
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

func (c *factsRunsController) RunOnce(ctx context.Context, key string) error {
	c.logger.Info("Reconciling one key.", "key", key)
	return c.processKey(ctx, key)
}

func (c *factsRunsController) SyncOnce(ctx context.Context) error {
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

func (c *factsRunsController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *factsRunsController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.processKey(ctx, key); err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, fmt.Errorf("failed processing key %q", key), "Error syncing; requeuing for later retry", "controller", FactsRunsControllerName, "key", key)
	c.queue.AddRateLimited(key)
	return true
}

func (c *factsRunsController) queueMetadata(ctx context.Context) {
	keys, err := c.listKeys(ctx)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed listing keys for periodic enqueue", "controller", FactsRunsControllerName)
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.queue.Add(key)
	}
}

func (c *factsRunsController) listKeys(ctx context.Context) ([]string, error) {
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
		if !c.isEnvironmentEnabled(environment) {
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

func (c *factsRunsController) isEnvironmentEnabled(environment string) bool {
	normalized := normalizeEnvironment(environment)
	if normalized == "" {
		return false
	}
	_, enabled := c.envSet[normalized]
	return enabled
}

func (c *factsRunsController) processKey(ctx context.Context, key string) error {
	environment, runURL, err := splitEnvironmentRunKey(key)
	if err != nil {
		return err
	}
	run, found, err := c.store.GetRun(ctx, environment, runURL)
	if err != nil {
		return fmt.Errorf("get run for key %q: %w", key, err)
	}
	if !found {
		return nil
	}

	reconciled, changed, err := c.reconcileRun(ctx, run)
	if err != nil {
		return fmt.Errorf("reconcile run for key %q: %w", key, err)
	}
	if !changed {
		c.logger.V(1).Info("Skipping run; metadata already materialized.", "key", key)
		return nil
	}
	if err := c.store.UpsertRuns(ctx, []contracts.RunRecord{reconciled}); err != nil {
		return fmt.Errorf("upsert reconciled run for key %q: %w", key, err)
	}
	c.logger.V(1).Info(
		"Materialized run metadata from pull request facts.",
		"key", key,
		"pr_number", reconciled.PRNumber,
		"pr_state", reconciled.PRState,
		"merged_pr", reconciled.MergedPR,
		"post_good_commit", reconciled.PostGoodCommit,
	)
	return nil
}

func (c *factsRunsController) reconcileRun(ctx context.Context, run contracts.RunRecord) (contracts.RunRecord, bool, error) {
	normalizedEnvironment := normalizeEnvironment(run.Environment)
	next := run

	if !supportsPRLookupForEnvironment(normalizedEnvironment) {
		next.PRState = ""
		next.FinalMergedSHA = ""
		next.MergedPR = true
		next.PostGoodCommit = true
		return next, !runRecordSignalsEqual(run, next), nil
	}

	if next.PRNumber <= 0 {
		next.PRState = ""
		next.FinalMergedSHA = ""
		next.MergedPR = false
		next.PostGoodCommit = false
		return next, !runRecordSignalsEqual(run, next), nil
	}

	pr, found, err := c.store.GetPullRequest(ctx, next.PRNumber)
	if err != nil {
		return contracts.RunRecord{}, false, err
	}
	if !found {
		next.PRState = ""
		next.FinalMergedSHA = ""
		next.MergedPR = false
		next.PostGoodCommit = false
		return next, !runRecordSignalsEqual(run, next), nil
	}

	headSHA := strings.TrimSpace(pr.HeadSHA)
	next.PRState = strings.TrimSpace(pr.State)
	if pr.Merged && headSHA != "" {
		next.FinalMergedSHA = headSHA
		next.MergedPR = true
		next.PostGoodCommit = strings.TrimSpace(next.PRSHA) != "" && strings.TrimSpace(next.PRSHA) == headSHA
	} else {
		next.FinalMergedSHA = ""
		next.MergedPR = false
		next.PostGoodCommit = false
	}
	return next, !runRecordSignalsEqual(run, next), nil
}

func runRecordSignalsEqual(a contracts.RunRecord, b contracts.RunRecord) bool {
	return strings.TrimSpace(a.PRState) == strings.TrimSpace(b.PRState) &&
		strings.TrimSpace(a.FinalMergedSHA) == strings.TrimSpace(b.FinalMergedSHA) &&
		a.MergedPR == b.MergedPR &&
		a.PostGoodCommit == b.PostGoodCommit
}
