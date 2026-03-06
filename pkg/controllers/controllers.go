package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	"ci-failure-atlas/pkg/sourceoptions"
	"ci-failure-atlas/pkg/store/contracts"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	SourceSippyRunsControllerName    = "source.sippy.runs"
	SourceProwFailuresControllerName = "source.prow.failures"
	FactsRawFailuresControllerName   = "facts.raw-failures"
	MetricsRollupDailyControllerName = "metrics.rollup.daily"
)

type Dependencies struct {
	Store  contracts.Store
	Source *sourceoptions.Options
}

type noopController struct {
	name              string
	logger            logr.Logger
	reconcileInterval time.Duration
	queue             workqueue.TypedRateLimitingInterface[string]
}

var _ Controller = (*noopController)(nil)

func NewByName(name string, logger logr.Logger, deps Dependencies) (Controller, error) {
	switch name {
	case SourceSippyRunsControllerName:
		return NewSourceSippyRuns(logger, deps)
	case SourceProwFailuresControllerName:
		return NewSourceProwFailures(logger, deps)
	case FactsRawFailuresControllerName:
		return NewFactsRawFailures(logger), nil
	case MetricsRollupDailyControllerName:
		return NewMetricsRollupDaily(logger), nil
	default:
		return nil, fmt.Errorf("unknown controller %q", name)
	}
}

func NewFactsRawFailures(logger logr.Logger) Controller {
	return newNoopController(logger, FactsRawFailuresControllerName, 2*time.Minute)
}

func NewMetricsRollupDaily(logger logr.Logger) Controller {
	return newNoopController(logger, MetricsRollupDailyControllerName, 5*time.Minute)
}

func newNoopController(logger logr.Logger, name string, interval time.Duration) *noopController {
	return &noopController{
		name:              name,
		logger:            logger.WithValues("controller", name),
		reconcileInterval: interval,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: name,
			},
		),
	}
}

func (c *noopController) Run(ctx context.Context, threadiness int) {
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

func (c *noopController) RunOnce(ctx context.Context, key string) error {
	c.logger.Info("Reconciling one key.", "key", key)
	return c.processKey(ctx, key)
}

func (c *noopController) SyncOnce(ctx context.Context) error {
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

func (c *noopController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *noopController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.processKey(ctx, key); err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, fmt.Errorf("failed processing key %q", key), "Error syncing; requeuing for later retry", "controller", c.name, "key", key)
	c.queue.AddRateLimited(key)
	return true
}

func (c *noopController) queueMetadata(ctx context.Context) {
	keys, err := c.listKeys(ctx)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed listing keys for periodic enqueue", "controller", c.name)
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.queue.Add(key)
	}
}

func (c *noopController) listKeys(_ context.Context) ([]string, error) {
	return []string{}, nil
}

func (c *noopController) processKey(_ context.Context, key string) error {
	c.logger.V(1).Info("No-op reconcile.", "key", key)
	return nil
}
