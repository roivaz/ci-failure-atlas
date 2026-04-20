package controllers

import (
	"fmt"

	"github.com/go-logr/logr"

	sourceoptions "ci-failure-atlas/pkg/source/options"
	"ci-failure-atlas/pkg/store/contracts"
)

const (
	SourceSippyRunsControllerName          = "source.sippy.runs"
	SourceProwRunsControllerName           = "source.prow.runs"
	SourceSippyTestsDailyControllerName    = "source.sippy.tests-daily"
	SourceGitHubPullRequestsControllerName = "source.github.pull-requests"
	SourceProwFailuresControllerName       = "source.prow.failures"
	FactsRunsControllerName                = "facts.runs"
	FactsRawFailuresControllerName         = "facts.raw-failures"
	MetricsRollupDailyControllerName       = "metrics.rollup.daily"
)

type Dependencies struct {
	Store  contracts.Store
	Source *sourceoptions.Options
}

func NewByName(name string, logger logr.Logger, deps Dependencies) (Controller, error) {
	switch name {
	case SourceSippyRunsControllerName:
		return NewSourceSippyRuns(logger, deps)
	case SourceProwRunsControllerName:
		return NewSourceProwRuns(logger, deps)
	case SourceSippyTestsDailyControllerName:
		return NewSourceSippyTestsDaily(logger, deps)
	case SourceGitHubPullRequestsControllerName:
		return NewSourceGitHubPullRequests(logger, deps)
	case SourceProwFailuresControllerName:
		return NewSourceProwFailures(logger, deps)
	case FactsRunsControllerName:
		return NewFactsRuns(logger, deps)
	case FactsRawFailuresControllerName:
		return NewFactsRawFailures(logger, deps)
	case MetricsRollupDailyControllerName:
		return NewMetricsRollupDaily(logger, deps)
	default:
		return nil, fmt.Errorf("unknown controller %q", name)
	}
}
