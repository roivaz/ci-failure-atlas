package readmodel

import (
	"context"

	frontservice "ci-failure-atlas/pkg/frontend/service"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
)

var (
	ErrNoSemanticWeeks      = frontservice.ErrNoSemanticWeeks
	ErrSemanticWeekNotFound = frontservice.ErrSemanticWeekNotFound
)

type Options = frontservice.Options
type Service = frontservice.Service
type WeekWindow = frontservice.WeekWindow

type WindowDefaultMode = frontservice.WindowDefaultMode

const (
	WindowDefaultNone       = frontservice.WindowDefaultNone
	WindowDefaultLatestWeek = frontservice.WindowDefaultLatestWeek
	WindowDefaultRolling    = frontservice.WindowDefaultRolling
)

type WindowRequest = frontservice.WindowRequest
type WindowScope = frontservice.WindowScope

type ReportQuery = frontservice.ReportQuery
type ReportData = frontservice.ReportData

type FailurePatternsQuery = frontservice.FailurePatternsQuery
type FailurePatternsData = frontservice.FailurePatternsData
type FailurePatternsMeta = frontservice.FailurePatternsMeta
type FailurePatternsEnvironment = frontservice.FailurePatternsEnvironment
type FailurePatternsSummary = frontservice.FailurePatternsSummary
type FailurePatternsRow = frontservice.FailurePatternsRow

type WeeklyReportBuildOptions = frontservice.WeeklyReportBuildOptions
type WeeklyCounts = frontservice.WeeklyCounts
type WeeklyRunOutcomes = frontservice.WeeklyRunOutcomes
type WeeklyDayReport = frontservice.WeeklyDayReport
type WeeklyEnvReport = frontservice.WeeklyEnvReport
type WeeklySemanticEnvSummary = frontservice.WeeklySemanticEnvSummary
type WeeklyTopSignature = frontservice.WeeklyTopSignature
type WeeklySemanticSnapshot = frontservice.WeeklySemanticSnapshot
type WeeklyBelowTargetTest = frontservice.WeeklyBelowTargetTest
type WeeklyReportData = frontservice.WeeklyReportData

type FailurePatternReportBuildOptions = frontservice.FailurePatternReportBuildOptions
type FailurePatternReportReference = frontservice.FailurePatternReportReference
type FailurePatternReportContributingTest = frontservice.FailurePatternReportContributingTest
type FailurePatternReportCluster = frontservice.FailurePatternReportCluster
type FailurePatternReportData = frontservice.FailurePatternReportData

type ReviewPhase3Anchor = frontservice.ReviewPhase3Anchor
type ReviewWeekSnapshot = frontservice.ReviewWeekSnapshot

type RunLogDayQuery = frontservice.RunLogDayQuery
type RunLogDayData = frontservice.RunLogDayData
type RunLogDayMeta = frontservice.RunLogDayMeta
type RunLogDayEnvironment = frontservice.RunLogDayEnvironment
type RunLogDaySummary = frontservice.RunLogDaySummary
type JobHistoryRunRow = frontservice.JobHistoryRunRow
type JobHistoryFailureRow = frontservice.JobHistoryFailureRow
type JobHistorySemanticAttachment = frontservice.JobHistorySemanticAttachment
type JobHistorySemanticRollups = frontservice.JobHistorySemanticRollups

func New(opts Options) (*Service, error) {
	return frontservice.New(opts)
}

func BuildWeeklyReportData(
	ctx context.Context,
	store storecontracts.Store,
	previousSemanticStore storecontracts.Store,
	opts WeeklyReportBuildOptions,
) (WeeklyReportData, error) {
	return frontservice.BuildWeeklyReportData(ctx, store, previousSemanticStore, opts)
}

func BuildFailurePatternReportData(
	ctx context.Context,
	store storecontracts.Store,
	opts FailurePatternReportBuildOptions,
) (FailurePatternReportData, error) {
	return frontservice.BuildFailurePatternReportData(ctx, store, opts)
}
