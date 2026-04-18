package service

import (
	"context"
	"fmt"
	"time"
)

type ReportQuery struct {
	StartDate   string
	EndDate     string
	Week        string
	GeneratedAt time.Time
}

type ReportData struct {
	StartDate             time.Time
	EndDate               time.Time
	PreviousStartDate     time.Time
	PreviousEndDate       time.Time
	GeneratedAt           time.Time
	Timezone              string
	CurrentReports        []WeeklyEnvReport
	PreviousReports       []WeeklyEnvReport
	TargetRate            float64
	TestsBelowTargetByEnv map[string][]WeeklyBelowTargetTest
	TopSignaturesByEnv    map[string][]WindowedTriageRow
	NavigationAnchorWeek  string
}

func (s *Service) BuildReportData(ctx context.Context, query ReportQuery) (ReportData, error) {
	if s == nil {
		return ReportData{}, fmt.Errorf("service is required")
	}

	scope, err := s.resolvePresentationWindow(ctx, presentationWindowRequest{
		StartDate:   query.StartDate,
		EndDate:     query.EndDate,
		Week:        query.Week,
		DefaultMode: presentationWindowDefaultLatestWeek,
	})
	if err != nil {
		return ReportData{}, err
	}

	store, err := s.OpenStoreForWeek(scope.AnchorWeek)
	if err != nil {
		return ReportData{}, err
	}
	defer func() {
		_ = store.Close()
	}()

	currentReports, err := buildEnvReports(ctx, store, scope.DateLabels)
	if err != nil {
		return ReportData{}, err
	}

	previousWindowDays := len(scope.DateLabels)
	previousEndExclusive := scope.StartTime
	previousStart := previousEndExclusive.AddDate(0, 0, -previousWindowDays)
	previousDates := metricDateLabelsFromWindow(previousStart, previousEndExclusive)
	previousReports, err := buildEnvReports(ctx, store, previousDates)
	if err != nil {
		return ReportData{}, err
	}

	testsBelowTargetByEnv, err := loadBelowTargetTestsByEnvironment(
		ctx,
		store,
		scope.DateLabels,
		weeklyDefaultPeriod,
		weeklyTestSuccessTarget,
		weeklyTestSuccessMinRuns,
		weeklyTestsBelowTargetTopLimit,
	)
	if err != nil {
		return ReportData{}, fmt.Errorf("load report tests below target: %w", err)
	}

	triageData, err := s.BuildWindowedTriage(ctx, WindowedTriageQuery{
		StartDate: scope.StartDate,
		EndDate:   scope.EndDate,
	})
	if err != nil {
		return ReportData{}, fmt.Errorf("build report signature data: %w", err)
	}
	topSignaturesByEnv := make(map[string][]WindowedTriageRow, len(triageData.Environments))
	for _, environment := range triageData.Environments {
		topSignaturesByEnv[environment.Environment] = append([]WindowedTriageRow(nil), environment.Rows...)
	}

	generatedAt := query.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	return ReportData{
		StartDate:             scope.StartTime,
		EndDate:               scope.EndTime.AddDate(0, 0, -1),
		PreviousStartDate:     previousStart,
		PreviousEndDate:       previousEndExclusive.AddDate(0, 0, -1),
		GeneratedAt:           generatedAt.UTC(),
		Timezone:              "UTC",
		CurrentReports:        currentReports,
		PreviousReports:       previousReports,
		TargetRate:            weeklyTestSuccessTarget,
		TestsBelowTargetByEnv: testsBelowTargetByEnv,
		TopSignaturesByEnv:    topSignaturesByEnv,
		NavigationAnchorWeek:  scope.AnchorWeek,
	}, nil
}
