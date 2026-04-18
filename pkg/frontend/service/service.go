package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	semhistory "ci-failure-atlas/pkg/semantic/history"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultHistoryWeeks = 4

var (
	ErrNoSemanticWeeks      = errors.New("no semantic weeks found in postgres store")
	ErrSemanticWeekNotFound = errors.New("semantic week not found in postgres store")
)

type Options struct {
	DefaultWeek         string
	HistoryHorizonWeeks int
	PostgresPool        *pgxpool.Pool
}

type Service struct {
	defaultWeek  string
	historyWeeks int
	postgresPool *pgxpool.Pool
}

type WeekWindow struct {
	Weeks        []string `json:"weeks,omitempty"`
	CurrentWeek  string   `json:"current_week"`
	PreviousWeek string   `json:"previous_week,omitempty"`
	NextWeek     string   `json:"next_week,omitempty"`
	Index        int      `json:"-"`
}

func New(opts Options) (*Service, error) {
	if opts.PostgresPool == nil {
		return nil, fmt.Errorf("postgres pool is required")
	}
	defaultWeek, err := postgresstore.NormalizeWeek(opts.DefaultWeek)
	if err != nil {
		return nil, fmt.Errorf("invalid default week: %w", err)
	}
	historyWeeks := opts.HistoryHorizonWeeks
	if historyWeeks <= 0 {
		historyWeeks = DefaultHistoryWeeks
	}
	return &Service{
		defaultWeek:  defaultWeek,
		historyWeeks: historyWeeks,
		postgresPool: opts.PostgresPool,
	}, nil
}

func (s *Service) DefaultWeek() string {
	if s == nil {
		return ""
	}
	return s.defaultWeek
}

func (s *Service) HistoryHorizonWeeks() int {
	if s == nil {
		return 0
	}
	return s.historyWeeks
}

func (s *Service) DiscoverSemanticWeeks(ctx context.Context) ([]string, error) {
	if s == nil {
		return nil, fmt.Errorf("service is required")
	}
	weeks, err := postgresstore.ListWeeks(ctx, s.postgresPool)
	if err != nil {
		return nil, fmt.Errorf("list semantic weeks from postgres: %w", err)
	}
	if len(weeks) == 0 {
		return nil, ErrNoSemanticWeeks
	}
	return weeks, nil
}

func (s *Service) ResolveWeekWindow(ctx context.Context, requestedWeek string, now time.Time) (WeekWindow, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	weeks, err := s.DiscoverSemanticWeeks(ctx)
	if err != nil {
		return WeekWindow{}, err
	}
	week, previousWeek, nextWeek, index := ResolveWindow(weeks, strings.TrimSpace(requestedWeek), s.defaultWeek, now.UTC())
	if strings.TrimSpace(week) == "" {
		return WeekWindow{}, ErrNoSemanticWeeks
	}
	return WeekWindow{
		Weeks:        append([]string(nil), weeks...),
		CurrentWeek:  week,
		PreviousWeek: previousWeek,
		NextWeek:     nextWeek,
		Index:        index,
	}, nil
}

func (s *Service) OpenStoreForWeek(week string) (storecontracts.Store, error) {
	if s == nil {
		return nil, fmt.Errorf("service is required")
	}
	normalizedWeek, err := postgresstore.NormalizeWeek(week)
	if err != nil {
		return nil, fmt.Errorf("invalid semantic week %q: %w", strings.TrimSpace(week), err)
	}
	if normalizedWeek == "" {
		return nil, fmt.Errorf("week is required")
	}
	store, err := postgresstore.New(s.postgresPool, postgresstore.Options{
		Week: normalizedWeek,
	})
	if err != nil {
		return nil, fmt.Errorf("open postgres store for week %q: %w", normalizedWeek, err)
	}
	return store, nil
}

func (s *Service) BuildHistoryResolver(ctx context.Context, week string) (semhistory.FailurePatternHistoryResolver, error) {
	if s == nil {
		return nil, fmt.Errorf("service is required")
	}
	return semhistory.BuildFailurePatternHistoryResolver(ctx, semhistory.BuildOptions{
		CurrentWeek:                        strings.TrimSpace(week),
		FailurePatternHistoryLookbackWeeks: s.historyWeeks,
		ListWeeks: func(ctx context.Context) ([]string, error) {
			return s.DiscoverSemanticWeeks(ctx)
		},
		OpenStore: func(_ context.Context, week string) (storecontracts.Store, error) {
			return s.OpenStoreForWeek(week)
		},
	})
}

func (s *Service) ensureWeekExists(ctx context.Context, week string) (string, error) {
	normalizedWeek, err := postgresstore.NormalizeWeek(week)
	if err != nil {
		return "", fmt.Errorf("invalid semantic week %q: %w", strings.TrimSpace(week), err)
	}
	if normalizedWeek == "" {
		return "", fmt.Errorf("week is required")
	}
	weeks, err := s.DiscoverSemanticWeeks(ctx)
	if err != nil {
		return "", err
	}
	index := sort.SearchStrings(weeks, normalizedWeek)
	if index >= len(weeks) || weeks[index] != normalizedWeek {
		return "", fmt.Errorf("%w: %s", ErrSemanticWeekNotFound, normalizedWeek)
	}
	return normalizedWeek, nil
}
