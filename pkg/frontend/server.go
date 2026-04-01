package frontend

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	reportreview "ci-failure-atlas/pkg/report/review"
	reportsummary "ci-failure-atlas/pkg/report/summary"
	"ci-failure-atlas/pkg/report/triagehtml"
	"ci-failure-atlas/pkg/report/weeknav"
	reportweekly "ci-failure-atlas/pkg/report/weekly"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultHistoryWeeks = 4

type HandlerOptions struct {
	DefaultWeek         string
	HistoryHorizonWeeks int
	PostgresPool        *pgxpool.Pool
}

type handler struct {
	defaultWeek  string
	historyWeeks int
	postgresPool *pgxpool.Pool
}

func NewHandler(opts HandlerOptions) (http.Handler, error) {
	if opts.PostgresPool == nil {
		return nil, fmt.Errorf("postgres pool is required")
	}
	defaultWeek, err := postgresstore.NormalizeWeek(opts.DefaultWeek)
	if err != nil {
		return nil, fmt.Errorf("invalid default week: %w", err)
	}
	historyWeeks := opts.HistoryHorizonWeeks
	if historyWeeks <= 0 {
		historyWeeks = defaultHistoryWeeks
	}
	reviewHandler, err := reportreview.NewHandler(reportreview.HandlerOptions{
		DefaultWeek:         defaultWeek,
		HistoryHorizonWeeks: historyWeeks,
		PostgresPool:        opts.PostgresPool,
		RoutePrefix:         "/review",
		WeeklyPath:          "/weekly",
		GlobalPath:          "/global",
	})
	if err != nil {
		return nil, fmt.Errorf("create review handler: %w", err)
	}
	h := &handler{
		defaultWeek:  defaultWeek,
		historyWeeks: historyWeeks,
		postgresPool: opts.PostgresPool,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", h.handleRoot)
	mux.HandleFunc("/global", h.handleGlobalPage)
	mux.HandleFunc("/weekly", h.handleWeeklyPage)
	mux.HandleFunc("/review", h.handleReviewRoot)
	mux.Handle("/review/", http.StripPrefix("/review", reviewHandler))
	return mux, nil
}

func (h *handler) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.Redirect(w, r, viewHref("/weekly", strings.TrimSpace(r.URL.Query().Get("week"))), http.StatusFound)
}

func (h *handler) handleGlobalPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	week, previousWeek, nextWeek, err := h.resolveWeekWindow(r.Context(), strings.TrimSpace(r.URL.Query().Get("week")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reportHTML, err := h.generateGlobalReport(r.Context(), week, previousWeek, nextWeek)
	if err != nil {
		http.Error(w, fmt.Sprintf("generate global report: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(reportHTML))
}

func (h *handler) handleWeeklyPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	week, previousWeek, nextWeek, err := h.resolveWeekWindow(r.Context(), strings.TrimSpace(r.URL.Query().Get("week")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reportHTML, err := h.generateWeeklyReport(r.Context(), week, previousWeek, nextWeek)
	if err != nil {
		http.Error(w, fmt.Sprintf("generate weekly report: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(reportHTML))
}

func (h *handler) handleReviewRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.Redirect(w, r, viewHref("/review/", strings.TrimSpace(r.URL.Query().Get("week"))), http.StatusFound)
}

func (h *handler) discoverSemanticWeeks(ctx context.Context) ([]string, error) {
	weeks, err := postgresstore.ListWeeks(ctx, h.postgresPool)
	if err != nil {
		return nil, fmt.Errorf("list semantic weeks from postgres: %w", err)
	}
	if len(weeks) == 0 {
		return nil, fmt.Errorf("no semantic weeks found in postgres store")
	}
	return weeks, nil
}

func (h *handler) resolveWeekWindow(ctx context.Context, requestedWeek string) (string, string, string, error) {
	weeks, err := h.discoverSemanticWeeks(ctx)
	if err != nil {
		return "", "", "", err
	}
	week, previousWeek, nextWeek, _ := weeknav.ResolveWindow(weeks, requestedWeek, h.defaultWeek, time.Now().UTC())
	if week == "" {
		return "", "", "", fmt.Errorf("no semantic snapshots found in postgres store")
	}
	return week, previousWeek, nextWeek, nil
}

func (h *handler) openStoreForWeek(week string) (storecontracts.Store, error) {
	trimmedWeek := strings.TrimSpace(week)
	if trimmedWeek == "" {
		return nil, fmt.Errorf("week is required")
	}
	store, err := postgresstore.New(h.postgresPool, postgresstore.Options{
		Week: trimmedWeek,
	})
	if err != nil {
		return nil, fmt.Errorf("open postgres store for week %q: %w", trimmedWeek, err)
	}
	return store, nil
}

func (h *handler) buildHistoryResolver(ctx context.Context, week string) (semhistory.GlobalSignatureResolver, error) {
	return semhistory.BuildGlobalSignatureResolver(ctx, semhistory.BuildOptions{
		CurrentWeek:                  strings.TrimSpace(week),
		GlobalSignatureLookbackWeeks: h.historyWeeks,
		ListWeeks: func(ctx context.Context) ([]string, error) {
			return postgresstore.ListWeeks(ctx, h.postgresPool)
		},
		OpenStore: func(_ context.Context, week string) (storecontracts.Store, error) {
			return h.openStoreForWeek(week)
		},
	})
}

func (h *handler) generateGlobalReport(ctx context.Context, week string, previousWeek string, nextWeek string) (string, error) {
	store, err := h.openStoreForWeek(week)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = store.Close()
	}()

	historyResolver, err := h.buildHistoryResolver(ctx, week)
	if err != nil {
		return "", fmt.Errorf("build global history resolver: %w", err)
	}

	opts := reportsummary.DefaultOptions()
	opts.Top = 25
	opts.MinPercent = 1.0
	opts.Week = strings.TrimSpace(week)
	opts.HistoryHorizonWeeks = h.historyWeeks
	opts.HistoryResolver = historyResolver
	opts.Chrome = triagehtml.ReportChromeOptions{
		CurrentWeek:  strings.TrimSpace(week),
		CurrentView:  triagehtml.ReportViewGlobal,
		PreviousWeek: strings.TrimSpace(previousWeek),
		PreviousHref: navigationHref("/global", previousWeek),
		NextWeek:     strings.TrimSpace(nextWeek),
		NextHref:     navigationHref("/global", nextWeek),
		WeeklyHref:   viewHref("/weekly", week),
		GlobalHref:   viewHref("/global", week),
	}
	return reportsummary.GenerateHTML(ctx, store, opts)
}

func (h *handler) generateWeeklyReport(ctx context.Context, week string, previousWeek string, nextWeek string) (string, error) {
	store, err := h.openStoreForWeek(week)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = store.Close()
	}()

	var previousStore storecontracts.Store
	if strings.TrimSpace(previousWeek) != "" {
		openedPreviousStore, openErr := h.openStoreForWeek(previousWeek)
		if openErr != nil {
			return "", openErr
		}
		previousStore = openedPreviousStore
		defer func() {
			_ = previousStore.Close()
		}()
	}

	historyResolver, err := h.buildHistoryResolver(ctx, week)
	if err != nil {
		return "", fmt.Errorf("build weekly history resolver: %w", err)
	}

	opts := reportweekly.DefaultOptions()
	opts.StartDate = strings.TrimSpace(week)
	opts.Week = strings.TrimSpace(week)
	opts.HistoryHorizonWeeks = h.historyWeeks
	opts.HistoryResolver = historyResolver
	opts.Chrome = triagehtml.ReportChromeOptions{
		CurrentWeek:  strings.TrimSpace(week),
		CurrentView:  triagehtml.ReportViewWeekly,
		PreviousWeek: strings.TrimSpace(previousWeek),
		PreviousHref: navigationHref("/weekly", previousWeek),
		NextWeek:     strings.TrimSpace(nextWeek),
		NextHref:     navigationHref("/weekly", nextWeek),
		WeeklyHref:   viewHref("/weekly", week),
		GlobalHref:   viewHref("/global", week),
	}
	return reportweekly.GenerateHTMLWithComparison(ctx, store, previousStore, opts)
}

func viewHref(path string, week string) string {
	trimmedPath := strings.TrimSpace(path)
	if trimmedPath == "" {
		return ""
	}
	if !strings.HasPrefix(trimmedPath, "/") {
		trimmedPath = "/" + trimmedPath
	}
	q := url.Values{}
	if strings.TrimSpace(week) != "" {
		q.Set("week", strings.TrimSpace(week))
	}
	if encoded := q.Encode(); encoded != "" {
		return trimmedPath + "?" + encoded
	}
	return trimmedPath
}

func navigationHref(path string, week string) string {
	if strings.TrimSpace(week) == "" {
		return ""
	}
	return viewHref(path, week)
}
