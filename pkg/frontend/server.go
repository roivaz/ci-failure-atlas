package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	frontservice "ci-failure-atlas/pkg/frontend/service"
	reportreview "ci-failure-atlas/pkg/report/review"
	reportsummary "ci-failure-atlas/pkg/report/summary"
	"ci-failure-atlas/pkg/report/triagehtml"
	reportweekly "ci-failure-atlas/pkg/report/weekly"
	storecontracts "ci-failure-atlas/pkg/store/contracts"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HandlerOptions struct {
	DefaultWeek         string
	HistoryHorizonWeeks int
	PostgresPool        *pgxpool.Pool
}

type handler struct {
	service *frontservice.Service
}

func NewHandler(opts HandlerOptions) (http.Handler, error) {
	service, err := frontservice.New(frontservice.Options{
		DefaultWeek:         opts.DefaultWeek,
		HistoryHorizonWeeks: opts.HistoryHorizonWeeks,
		PostgresPool:        opts.PostgresPool,
	})
	if err != nil {
		return nil, err
	}
	reviewHandler, err := reportreview.NewHandler(reportreview.HandlerOptions{
		DefaultWeek:         service.DefaultWeek(),
		HistoryHorizonWeeks: service.HistoryHorizonWeeks(),
		PostgresPool:        opts.PostgresPool,
		RoutePrefix:         "/review",
		WeeklyPath:          "/weekly",
		GlobalPath:          "/global",
	})
	if err != nil {
		return nil, fmt.Errorf("create review handler: %w", err)
	}
	h := &handler{
		service: service,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", h.handleRoot)
	mux.HandleFunc("/api/triage/daily", h.handleAPIDailyTriage)
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

func (h *handler) handleAPIDailyTriage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}
	response, err := h.service.BuildDailyTriage(r.Context(), frontservice.DailyTriageQuery{
		Date:         strings.TrimSpace(r.URL.Query().Get("date")),
		Week:         strings.TrimSpace(r.URL.Query().Get("week")),
		Environments: parseListQueryValues(r.URL.Query()["env"]),
	})
	if err != nil {
		statusCode := http.StatusBadRequest
		if errors.Is(err, frontservice.ErrNoSemanticWeeks) || errors.Is(err, frontservice.ErrSemanticWeekNotFound) {
			statusCode = http.StatusNotFound
		}
		writeJSONError(w, statusCode, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *handler) resolveWeekWindow(ctx context.Context, requestedWeek string) (string, string, string, error) {
	window, err := h.service.ResolveWeekWindow(ctx, requestedWeek, time.Time{})
	if err != nil {
		return "", "", "", err
	}
	return window.CurrentWeek, window.PreviousWeek, window.NextWeek, nil
}

func (h *handler) generateGlobalReport(ctx context.Context, week string, previousWeek string, nextWeek string) (string, error) {
	store, err := h.service.OpenStoreForWeek(week)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = store.Close()
	}()

	historyResolver, err := h.service.BuildHistoryResolver(ctx, week)
	if err != nil {
		return "", fmt.Errorf("build global history resolver: %w", err)
	}

	opts := reportsummary.DefaultOptions()
	opts.Top = 25
	opts.MinPercent = 1.0
	opts.Week = strings.TrimSpace(week)
	opts.HistoryHorizonWeeks = h.service.HistoryHorizonWeeks()
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
	store, err := h.service.OpenStoreForWeek(week)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = store.Close()
	}()

	var previousStore storecontracts.Store
	if strings.TrimSpace(previousWeek) != "" {
		openedPreviousStore, openErr := h.service.OpenStoreForWeek(previousWeek)
		if openErr != nil {
			return "", openErr
		}
		previousStore = openedPreviousStore
		defer func() {
			_ = previousStore.Close()
		}()
	}

	historyResolver, err := h.service.BuildHistoryResolver(ctx, week)
	if err != nil {
		return "", fmt.Errorf("build weekly history resolver: %w", err)
	}

	opts := reportweekly.DefaultOptions()
	opts.StartDate = strings.TrimSpace(week)
	opts.Week = strings.TrimSpace(week)
	opts.HistoryHorizonWeeks = h.service.HistoryHorizonWeeks()
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

func parseListQueryValues(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed == "" {
				continue
			}
			out = append(out, trimmed)
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, statusCode int, err error) {
	writeJSON(w, statusCode, map[string]any{
		"error": strings.TrimSpace(err.Error()),
	})
}
