package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	frontservice "ci-failure-atlas/pkg/frontend/service"
	reportreview "ci-failure-atlas/pkg/report/review"
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
		TriagePath:          "/triage",
	})
	if err != nil {
		return nil, fmt.Errorf("create review handler: %w", err)
	}
	h := &handler{
		service: service,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", h.handleRoot)
	mux.HandleFunc("/api/runs/day", h.handleAPIRunsDay)
	mux.HandleFunc("/api/triage/window", h.handleAPIWindowedTriage)
	mux.HandleFunc("/runs", h.handleRunsPage)
	mux.HandleFunc("/triage", h.handleTriagePage)
	mux.HandleFunc("/global", h.handleLegacyGlobalRedirect)
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
	if r.URL == nil || strings.TrimSpace(r.URL.Path) != "/" {
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, viewHref("/weekly", strings.TrimSpace(r.URL.Query().Get("week"))), http.StatusFound)
}

func (h *handler) handleTriagePage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	windowedQuery, err := h.resolveWindowedTriagePageQuery(r.Context(), windowedTriageQueryFromRequest(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reportHTML, err := h.generateWindowedTriageReport(r.Context(), windowedQuery)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(reportHTML))
}

func (h *handler) handleRunsPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	reportHTML, err := h.generateDayRunHistoryPage(r.Context(), jobHistoryDayQueryFromRequest(r))
	if err != nil {
		statusCode := http.StatusBadRequest
		if errors.Is(err, frontservice.ErrNoSemanticWeeks) || errors.Is(err, frontservice.ErrSemanticWeekNotFound) {
			statusCode = http.StatusNotFound
		}
		http.Error(w, err.Error(), statusCode)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(reportHTML))
}

func (h *handler) handleLegacyGlobalRedirect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.Redirect(w, r, viewHref("/triage", strings.TrimSpace(r.URL.Query().Get("week"))), http.StatusMovedPermanently)
}

func (h *handler) handleWeeklyPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	window, err := h.service.ResolveWeekWindow(r.Context(), strings.TrimSpace(r.URL.Query().Get("week")), time.Time{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reportHTML, err := h.generateWeeklyReport(r.Context(), window)
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

func (h *handler) handleAPIWindowedTriage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}
	response, err := h.service.BuildWindowedTriage(r.Context(), windowedTriageQueryFromRequest(r))
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

func (h *handler) handleAPIRunsDay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}
	response, err := h.service.BuildJobHistoryDay(r.Context(), jobHistoryDayQueryFromRequest(r))
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

func (h *handler) generateWindowedTriageReport(ctx context.Context, query frontservice.WindowedTriageQuery) (string, error) {
	data, err := h.service.BuildWindowedTriage(ctx, query)
	if err != nil {
		return "", err
	}
	window, err := h.service.ResolveWeekWindow(ctx, data.Meta.ResolvedWeek, time.Time{})
	if err != nil {
		return "", err
	}
	week := strings.TrimSpace(data.Meta.ResolvedWeek)
	runsHref := ""
	if isSingleDayWindow(data.Meta.StartDate, data.Meta.EndDate) {
		runsHref = dayRunHistoryHref("/runs", data.Meta.StartDate, week, query.Environments)
	}
	return buildWindowedTriageReportHTML(data, windowedTriagePageOptions{
		Query: query,
		Chrome: triagehtml.ReportChromeOptions{
			CurrentWeek:  week,
			CurrentView:  triagehtml.ReportViewTriage,
			PreviousWeek: strings.TrimSpace(window.PreviousWeek),
			PreviousHref: shiftedWindowedTriageHref("/triage", window.PreviousWeek, week, data.Meta.StartDate, data.Meta.EndDate, query.Environments),
			NextWeek:     strings.TrimSpace(window.NextWeek),
			NextHref:     shiftedWindowedTriageHref("/triage", window.NextWeek, week, data.Meta.StartDate, data.Meta.EndDate, query.Environments),
			WeeklyHref:   viewHref("/weekly", week),
			TriageHref:   windowedTriageHref("/triage", week, data.Meta.StartDate, data.Meta.EndDate, query.Environments),
			RunsHref:     runsHref,
		},
	}), nil
}

func (h *handler) generateDayRunHistoryPage(ctx context.Context, query frontservice.JobHistoryDayQuery) (string, error) {
	data, err := h.service.BuildJobHistoryDay(ctx, query)
	if err != nil {
		return "", err
	}
	window, err := h.service.ResolveWeekWindow(ctx, data.Meta.ResolvedWeek, time.Time{})
	if err != nil {
		return "", err
	}
	week := strings.TrimSpace(data.Meta.ResolvedWeek)
	environments := query.Environments
	return buildDayRunHistoryPageHTML(data, dayRunHistoryPageOptions{
		Query:      query,
		TriageHref: windowedTriageHref("/triage", week, data.Meta.Date, data.Meta.Date, environments),
		APIHref:    dayRunHistoryHref("/api/runs/day", data.Meta.Date, week, environments),
		Chrome: triagehtml.ReportChromeOptions{
			CurrentWeek:  week,
			CurrentView:  triagehtml.ReportViewRuns,
			PreviousWeek: strings.TrimSpace(window.PreviousWeek),
			PreviousHref: shiftedDayRunHistoryHref("/runs", window.PreviousWeek, data.Meta.Date, week, environments),
			NextWeek:     strings.TrimSpace(window.NextWeek),
			NextHref:     shiftedDayRunHistoryHref("/runs", window.NextWeek, data.Meta.Date, week, environments),
			WeeklyHref:   viewHref("/weekly", week),
			TriageHref:   windowedTriageHref("/triage", week, data.Meta.Date, data.Meta.Date, environments),
			RunsHref:     dayRunHistoryHref("/runs", data.Meta.Date, week, environments),
		},
	}), nil
}

func (h *handler) generateWeeklyReport(ctx context.Context, window frontservice.WeekWindow) (string, error) {
	week := strings.TrimSpace(window.CurrentWeek)
	store, err := h.service.OpenStoreForWeek(week)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = store.Close()
	}()

	var previousStore storecontracts.Store
	if strings.TrimSpace(window.PreviousWeek) != "" {
		openedPreviousStore, openErr := h.service.OpenStoreForWeek(window.PreviousWeek)
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
	opts.StartDate = week
	opts.Week = week
	opts.HistoryHorizonWeeks = h.service.HistoryHorizonWeeks()
	opts.HistoryResolver = historyResolver
	opts.DayRunHistoryBasePath = "/runs"
	opts.Chrome = triagehtml.ReportChromeOptions{
		CurrentWeek:  week,
		CurrentView:  triagehtml.ReportViewWeekly,
		PreviousWeek: strings.TrimSpace(window.PreviousWeek),
		PreviousHref: navigationHref("/weekly", window.PreviousWeek),
		NextWeek:     strings.TrimSpace(window.NextWeek),
		NextHref:     navigationHref("/weekly", window.NextWeek),
		WeeklyHref:   viewHref("/weekly", week),
		TriageHref:   viewHref("/triage", week),
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

func windowedTriageHref(path string, week string, startDate string, endDate string, environments []string) string {
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
	if strings.TrimSpace(startDate) != "" {
		q.Set("start_date", strings.TrimSpace(startDate))
	}
	if strings.TrimSpace(endDate) != "" {
		q.Set("end_date", strings.TrimSpace(endDate))
	}
	for _, environment := range normalizedQueryEnvironments(environments) {
		q.Add("env", environment)
	}
	if encoded := q.Encode(); encoded != "" {
		return trimmedPath + "?" + encoded
	}
	return trimmedPath
}

func hasWindowedTriageQuery(query frontservice.WindowedTriageQuery) bool {
	return strings.TrimSpace(query.StartDate) != "" || strings.TrimSpace(query.EndDate) != ""
}

func windowedTriageQueryFromRequest(r *http.Request) frontservice.WindowedTriageQuery {
	if r == nil {
		return frontservice.WindowedTriageQuery{}
	}
	return frontservice.WindowedTriageQuery{
		StartDate:    strings.TrimSpace(r.URL.Query().Get("start_date")),
		EndDate:      strings.TrimSpace(r.URL.Query().Get("end_date")),
		Week:         strings.TrimSpace(r.URL.Query().Get("week")),
		Environments: parseListQueryValues(r.URL.Query()["env"]),
	}
}

func (h *handler) resolveWindowedTriagePageQuery(
	ctx context.Context,
	query frontservice.WindowedTriageQuery,
) (frontservice.WindowedTriageQuery, error) {
	startDate := strings.TrimSpace(query.StartDate)
	endDate := strings.TrimSpace(query.EndDate)
	switch {
	case startDate == "" && endDate == "":
		window, err := h.service.ResolveWeekWindow(ctx, strings.TrimSpace(query.Week), time.Time{})
		if err != nil {
			return frontservice.WindowedTriageQuery{}, err
		}
		query.Week = strings.TrimSpace(window.CurrentWeek)
		query.StartDate, query.EndDate = semanticWeekDateRange(query.Week)
	case startDate == "" || endDate == "":
		return frontservice.WindowedTriageQuery{}, fmt.Errorf("start_date and end_date must both be set when filtering the triage window")
	default:
		inferredWeek, err := semanticWeekForDateWindow(startDate, endDate)
		if err != nil {
			return frontservice.WindowedTriageQuery{}, err
		}
		query.Week = inferredWeek
		query.StartDate = startDate
		query.EndDate = endDate
	}
	return query, nil
}

func semanticWeekDateRange(week string) (string, string) {
	startDate, err := time.Parse("2006-01-02", strings.TrimSpace(week))
	if err != nil {
		return "", ""
	}
	startDate = startDate.UTC()
	return startDate.Format("2006-01-02"), startDate.AddDate(0, 0, 6).Format("2006-01-02")
}

func semanticWeekForDateWindow(startDate string, endDate string) (string, error) {
	startValue, err := parseDateInputValue("start_date", startDate)
	if err != nil {
		return "", err
	}
	endValue, err := parseDateInputValue("end_date", endDate)
	if err != nil {
		return "", err
	}
	if endValue.Before(startValue) {
		return "", fmt.Errorf("end_date %s must be on or after start_date %s", endValue.Format("2006-01-02"), startValue.Format("2006-01-02"))
	}
	startWeek := startValue.AddDate(0, 0, -int(startValue.Weekday())).Format("2006-01-02")
	endWeek := endValue.AddDate(0, 0, -int(endValue.Weekday())).Format("2006-01-02")
	if startWeek != endWeek {
		return "", fmt.Errorf("window %s..%s crosses semantic week boundaries (%s vs %s)", startValue.Format("2006-01-02"), endValue.Format("2006-01-02"), startWeek, endWeek)
	}
	return startWeek, nil
}

func parseDateInputValue(fieldName string, value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("%s is required (YYYY-MM-DD)", strings.TrimSpace(fieldName))
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil || parsed.Format("2006-01-02") != trimmed {
		return time.Time{}, fmt.Errorf("%s must use YYYY-MM-DD format", strings.TrimSpace(fieldName))
	}
	return parsed.UTC(), nil
}

func shiftedWindowedTriageHref(
	path string,
	targetWeek string,
	currentWeek string,
	startDate string,
	endDate string,
	environments []string,
) string {
	trimmedTargetWeek := strings.TrimSpace(targetWeek)
	if trimmedTargetWeek == "" {
		return ""
	}
	targetStart, targetEnd := semanticWeekDateRange(trimmedTargetWeek)
	if targetStart == "" || targetEnd == "" {
		return viewHref(path, trimmedTargetWeek)
	}
	currentWeekStart, errCurrentWeek := time.Parse("2006-01-02", strings.TrimSpace(currentWeek))
	currentStartDate, errCurrentStart := time.Parse("2006-01-02", strings.TrimSpace(startDate))
	currentEndDate, errCurrentEnd := time.Parse("2006-01-02", strings.TrimSpace(endDate))
	targetWeekStart, errTargetWeek := time.Parse("2006-01-02", trimmedTargetWeek)
	if errCurrentWeek != nil || errCurrentStart != nil || errCurrentEnd != nil || errTargetWeek != nil {
		return windowedTriageHref(path, trimmedTargetWeek, targetStart, targetEnd, environments)
	}
	startOffset := int(currentStartDate.UTC().Sub(currentWeekStart.UTC()) / (24 * time.Hour))
	endOffset := int(currentEndDate.UTC().Sub(currentWeekStart.UTC()) / (24 * time.Hour))
	if startOffset < 0 || endOffset < startOffset || endOffset > 6 {
		return windowedTriageHref(path, trimmedTargetWeek, targetStart, targetEnd, environments)
	}
	shiftedStart := targetWeekStart.UTC().AddDate(0, 0, startOffset).Format("2006-01-02")
	shiftedEnd := targetWeekStart.UTC().AddDate(0, 0, endOffset).Format("2006-01-02")
	return windowedTriageHref(path, trimmedTargetWeek, shiftedStart, shiftedEnd, environments)
}

func dayRunHistoryHref(path string, date string, week string, environments []string) string {
	trimmedPath := strings.TrimSpace(path)
	if trimmedPath == "" {
		return ""
	}
	if !strings.HasPrefix(trimmedPath, "/") {
		trimmedPath = "/" + trimmedPath
	}
	q := url.Values{}
	if strings.TrimSpace(date) != "" {
		q.Set("date", strings.TrimSpace(date))
	}
	if strings.TrimSpace(week) != "" {
		q.Set("week", strings.TrimSpace(week))
	}
	for _, environment := range normalizedQueryEnvironments(environments) {
		q.Add("env", environment)
	}
	if encoded := q.Encode(); encoded != "" {
		return trimmedPath + "?" + encoded
	}
	return trimmedPath
}

func shiftedDayRunHistoryHref(
	path string,
	targetWeek string,
	currentDate string,
	currentWeek string,
	environments []string,
) string {
	trimmedTargetWeek := strings.TrimSpace(targetWeek)
	if trimmedTargetWeek == "" {
		return ""
	}
	targetWeekStart, errTargetWeek := time.Parse("2006-01-02", trimmedTargetWeek)
	currentWeekStart, errCurrentWeek := time.Parse("2006-01-02", strings.TrimSpace(currentWeek))
	currentDateValue, errCurrentDate := time.Parse("2006-01-02", strings.TrimSpace(currentDate))
	if errTargetWeek != nil || errCurrentWeek != nil || errCurrentDate != nil {
		return dayRunHistoryHref(path, trimmedTargetWeek, trimmedTargetWeek, environments)
	}
	offset := int(currentDateValue.UTC().Sub(currentWeekStart.UTC()) / (24 * time.Hour))
	if offset < 0 || offset > 6 {
		return dayRunHistoryHref(path, trimmedTargetWeek, trimmedTargetWeek, environments)
	}
	targetDate := targetWeekStart.UTC().AddDate(0, 0, offset).Format("2006-01-02")
	return dayRunHistoryHref(path, targetDate, trimmedTargetWeek, environments)
}

func isSingleDayWindow(startDate string, endDate string) bool {
	trimmedStartDate := strings.TrimSpace(startDate)
	return trimmedStartDate != "" && trimmedStartDate == strings.TrimSpace(endDate)
}

func normalizedQueryEnvironments(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.ToLower(strings.TrimSpace(value))
		if trimmed == "" {
			continue
		}
		seen[trimmed] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func jobHistoryDayQueryFromRequest(r *http.Request) frontservice.JobHistoryDayQuery {
	if r == nil {
		return frontservice.JobHistoryDayQuery{}
	}
	return frontservice.JobHistoryDayQuery{
		Date:         strings.TrimSpace(r.URL.Query().Get("date")),
		Week:         strings.TrimSpace(r.URL.Query().Get("week")),
		Environments: parseListQueryValues(r.URL.Query()["env"]),
	}
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
