package review

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	frontservice "ci-failure-atlas/pkg/frontend/readmodel"
	triagehtml "ci-failure-atlas/pkg/frontend/ui"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	sourceoptions "ci-failure-atlas/pkg/source/options"
	storecontracts "ci-failure-atlas/pkg/store/contracts"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultHistoryWeeks     = 4
	selectionInputName      = "cluster_id"
	unlinkChildInputName    = "unlink_child"
	phase3ActionLink        = "link"
	phase3ActionDisband     = "disband"
	phase3ActionUnlink      = "unlink"
	phase3ActionUnlinkChild = "unlink_child"
	phase3ClusterIDPrefix   = "p3c-"
)

type HandlerOptions struct {
	DefaultWeek         string
	HistoryHorizonWeeks int
	PostgresPool        *pgxpool.Pool
	RoutePrefix         string
	ReportPath          string
	FailurePatternsPath string
}

type handler struct {
	service             *frontservice.Service
	historyHorizonWeeks int
	routePrefix         string
	reportPath          string
	failurePatternsPath string
}

type phase3Anchor = frontservice.ReviewPhase3Anchor
type weekSnapshot = frontservice.ReviewWeekSnapshot

type apiWeekRunReference struct {
	RunURL      string `json:"run_url"`
	OccurredAt  string `json:"occurred_at"`
	SignatureID string `json:"signature_id"`
	PRNumber    int    `json:"pr_number"`
}

type apiWeekContributingTest struct {
	Lane         string `json:"lane"`
	JobName      string `json:"job_name"`
	TestName     string `json:"test_name"`
	SupportCount int    `json:"support_count"`
}

type apiWeekRow struct {
	Environment         string                    `json:"environment"`
	Lane                string                    `json:"lane"`
	JobName             string                    `json:"job_name"`
	TestName            string                    `json:"test_name"`
	TestSuite           string                    `json:"test_suite"`
	Phrase              string                    `json:"phrase"`
	ClusterID           string                    `json:"cluster_id"`
	SelectionID         string                    `json:"selection_id"`
	SearchQuery         string                    `json:"search_query"`
	SupportCount        int                       `json:"support_count"`
	TrendSparkline      string                    `json:"trend_sparkline"`
	TrendCounts         []int                     `json:"trend_counts,omitempty"`
	TrendRange          string                    `json:"trend_range"`
	SupportShare        float64                   `json:"support_share"`
	PostGoodCount       int                       `json:"post_good_count"`
	AlsoSeenIn          []string                  `json:"also_seen_in,omitempty"`
	QualityScore        int                       `json:"quality_score"`
	QualityFlags        []string                  `json:"quality_flags,omitempty"`
	ReviewFlags         []string                  `json:"review_flags,omitempty"`
	ContributingTests   []apiWeekContributingTest `json:"contributing_tests,omitempty"`
	FullErrorSamples    []string                  `json:"full_error_samples,omitempty"`
	References          []apiWeekRunReference     `json:"references,omitempty"`
	ScoringReferences   []apiWeekRunReference     `json:"scoring_references,omitempty"`
	PriorWeeksPresent   int                       `json:"prior_weeks_present"`
	PriorWeekStarts     []string                  `json:"prior_week_starts,omitempty"`
	PriorJobsAffected   int                       `json:"prior_jobs_affected"`
	PriorLastSeenAt     string                    `json:"prior_last_seen_at"`
	Phase3ClusterID     string                    `json:"phase3_cluster_id"`
	ManualIssueConflict bool                      `json:"manual_issue_conflict"`
	IsLinked            bool                      `json:"is_linked"`
	LinkedChildren      []apiWeekRow              `json:"linked_children,omitempty"`
}

type apiWeekResponse struct {
	Week                 string         `json:"week"`
	PreviousWeek         string         `json:"previous_week"`
	NextWeek             string         `json:"next_week"`
	Timezone             string         `json:"timezone"`
	TotalClusters        int            `json:"total_clusters"`
	AnchoredClusters     int            `json:"anchored_clusters"`
	MissingAnchorCount   int            `json:"missing_anchor_count"`
	UnassignedQueueCount int            `json:"unassigned_queue_count"`
	OverallJobsByEnv     map[string]int `json:"overall_jobs_by_environment"`
	Rows                 []apiWeekRow   `json:"rows"`
}

type actionResponse struct {
	OK                     bool     `json:"ok"`
	Week                   string   `json:"week,omitempty"`
	Timezone               string   `json:"timezone,omitempty"`
	Action                 string   `json:"action,omitempty"`
	Notice                 string   `json:"notice,omitempty"`
	RedirectURL            string   `json:"redirect_url,omitempty"`
	SelectedClusterIDs     []string `json:"selected_cluster_ids,omitempty"`
	Phase3ClusterID        string   `json:"phase3_cluster_id,omitempty"`
	Created                bool     `json:"created,omitempty"`
	SelectedAnchorCount    int      `json:"selected_anchor_count,omitempty"`
	CrossWeekAnchorCount   int      `json:"cross_week_anchor_count,omitempty"`
	TotalAnchorCount       int      `json:"total_anchor_count,omitempty"`
	UnlinkedAnchorCount    int      `json:"unlinked_anchor_count,omitempty"`
	AggregatedClusterCount int      `json:"aggregated_cluster_count,omitempty"`
}

type actionErrorResponse struct {
	Error       string `json:"error"`
	Week        string `json:"week,omitempty"`
	Timezone    string `json:"timezone,omitempty"`
	Action      string `json:"action,omitempty"`
	RedirectURL string `json:"redirect_url,omitempty"`
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

	h := &handler{
		service:             service,
		historyHorizonWeeks: service.HistoryHorizonWeeks(),
		routePrefix:         normalizeRoutePrefix(opts.RoutePrefix),
		reportPath:          normalizeAbsolutePath(opts.ReportPath),
		failurePatternsPath: normalizeAbsolutePath(opts.FailurePatternsPath),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", h.handleRoot)
	mux.HandleFunc("/actions/links", h.handleLinksAction)
	mux.HandleFunc("/api/weeks", h.handleAPIWeeks)
	mux.HandleFunc("/api/week", h.handleAPIWeek)
	return mux, nil
}

func (h *handler) openStoreForWeek(week string) (storecontracts.Store, error) {
	return h.service.OpenStoreForWeek(week)
}

func (h *handler) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snapshot, err := h.loadWeekSnapshot(r.Context(), strings.TrimSpace(r.URL.Query().Get("week")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	notice := strings.TrimSpace(r.URL.Query().Get("notice"))
	rendered := h.renderPage(snapshot, notice)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(rendered))
}

func (h *handler) handleLinksAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		if h.prefersJSONResponse(r) {
			writeJSON(w, http.StatusMethodNotAllowed, actionErrorResponse{Error: "method not allowed"})
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		h.respondActionError(w, r, http.StatusBadRequest, "", "", "invalid form payload")
		return
	}
	requestedWeek := strings.TrimSpace(r.FormValue("week"))
	snapshot, err := h.loadWeekSnapshot(r.Context(), requestedWeek)
	if err != nil {
		h.respondActionError(w, r, http.StatusBadRequest, requestedWeek, "", err.Error())
		return
	}
	action := strings.ToLower(strings.TrimSpace(r.FormValue("action")))
	selectedClusterIDs := normalizeStringSlice(r.Form[selectionInputName])
	if childSelection := strings.TrimSpace(r.FormValue(unlinkChildInputName)); childSelection != "" {
		action = phase3ActionUnlinkChild
		selectedClusterIDs = []string{childSelection}
	}
	if len(selectedClusterIDs) == 0 {
		switch action {
		case phase3ActionDisband:
			h.respondActionError(w, r, http.StatusBadRequest, snapshot.Week, action, "select at least one aggregated phase3 cluster to disband")
		case phase3ActionUnlinkChild:
			h.respondActionError(w, r, http.StatusBadRequest, snapshot.Week, action, "select a linked signature to remove from the cluster")
		default:
			h.respondActionError(w, r, http.StatusBadRequest, snapshot.Week, action, "select at least one cluster")
		}
		return
	}

	store, err := h.openStoreForWeek(snapshot.Week)
	if err != nil {
		h.respondActionError(w, r, http.StatusInternalServerError, snapshot.Week, action, fmt.Sprintf("open semantic store: %v", err))
		return
	}
	defer func() {
		_ = store.Close()
	}()

	switch action {
	case phase3ActionLink:
		anchors := selectedAnchors(snapshot.AnchorsByClusterID, selectedClusterIDs)
		if len(anchors) == 0 {
			h.respondActionError(w, r, http.StatusBadRequest, snapshot.Week, action, "selected clusters do not have row_id anchors yet; rerun semantic workflow and refresh")
			return
		}
		selectedLanes := selectedLaneKeys(snapshot.LaneKeysByClusterID, selectedClusterIDs)
		if len(selectedLanes) > 1 {
			h.respondActionError(
				w,
				r,
				http.StatusBadRequest,
				snapshot.Week,
				action,
				fmt.Sprintf(
					"selected clusters span multiple lanes (%s); linking across lanes is not allowed",
					strings.Join(selectedLanes, ", "),
				),
			)
			return
		}
		matchKeys := signatureMatchKeysForSelectedClusters(snapshot.Rows, selectedClusterIDs)
		windowWeeks := resolveReconcileWindowWeeks(snapshot.Weeks, h.historyHorizonWeeks)
		windowAnchors, err := h.collectAnchorsForSignatureMatchKeys(r.Context(), windowWeeks, matchKeys)
		if err != nil {
			h.respondActionError(w, r, http.StatusInternalServerError, snapshot.Week, action, fmt.Sprintf("link selected: %v", err))
			return
		}
		expandedAnchors := dedupeAnchors(append(append([]phase3Anchor{}, anchors...), windowAnchors...))

		phase3ClusterID, created, err := resolvePhase3ClusterIDForAnchors(r.Context(), store, expandedAnchors)
		if err != nil {
			h.respondActionError(w, r, http.StatusInternalServerError, snapshot.Week, action, fmt.Sprintf("link selected: %v", err))
			return
		}
		if err := linkAnchors(r.Context(), store, phase3ClusterID, expandedAnchors); err != nil {
			h.respondActionError(w, r, http.StatusInternalServerError, snapshot.Week, action, fmt.Sprintf("link selected: %v", err))
			return
		}
		crossWeekCount := len(expandedAnchors) - len(anchors)
		if crossWeekCount < 0 {
			crossWeekCount = 0
		}
		notice := fmt.Sprintf(
			"linked %d anchors (%d selected + %d cross-week) into existing phase3 cluster %s",
			len(expandedAnchors),
			len(anchors),
			crossWeekCount,
			phase3ClusterID,
		)
		if created {
			notice = fmt.Sprintf(
				"linked %d anchors (%d selected + %d cross-week) using new phase3 cluster %s",
				len(expandedAnchors),
				len(anchors),
				crossWeekCount,
				phase3ClusterID,
			)
		}
		h.respondActionSuccess(w, r, actionResponse{
			Week:                 snapshot.Week,
			Action:               action,
			Notice:               notice,
			SelectedClusterIDs:   append([]string(nil), selectedClusterIDs...),
			Phase3ClusterID:      phase3ClusterID,
			Created:              created,
			SelectedAnchorCount:  len(anchors),
			CrossWeekAnchorCount: crossWeekCount,
			TotalAnchorCount:     len(expandedAnchors),
		})
		return
	case phase3ActionDisband:
		aggregatedSelections := filterAggregatedSelectionIDs(snapshot.AggregatedSelection, selectedClusterIDs)
		if len(aggregatedSelections) == 0 {
			h.respondActionError(w, r, http.StatusBadRequest, snapshot.Week, action, "disband cluster only works on aggregated phase3 rows")
			return
		}
		anchors := selectedAnchors(snapshot.AnchorsByClusterID, aggregatedSelections)
		if len(anchors) == 0 {
			h.respondActionError(w, r, http.StatusBadRequest, snapshot.Week, action, "selected aggregated clusters do not have row_id anchors yet; rerun semantic workflow and refresh")
			return
		}
		if err := unlinkAnchors(r.Context(), store, anchors); err != nil {
			h.respondActionError(w, r, http.StatusInternalServerError, snapshot.Week, action, fmt.Sprintf("disband cluster: %v", err))
			return
		}
		h.respondActionSuccess(w, r, actionResponse{
			Week:                   snapshot.Week,
			Action:                 action,
			Notice:                 fmt.Sprintf("disbanded %d aggregated cluster(s) and unlinked %d anchors", len(aggregatedSelections), len(anchors)),
			SelectedClusterIDs:     append([]string(nil), aggregatedSelections...),
			AggregatedClusterCount: len(aggregatedSelections),
			UnlinkedAnchorCount:    len(anchors),
		})
		return
	case phase3ActionUnlinkChild, phase3ActionUnlink:
		anchors := selectedAnchors(snapshot.AnchorsByClusterID, selectedClusterIDs)
		if len(anchors) == 0 {
			h.respondActionError(w, r, http.StatusBadRequest, snapshot.Week, action, "selected signatures do not have row_id anchors yet; rerun semantic workflow and refresh")
			return
		}
		if err := unlinkAnchors(r.Context(), store, anchors); err != nil {
			h.respondActionError(w, r, http.StatusInternalServerError, snapshot.Week, action, fmt.Sprintf("unlink selected: %v", err))
			return
		}
		if action == phase3ActionUnlinkChild {
			h.respondActionSuccess(w, r, actionResponse{
				Week:                snapshot.Week,
				Action:              action,
				Notice:              fmt.Sprintf("removed linked signature and unlinked %d anchors", len(anchors)),
				SelectedClusterIDs:  append([]string(nil), selectedClusterIDs...),
				UnlinkedAnchorCount: len(anchors),
			})
			return
		}
		h.respondActionSuccess(w, r, actionResponse{
			Week:                snapshot.Week,
			Action:              action,
			Notice:              fmt.Sprintf("unlinked %d anchors", len(anchors)),
			SelectedClusterIDs:  append([]string(nil), selectedClusterIDs...),
			UnlinkedAnchorCount: len(anchors),
		})
		return
	default:
		h.respondActionError(w, r, http.StatusBadRequest, snapshot.Week, action, "unsupported action")
		return
	}
}

func (h *handler) handleAPIWeeks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	weeks, err := h.discoverSemanticWeeks(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	window, err := h.service.ResolveWeekWindow(r.Context(), strings.TrimSpace(r.URL.Query().Get("week")), time.Time{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"weeks":         weeks,
		"current_week":  window.CurrentWeek,
		"previous_week": window.PreviousWeek,
		"next_week":     window.NextWeek,
		"timezone":      "UTC",
	})
}

func (h *handler) handleAPIWeek(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snapshot, err := h.loadWeekSnapshot(r.Context(), strings.TrimSpace(r.URL.Query().Get("week")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, apiWeekResponse{
		Week:                 snapshot.Week,
		PreviousWeek:         snapshot.PreviousWeek,
		NextWeek:             snapshot.NextWeek,
		Timezone:             "UTC",
		TotalClusters:        snapshot.TotalClusters,
		AnchoredClusters:     snapshot.AnchoredClusterCount,
		MissingAnchorCount:   snapshot.MissingAnchorCount,
		UnassignedQueueCount: snapshot.UnassignedCount,
		OverallJobsByEnv:     reviewCloneIntMap(snapshot.OverallJobsByEnv),
		Rows:                 reviewAPIRows(snapshot.Rows),
	})
}

func (h *handler) redirectWithNotice(w http.ResponseWriter, r *http.Request, week string, notice string) {
	http.Redirect(w, r, h.noticeRedirectURL(week, notice), http.StatusSeeOther)
}

func (h *handler) loadWeekSnapshot(ctx context.Context, requestedWeek string) (weekSnapshot, error) {
	return h.service.BuildReviewWeek(ctx, requestedWeek)
}

func (h *handler) discoverSemanticWeeks(ctx context.Context) ([]string, error) {
	return h.service.DiscoverSemanticWeeks(ctx)
}

func (h *handler) respondActionSuccess(w http.ResponseWriter, r *http.Request, payload actionResponse) {
	payload.OK = true
	payload.Week = strings.TrimSpace(payload.Week)
	payload.Timezone = "UTC"
	payload.Action = strings.TrimSpace(payload.Action)
	payload.Notice = strings.TrimSpace(payload.Notice)
	payload.RedirectURL = h.noticeRedirectURL(payload.Week, payload.Notice)
	if h.prefersJSONResponse(r) {
		writeJSON(w, http.StatusOK, payload)
		return
	}
	h.redirectWithNotice(w, r, payload.Week, payload.Notice)
}

func (h *handler) respondActionError(
	w http.ResponseWriter,
	r *http.Request,
	statusCode int,
	week string,
	action string,
	message string,
) {
	trimmedWeek := strings.TrimSpace(week)
	trimmedAction := strings.TrimSpace(action)
	trimmedMessage := strings.TrimSpace(message)
	if h.prefersJSONResponse(r) {
		writeJSON(w, statusCode, actionErrorResponse{
			Error:       trimmedMessage,
			Week:        trimmedWeek,
			Timezone:    "UTC",
			Action:      trimmedAction,
			RedirectURL: h.noticeRedirectURL(trimmedWeek, trimmedMessage),
		})
		return
	}
	h.redirectWithNotice(w, r, trimmedWeek, trimmedMessage)
}

func (h *handler) prefersJSONResponse(r *http.Request) bool {
	accept := strings.ToLower(strings.TrimSpace(r.Header.Get("Accept")))
	if strings.Contains(accept, "text/html") || strings.Contains(accept, "application/xhtml+xml") {
		return false
	}
	return true
}

func (h *handler) noticeRedirectURL(week string, notice string) string {
	q := url.Values{}
	if strings.TrimSpace(week) != "" {
		q.Set("week", strings.TrimSpace(week))
	}
	if strings.TrimSpace(notice) != "" {
		q.Set("notice", strings.TrimSpace(notice))
	}
	target := h.routePath("/")
	if encoded := q.Encode(); encoded != "" {
		target += "?" + encoded
	}
	return target
}

func reviewAPIRows(rows []triagehtml.FailurePatternRow) []apiWeekRow {
	if len(rows) == 0 {
		return nil
	}
	out := make([]apiWeekRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, reviewAPIRow(row))
	}
	return out
}

func reviewAPIRow(row triagehtml.FailurePatternRow) apiWeekRow {
	return apiWeekRow{
		Environment:         row.Environment,
		Lane:                row.FailedAt,
		JobName:             row.JobName,
		TestName:            row.TestName,
		TestSuite:           row.TestSuite,
		Phrase:              row.FailurePattern,
		ClusterID:           row.FailurePatternID,
		SelectionID:         row.SelectionValue,
		SearchQuery:         row.SearchQuery,
		SupportCount:        row.Occurrences,
		TrendSparkline:      row.TrendSparkline,
		TrendCounts:         append([]int(nil), row.TrendCounts...),
		TrendRange:          row.TrendRange,
		SupportShare:        row.OccurrenceShare,
		PostGoodCount:       row.AfterLastPushCount,
		AlsoSeenIn:          append([]string(nil), row.AlsoIn...),
		QualityScore:        row.QualityScore,
		QualityFlags:        append([]string(nil), row.QualityNoteLabels...),
		ReviewFlags:         append([]string(nil), row.ReviewNoteLabels...),
		ContributingTests:   reviewAPIContributingTests(row.ContributingTests),
		FullErrorSamples:    append([]string(nil), row.FullErrorSamples...),
		References:          reviewAPIRunReferences(row.AffectedRuns),
		ScoringReferences:   reviewAPIRunReferences(row.ScoringReferences),
		PriorWeeksPresent:   row.PriorWeeksPresent,
		PriorWeekStarts:     append([]string(nil), row.PriorWeekStarts...),
		PriorJobsAffected:   row.PriorRunsAffected,
		PriorLastSeenAt:     row.PriorLastSeenAt,
		Phase3ClusterID:     row.ManualIssueID,
		ManualIssueConflict: row.ManualIssueConflict,
		IsLinked:            strings.TrimSpace(row.ManualIssueID) != "",
		LinkedChildren:      reviewAPIRows(row.LinkedPatterns),
	}
}

func reviewAPIContributingTests(rows []triagehtml.ContributingTest) []apiWeekContributingTest {
	if len(rows) == 0 {
		return nil
	}
	out := make([]apiWeekContributingTest, 0, len(rows))
	for _, row := range rows {
		out = append(out, apiWeekContributingTest{
			Lane:         row.FailedAt,
			JobName:      row.JobName,
			TestName:     row.TestName,
			SupportCount: row.Occurrences,
		})
	}
	return out
}

func reviewAPIRunReferences(rows []triagehtml.RunReference) []apiWeekRunReference {
	if len(rows) == 0 {
		return nil
	}
	out := make([]apiWeekRunReference, 0, len(rows))
	for _, row := range rows {
		out = append(out, apiWeekRunReference{
			RunURL:      row.RunURL,
			OccurredAt:  row.OccurredAt,
			SignatureID: row.SignatureID,
			PRNumber:    row.PRNumber,
		})
	}
	return out
}

func reviewCloneIntMap(source map[string]int) map[string]int {
	if len(source) == 0 {
		return map[string]int{}
	}
	out := make(map[string]int, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func resolveReconcileWindowWeeks(weeks []string, reconcileWeeks int) []string {
	limit := reconcileWeeks
	if limit <= 0 {
		limit = defaultHistoryWeeks
	}
	validWeeks := make([]string, 0, len(weeks))
	for _, week := range weeks {
		trimmed := strings.TrimSpace(week)
		if trimmed == "" {
			continue
		}
		if _, ok := parseSemanticWeek(trimmed); !ok {
			continue
		}
		validWeeks = append(validWeeks, trimmed)
	}
	sort.Strings(validWeeks)
	if len(validWeeks) <= limit {
		return validWeeks
	}
	return append([]string(nil), validWeeks[len(validWeeks)-limit:]...)
}

func parseSemanticWeek(value string) (time.Time, bool) {
	parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func signatureMatchKeysForSelectedClusters(
	rows []triagehtml.FailurePatternRow,
	selectedClusterIDs []string,
) map[string]struct{} {
	selected := map[string]struct{}{}
	for _, clusterID := range selectedClusterIDs {
		trimmed := strings.TrimSpace(clusterID)
		if trimmed == "" {
			continue
		}
		selected[trimmed] = struct{}{}
	}
	keys := map[string]struct{}{}
	for _, row := range rows {
		if !rowMatchesSelection(row, selected) {
			for _, child := range row.LinkedPatterns {
				if !rowMatchesSelection(child, selected) {
					continue
				}
				key := signatureMatchKey(child.Environment, child.FailedAt, child.FailurePattern)
				if key == "" {
					continue
				}
				keys[key] = struct{}{}
			}
			continue
		}
		key := signatureMatchKey(row.Environment, row.FailedAt, row.FailurePattern)
		if key == "" {
			continue
		}
		keys[key] = struct{}{}
	}
	return keys
}

func rowMatchesSelection(row triagehtml.FailurePatternRow, selected map[string]struct{}) bool {
	selectionID := strings.TrimSpace(row.SelectionValue)
	if selectionID != "" {
		if _, include := selected[selectionID]; include {
			return true
		}
	}
	clusterID := strings.TrimSpace(row.FailurePatternID)
	if clusterID == "" {
		return false
	}
	_, include := selected[clusterID]
	return include
}

func signatureMatchKey(environment string, lane string, phrase string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	normalizedLane := normalizeLaneKey(lane)
	normalizedPhrase := normalizePhraseForMatching(phrase)
	if normalizedEnvironment == "" || normalizedLane == "" || normalizedPhrase == "" {
		return ""
	}
	return normalizedEnvironment + "|lane:" + normalizedLane + "|phrase:" + normalizedPhrase
}

func normalizePhraseForMatching(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(strings.Join(strings.Fields(trimmed), " "))
}

func (h *handler) collectAnchorsForSignatureMatchKeys(
	ctx context.Context,
	weeks []string,
	matchKeys map[string]struct{},
) ([]phase3Anchor, error) {
	if len(matchKeys) == 0 || len(weeks) == 0 {
		return nil, nil
	}
	collected := make([]phase3Anchor, 0, len(weeks)*4)
	for _, week := range weeks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		store, err := h.openStoreForWeek(week)
		if err != nil {
			return nil, fmt.Errorf("open semantic store for reconcile week %q: %w", week, err)
		}
		clusters, listErr := store.ListFailurePatterns(ctx)
		_ = store.Close()
		if listErr != nil {
			return nil, fmt.Errorf("list phase2 clusters for reconcile week %q: %w", week, listErr)
		}
		for _, cluster := range clusters {
			environment := normalizeEnvironment(cluster.Environment)
			clusterLaneKeys := laneKeysForContributingTests(cluster.ContributingTests)
			matched := false
			for _, laneKey := range clusterLaneKeys {
				matchKey := signatureMatchKey(environment, laneKey, cluster.CanonicalEvidencePhrase)
				if _, ok := matchKeys[matchKey]; ok {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			collected = append(collected, anchorsForCluster(environment, cluster.References)...)
		}
	}
	return collected, nil
}

func dedupeAnchors(anchors []phase3Anchor) []phase3Anchor {
	set := map[string]phase3Anchor{}
	for _, anchor := range anchors {
		key := anchor.Key()
		if key == "" {
			continue
		}
		set[key] = anchor
	}
	out := make([]phase3Anchor, 0, len(set))
	for _, anchor := range set {
		out = append(out, anchor)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Environment != out[j].Environment {
			return out[i].Environment < out[j].Environment
		}
		if out[i].RunURL != out[j].RunURL {
			return out[i].RunURL < out[j].RunURL
		}
		return out[i].RowID < out[j].RowID
	})
	return out
}

func resolvePhase3ClusterIDForAnchors(ctx context.Context, store storecontracts.Store, anchors []phase3Anchor) (string, bool, error) {
	existingLinks, err := store.ListPhase3Links(ctx)
	if err != nil {
		return "", false, fmt.Errorf("list phase3 links: %w", err)
	}
	existingClusterByAnchor := map[string]string{}
	for _, row := range existingLinks {
		anchor := phase3Anchor{
			Environment: row.Environment,
			RunURL:      row.RunURL,
			RowID:       row.RowID,
		}
		key := anchor.Key()
		if key == "" {
			continue
		}
		phase3ClusterID := strings.TrimSpace(row.IssueID)
		if phase3ClusterID == "" {
			continue
		}
		existingClusterByAnchor[key] = phase3ClusterID
	}
	clusterIDs := phase3ClusterIDsForAnchors(anchors, existingClusterByAnchor)
	switch len(clusterIDs) {
	case 0:
		return newPhase3ClusterID(anchors), true, nil
	case 1:
		return clusterIDs[0], false, nil
	default:
		return "", false, fmt.Errorf("selected signatures are already linked to different phase3 clusters (%s)", strings.Join(clusterIDs, ", "))
	}
}

func linkAnchors(ctx context.Context, store storecontracts.Store, phase3ClusterID string, anchors []phase3Anchor) error {
	now := time.Now().UTC().Format(time.RFC3339)
	normalizedClusterID := strings.TrimSpace(phase3ClusterID)
	if normalizedClusterID == "" {
		return fmt.Errorf("phase3 cluster id is required")
	}
	existingLinks, err := store.ListPhase3Links(ctx)
	if err != nil {
		return fmt.Errorf("list phase3 links: %w", err)
	}
	existingClusterByAnchor := map[string]string{}
	for _, row := range existingLinks {
		anchor := phase3Anchor{
			Environment: row.Environment,
			RunURL:      row.RunURL,
			RowID:       row.RowID,
		}
		key := anchor.Key()
		if key == "" {
			continue
		}
		phase3Cluster := strings.TrimSpace(row.IssueID)
		if phase3Cluster == "" {
			continue
		}
		existingClusterByAnchor[key] = phase3Cluster
	}
	for _, anchor := range anchors {
		key := anchor.Key()
		if key == "" {
			continue
		}
		existingCluster := strings.TrimSpace(existingClusterByAnchor[key])
		if existingCluster != "" && existingCluster != normalizedClusterID {
			return fmt.Errorf(
				"hard failure: anchor %s is already linked to phase3 cluster %s (cannot relink to %s)",
				key,
				existingCluster,
				normalizedClusterID,
			)
		}
	}

	existingIssues, err := store.ListPhase3Issues(ctx)
	if err != nil {
		return fmt.Errorf("list phase3 issues: %w", err)
	}
	createdAt := now
	for _, row := range existingIssues {
		if strings.TrimSpace(row.IssueID) != normalizedClusterID {
			continue
		}
		if strings.TrimSpace(row.CreatedAt) != "" {
			createdAt = strings.TrimSpace(row.CreatedAt)
		}
		break
	}
	if err := store.UpsertPhase3Issues(ctx, []semanticcontracts.Phase3IssueRecord{
		{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       normalizedClusterID,
			CreatedAt:     createdAt,
			UpdatedAt:     now,
		},
	}); err != nil {
		return fmt.Errorf("upsert phase3 issue: %w", err)
	}

	linkRows := make([]semanticcontracts.Phase3LinkRecord, 0, len(anchors))
	eventRows := make([]semanticcontracts.Phase3EventRecord, 0, len(anchors))
	for _, anchor := range anchors {
		key := anchor.Key()
		if key == "" {
			continue
		}
		linkRows = append(linkRows, semanticcontracts.Phase3LinkRecord{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			IssueID:       normalizedClusterID,
			Environment:   normalizeEnvironment(anchor.Environment),
			RunURL:        strings.TrimSpace(anchor.RunURL),
			RowID:         strings.TrimSpace(anchor.RowID),
			UpdatedAt:     now,
		})
		eventRows = append(eventRows, semanticcontracts.Phase3EventRecord{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			EventID:       phase3EventID(phase3ActionLink, normalizedClusterID, anchor, now),
			Action:        phase3ActionLink,
			IssueID:       normalizedClusterID,
			Environment:   normalizeEnvironment(anchor.Environment),
			RunURL:        strings.TrimSpace(anchor.RunURL),
			RowID:         strings.TrimSpace(anchor.RowID),
			At:            now,
		})
	}
	if len(linkRows) == 0 {
		return nil
	}
	if err := store.UpsertPhase3Links(ctx, linkRows); err != nil {
		return fmt.Errorf("upsert phase3 links: %w", err)
	}
	if err := store.AppendPhase3Events(ctx, eventRows); err != nil {
		return fmt.Errorf("append phase3 events: %w", err)
	}
	return nil
}

func unlinkAnchors(ctx context.Context, store storecontracts.Store, anchors []phase3Anchor) error {
	now := time.Now().UTC().Format(time.RFC3339)
	existingLinks, err := store.ListPhase3Links(ctx)
	if err != nil {
		return fmt.Errorf("list phase3 links: %w", err)
	}
	existingClusterByAnchor := map[string]string{}
	for _, row := range existingLinks {
		anchor := phase3Anchor{
			Environment: row.Environment,
			RunURL:      row.RunURL,
			RowID:       row.RowID,
		}
		key := anchor.Key()
		if key == "" {
			continue
		}
		existingClusterByAnchor[key] = strings.TrimSpace(row.IssueID)
	}
	deleteRows := make([]semanticcontracts.Phase3LinkRecord, 0, len(anchors))
	eventRows := make([]semanticcontracts.Phase3EventRecord, 0, len(anchors))
	for _, anchor := range anchors {
		key := anchor.Key()
		if key == "" {
			continue
		}
		phase3ClusterID := strings.TrimSpace(existingClusterByAnchor[key])
		deleteRows = append(deleteRows, semanticcontracts.Phase3LinkRecord{
			Environment: normalizeEnvironment(anchor.Environment),
			RunURL:      strings.TrimSpace(anchor.RunURL),
			RowID:       strings.TrimSpace(anchor.RowID),
		})
		eventRows = append(eventRows, semanticcontracts.Phase3EventRecord{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			EventID:       phase3EventID(phase3ActionUnlink, phase3ClusterID, anchor, now),
			Action:        phase3ActionUnlink,
			IssueID:       phase3ClusterID,
			Environment:   normalizeEnvironment(anchor.Environment),
			RunURL:        strings.TrimSpace(anchor.RunURL),
			RowID:         strings.TrimSpace(anchor.RowID),
			At:            now,
		})
	}
	if len(deleteRows) == 0 {
		return nil
	}
	if err := store.DeletePhase3Links(ctx, deleteRows); err != nil {
		return fmt.Errorf("delete phase3 links: %w", err)
	}
	if err := store.AppendPhase3Events(ctx, eventRows); err != nil {
		return fmt.Errorf("append phase3 events: %w", err)
	}
	return nil
}

func (h *handler) renderPage(snapshot weekSnapshot, notice string) string {
	previousHref := ""
	nextHref := ""
	if strings.TrimSpace(snapshot.PreviousWeek) != "" {
		previousHref = h.viewHref(h.routePath("/"), snapshot.PreviousWeek)
	}
	if strings.TrimSpace(snapshot.NextWeek) != "" {
		nextHref = h.viewHref(h.routePath("/"), snapshot.NextWeek)
	}
	chrome := triagehtml.ReportChromeHTML(triagehtml.ReportChromeOptions{
		CurrentWeek:         snapshot.Week,
		PreviousWeek:        snapshot.PreviousWeek,
		PreviousHref:        previousHref,
		NextWeek:            snapshot.NextWeek,
		NextHref:            nextHref,
		ReportHref:          h.viewHref(h.reportPath, snapshot.Week),
		FailurePatternsHref: h.viewHref(h.failurePatternsPath, snapshot.Week),
	})

	var b strings.Builder
	b.WriteString("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Failure Atlas - Semantic Review</title>\n")
	b.WriteString(triagehtml.ThemeInitScriptTag())
	b.WriteString("  <style>\n")
	b.WriteString(triagehtml.StylesCSS())
	b.WriteString("\n")
	b.WriteString(triagehtml.ReportChromeCSS())
	b.WriteString(triagehtml.ThemeCSS())
	b.WriteString("\n")
	b.WriteString(strings.Join([]string{
		"    body { font-family: Arial, sans-serif; margin: 20px; color: #1f2937; }",
		"    h1 { margin: 0 0 8px; font-size: 24px; }",
		"    .muted { color: #6b7280; }",
		"    .phase3-shell { max-width: 1400px; margin: 0 auto; }",
		"    .phase3-top { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin: 8px 0 14px; }",
		"    .phase3-controls { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin: 10px 0 12px; }",
		"    .phase3-controls input[type=\"text\"] { min-width: 180px; padding: 6px 8px; border: 1px solid #cbd5e1; border-radius: 6px; }",
		"    .phase3-controls button, .phase3-controls a { padding: 6px 10px; border-radius: 6px; border: 1px solid #cbd5e1; background: #fff; color: #111; text-decoration: none; cursor: pointer; }",
		"    .phase3-controls button.primary { background: #2563eb; border-color: #2563eb; color: #fff; }",
		"    .phase3-summary { display: flex; gap: 10px; flex-wrap: wrap; margin: 8px 0 14px; }",
		"    .phase3-card { border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px 12px; min-width: 180px; background: #f8fafc; }",
		"    .phase3-notice { border: 1px solid #dbeafe; background: #eff6ff; padding: 8px 10px; border-radius: 8px; margin: 10px 0; }",
		"    .phase3-environment-section { border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; margin: 12px 0; }",
		"    .phase3-environment-section h2 { margin: 0 0 8px; font-size: 18px; }",
		"    .phase3-environment-note { margin: 0 0 10px; font-size: 12px; color: #6b7280; }",
		"    .phase3-api-links { margin-top: 10px; font-size: 12px; }",
		"    :root[data-theme=\"dark\"] .phase3-card { background: #111827; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .phase3-notice { background: #0b2440; border-color: #1d4ed8; }",
		"    :root[data-theme=\"dark\"] .phase3-environment-section { border-color: #334155; background: #0f172a; }",
		"    :root[data-theme=\"dark\"] .phase3-environment-note { color: #94a3b8; }",
		"    :root[data-theme=\"dark\"] .phase3-controls input[type=\"text\"], :root[data-theme=\"dark\"] .phase3-controls button, :root[data-theme=\"dark\"] .phase3-controls a { background: #111827; color: #e5e7eb; border-color: #334155; }",
		"    :root[data-theme=\"dark\"] .phase3-controls button.primary { background: #1d4ed8; border-color: #1d4ed8; color: #e2e8f0; }",
	}, "\n"))
	b.WriteString("\n  </style>\n</head>\n<body>\n")
	b.WriteString("  <div class=\"phase3-shell\">\n")
	b.WriteString("    <h1>Semantic Review (Phase3)</h1>\n")
	b.WriteString("    <p class=\"muted\">Human-only linking with durable row-level anchors. Rematerialize weeks via CLI, then use Refresh here.</p>\n")
	b.WriteString(chrome)
	b.WriteString("    <div class=\"phase3-top\">\n")
	b.WriteString(fmt.Sprintf("      <div class=\"phase3-card\"><strong>Week</strong><br /><span class=\"muted\">%s</span></div>\n", html.EscapeString(snapshot.Week)))
	b.WriteString(fmt.Sprintf("      <div class=\"phase3-card\"><strong>Clusters</strong><br /><span class=\"muted\">%d total</span></div>\n", snapshot.TotalClusters))
	b.WriteString(fmt.Sprintf("      <div class=\"phase3-card\"><strong>Anchored</strong><br /><span class=\"muted\">%d with row_id</span></div>\n", snapshot.AnchoredClusterCount))
	b.WriteString(fmt.Sprintf("      <div class=\"phase3-card\"><strong>Missing anchors</strong><br /><span class=\"muted\">%d</span></div>\n", snapshot.MissingAnchorCount))
	b.WriteString("    </div>\n")
	if strings.TrimSpace(notice) != "" {
		b.WriteString(fmt.Sprintf("    <div class=\"phase3-notice\">%s</div>\n", html.EscapeString(notice)))
	}
	b.WriteString(fmt.Sprintf("    <form method=\"post\" action=\"%s\">\n", html.EscapeString(h.routePath("/actions/links"))))
	b.WriteString(fmt.Sprintf("      <input type=\"hidden\" name=\"week\" value=\"%s\" />\n", html.EscapeString(snapshot.Week)))
	b.WriteString("      <div class=\"phase3-controls\">\n")
	b.WriteString(fmt.Sprintf("        <button class=\"primary\" type=\"submit\" name=\"action\" value=\"%s\">Link selected</button>\n", phase3ActionLink))
	b.WriteString(fmt.Sprintf("        <button type=\"submit\" name=\"action\" value=\"%s\">Disband cluster</button>\n", phase3ActionDisband))
	b.WriteString(fmt.Sprintf("        <a href=\"%s\">Refresh</a>\n", html.EscapeString(h.viewHref(h.routePath("/"), snapshot.Week))))
	b.WriteString("        <button type=\"button\" id=\"phase3-select-all\">Select all</button>\n")
	b.WriteString("        <button type=\"button\" id=\"phase3-clear-all\">Clear selection</button>\n")
	b.WriteString("      </div>\n")
	rowsByEnvironment, orderedEnvironments := groupRowsByEnvironment(snapshot.Rows)
	if len(orderedEnvironments) == 0 {
		b.WriteString("      <p class=\"muted\">No signatures available for this week.</p>\n")
	} else {
		for _, environment := range orderedEnvironments {
			environmentRows := rowsByEnvironment[environment]
			b.WriteString(fmt.Sprintf("      <section id=\"%s\" class=\"phase3-environment-section\">\n", html.EscapeString("env-"+environment)))
			b.WriteString(fmt.Sprintf("        <h2>Environment: %s</h2>\n", html.EscapeString(strings.ToUpper(environment))))
			b.WriteString(fmt.Sprintf("        <p class=\"phase3-environment-note\">Rows loaded: %d</p>\n", len(environmentRows)))
			b.WriteString(triagehtml.RenderTable(environmentRows, triagehtml.TableOptions{
				ShowQualityScore:       true,
				ShowQualityFlags:       true,
				ShowReviewFlags:        true,
				ShowLinkedChildQuality: true,
				ShowLinkedChildReview:  true,
				ShowLinkedChildRemove:  true,
				ShowManualIssue:        true,
				ImpactTotalJobs:        snapshot.OverallJobsByEnv[environment],
				IncludeSelection:       true,
				SelectionInputName:     selectionInputName,
				LoadedRowsLimit:        -1,
				InitialVisibleRows:     -1,
			}))
			b.WriteString("      </section>\n")
		}
	}
	b.WriteString("    </form>\n")
	b.WriteString(fmt.Sprintf(
		"    <div class=\"phase3-api-links muted\">JSON: <a href=\"%s\">%s</a> | ",
		html.EscapeString(h.routePath("/api/weeks")),
		html.EscapeString(h.routePath("/api/weeks")),
	))
	b.WriteString(fmt.Sprintf(
		"<a href=\"%s\">%s</a></div>\n",
		html.EscapeString(h.viewHref(h.routePath("/api/week"), snapshot.Week)),
		html.EscapeString(h.routePath("/api/week?week="+snapshot.Week)),
	))
	b.WriteString("  </div>\n")
	b.WriteString(triagehtml.ThemeToggleScriptTag())
	b.WriteString("  <script>\n")
	b.WriteString(strings.Join([]string{
		"    (function() {",
		"      function rowCheckboxes() {",
		"        return Array.prototype.slice.call(document.querySelectorAll('.failure-patterns-row-select'));",
		"      }",
		"      var selectAll = document.getElementById('phase3-select-all');",
		"      if (selectAll) {",
		"        selectAll.addEventListener('click', function() {",
		"          rowCheckboxes().forEach(function(node) { node.checked = true; });",
		"        });",
		"      }",
		"      var clearAll = document.getElementById('phase3-clear-all');",
		"      if (clearAll) {",
		"        clearAll.addEventListener('click', function() {",
		"          rowCheckboxes().forEach(function(node) { node.checked = false; });",
		"        });",
		"      }",
		"    })();",
	}, "\n"))
	b.WriteString("\n  </script>\n")
	b.WriteString("</body>\n</html>\n")
	return b.String()
}

func groupRowsByEnvironment(rows []triagehtml.FailurePatternRow) (map[string][]triagehtml.FailurePatternRow, []string) {
	grouped := map[string][]triagehtml.FailurePatternRow{}
	for _, row := range rows {
		environment := normalizeEnvironment(row.Environment)
		if environment == "" {
			environment = "unknown"
		}
		grouped[environment] = append(grouped[environment], row)
	}
	ordered := orderedReviewEnvironments(grouped)
	return grouped, ordered
}

func orderedReviewEnvironments(grouped map[string][]triagehtml.FailurePatternRow) []string {
	fixedOrder := sourceoptions.SupportedEnvironments()
	set := map[string]struct{}{}
	for environment := range grouped {
		normalized := normalizeEnvironment(environment)
		if normalized == "" {
			normalized = "unknown"
		}
		set[normalized] = struct{}{}
	}
	ordered := make([]string, 0, len(set))
	for _, environment := range fixedOrder {
		if _, exists := set[environment]; !exists {
			continue
		}
		ordered = append(ordered, environment)
		delete(set, environment)
	}
	extras := make([]string, 0, len(set))
	for environment := range set {
		extras = append(extras, environment)
	}
	sort.Strings(extras)
	ordered = append(ordered, extras...)
	return ordered
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func anchorsForCluster(
	environment string,
	references []semanticcontracts.ReferenceRecord,
) []phase3Anchor {
	set := map[string]phase3Anchor{}
	for _, reference := range references {
		runURL := strings.TrimSpace(reference.RunURL)
		if runURL == "" {
			continue
		}
		rowID := strings.TrimSpace(reference.RowID)
		if rowID == "" {
			continue
		}
		anchor := phase3Anchor{
			Environment: environment,
			RunURL:      runURL,
			RowID:       rowID,
		}
		key := anchor.Key()
		if key == "" {
			continue
		}
		set[key] = anchor
	}
	out := make([]phase3Anchor, 0, len(set))
	for _, anchor := range set {
		out = append(out, anchor)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Environment != out[j].Environment {
			return out[i].Environment < out[j].Environment
		}
		if out[i].RunURL != out[j].RunURL {
			return out[i].RunURL < out[j].RunURL
		}
		return out[i].RowID < out[j].RowID
	})
	return out
}

func phase3ClusterIDsForAnchors(anchors []phase3Anchor, phase3ClusterByAnchor map[string]string) []string {
	set := map[string]struct{}{}
	for _, anchor := range anchors {
		phase3ClusterID := strings.TrimSpace(phase3ClusterByAnchor[anchor.Key()])
		if phase3ClusterID == "" {
			continue
		}
		set[phase3ClusterID] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for phase3ClusterID := range set {
		out = append(out, phase3ClusterID)
	}
	sort.Strings(out)
	return out
}

func filterAggregatedSelectionIDs(aggregated map[string]struct{}, selectedClusterIDs []string) []string {
	if len(selectedClusterIDs) == 0 || len(aggregated) == 0 {
		return nil
	}
	out := make([]string, 0, len(selectedClusterIDs))
	seen := map[string]struct{}{}
	for _, clusterID := range selectedClusterIDs {
		trimmed := strings.TrimSpace(clusterID)
		if trimmed == "" {
			continue
		}
		if _, ok := aggregated[trimmed]; !ok {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}

func selectedLaneKeys(laneKeysByClusterID map[string][]string, selectedClusterIDs []string) []string {
	set := map[string]struct{}{}
	for _, clusterID := range selectedClusterIDs {
		trimmedClusterID := strings.TrimSpace(clusterID)
		if trimmedClusterID == "" {
			continue
		}
		laneKeys := laneKeysByClusterID[trimmedClusterID]
		if len(laneKeys) == 0 {
			set[normalizeLaneKey("")] = struct{}{}
			continue
		}
		for _, laneKey := range laneKeys {
			set[normalizeLaneKey(laneKey)] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for laneKey := range set {
		out = append(out, laneKey)
	}
	sort.Strings(out)
	return out
}

func laneKeysForContributingTests(rows []semanticcontracts.ContributingTestRecord) []string {
	set := map[string]struct{}{}
	for _, row := range rows {
		set[normalizeLaneKey(row.Lane)] = struct{}{}
	}
	if len(set) == 0 {
		set[normalizeLaneKey("")] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for laneKey := range set {
		out = append(out, laneKey)
	}
	sort.Strings(out)
	return out
}

func selectedAnchors(anchorsByClusterID map[string][]phase3Anchor, selectedClusterIDs []string) []phase3Anchor {
	set := map[string]phase3Anchor{}
	for _, clusterID := range selectedClusterIDs {
		for _, anchor := range anchorsByClusterID[strings.TrimSpace(clusterID)] {
			key := anchor.Key()
			if key == "" {
				continue
			}
			set[key] = anchor
		}
	}
	out := make([]phase3Anchor, 0, len(set))
	for _, anchor := range set {
		out = append(out, anchor)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Environment != out[j].Environment {
			return out[i].Environment < out[j].Environment
		}
		if out[i].RunURL != out[j].RunURL {
			return out[i].RunURL < out[j].RunURL
		}
		return out[i].RowID < out[j].RowID
	})
	return out
}

func phase3EventID(action string, issueID string, anchor phase3Anchor, at string) string {
	seed := strings.Join([]string{
		strings.TrimSpace(action),
		strings.TrimSpace(issueID),
		normalizeEnvironment(anchor.Environment),
		strings.TrimSpace(anchor.RunURL),
		strings.TrimSpace(anchor.RowID),
		strings.TrimSpace(at),
	}, "|")
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

func newPhase3ClusterID(anchors []phase3Anchor) string {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	keys := make([]string, 0, len(anchors))
	for _, anchor := range anchors {
		key := anchor.Key()
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	seed := now + "|" + strings.Join(keys, ",")
	sum := sha256.Sum256([]byte(seed))
	return phase3ClusterIDPrefix + hex.EncodeToString(sum[:])[:12]
}

func normalizeRoutePrefix(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "/" {
		return ""
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	return strings.TrimRight(trimmed, "/")
}

func normalizeAbsolutePath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	return trimmed
}

func (h *handler) routePath(path string) string {
	normalizedPath := normalizeAbsolutePath(path)
	if normalizedPath == "" {
		normalizedPath = "/"
	}
	if h.routePrefix == "" {
		return normalizedPath
	}
	if normalizedPath == "/" {
		return h.routePrefix + "/"
	}
	return h.routePrefix + normalizedPath
}

func (h *handler) viewHref(path string, week string) string {
	normalizedPath := normalizeAbsolutePath(path)
	if normalizedPath == "" {
		return ""
	}
	q := url.Values{}
	if strings.TrimSpace(week) != "" {
		q.Set("week", strings.TrimSpace(week))
	}
	if encoded := q.Encode(); encoded != "" {
		return normalizedPath + "?" + encoded
	}
	return normalizedPath
}

func supportShare(value int, total int) float64 {
	if total <= 0 {
		return 0
	}
	return (float64(value) * 100.0) / float64(total)
}

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeLaneKey(value string) string {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" {
		return "unknown"
	}
	return trimmed
}

func normalizeStringSlice(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		set[trimmed] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
