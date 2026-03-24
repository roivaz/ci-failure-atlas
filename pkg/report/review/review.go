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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	triagehtml "ci-failure-atlas/pkg/report/triagehtml"
	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	phase3engine "ci-failure-atlas/pkg/semantic/engine/phase3"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	"ci-failure-atlas/pkg/store/ndjson"
)

const (
	defaultDataDirectory    = "data"
	defaultHistoryWeeks     = 4
	selectionInputName      = "cluster_id"
	unlinkChildInputName    = "unlink_child"
	metricRunCount          = "run_count"
	reviewTrendWindowDays   = 7
	phase3ActionLink        = "link"
	phase3ActionDisband     = "disband"
	phase3ActionUnlink      = "unlink"
	phase3ActionUnlinkChild = "unlink_child"
	phase3InformationalCode = "phase1_cluster_id_collision"
	phase3ClusterIDPrefix   = "p3c-"
)

type HandlerOptions struct {
	DataDirectory        string
	SemanticSubdirectory string
	HistoryHorizonWeeks  int
}

type handler struct {
	dataDirectory       string
	defaultWeek         string
	historyHorizonWeeks int
}

type phase3Anchor struct {
	Environment string
	RunURL      string
	RowID       string
}

func (a phase3Anchor) key() string {
	environment := normalizeEnvironment(a.Environment)
	runURL := strings.TrimSpace(a.RunURL)
	rowID := strings.TrimSpace(a.RowID)
	if environment == "" || runURL == "" || rowID == "" {
		return ""
	}
	return environment + "|" + runURL + "|" + rowID
}

type weekSnapshot struct {
	Weeks                []string
	Week                 string
	PreviousWeek         string
	NextWeek             string
	Rows                 []triagehtml.SignatureRow
	OverallJobsByEnv     map[string]int
	AnchorsByClusterID   map[string][]phase3Anchor
	LaneKeysByClusterID  map[string][]string
	AggregatedSelection  map[string]struct{}
	UnassignedCount      int
	MissingAnchorCount   int
	TotalClusters        int
	AnchoredClusterCount int
}

type reviewSignalIndex struct {
	ByPhase1ClusterID map[string]map[string]struct{}
	BySignatureID     map[string]map[string]struct{}
}

func NewHandler(opts HandlerOptions) (http.Handler, error) {
	dataDirectory := strings.TrimSpace(opts.DataDirectory)
	if dataDirectory == "" {
		dataDirectory = defaultDataDirectory
	}
	historyHorizonWeeks := opts.HistoryHorizonWeeks
	if historyHorizonWeeks <= 0 {
		historyHorizonWeeks = defaultHistoryWeeks
	}
	absoluteDataDirectory, err := filepath.Abs(filepath.Clean(dataDirectory))
	if err != nil {
		return nil, fmt.Errorf("resolve absolute data directory %q: %w", dataDirectory, err)
	}
	dataInfo, err := os.Stat(absoluteDataDirectory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("data directory %q does not exist", absoluteDataDirectory)
		}
		return nil, fmt.Errorf("stat data directory %q: %w", absoluteDataDirectory, err)
	}
	if !dataInfo.IsDir() {
		return nil, fmt.Errorf("data directory %q must be a directory", absoluteDataDirectory)
	}
	h := &handler{
		dataDirectory:       absoluteDataDirectory,
		defaultWeek:         strings.TrimSpace(opts.SemanticSubdirectory),
		historyHorizonWeeks: historyHorizonWeeks,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", h.handleRoot)
	mux.HandleFunc("/actions/links", h.handleLinksAction)
	mux.HandleFunc("/api/weeks", h.handleAPIWeeks)
	mux.HandleFunc("/api/week", h.handleAPIWeek)
	return mux, nil
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
	rendered := renderPage(snapshot, notice)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(rendered))
}

func (h *handler) handleLinksAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		h.redirectWithNotice(w, r, "", "invalid form payload")
		return
	}
	requestedWeek := strings.TrimSpace(r.FormValue("week"))
	snapshot, err := h.loadWeekSnapshot(r.Context(), requestedWeek)
	if err != nil {
		h.redirectWithNotice(w, r, requestedWeek, err.Error())
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
			h.redirectWithNotice(w, r, snapshot.Week, "select at least one aggregated phase3 cluster to disband")
		case phase3ActionUnlinkChild:
			h.redirectWithNotice(w, r, snapshot.Week, "select a linked signature to remove from the cluster")
		default:
			h.redirectWithNotice(w, r, snapshot.Week, "select at least one cluster")
		}
		return
	}

	store, err := ndjson.NewWithOptions(h.dataDirectory, ndjson.Options{
		SemanticSubdirectory: snapshot.Week,
	})
	if err != nil {
		h.redirectWithNotice(w, r, snapshot.Week, fmt.Sprintf("open ndjson store: %v", err))
		return
	}
	defer func() {
		_ = store.Close()
	}()

	switch action {
	case phase3ActionLink:
		anchors := selectedAnchors(snapshot.AnchorsByClusterID, selectedClusterIDs)
		if len(anchors) == 0 {
			h.redirectWithNotice(w, r, snapshot.Week, "selected clusters do not have row_id anchors yet; rerun semantic workflow and refresh")
			return
		}
		selectedLanes := selectedLaneKeys(snapshot.LaneKeysByClusterID, selectedClusterIDs)
		if len(selectedLanes) > 1 {
			h.redirectWithNotice(
				w,
				r,
				snapshot.Week,
				fmt.Sprintf(
					"selected clusters span multiple lanes (%s); linking across lanes is not allowed",
					strings.Join(selectedLanes, ", "),
				),
			)
			return
		}
		matchKeys := signatureMatchKeysForSelectedClusters(snapshot.Rows, selectedClusterIDs)
		windowWeeks := resolveReconcileWindowWeeks(snapshot.Weeks, h.historyHorizonWeeks)
		windowAnchors, err := collectAnchorsForSignatureMatchKeys(r.Context(), h.dataDirectory, windowWeeks, matchKeys)
		if err != nil {
			h.redirectWithNotice(w, r, snapshot.Week, fmt.Sprintf("link selected: %v", err))
			return
		}
		expandedAnchors := dedupeAnchors(append(append([]phase3Anchor{}, anchors...), windowAnchors...))

		phase3ClusterID, created, err := resolvePhase3ClusterIDForAnchors(r.Context(), store, expandedAnchors)
		if err != nil {
			h.redirectWithNotice(w, r, snapshot.Week, fmt.Sprintf("link selected: %v", err))
			return
		}
		if err := linkAnchors(r.Context(), store, phase3ClusterID, expandedAnchors); err != nil {
			h.redirectWithNotice(w, r, snapshot.Week, fmt.Sprintf("link selected: %v", err))
			return
		}
		crossWeekCount := len(expandedAnchors) - len(anchors)
		if crossWeekCount < 0 {
			crossWeekCount = 0
		}
		if created {
			h.redirectWithNotice(
				w,
				r,
				snapshot.Week,
				fmt.Sprintf(
					"linked %d anchors (%d selected + %d cross-week) using new phase3 cluster %s",
					len(expandedAnchors),
					len(anchors),
					crossWeekCount,
					phase3ClusterID,
				),
			)
			return
		}
		h.redirectWithNotice(
			w,
			r,
			snapshot.Week,
			fmt.Sprintf(
				"linked %d anchors (%d selected + %d cross-week) into existing phase3 cluster %s",
				len(expandedAnchors),
				len(anchors),
				crossWeekCount,
				phase3ClusterID,
			),
		)
		return
	case phase3ActionDisband:
		aggregatedSelections := filterAggregatedSelectionIDs(snapshot.AggregatedSelection, selectedClusterIDs)
		if len(aggregatedSelections) == 0 {
			h.redirectWithNotice(w, r, snapshot.Week, "disband cluster only works on aggregated phase3 rows")
			return
		}
		anchors := selectedAnchors(snapshot.AnchorsByClusterID, aggregatedSelections)
		if len(anchors) == 0 {
			h.redirectWithNotice(w, r, snapshot.Week, "selected aggregated clusters do not have row_id anchors yet; rerun semantic workflow and refresh")
			return
		}
		if err := unlinkAnchors(r.Context(), store, anchors); err != nil {
			h.redirectWithNotice(w, r, snapshot.Week, fmt.Sprintf("disband cluster: %v", err))
			return
		}
		h.redirectWithNotice(
			w,
			r,
			snapshot.Week,
			fmt.Sprintf("disbanded %d aggregated cluster(s) and unlinked %d anchors", len(aggregatedSelections), len(anchors)),
		)
		return
	case phase3ActionUnlinkChild, phase3ActionUnlink:
		anchors := selectedAnchors(snapshot.AnchorsByClusterID, selectedClusterIDs)
		if len(anchors) == 0 {
			h.redirectWithNotice(w, r, snapshot.Week, "selected signatures do not have row_id anchors yet; rerun semantic workflow and refresh")
			return
		}
		if err := unlinkAnchors(r.Context(), store, anchors); err != nil {
			h.redirectWithNotice(w, r, snapshot.Week, fmt.Sprintf("unlink selected: %v", err))
			return
		}
		if action == phase3ActionUnlinkChild {
			h.redirectWithNotice(w, r, snapshot.Week, fmt.Sprintf("removed linked signature and unlinked %d anchors", len(anchors)))
			return
		}
		h.redirectWithNotice(w, r, snapshot.Week, fmt.Sprintf("unlinked %d anchors", len(anchors)))
		return
	default:
		h.redirectWithNotice(w, r, snapshot.Week, "unsupported action")
		return
	}
}

func (h *handler) handleAPIWeeks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	weeks, err := h.discoverSemanticWeeks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	currentWeek, previousWeek, nextWeek, _ := resolveWeekWindow(weeks, strings.TrimSpace(r.URL.Query().Get("week")), h.defaultWeek)
	writeJSON(w, http.StatusOK, map[string]any{
		"weeks":         weeks,
		"current_week":  currentWeek,
		"previous_week": previousWeek,
		"next_week":     nextWeek,
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
	rows := make([]map[string]any, 0, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		rows = append(rows, map[string]any{
			"environment":       row.Environment,
			"cluster_id":        row.ClusterID,
			"selection_id":      row.SelectionValue,
			"phrase":            row.Phrase,
			"support_count":     row.SupportCount,
			"phase3_cluster_id": row.ManualIssueID,
			"quality_score":     row.QualityScore,
			"quality_flags":     row.QualityNoteLabels,
			"review_flags":      row.ReviewNoteLabels,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"week":                   snapshot.Week,
		"previous_week":          snapshot.PreviousWeek,
		"next_week":              snapshot.NextWeek,
		"total_clusters":         snapshot.TotalClusters,
		"anchored_clusters":      snapshot.AnchoredClusterCount,
		"missing_anchor_count":   snapshot.MissingAnchorCount,
		"unassigned_queue_count": snapshot.UnassignedCount,
		"rows":                   rows,
	})
}

func (h *handler) redirectWithNotice(w http.ResponseWriter, r *http.Request, week string, notice string) {
	q := url.Values{}
	if strings.TrimSpace(week) != "" {
		q.Set("week", strings.TrimSpace(week))
	}
	if strings.TrimSpace(notice) != "" {
		q.Set("notice", strings.TrimSpace(notice))
	}
	target := "/"
	if encoded := q.Encode(); encoded != "" {
		target = "/?" + encoded
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (h *handler) loadWeekSnapshot(ctx context.Context, requestedWeek string) (weekSnapshot, error) {
	weeks, err := h.discoverSemanticWeeks()
	if err != nil {
		return weekSnapshot{}, err
	}
	week, previousWeek, nextWeek, _ := resolveWeekWindow(weeks, requestedWeek, h.defaultWeek)
	store, err := ndjson.NewWithOptions(h.dataDirectory, ndjson.Options{
		SemanticSubdirectory: week,
	})
	if err != nil {
		return weekSnapshot{}, fmt.Errorf("open ndjson store for semantic week %q: %w", week, err)
	}
	defer func() {
		_ = store.Close()
	}()

	clusters, err := store.ListGlobalClusters(ctx)
	if err != nil {
		return weekSnapshot{}, fmt.Errorf("list global clusters: %w", err)
	}
	reviewQueue, err := store.ListReviewQueue(ctx)
	if err != nil {
		return weekSnapshot{}, fmt.Errorf("list review queue: %w", err)
	}
	links, err := store.ListPhase3Links(ctx)
	if err != nil {
		return weekSnapshot{}, fmt.Errorf("list phase3 links: %w", err)
	}
	childAnchorsByClusterID := map[string][]phase3Anchor{}
	childLaneKeysByClusterID := map[string][]string{}
	for _, cluster := range clusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		if clusterID == "" {
			continue
		}
		selectionID := rowSelectionID(environment, clusterID)
		anchors := anchorsForCluster(environment, cluster.References)
		laneKeys := laneKeysForContributingTests(cluster.ContributingTests)
		if selectionID != "" {
			childAnchorsByClusterID[selectionID] = dedupeAnchors(append(childAnchorsByClusterID[selectionID], anchors...))
			childLaneKeysByClusterID[selectionID] = mergeLaneKeys(childLaneKeysByClusterID[selectionID], laneKeys)
		}
		childAnchorsByClusterID[clusterID] = dedupeAnchors(append(childAnchorsByClusterID[clusterID], anchors...))
		childLaneKeysByClusterID[clusterID] = mergeLaneKeys(childLaneKeysByClusterID[clusterID], laneKeys)
	}
	linkedChildrenBySelectionID, err := linkedChildrenByMergedSelectionID(clusters, links)
	if err != nil {
		return weekSnapshot{}, fmt.Errorf("build linked child clusters: %w", err)
	}
	clusters, err = phase3engine.Merge(clusters, links)
	if err != nil {
		return weekSnapshot{}, fmt.Errorf("apply phase3 materialized view: %w", err)
	}
	phase1Workset, err := store.ListPhase1Workset(ctx)
	if err != nil {
		return weekSnapshot{}, fmt.Errorf("list phase1 workset: %w", err)
	}

	reviewIndex := buildReviewSignalIndex(reviewQueue)
	rawTextIndex := buildPhase1WorksetRawTextIndex(phase1Workset)
	phase3ClusterByAnchor := map[string]string{}
	for _, row := range links {
		anchor := phase3Anchor{
			Environment: row.Environment,
			RunURL:      row.RunURL,
			RowID:       row.RowID,
		}
		key := anchor.key()
		if key == "" {
			continue
		}
		phase3ClusterID := strings.TrimSpace(row.IssueID)
		if phase3ClusterID == "" {
			continue
		}
		phase3ClusterByAnchor[key] = phase3ClusterID
	}

	totalSupportByEnvironment := map[string]int{}
	phraseEnvironments := map[string]map[string]struct{}{}
	for _, cluster := range clusters {
		environment := normalizeEnvironment(cluster.Environment)
		totalSupportByEnvironment[environment] += cluster.SupportCount
		phraseKey := strings.ToLower(strings.TrimSpace(cluster.CanonicalEvidencePhrase))
		if phraseKey == "" {
			continue
		}
		envSet := phraseEnvironments[phraseKey]
		if envSet == nil {
			envSet = map[string]struct{}{}
			phraseEnvironments[phraseKey] = envSet
		}
		envSet[environment] = struct{}{}
	}
	metricWindowStart := time.Time{}
	metricWindowEnd := time.Time{}
	if weekStart, ok := parseSemanticWeek(week); ok {
		metricWindowStart = weekStart.UTC()
		metricWindowEnd = weekStart.AddDate(0, 0, 7).UTC()
	}
	overallJobsByEnv, err := metricRunTotalsByEnvironment(
		ctx,
		store,
		sortedEnvironmentKeys(totalSupportByEnvironment),
		metricWindowStart,
		metricWindowEnd,
	)
	if err != nil {
		return weekSnapshot{}, fmt.Errorf("load overall metric run counts: %w", err)
	}
	historyResolver, err := semhistory.BuildGlobalSignatureResolver(ctx, semhistory.BuildOptions{
		DataDirectory:                h.dataDirectory,
		CurrentSemanticSubdir:        week,
		GlobalSignatureLookbackWeeks: h.historyHorizonWeeks,
	})
	if err != nil {
		return weekSnapshot{}, fmt.Errorf("build global signature history resolver: %w", err)
	}
	trendAnchor := time.Now().UTC()
	if weekStart, ok := parseSemanticWeek(week); ok {
		trendAnchor = weekStart.AddDate(0, 0, reviewTrendWindowDays-1).UTC()
	}

	rows := make([]triagehtml.SignatureRow, 0, len(clusters))
	anchorsByClusterID := map[string][]phase3Anchor{}
	for key, anchors := range childAnchorsByClusterID {
		anchorsByClusterID[key] = append([]phase3Anchor(nil), anchors...)
	}
	laneKeysByClusterID := map[string][]string{}
	for key, laneKeys := range childLaneKeysByClusterID {
		laneKeysByClusterID[key] = append([]string(nil), laneKeys...)
	}
	aggregatedSelections := map[string]struct{}{}
	unassignedCount := 0
	missingAnchorCount := 0
	anchoredClusterCount := 0

	for _, cluster := range clusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		selectionID := rowSelectionID(environment, clusterID)
		anchors := anchorsForCluster(environment, cluster.References)
		if len(anchors) == 0 {
			missingAnchorCount++
		} else {
			anchoredClusterCount++
		}
		if selectionID != "" {
			anchorsByClusterID[selectionID] = dedupeAnchors(append(anchorsByClusterID[selectionID], anchors...))
		}
		laneKeys := laneKeysForContributingTests(cluster.ContributingTests)
		if selectionID != "" {
			laneKeysByClusterID[selectionID] = mergeLaneKeys(laneKeysByClusterID[selectionID], laneKeys)
		}
		// Backward compatibility for callers still posting raw cluster IDs.
		if clusterID != "" {
			anchorsByClusterID[clusterID] = dedupeAnchors(append(anchorsByClusterID[clusterID], anchors...))
			laneKeysByClusterID[clusterID] = mergeLaneKeys(laneKeysByClusterID[clusterID], laneKeys)
		}
		phase3ClusterIDs := phase3ClusterIDsForAnchors(anchors, phase3ClusterByAnchor)
		manualIssueID := ""
		switch len(phase3ClusterIDs) {
		case 0:
			manualIssueID = ""
		case 1:
			manualIssueID = phase3ClusterIDs[0]
		default:
			return weekSnapshot{}, fmt.Errorf(
				"phase3 conflict: semantic cluster %s resolves to multiple phase3 cluster IDs (%s); unlink and relink this cluster",
				clusterID,
				strings.Join(phase3ClusterIDs, ", "),
			)
		}

		if len(phase3ClusterIDs) == 0 {
			unassignedCount++
		}

		qualityCodes := triagehtml.QualityIssueCodes(cluster.CanonicalEvidencePhrase)
		qualityLabels := make([]string, 0, len(qualityCodes))
		for _, code := range qualityCodes {
			qualityLabels = append(qualityLabels, triagehtml.QualityIssueLabel(code))
		}
		reviewReasons := reviewReasonsForGlobalCluster(cluster, reviewIndex)
		qualityScore := triagehtml.QualityScore(qualityCodes) + (len(reviewReasons) * 2)
		alsoSeenIn := environmentsForPhrase(
			phraseEnvironments[strings.ToLower(strings.TrimSpace(cluster.CanonicalEvidencePhrase))],
			environment,
		)
		primary := primaryContributingTest(cluster.ContributingTests)
		linkedChildren := buildLinkedChildSignatureRows(
			manualIssueID,
			linkedChildrenBySelectionID[selectionID],
			totalSupportByEnvironment[environment],
			reviewIndex,
			rawTextIndex,
		)
		isAggregatedRow := strings.TrimSpace(manualIssueID) != "" && len(linkedChildren) > 0
		if isAggregatedRow {
			if selectionID != "" {
				aggregatedSelections[selectionID] = struct{}{}
			}
			if clusterID != "" {
				aggregatedSelections[clusterID] = struct{}{}
			}
		}
		displayReferences := toRunReferences(cluster.References, 0)
		scoreReferences := []triagehtml.RunReference(nil)
		trendSparkline := ""
		trendCounts := []int(nil)
		trendRange := ""
		if isAggregatedRow {
			scoreReferences = toRunReferences(cluster.References, 0)
			if sparkline, counts, sparkRange, ok := triagehtml.DailyDensitySparkline(
				scoreReferences,
				reviewTrendWindowDays,
				trendAnchor,
			); ok {
				trendSparkline = sparkline
				trendCounts = append([]int(nil), counts...)
				trendRange = sparkRange
			}
		}
		historyPresence := semhistory.SignaturePresence{}
		if historyResolver != nil && isAggregatedRow {
			historyPresence = historyResolver.PresenceForPhase3Cluster(environment, manualIssueID)
		}
		priorLastSeenAt := ""
		if !historyPresence.PriorLastSeenAt.IsZero() {
			priorLastSeenAt = historyPresence.PriorLastSeenAt.UTC().Format(time.RFC3339)
		}

		rows = append(rows, triagehtml.SignatureRow{
			Environment:         environment,
			Lane:                strings.TrimSpace(primary.Lane),
			JobName:             strings.TrimSpace(primary.JobName),
			TestName:            strings.TrimSpace(primary.TestName),
			Phrase:              strings.TrimSpace(cluster.CanonicalEvidencePhrase),
			ClusterID:           clusterID,
			SearchQuery:         strings.TrimSpace(cluster.SearchQueryPhrase),
			SupportCount:        cluster.SupportCount,
			SupportShare:        supportShare(cluster.SupportCount, totalSupportByEnvironment[environment]),
			PostGoodCount:       cluster.PostGoodCommitCount,
			AlsoSeenIn:          alsoSeenIn,
			QualityScore:        qualityScore,
			QualityNoteLabels:   qualityLabels,
			ReviewNoteLabels:    reviewReasons,
			ContributingTests:   toContributingTests(cluster.ContributingTests),
			FullErrorSamples:    fullErrorSamplesForReferences(environment, cluster.References, rawTextIndex, 0),
			References:          displayReferences,
			ScoringReferences:   scoreReferences,
			TrendSparkline:      trendSparkline,
			TrendCounts:         trendCounts,
			TrendRange:          trendRange,
			PriorWeeksPresent:   historyPresence.PriorWeeksPresent,
			PriorWeekStarts:     append([]string(nil), historyPresence.PriorWeekStarts...),
			PriorJobsAffected:   historyPresence.PriorJobsAffected,
			PriorLastSeenAt:     priorLastSeenAt,
			ManualIssueID:       manualIssueID,
			ManualIssueConflict: false,
			SelectionValue:      selectionID,
			LinkedChildren:      linkedChildren,
			SearchIndex: strings.Join([]string{
				environment,
				strings.TrimSpace(primary.Lane),
				strings.TrimSpace(primary.JobName),
				strings.TrimSpace(primary.TestName),
				strings.TrimSpace(cluster.CanonicalEvidencePhrase),
				clusterID,
				selectionID,
				strings.Join(qualityLabels, " "),
				strings.Join(reviewReasons, " "),
				manualIssueID,
			}, " "),
		})
	}

	return weekSnapshot{
		Weeks:                weeks,
		Week:                 week,
		PreviousWeek:         previousWeek,
		NextWeek:             nextWeek,
		Rows:                 rows,
		OverallJobsByEnv:     overallJobsByEnv,
		AnchorsByClusterID:   anchorsByClusterID,
		LaneKeysByClusterID:  laneKeysByClusterID,
		AggregatedSelection:  aggregatedSelections,
		UnassignedCount:      unassignedCount,
		MissingAnchorCount:   missingAnchorCount,
		TotalClusters:        len(clusters),
		AnchoredClusterCount: anchoredClusterCount,
	}, nil
}

func (h *handler) discoverSemanticWeeks() ([]string, error) {
	semanticRoot := filepath.Join(h.dataDirectory, "semantic")
	rootInfo, err := os.Stat(semanticRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("semantic directory %q does not exist", semanticRoot)
		}
		return nil, fmt.Errorf("stat semantic directory %q: %w", semanticRoot, err)
	}
	if !rootInfo.IsDir() {
		return nil, fmt.Errorf("semantic path %q must be a directory", semanticRoot)
	}
	entries, err := os.ReadDir(semanticRoot)
	if err != nil {
		return nil, fmt.Errorf("read semantic directory %q: %w", semanticRoot, err)
	}
	weeks := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" || strings.HasPrefix(name, ".") {
			continue
		}
		weeks = append(weeks, name)
	}
	sort.Strings(weeks)
	if len(weeks) == 0 {
		return nil, fmt.Errorf("no semantic snapshots found under %q", semanticRoot)
	}
	return weeks, nil
}

func resolveWeekWindow(weeks []string, requestedWeek string, defaultWeek string) (string, string, string, int) {
	if len(weeks) == 0 {
		return "", "", "", -1
	}
	week := strings.TrimSpace(requestedWeek)
	if week == "" {
		defaultWeek = strings.TrimSpace(defaultWeek)
		if defaultWeek != "" && containsString(weeks, defaultWeek) {
			week = defaultWeek
		}
	}
	if week == "" || !containsString(weeks, week) {
		week = weeks[len(weeks)-1]
	}
	index := sort.SearchStrings(weeks, week)
	if index < 0 || index >= len(weeks) || weeks[index] != week {
		index = len(weeks) - 1
		week = weeks[index]
	}
	previous := ""
	next := ""
	if index > 0 {
		previous = weeks[index-1]
	}
	if index+1 < len(weeks) {
		next = weeks[index+1]
	}
	return week, previous, next, index
}

func sortedEnvironmentKeys(values map[string]int) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for environment := range values {
		normalized := normalizeEnvironment(environment)
		if normalized == "" {
			continue
		}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func metricRunTotalsByEnvironment(
	ctx context.Context,
	store *ndjson.Store,
	environments []string,
	windowStart time.Time,
	windowEnd time.Time,
) (map[string]int, error) {
	totals := map[string]int{}
	if store == nil {
		return totals, nil
	}
	environmentSet := map[string]struct{}{}
	for _, environment := range environments {
		normalized := normalizeEnvironment(environment)
		if normalized == "" {
			continue
		}
		environmentSet[normalized] = struct{}{}
	}
	if len(environmentSet) == 0 {
		return totals, nil
	}
	rows, err := store.ListMetricsDaily(ctx)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		environment := normalizeEnvironment(row.Environment)
		if _, ok := environmentSet[environment]; !ok {
			continue
		}
		if strings.TrimSpace(row.Metric) != metricRunCount {
			continue
		}
		trimmedDate := strings.TrimSpace(row.Date)
		if !windowStart.IsZero() && !windowEnd.IsZero() {
			dateValue, ok := parseMetricDate(trimmedDate)
			if !ok {
				continue
			}
			if dateValue.Before(windowStart) || !dateValue.Before(windowEnd) {
				continue
			}
		}
		value := int(row.Value)
		if value <= 0 {
			continue
		}
		totals[environment] += value
	}
	return totals, nil
}

func parseMetricDate(value string) (time.Time, bool) {
	parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
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
	rows []triagehtml.SignatureRow,
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
			for _, child := range row.LinkedChildren {
				if !rowMatchesSelection(child, selected) {
					continue
				}
				key := signatureMatchKey(child.Environment, child.Lane, child.Phrase)
				if key == "" {
					continue
				}
				keys[key] = struct{}{}
			}
			continue
		}
		key := signatureMatchKey(row.Environment, row.Lane, row.Phrase)
		if key == "" {
			continue
		}
		keys[key] = struct{}{}
	}
	return keys
}

func rowMatchesSelection(row triagehtml.SignatureRow, selected map[string]struct{}) bool {
	selectionID := strings.TrimSpace(row.SelectionValue)
	if selectionID != "" {
		if _, include := selected[selectionID]; include {
			return true
		}
	}
	clusterID := strings.TrimSpace(row.ClusterID)
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

func collectAnchorsForSignatureMatchKeys(
	ctx context.Context,
	dataDirectory string,
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
		store, err := ndjson.NewWithOptions(dataDirectory, ndjson.Options{
			SemanticSubdirectory: week,
		})
		if err != nil {
			return nil, fmt.Errorf("open ndjson store for reconcile week %q: %w", week, err)
		}
		clusters, listErr := store.ListGlobalClusters(ctx)
		_ = store.Close()
		if listErr != nil {
			return nil, fmt.Errorf("list global clusters for reconcile week %q: %w", week, listErr)
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
		key := anchor.key()
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

func resolvePhase3ClusterIDForAnchors(ctx context.Context, store *ndjson.Store, anchors []phase3Anchor) (string, bool, error) {
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
		key := anchor.key()
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

func linkAnchors(ctx context.Context, store *ndjson.Store, phase3ClusterID string, anchors []phase3Anchor) error {
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
		key := anchor.key()
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
		key := anchor.key()
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
		key := anchor.key()
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

func unlinkAnchors(ctx context.Context, store *ndjson.Store, anchors []phase3Anchor) error {
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
		key := anchor.key()
		if key == "" {
			continue
		}
		existingClusterByAnchor[key] = strings.TrimSpace(row.IssueID)
	}
	deleteRows := make([]semanticcontracts.Phase3LinkRecord, 0, len(anchors))
	eventRows := make([]semanticcontracts.Phase3EventRecord, 0, len(anchors))
	for _, anchor := range anchors {
		key := anchor.key()
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

func renderPage(snapshot weekSnapshot, notice string) string {
	previousHref := ""
	nextHref := ""
	if strings.TrimSpace(snapshot.PreviousWeek) != "" {
		previousHref = "/?week=" + url.QueryEscape(snapshot.PreviousWeek)
	}
	if strings.TrimSpace(snapshot.NextWeek) != "" {
		nextHref = "/?week=" + url.QueryEscape(snapshot.NextWeek)
	}
	chrome := triagehtml.ReportChromeHTML(triagehtml.ReportChromeOptions{
		CurrentWeek:  snapshot.Week,
		PreviousWeek: snapshot.PreviousWeek,
		PreviousHref: previousHref,
		NextWeek:     snapshot.NextWeek,
		NextHref:     nextHref,
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
	b.WriteString("    <p class=\"muted\">Human-only linking with durable row-level anchors. Rebuild snapshots via CLI, then use Refresh here.</p>\n")
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
	b.WriteString("    <form method=\"post\" action=\"/actions/links\">\n")
	b.WriteString(fmt.Sprintf("      <input type=\"hidden\" name=\"week\" value=\"%s\" />\n", html.EscapeString(snapshot.Week)))
	b.WriteString("      <div class=\"phase3-controls\">\n")
	b.WriteString(fmt.Sprintf("        <button class=\"primary\" type=\"submit\" name=\"action\" value=\"%s\">Link selected</button>\n", phase3ActionLink))
	b.WriteString(fmt.Sprintf("        <button type=\"submit\" name=\"action\" value=\"%s\">Disband cluster</button>\n", phase3ActionDisband))
	b.WriteString(fmt.Sprintf("        <a href=\"/?week=%s\">Refresh</a>\n", url.QueryEscape(snapshot.Week)))
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
	b.WriteString("    <div class=\"phase3-api-links muted\">JSON: <a href=\"/api/weeks\">/api/weeks</a> | ")
	b.WriteString(fmt.Sprintf("<a href=\"/api/week?week=%s\">/api/week?week=%s</a></div>\n", url.QueryEscape(snapshot.Week), html.EscapeString(snapshot.Week)))
	b.WriteString("  </div>\n")
	b.WriteString(triagehtml.ThemeToggleScriptTag())
	b.WriteString("  <script>\n")
	b.WriteString(strings.Join([]string{
		"    (function() {",
		"      function rowCheckboxes() {",
		"        return Array.prototype.slice.call(document.querySelectorAll('.triage-row-select'));",
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

func groupRowsByEnvironment(rows []triagehtml.SignatureRow) (map[string][]triagehtml.SignatureRow, []string) {
	grouped := map[string][]triagehtml.SignatureRow{}
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

func orderedReviewEnvironments(grouped map[string][]triagehtml.SignatureRow) []string {
	fixedOrder := []string{"dev", "int", "stg", "prod"}
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

func buildReviewSignalIndex(rows []semanticcontracts.ReviewItemRecord) reviewSignalIndex {
	index := reviewSignalIndex{
		ByPhase1ClusterID: map[string]map[string]struct{}{},
		BySignatureID:     map[string]map[string]struct{}{},
	}
	for _, row := range rows {
		reason := strings.TrimSpace(row.Reason)
		if reason == "" || strings.EqualFold(reason, phase3InformationalCode) {
			continue
		}
		for _, phase1ID := range row.SourcePhase1ClusterIDs {
			key := strings.TrimSpace(phase1ID)
			if key == "" {
				continue
			}
			set := index.ByPhase1ClusterID[key]
			if set == nil {
				set = map[string]struct{}{}
				index.ByPhase1ClusterID[key] = set
			}
			set[reason] = struct{}{}
		}
		for _, signatureID := range row.MemberSignatureIDs {
			key := strings.TrimSpace(signatureID)
			if key == "" {
				continue
			}
			set := index.BySignatureID[key]
			if set == nil {
				set = map[string]struct{}{}
				index.BySignatureID[key] = set
			}
			set[reason] = struct{}{}
		}
	}
	return index
}

func reviewReasonsForGlobalCluster(cluster semanticcontracts.GlobalClusterRecord, index reviewSignalIndex) []string {
	set := map[string]struct{}{}
	for _, phase1ID := range cluster.MemberPhase1ClusterIDs {
		for reason := range index.ByPhase1ClusterID[strings.TrimSpace(phase1ID)] {
			set[reason] = struct{}{}
		}
	}
	for _, signatureID := range cluster.MemberSignatureIDs {
		for reason := range index.BySignatureID[strings.TrimSpace(signatureID)] {
			set[reason] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for reason := range set {
		out = append(out, reason)
	}
	sort.Strings(out)
	return out
}

func buildPhase1WorksetRawTextIndex(rows []semanticcontracts.Phase1WorksetRecord) map[string]string {
	byRowID := map[string]string{}
	for _, row := range rows {
		environment := normalizeEnvironment(row.Environment)
		rowID := strings.TrimSpace(row.RowID)
		rawText := strings.TrimSpace(row.RawText)
		if environment == "" {
			continue
		}
		rowKey := phase1WorksetRowKey(environment, rowID)
		if rowKey == "" || rawText == "" {
			continue
		}
		if _, exists := byRowID[rowKey]; !exists {
			byRowID[rowKey] = rawText
		}
	}
	return byRowID
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
		key := anchor.key()
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

func fullErrorSamplesForReferences(
	environment string,
	references []semanticcontracts.ReferenceRecord,
	rawTextByRowKey map[string]string,
	limit int,
) []string {
	if len(references) == 0 {
		return nil
	}
	capacity := len(references)
	if limit > 0 && limit < capacity {
		capacity = limit
	}
	seen := map[string]struct{}{}
	samples := make([]string, 0, capacity)
	for _, reference := range references {
		rowID := strings.TrimSpace(reference.RowID)
		if rowID == "" {
			continue
		}
		sample := strings.TrimSpace(rawTextByRowKey[phase1WorksetRowKey(environment, rowID)])
		if sample == "" {
			continue
		}
		if _, exists := seen[sample]; exists {
			continue
		}
		seen[sample] = struct{}{}
		samples = append(samples, sample)
		if limit > 0 && len(samples) >= limit {
			break
		}
	}
	return samples
}

func toRunReferences(rows []semanticcontracts.ReferenceRecord, limit int) []triagehtml.RunReference {
	if len(rows) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(rows) {
		limit = len(rows)
	}
	out := make([]triagehtml.RunReference, 0, limit)
	for _, row := range rows[:limit] {
		out = append(out, triagehtml.RunReference{
			RunURL:      strings.TrimSpace(row.RunURL),
			OccurredAt:  strings.TrimSpace(row.OccurredAt),
			SignatureID: strings.TrimSpace(row.SignatureID),
			PRNumber:    row.PRNumber,
		})
	}
	return out
}

func toContributingTests(rows []semanticcontracts.ContributingTestRecord) []triagehtml.ContributingTest {
	if len(rows) == 0 {
		return nil
	}
	out := make([]triagehtml.ContributingTest, 0, len(rows))
	for _, row := range rows {
		out = append(out, triagehtml.ContributingTest{
			Lane:         strings.TrimSpace(row.Lane),
			JobName:      strings.TrimSpace(row.JobName),
			TestName:     strings.TrimSpace(row.TestName),
			SupportCount: row.SupportCount,
		})
	}
	return out
}

func primaryContributingTest(rows []semanticcontracts.ContributingTestRecord) semanticcontracts.ContributingTestRecord {
	if len(rows) == 0 {
		return semanticcontracts.ContributingTestRecord{}
	}
	best := rows[0]
	for _, row := range rows[1:] {
		if row.SupportCount > best.SupportCount {
			best = row
			continue
		}
		if row.SupportCount == best.SupportCount {
			currentKey := strings.TrimSpace(row.Lane) + "|" + strings.TrimSpace(row.JobName) + "|" + strings.TrimSpace(row.TestName)
			bestKey := strings.TrimSpace(best.Lane) + "|" + strings.TrimSpace(best.JobName) + "|" + strings.TrimSpace(best.TestName)
			if currentKey < bestKey {
				best = row
			}
		}
	}
	return best
}

func phase3ClusterIDsForAnchors(anchors []phase3Anchor, phase3ClusterByAnchor map[string]string) []string {
	set := map[string]struct{}{}
	for _, anchor := range anchors {
		phase3ClusterID := strings.TrimSpace(phase3ClusterByAnchor[anchor.key()])
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

func linkedChildrenByMergedSelectionID(
	globalClusters []semanticcontracts.GlobalClusterRecord,
	phase3Links []semanticcontracts.Phase3LinkRecord,
) (map[string][]semanticcontracts.GlobalClusterRecord, error) {
	phase3ClusterByAnchor := map[string]string{}
	for _, row := range phase3Links {
		phase3ClusterID := strings.TrimSpace(row.IssueID)
		if phase3ClusterID == "" {
			continue
		}
		anchor := phase3Anchor{
			Environment: row.Environment,
			RunURL:      row.RunURL,
			RowID:       row.RowID,
		}
		key := anchor.key()
		if key == "" {
			continue
		}
		phase3ClusterByAnchor[key] = phase3ClusterID
	}
	grouped := map[string][]semanticcontracts.GlobalClusterRecord{}
	for _, cluster := range globalClusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		if environment == "" || clusterID == "" {
			return nil, fmt.Errorf("global cluster record missing environment and/or phase2_cluster_id")
		}
		anchors := anchorsForCluster(environment, cluster.References)
		phase3ClusterIDs := phase3ClusterIDsForAnchors(anchors, phase3ClusterByAnchor)
		if len(phase3ClusterIDs) > 1 {
			return nil, fmt.Errorf(
				"phase3 conflict: semantic cluster %s resolves to multiple phase3 cluster IDs (%s)",
				clusterID,
				strings.Join(phase3ClusterIDs, ", "),
			)
		}
		if len(phase3ClusterIDs) == 0 {
			continue
		}
		selectionID := rowSelectionID(environment, phase3ClusterIDs[0])
		grouped[selectionID] = append(grouped[selectionID], cluster)
	}
	for key := range grouped {
		rows := grouped[key]
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].SupportCount != rows[j].SupportCount {
				return rows[i].SupportCount > rows[j].SupportCount
			}
			if strings.TrimSpace(rows[i].CanonicalEvidencePhrase) != strings.TrimSpace(rows[j].CanonicalEvidencePhrase) {
				return strings.TrimSpace(rows[i].CanonicalEvidencePhrase) < strings.TrimSpace(rows[j].CanonicalEvidencePhrase)
			}
			return strings.TrimSpace(rows[i].Phase2ClusterID) < strings.TrimSpace(rows[j].Phase2ClusterID)
		})
		grouped[key] = rows
	}
	return grouped, nil
}

func buildLinkedChildSignatureRows(
	manualIssueID string,
	childClusters []semanticcontracts.GlobalClusterRecord,
	totalSupportByEnvironment int,
	reviewIndex reviewSignalIndex,
	rawTextIndex map[string]string,
) []triagehtml.SignatureRow {
	if strings.TrimSpace(manualIssueID) == "" || len(childClusters) == 0 {
		return nil
	}
	out := make([]triagehtml.SignatureRow, 0, len(childClusters))
	for _, cluster := range childClusters {
		environment := normalizeEnvironment(cluster.Environment)
		clusterID := strings.TrimSpace(cluster.Phase2ClusterID)
		qualityCodes := triagehtml.QualityIssueCodes(cluster.CanonicalEvidencePhrase)
		qualityLabels := make([]string, 0, len(qualityCodes))
		for _, code := range qualityCodes {
			qualityLabels = append(qualityLabels, triagehtml.QualityIssueLabel(code))
		}
		reviewReasons := reviewReasonsForGlobalCluster(cluster, reviewIndex)
		qualityScore := triagehtml.QualityScore(qualityCodes) + (len(reviewReasons) * 2)
		primary := primaryContributingTest(cluster.ContributingTests)

		out = append(out, triagehtml.SignatureRow{
			Environment:         environment,
			Lane:                strings.TrimSpace(primary.Lane),
			JobName:             strings.TrimSpace(primary.JobName),
			TestName:            strings.TrimSpace(primary.TestName),
			Phrase:              strings.TrimSpace(cluster.CanonicalEvidencePhrase),
			ClusterID:           clusterID,
			SearchQuery:         strings.TrimSpace(cluster.SearchQueryPhrase),
			SupportCount:        cluster.SupportCount,
			SupportShare:        supportShare(cluster.SupportCount, totalSupportByEnvironment),
			PostGoodCount:       cluster.PostGoodCommitCount,
			QualityScore:        qualityScore,
			QualityNoteLabels:   qualityLabels,
			ReviewNoteLabels:    reviewReasons,
			ContributingTests:   toContributingTests(cluster.ContributingTests),
			FullErrorSamples:    fullErrorSamplesForReferences(environment, cluster.References, rawTextIndex, 0),
			References:          toRunReferences(cluster.References, 0),
			ManualIssueID:       strings.TrimSpace(manualIssueID),
			ManualIssueConflict: false,
			SelectionValue:      rowSelectionID(environment, clusterID),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SupportCount != out[j].SupportCount {
			return out[i].SupportCount > out[j].SupportCount
		}
		if out[i].PostGoodCount != out[j].PostGoodCount {
			return out[i].PostGoodCount > out[j].PostGoodCount
		}
		if strings.TrimSpace(out[i].Phrase) != strings.TrimSpace(out[j].Phrase) {
			return strings.TrimSpace(out[i].Phrase) < strings.TrimSpace(out[j].Phrase)
		}
		return strings.TrimSpace(out[i].ClusterID) < strings.TrimSpace(out[j].ClusterID)
	})
	return out
}

func rowSelectionID(environment string, clusterID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedClusterID := strings.TrimSpace(clusterID)
	if normalizedEnvironment == "" || trimmedClusterID == "" {
		return trimmedClusterID
	}
	return normalizedEnvironment + "|" + trimmedClusterID
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

func mergeLaneKeys(existing []string, incoming []string) []string {
	set := map[string]struct{}{}
	for _, laneKey := range existing {
		set[normalizeLaneKey(laneKey)] = struct{}{}
	}
	for _, laneKey := range incoming {
		set[normalizeLaneKey(laneKey)] = struct{}{}
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
			key := anchor.key()
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
		key := anchor.key()
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

func phase1WorksetRowKey(environment string, rowID string) string {
	normalizedEnvironment := normalizeEnvironment(environment)
	trimmedRowID := strings.TrimSpace(rowID)
	if normalizedEnvironment == "" || trimmedRowID == "" {
		return ""
	}
	return normalizedEnvironment + "|" + trimmedRowID
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

func environmentsForPhrase(set map[string]struct{}, currentEnvironment string) []string {
	if len(set) == 0 {
		return nil
	}
	current := normalizeEnvironment(currentEnvironment)
	out := make([]string, 0, len(set))
	for environment := range set {
		normalized := normalizeEnvironment(environment)
		if normalized == "" || normalized == current {
			continue
		}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == strings.TrimSpace(target) {
			return true
		}
	}
	return false
}
