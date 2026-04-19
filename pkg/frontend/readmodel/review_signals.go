package readmodel

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	semanticquery "ci-failure-atlas/pkg/semantic/query"
)

type ReviewSignalReference struct {
	RowID          string `json:"row_id,omitempty"`
	RunURL         string `json:"run_url"`
	OccurredAt     string `json:"occurred_at"`
	SignatureID    string `json:"signature_id"`
	PRNumber       int    `json:"pr_number"`
	PostGoodCommit bool   `json:"after_last_push_of_merged_pr"`
}

type ReviewSignalMatchedFailurePattern struct {
	Environment      string `json:"environment"`
	FailurePatternID string `json:"failure_pattern_id"`
	FailurePattern   string `json:"failure_pattern"`
	SearchQuery      string `json:"search_query,omitempty"`
}

type ReviewSignalRow struct {
	Environment                          string                              `json:"environment"`
	ReviewItemID                         string                              `json:"review_item_id"`
	Phase                                string                              `json:"phase"`
	Reason                               string                              `json:"reason"`
	Severity                             string                              `json:"severity,omitempty"`
	ProposedFailurePattern               string                              `json:"proposed_failure_pattern,omitempty"`
	ProposedSearchQuery                  string                              `json:"proposed_search_query,omitempty"`
	ProposedSearchQuerySourceRunURL      string                              `json:"proposed_search_query_source_run_url,omitempty"`
	ProposedSearchQuerySourceSignatureID string                              `json:"proposed_search_query_source_signature_id,omitempty"`
	SourcePhase1ClusterIDs               []string                            `json:"source_phase1_cluster_ids,omitempty"`
	MemberSignatureIDs                   []string                            `json:"member_signature_ids,omitempty"`
	References                           []ReviewSignalReference             `json:"references,omitempty"`
	MatchedFailurePatterns               []ReviewSignalMatchedFailurePattern `json:"matched_failure_patterns,omitempty"`
}

type ReviewSignalsWeekSnapshot struct {
	Weeks             []string          `json:"weeks,omitempty"`
	Week              string            `json:"week"`
	PreviousWeek      string            `json:"previous_week,omitempty"`
	NextWeek          string            `json:"next_week,omitempty"`
	Timezone          string            `json:"timezone"`
	TotalSignals      int               `json:"total_signals"`
	SignalsByReason   map[string]int    `json:"signals_by_reason,omitempty"`
	SignalsBySeverity map[string]int    `json:"signals_by_severity,omitempty"`
	Rows              []ReviewSignalRow `json:"rows"`
}

func (s *Service) BuildReviewSignalsWeek(ctx context.Context, requestedWeek string) (ReviewSignalsWeekSnapshot, error) {
	if s == nil {
		return ReviewSignalsWeekSnapshot{}, fmt.Errorf("service is required")
	}
	window, err := s.ResolveWeekWindow(ctx, requestedWeek, time.Time{})
	if err != nil {
		return ReviewSignalsWeekSnapshot{}, err
	}
	week := window.CurrentWeek
	store, err := s.OpenStoreForWeek(week)
	if err != nil {
		return ReviewSignalsWeekSnapshot{}, fmt.Errorf("open semantic store for semantic week %q: %w", week, err)
	}
	defer func() {
		_ = store.Close()
	}()

	weekData, err := semanticquery.LoadWeekData(ctx, store, semanticquery.LoadWeekDataOptions{})
	if err != nil {
		return ReviewSignalsWeekSnapshot{}, err
	}

	previousWeekCanonicals := loadPreviousWeekCanonicals(ctx, s, window.PreviousWeek)

	sourceClusters := append([]semanticcontracts.FailurePatternRecord(nil), weekData.SourceFailurePatterns...)
	rows := make([]ReviewSignalRow, 0, len(weekData.ReviewQueue))
	signalsByReason := map[string]int{}

	if len(previousWeekCanonicals) > 0 {
		newPatternRows := crossWeekNewPatternSignals(weekData.SourceFailurePatterns, previousWeekCanonicals)
		for i := range newPatternRows {
			reason := newPatternRows[i].Reason
			if reason != "" {
				signalsByReason[reason]++
			}
			rows = append(rows, newPatternRows[i])
		}
	}

	for _, item := range weekData.ReviewQueue {
		reason := strings.TrimSpace(item.Reason)
		if reason != "" {
			signalsByReason[reason]++
		}
		rows = append(rows, ReviewSignalRow{
			Environment:                          normalizeEnvironment(item.Environment),
			ReviewItemID:                         strings.TrimSpace(item.ReviewItemID),
			Phase:                                strings.TrimSpace(item.Phase),
			Reason:                               reason,
			Severity:                             strings.TrimSpace(item.Severity),
			ProposedFailurePattern:               strings.TrimSpace(item.ProposedCanonicalEvidencePhrase),
			ProposedSearchQuery:                  strings.TrimSpace(item.ProposedSearchQueryPhrase),
			ProposedSearchQuerySourceRunURL:      strings.TrimSpace(item.ProposedSearchQuerySourceRunURL),
			ProposedSearchQuerySourceSignatureID: strings.TrimSpace(item.ProposedSearchQuerySourceSignatureID),
			SourcePhase1ClusterIDs:               reviewSignalCopyStrings(item.SourcePhase1ClusterIDs),
			MemberSignatureIDs:                   reviewSignalCopyStrings(item.MemberSignatureIDs),
			References:                           reviewSignalReferences(item.References),
			MatchedFailurePatterns:               reviewSignalMatchedFailurePatterns(item, sourceClusters),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		si := reviewSignalSeverityRank(rows[i].Severity)
		sj := reviewSignalSeverityRank(rows[j].Severity)
		if si != sj {
			return si < sj
		}
		if rows[i].Environment != rows[j].Environment {
			return rows[i].Environment < rows[j].Environment
		}
		if rows[i].Phase != rows[j].Phase {
			return rows[i].Phase < rows[j].Phase
		}
		if rows[i].Reason != rows[j].Reason {
			return rows[i].Reason < rows[j].Reason
		}
		return rows[i].ReviewItemID < rows[j].ReviewItemID
	})

	signalsBySeverity := map[string]int{}
	for _, row := range rows {
		sev := strings.TrimSpace(row.Severity)
		if sev == "" {
			sev = "unset"
		}
		signalsBySeverity[sev]++
	}

	return ReviewSignalsWeekSnapshot{
		Weeks:             append([]string(nil), window.Weeks...),
		Week:              week,
		PreviousWeek:      window.PreviousWeek,
		NextWeek:          window.NextWeek,
		Timezone:          "UTC",
		TotalSignals:      len(rows),
		SignalsByReason:   signalsByReason,
		SignalsBySeverity: signalsBySeverity,
		Rows:              rows,
	}, nil
}

func reviewSignalReferences(rows []semanticcontracts.ReferenceRecord) []ReviewSignalReference {
	if len(rows) == 0 {
		return nil
	}
	out := make([]ReviewSignalReference, 0, len(rows))
	for _, row := range rows {
		out = append(out, ReviewSignalReference{
			RowID:          strings.TrimSpace(row.RowID),
			RunURL:         strings.TrimSpace(row.RunURL),
			OccurredAt:     strings.TrimSpace(row.OccurredAt),
			SignatureID:    strings.TrimSpace(row.SignatureID),
			PRNumber:       row.PRNumber,
			PostGoodCommit: row.PostGoodCommit,
		})
	}
	return out
}

func reviewSignalMatchedFailurePatterns(
	item semanticcontracts.ReviewItemRecord,
	clusters []semanticcontracts.FailurePatternRecord,
) []ReviewSignalMatchedFailurePattern {
	if len(clusters) == 0 {
		return nil
	}
	environment := normalizeEnvironment(item.Environment)
	if environment == "" {
		return nil
	}
	sourcePhase1IDs := map[string]struct{}{}
	for _, phase1ID := range item.SourcePhase1ClusterIDs {
		trimmed := strings.TrimSpace(phase1ID)
		if trimmed == "" {
			continue
		}
		sourcePhase1IDs[trimmed] = struct{}{}
	}
	referenceKeys := map[string]struct{}{}
	for _, key := range reviewSignalReferenceKeys(item.Environment, item.References) {
		referenceKeys[key] = struct{}{}
	}

	out := make([]ReviewSignalMatchedFailurePattern, 0, 2)
	seen := map[string]struct{}{}
	for _, cluster := range clusters {
		clusterEnvironment := normalizeEnvironment(cluster.Environment)
		if clusterEnvironment == "" || clusterEnvironment != environment {
			continue
		}
		if !reviewSignalMatchesCluster(sourcePhase1IDs, referenceKeys, cluster) {
			continue
		}
		matched := ReviewSignalMatchedFailurePattern{
			Environment:      clusterEnvironment,
			FailurePatternID: strings.TrimSpace(cluster.Phase2ClusterID),
			FailurePattern:   strings.TrimSpace(cluster.CanonicalEvidencePhrase),
			SearchQuery:      strings.TrimSpace(cluster.SearchQueryPhrase),
		}
		key := matched.Environment + "|" + matched.FailurePatternID
		if key == "|" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, matched)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Environment != out[j].Environment {
			return out[i].Environment < out[j].Environment
		}
		if out[i].FailurePattern != out[j].FailurePattern {
			return out[i].FailurePattern < out[j].FailurePattern
		}
		return out[i].FailurePatternID < out[j].FailurePatternID
	})
	return out
}

func reviewSignalMatchesCluster(
	sourcePhase1IDs map[string]struct{},
	referenceKeys map[string]struct{},
	cluster semanticcontracts.FailurePatternRecord,
) bool {
	for _, phase1ID := range cluster.MemberPhase1ClusterIDs {
		if _, exists := sourcePhase1IDs[strings.TrimSpace(phase1ID)]; exists {
			return true
		}
	}
	for _, key := range reviewSignalReferenceKeys(cluster.Environment, cluster.References) {
		if _, exists := referenceKeys[key]; exists {
			return true
		}
	}
	return false
}

func reviewSignalReferenceKeys(environment string, references []semanticcontracts.ReferenceRecord) []string {
	if len(references) == 0 {
		return nil
	}
	normalizedEnvironment := normalizeEnvironment(environment)
	if normalizedEnvironment == "" {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(references)*2)
	appendKey := func(key string) {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			return
		}
		if _, exists := seen[trimmed]; exists {
			return
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	for _, reference := range references {
		runURL := strings.TrimSpace(reference.RunURL)
		rowID := strings.TrimSpace(reference.RowID)
		signatureID := strings.TrimSpace(reference.SignatureID)
		if runURL != "" && rowID != "" {
			appendKey(normalizedEnvironment + "|" + runURL + "|" + rowID)
		}
		if runURL != "" && signatureID != "" {
			appendKey(normalizedEnvironment + "|" + runURL + "|" + signatureID)
		}
	}
	return out
}

func reviewSignalCopyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func reviewSignalSeverityRank(severity string) int {
	switch strings.ToLower(strings.TrimSpace(severity)) {
	case "high":
		return 0
	case "medium":
		return 1
	case "low":
		return 2
	default:
		return 3
	}
}

// loadPreviousWeekCanonicals attempts to load the set of canonical failure
// phrases from the previous week (keyed by environment+lowered canonical).
// Returns nil without error if the previous week is unavailable.
func loadPreviousWeekCanonicals(ctx context.Context, s *Service, previousWeek string) map[string]struct{} {
	if strings.TrimSpace(previousWeek) == "" || s == nil {
		return nil
	}
	prevStore, err := s.OpenStoreForWeek(previousWeek)
	if err != nil {
		return nil
	}
	defer func() { _ = prevStore.Close() }()
	prevData, err := semanticquery.LoadWeekData(ctx, prevStore, semanticquery.LoadWeekDataOptions{})
	if err != nil {
		return nil
	}
	canonicals := make(map[string]struct{}, len(prevData.SourceFailurePatterns))
	for _, fp := range prevData.SourceFailurePatterns {
		key := strings.ToLower(strings.TrimSpace(normalizeEnvironment(fp.Environment) +
			"|" + strings.TrimSpace(fp.CanonicalEvidencePhrase)))
		if key != "|" {
			canonicals[key] = struct{}{}
		}
	}
	return canonicals
}

// crossWeekNewPatternSignals generates review signals for failure patterns
// that appear in the current week but not in the previous week. These
// are patterns newly emerged since last week.
func crossWeekNewPatternSignals(
	currentPatterns []semanticcontracts.FailurePatternRecord,
	previousCanonicals map[string]struct{},
) []ReviewSignalRow {
	if len(previousCanonicals) == 0 {
		return nil
	}
	rows := make([]ReviewSignalRow, 0)
	for _, fp := range currentPatterns {
		env := normalizeEnvironment(fp.Environment)
		canonical := strings.TrimSpace(fp.CanonicalEvidencePhrase)
		key := strings.ToLower(env + "|" + canonical)
		if _, found := previousCanonicals[key]; found {
			continue
		}
		severity := "low"
		if fp.SupportCount >= 5 {
			severity = "medium"
		}
		refs := make([]ReviewSignalReference, 0)
		for _, ref := range fp.References {
			refs = append(refs, ReviewSignalReference{
				RowID:          strings.TrimSpace(ref.RowID),
				RunURL:         strings.TrimSpace(ref.RunURL),
				OccurredAt:     strings.TrimSpace(ref.OccurredAt),
				SignatureID:    strings.TrimSpace(ref.SignatureID),
				PRNumber:       ref.PRNumber,
				PostGoodCommit: ref.PostGoodCommit,
			})
		}
		rows = append(rows, ReviewSignalRow{
			Environment:                          env,
			ReviewItemID:                         "crossweek-" + strings.TrimSpace(fp.Phase2ClusterID),
			Phase:                                "crossweek",
			Reason:                               "new_this_week",
			Severity:                             severity,
			ProposedFailurePattern:               canonical,
			ProposedSearchQuery:                  strings.TrimSpace(fp.SearchQueryPhrase),
			ProposedSearchQuerySourceRunURL:      strings.TrimSpace(fp.SearchQuerySourceRunURL),
			ProposedSearchQuerySourceSignatureID: strings.TrimSpace(fp.SearchQuerySourceSignatureID),
			SourcePhase1ClusterIDs:               reviewSignalCopyStrings(fp.MemberPhase1ClusterIDs),
			MemberSignatureIDs:                   reviewSignalCopyStrings(fp.MemberSignatureIDs),
			References:                           refs,
		})
	}
	return rows
}
