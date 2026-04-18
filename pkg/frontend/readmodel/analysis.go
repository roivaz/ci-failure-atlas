package readmodel

import (
    "fmt"
    "sort"
    "strings"
    "time"
    "unicode"
)

func OrderedUniqueReferences(rows []RunReference) []RunReference {
	if len(rows) == 0 {
		return nil
	}
	byRunURL := map[string]RunReference{}
	withoutRunURL := make([]RunReference, 0)
	for _, row := range rows {
		runURL := strings.TrimSpace(row.RunURL)
		if runURL == "" {
			withoutRunURL = append(withoutRunURL, row)
			continue
		}
		existing, ok := byRunURL[runURL]
		if !ok || referenceIsNewer(row, existing) {
			byRunURL[runURL] = row
		}
	}
	ordered := make([]RunReference, 0, len(byRunURL)+len(withoutRunURL))
	for _, row := range byRunURL {
		ordered = append(ordered, row)
	}
	ordered = append(ordered, withoutRunURL...)
	sort.Slice(ordered, func(i, j int) bool {
		ti, okI := ParseReferenceTimestamp(ordered[i].OccurredAt)
		tj, okJ := ParseReferenceTimestamp(ordered[j].OccurredAt)
		switch {
		case okI && okJ && !ti.Equal(tj):
			return ti.After(tj)
		case okI != okJ:
			return okI
		}
		runURLI := strings.TrimSpace(ordered[i].RunURL)
		runURLJ := strings.TrimSpace(ordered[j].RunURL)
		if runURLI != runURLJ {
			return runURLI < runURLJ
		}
		return strings.TrimSpace(ordered[i].SignatureID) < strings.TrimSpace(ordered[j].SignatureID)
	})
	return ordered
}

func OrderedContributingTests(items []ContributingTest) []ContributingTest {
	if len(items) == 0 {
		return nil
	}
	out := append([]ContributingTest(nil), items...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Occurrences != out[j].Occurrences {
			return out[i].Occurrences > out[j].Occurrences
		}
		if out[i].FailedAt != out[j].FailedAt {
			return out[i].FailedAt < out[j].FailedAt
		}
		if out[i].JobName != out[j].JobName {
			return out[i].JobName < out[j].JobName
		}
		return out[i].TestName < out[j].TestName
	})
	return out
}

func ParseReferenceTimestamp(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, false
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts.UTC(), true
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts.UTC(), true
	}
	return time.Time{}, false
}

func QualityIssueCodes(phrase string) []string {
	trimmed := strings.TrimSpace(phrase)
	normalized := strings.ToLower(trimmed)
	set := map[string]struct{}{}
	add := func(code string) {
		if strings.TrimSpace(code) == "" {
			return
		}
		set[code] = struct{}{}
	}
	if trimmed == "" {
		add("empty_phrase")
	}
	if isGenericFailurePhrase(trimmed) {
		add("generic_failure_phrase")
	}
	if len([]rune(trimmed)) > 0 && len([]rune(trimmed)) <= 3 {
		add("too_short_phrase")
	}
	if strings.Contains(normalized, "<context.") {
		add("context_type_stub")
	}
	if strings.Contains(normalized, "errorcode:\"\"") || strings.Contains(normalized, "errorcode: \"\"") || strings.Contains(normalized, "errorcode:''") || strings.Contains(normalized, "errorcode: ''") {
		add("empty_error_code")
	}
	if phraseLooksLikeStructFragment(trimmed) {
		add("struct_fragment")
	}
	if phraseMostlyPunctuation(trimmed) {
		add("mostly_punctuation")
	}
	if containsDeserializationNoOutputSignal(trimmed) {
		add("source_deserialization_no_output")
	}
	out := make([]string, 0, len(set))
	for code := range set {
		out = append(out, code)
	}
	sort.Slice(out, func(i, j int) bool {
		if qualityIssueWeight(out[i]) != qualityIssueWeight(out[j]) {
			return qualityIssueWeight(out[i]) > qualityIssueWeight(out[j])
		}
		return out[i] < out[j]
	})
	return out
}

func QualityScore(issueCodes []string) int {
	score := 0
	for _, issue := range issueCodes {
		score += qualityIssueWeight(issue)
	}
	return score
}

func QualityIssueLabel(code string) string {
	switch strings.TrimSpace(code) {
	case "empty_phrase":
		return "empty phrase"
	case "too_short_phrase":
		return "very short phrase"
	case "generic_failure_phrase":
		return "generic fallback phrase"
	case "context_type_stub":
		return "context type stub leaked"
	case "empty_error_code":
		return "contains empty ErrorCode"
	case "struct_fragment":
		return "struct/object fragment"
	case "mostly_punctuation":
		return "mostly punctuation"
	case "source_deserialization_no_output":
		return "source deserialization/no-output error"
	default:
		return code
	}
}

func DailyDensitySparkline(references []RunReference, windowDays int, endAnchor time.Time) (string, []int, string, bool) {
	if windowDays <= 0 {
		return "", nil, "", false
	}
	endDay := endAnchor.UTC().Truncate(24 * time.Hour)
	if endDay.IsZero() {
		endDay = time.Now().UTC().Truncate(24 * time.Hour)
	}
	startDay := endDay.AddDate(0, 0, -(windowDays - 1))

	counts := make([]int, windowDays)
	seenTimestamp := false
	for _, reference := range references {
		ts, ok := ParseReferenceTimestamp(reference.OccurredAt)
		if !ok {
			continue
		}
		seenTimestamp = true
		day := ts.UTC().Truncate(24 * time.Hour)
		if day.Before(startDay) || day.After(endDay) {
			continue
		}
		index := int(day.Sub(startDay).Hours() / 24)
		if index < 0 || index >= windowDays {
			continue
		}
		counts[index]++
	}
	if !seenTimestamp {
		return "", nil, "", false
	}

	maxCount := 0
	for _, value := range counts {
		if value > maxCount {
			maxCount = value
		}
	}

	unicodeLevels := []rune("▁▂▃▄▅▆▇█")
	var unicodeBuilder strings.Builder
	for _, value := range counts {
		if value <= 0 {
			unicodeBuilder.WriteRune('·')
			continue
		}
		levelIndex := len(unicodeLevels) - 1
		if maxCount > 0 {
			levelIndex = value * (len(unicodeLevels) - 1) / maxCount
		}
		unicodeBuilder.WriteRune(unicodeLevels[levelIndex])
	}

	dateRange := fmt.Sprintf("%s..%s UTC", startDay.Format("2006-01-02"), endDay.Format("2006-01-02"))
	return unicodeBuilder.String(), counts, dateRange, true
}

func BadPRScoreAndReasons(row FailurePatternRow) (int, []string) {
	if rowPostGoodCount(row) > 0 {
		return 0, nil
	}

	score := 1
	reasons := []string{"post-good=0"}

	if isOnlySeenInDev(row) {
		score++
		reasons = append(reasons, "only seen in DEV")
	}
	if isSingleKnownPR(row) {
		score++
		reasons = append(reasons, "only seen in one PR")
	}
	return score, reasons
}

func FlakeScoreAndReasons(row FailurePatternRow) (int, []string) {
	score := 0
	reasons := make([]string, 0, 7)

	jobsAffected := rowJobsAffected(row)
	jobPoints := flakeAffectedJobPoints(jobsAffected)
	if jobPoints > 0 {
		score += jobPoints
		reasons = append(reasons, fmt.Sprintf("jobs affected +%d", jobPoints))
	}

	postGoodPoints := flakePostGoodPoints(rowPostGoodCount(row))
	if postGoodPoints > 0 {
		score += postGoodPoints
		reasons = append(reasons, fmt.Sprintf("after last push +%d", postGoodPoints))
	}

	spreadPoints := flakeSpreadPoints(row.TrendCounts)
	if spreadPoints > 0 {
		score += spreadPoints
		reasons = append(reasons, fmt.Sprintf("daily spread +%d", spreadPoints))
	}

	recentPoints := flakeRecentPoints(rowScoreReferences(row), row.TrendRange)
	if recentPoints > 0 {
		score += recentPoints
		reasons = append(reasons, fmt.Sprintf("recent occurrence +%d", recentPoints))
	}

	historyPoints := flakeHistoryWeeksPoints(row.PriorWeeksPresent)
	if historyPoints > 0 {
		score += historyPoints
		reason := fmt.Sprintf("present in %d prior week", row.PriorWeeksPresent)
		if row.PriorWeeksPresent != 1 {
			reason += "s"
		}
		reasons = append(reasons, fmt.Sprintf("%s +%d", reason, historyPoints))
	}

	badPRScore, _ := BadPRScoreAndReasons(row)
	if badPRScore > 0 {
		score -= badPRScore
		reasons = append(reasons, fmt.Sprintf("likely bad PR -%d", badPRScore))
	}

	if score < 0 {
		score = 0
	}
	if score > 14 {
		score = 14
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "no strong flake signals")
	}
	return score, reasons
}

func affectedJobCount(row FailurePatternRow) int {
	return len(OrderedUniqueReferences(row.AffectedRuns))
}

func rowAffectedReferences(row FailurePatternRow) []RunReference {
	combined := append([]RunReference(nil), row.AffectedRuns...)
	for _, child := range row.LinkedPatterns {
		combined = append(combined, child.AffectedRuns...)
	}
	return OrderedUniqueReferences(combined)
}

func rowScoreReferences(row FailurePatternRow) []RunReference {
	if len(row.ScoringReferences) > 0 {
		return row.ScoringReferences
	}
	return row.AffectedRuns
}

func rowPostGoodCount(row FailurePatternRow) int {
	if len(row.LinkedPatterns) == 0 {
		return row.AfterLastPushCount
	}
	total := 0
	for _, child := range row.LinkedPatterns {
		total += child.AfterLastPushCount
	}
	if total > 0 {
		return total
	}
	return row.AfterLastPushCount
}

func rowJobsAffected(row FailurePatternRow) int {
	if refs := rowAffectedReferences(row); len(refs) > 0 {
		return len(refs)
	}
	return affectedJobCount(row)
}

func totalAffectedJobs(rows []FailurePatternRow) int {
	seenRuns := map[string]struct{}{}
	for _, row := range rows {
		for _, reference := range rowAffectedReferences(row) {
			runURL := strings.TrimSpace(reference.RunURL)
			if runURL == "" {
				continue
			}
			seenRuns[runURL] = struct{}{}
		}
	}
	return len(seenRuns)
}

func impactShare(jobsAffected int, impactTotalJobs int) float64 {
	if impactTotalJobs <= 0 {
		return 0
	}
	return (float64(jobsAffected) * 100.0) / float64(impactTotalJobs)
}

func impactScoreClass(percent float64) string {
	switch {
	case percent >= 20:
		return "impact-high"
	case percent >= 8:
		return "impact-medium"
	default:
		return "impact-low"
	}
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func flakeAffectedJobPoints(jobsAffected int) int {
	switch {
	case jobsAffected >= 12:
		return 3
	case jobsAffected >= 6:
		return 2
	case jobsAffected >= 3:
		return 1
	default:
		return 0
	}
}

func flakePostGoodPoints(postGoodCount int) int {
	switch {
	case postGoodCount >= 8:
		return 3
	case postGoodCount >= 4:
		return 2
	case postGoodCount >= 1:
		return 1
	default:
		return 0
	}
}

func flakeSpreadPoints(counts []int) int {
	if len(counts) == 0 {
		return 0
	}
	total := 0
	activeDays := 0
	maxDaily := 0
	for _, count := range counts {
		if count <= 0 {
			continue
		}
		total += count
		activeDays++
		if count > maxDaily {
			maxDaily = count
		}
	}
	if total <= 0 || activeDays <= 0 {
		return 0
	}
	concentration := float64(maxDaily) / float64(total)
	switch {
	case activeDays >= 5 && concentration <= 0.5:
		return 2
	case activeDays >= 3 && concentration <= 0.75:
		return 1
	default:
		return 0
	}
}

func flakeRecentPoints(references []RunReference, trendRange string) int {
	latest, ok := latestReferenceTimestamp(references)
	if !ok {
		return 0
	}
	anchor, ok := trendRangeEndAnchor(trendRange)
	if !ok {
		return 0
	}
	if latest.After(anchor) {
		latest = anchor
	}
	age := anchor.Sub(latest)
	if age < 0 {
		return 0
	}
	switch {
	case age <= 24*time.Hour:
		return 2
	case age <= 48*time.Hour:
		return 1
	default:
		return 0
	}
}

func flakeHistoryWeeksPoints(priorWeeksPresent int) int {
	if priorWeeksPresent <= 0 {
		return 0
	}
	if priorWeeksPresent > 4 {
		return 4
	}
	return priorWeeksPresent
}

func latestReferenceTimestamp(references []RunReference) (time.Time, bool) {
	var latest time.Time
	for _, reference := range references {
		ts, ok := ParseReferenceTimestamp(reference.OccurredAt)
		if !ok {
			continue
		}
		if latest.IsZero() || ts.After(latest) {
			latest = ts
		}
	}
	if latest.IsZero() {
		return time.Time{}, false
	}
	return latest, true
}

func trendRangeEndAnchor(trendRange string) (time.Time, bool) {
	parts := strings.Split(strings.TrimSpace(trendRange), "..")
	if len(parts) != 2 {
		return time.Time{}, false
	}
	endLabel := strings.TrimSuffix(strings.TrimSpace(parts[1]), " UTC")
	endDay, err := time.Parse("2006-01-02", strings.TrimSpace(endLabel))
	if err != nil {
		return time.Time{}, false
	}
	return endDay.UTC().Add(24 * time.Hour), true
}

func flakeScoreClass(score int) string {
	switch {
	case score >= 10:
		return "flake-high"
	case score >= 5:
		return "flake-medium"
	default:
		return "flake-low"
	}
}

func flakeScoreLabel(score int) string {
	switch {
	case score >= 10:
		return "High"
	case score >= 5:
		return "Medium"
	default:
		return "Low"
	}
}

func SortRowsByDefaultPriority(rows []FailurePatternRow) {
	sortRowsByDefaultPriorityWithImpact(rows, totalAffectedJobs(rows))
}

func sortRowsByDefaultPriorityWithImpact(rows []FailurePatternRow, impactTotalJobs int) {
	sort.Slice(rows, func(i, j int) bool {
		impactI := impactShare(rowJobsAffected(rows[i]), impactTotalJobs)
		impactJ := impactShare(rowJobsAffected(rows[j]), impactTotalJobs)
		if impactI != impactJ {
			return impactI > impactJ
		}
		jobsI := rowJobsAffected(rows[i])
		jobsJ := rowJobsAffected(rows[j])
		if jobsI != jobsJ {
			return jobsI > jobsJ
		}
		flakeI, _ := FlakeScoreAndReasons(rows[i])
		flakeJ, _ := FlakeScoreAndReasons(rows[j])
		if flakeI != flakeJ {
			return flakeI > flakeJ
		}
		if rows[i].OccurrenceShare != rows[j].OccurrenceShare {
			return rows[i].OccurrenceShare > rows[j].OccurrenceShare
		}
		if rows[i].Occurrences != rows[j].Occurrences {
			return rows[i].Occurrences > rows[j].Occurrences
		}
		postGoodI := rowPostGoodCount(rows[i])
		postGoodJ := rowPostGoodCount(rows[j])
		if postGoodI != postGoodJ {
			return postGoodI > postGoodJ
		}
		if strings.TrimSpace(rows[i].Environment) != strings.TrimSpace(rows[j].Environment) {
			return strings.TrimSpace(rows[i].Environment) < strings.TrimSpace(rows[j].Environment)
		}
		if strings.TrimSpace(rows[i].FailurePattern) != strings.TrimSpace(rows[j].FailurePattern) {
			return strings.TrimSpace(rows[i].FailurePattern) < strings.TrimSpace(rows[j].FailurePattern)
		}
		return strings.TrimSpace(rows[i].FailurePatternID) < strings.TrimSpace(rows[j].FailurePatternID)
	})
}

func SortRowsByBadPRScore(rows []FailurePatternRow) {
	sort.Slice(rows, func(i, j int) bool {
		scoreI, _ := BadPRScoreAndReasons(rows[i])
		scoreJ, _ := BadPRScoreAndReasons(rows[j])
		if scoreI != scoreJ {
			return scoreI < scoreJ
		}
		if rows[i].Occurrences != rows[j].Occurrences {
			return rows[i].Occurrences > rows[j].Occurrences
		}
		postGoodI := rowPostGoodCount(rows[i])
		postGoodJ := rowPostGoodCount(rows[j])
		if postGoodI != postGoodJ {
			return postGoodI > postGoodJ
		}
		if rows[i].OccurrenceShare != rows[j].OccurrenceShare {
			return rows[i].OccurrenceShare > rows[j].OccurrenceShare
		}
		if strings.TrimSpace(rows[i].Environment) != strings.TrimSpace(rows[j].Environment) {
			return strings.TrimSpace(rows[i].Environment) < strings.TrimSpace(rows[j].Environment)
		}
		if strings.TrimSpace(rows[i].FailurePattern) != strings.TrimSpace(rows[j].FailurePattern) {
			return strings.TrimSpace(rows[i].FailurePattern) < strings.TrimSpace(rows[j].FailurePattern)
		}
		return strings.TrimSpace(rows[i].FailurePatternID) < strings.TrimSpace(rows[j].FailurePatternID)
	})
}

func isOnlySeenInDev(row FailurePatternRow) bool {
	if strings.ToLower(strings.TrimSpace(row.Environment)) != "dev" {
		return false
	}
	for _, value := range row.AlsoIn {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}
	return true
}

func isSingleKnownPR(row FailurePatternRow) bool {
	references := OrderedUniqueReferences(rowScoreReferences(row))
	if len(references) == 0 {
		return false
	}
	uniquePRs := map[int]struct{}{}
	for _, reference := range references {
		if reference.PRNumber <= 0 {
			return false
		}
		uniquePRs[reference.PRNumber] = struct{}{}
	}
	return len(uniquePRs) == 1
}

func qualityIssueWeight(code string) int {
	switch strings.TrimSpace(code) {
	case "empty_phrase":
		return 6
	case "struct_fragment":
		return 5
	case "context_type_stub":
		return 4
	case "empty_error_code":
		return 4
	case "too_short_phrase":
		return 3
	case "generic_failure_phrase":
		return 5
	case "mostly_punctuation":
		return 3
	case "source_deserialization_no_output":
		return 9
	default:
		return 1
	}
}

func phraseLooksLikeStructFragment(input string) bool {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return false
	}
	if strings.Contains(trimmed, "{") && strings.Contains(trimmed, "}") && strings.Contains(trimmed, ":") {
		return true
	}
	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		return true
	}
	if strings.Contains(trimmed, "map[") && strings.Contains(trimmed, "]") {
		return true
	}
	return false
}

func phraseMostlyPunctuation(input string) bool {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return false
	}
	total := 0
	punctuation := 0
	for _, r := range trimmed {
		if unicode.IsSpace(r) {
			continue
		}
		total++
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			punctuation++
		}
	}
	if total == 0 {
		return false
	}
	return float64(punctuation)/float64(total) >= 0.6
}

func isGenericFailurePhrase(input string) bool {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "failure", "failed", "error", "unknown error", "test failed":
		return true
	default:
		return false
	}
}

func containsDeserializationNoOutputSignal(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return false
	}
	return strings.Contains(normalized, "deserializaion error: no output from command") ||
		strings.Contains(normalized, "deserialization error: no output from command")
}

func referenceIsNewer(candidate RunReference, existing RunReference) bool {
	candidateTime, candidateHasTime := ParseReferenceTimestamp(candidate.OccurredAt)
	existingTime, existingHasTime := ParseReferenceTimestamp(existing.OccurredAt)
	switch {
	case candidateHasTime && existingHasTime && !candidateTime.Equal(existingTime):
		return candidateTime.After(existingTime)
	case candidateHasTime != existingHasTime:
		return candidateHasTime
	}
	return len(strings.TrimSpace(candidate.OccurredAt)) > len(strings.TrimSpace(existing.OccurredAt))
}
