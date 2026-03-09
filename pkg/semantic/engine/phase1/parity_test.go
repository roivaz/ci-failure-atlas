package phase1

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
)

type pythonInputRow struct {
	Lane       string `json:"lane"`
	JobName    string `json:"job_name"`
	TestName   string `json:"test_name"`
	TestSuite  string `json:"test_suite"`
	Signature  string `json:"signature_id"`
	OccurredAt string `json:"occurred_at"`
	RunURL     string `json:"run_url"`
	RawText    string `json:"raw_text"`
}

type referenceNormalizedRow struct {
	Lane          string `json:"lane"`
	JobName       string `json:"job_name"`
	TestName      string `json:"test_name"`
	SignatureID   string `json:"signature_id"`
	OccurredAt    string `json:"occurred_at"`
	RunURL        string `json:"run_url"`
	Phase1Key     string `json:"phase1_key"`
	Provider      string `json:"provider_anchor"`
	GenericPhrase bool   `json:"generic_phrase"`
}

type referenceAssignmentRow struct {
	RowID                 string   `json:"row_id"`
	GroupKey              string   `json:"group_key"`
	LocalClusterKey       string   `json:"phase1_local_cluster_key"`
	Confidence            string   `json:"confidence"`
	Reasons               []string `json:"reasons"`
	CanonicalEvidenceHint string   `json:"canonical_evidence_phrase_candidate"`
}

func TestPhase1BehavioralParityWithPythonFixture(t *testing.T) {
	t.Parallel()

	baseDir := filepath.Join("testdata", "parity")
	inputRows := readNDJSONFixture[pythonInputRow](t, filepath.Join(baseDir, "python_input.ndjson"))
	refNormalized := readNDJSONFixture[referenceNormalizedRow](t, filepath.Join(baseDir, "reference_normalized.ndjson"))
	refAssignments := readNDJSONFixture[referenceAssignmentRow](t, filepath.Join(baseDir, "reference_assignments.ndjson"))

	workset := make([]semanticcontracts.Phase1WorksetRecord, 0, len(inputRows))
	for _, row := range inputRows {
		workset = append(workset, semanticcontracts.Phase1WorksetRecord{
			SchemaVersion: semanticcontracts.SchemaVersionV1,
			RowID:         buildRowID(row.RunURL, row.Signature, row.OccurredAt),
			GroupKey:      buildGroupKey("", row.Lane, row.JobName, row.TestName),
			Lane:          row.Lane,
			JobName:       row.JobName,
			TestName:      row.TestName,
			TestSuite:     row.TestSuite,
			SignatureID:   row.Signature,
			OccurredAt:    row.OccurredAt,
			RunURL:        row.RunURL,
			RawText:       row.RawText,
		})
	}

	goNormalized := Normalize(workset)
	if len(goNormalized) != len(refNormalized) {
		t.Fatalf("normalized row count mismatch: got=%d want=%d", len(goNormalized), len(refNormalized))
	}

	type normKey struct {
		RunURL      string
		SignatureID string
		OccurredAt  string
	}
	goNormByKey := map[normKey]semanticcontracts.Phase1NormalizedRecord{}
	for _, row := range goNormalized {
		goNormByKey[normKey{RunURL: row.RunURL, SignatureID: row.SignatureID, OccurredAt: row.OccurredAt}] = row
	}
	for _, ref := range refNormalized {
		key := normKey{RunURL: ref.RunURL, SignatureID: ref.SignatureID, OccurredAt: ref.OccurredAt}
		got, ok := goNormByKey[key]
		if !ok {
			t.Fatalf("missing normalized row for key=%+v", key)
		}
		if got.Phase1Key != ref.Phase1Key {
			t.Fatalf("phase1_key mismatch for %v: got=%q want=%q", key, got.Phase1Key, ref.Phase1Key)
		}
		if got.ProviderAnchor != ref.Provider {
			t.Fatalf("provider anchor mismatch for %v: got=%q want=%q", key, got.ProviderAnchor, ref.Provider)
		}
		if got.GenericPhrase != ref.GenericPhrase {
			t.Fatalf("generic phrase mismatch for %v: got=%v want=%v", key, got.GenericPhrase, ref.GenericPhrase)
		}
	}

	goAssignments := Classify(goNormalized)
	if len(goAssignments) != len(refAssignments) {
		t.Fatalf("assignment row count mismatch: got=%d want=%d", len(goAssignments), len(refAssignments))
	}

	goAssignByRow := map[string]semanticcontracts.Phase1AssignmentRecord{}
	for _, row := range goAssignments {
		goAssignByRow[row.RowID] = row
	}
	for _, ref := range refAssignments {
		got, ok := goAssignByRow[ref.RowID]
		if !ok {
			t.Fatalf("missing assignment for row_id=%s", ref.RowID)
		}
		if got.GroupKey != ref.GroupKey {
			t.Fatalf("assignment group_key mismatch for %s: got=%q want=%q", ref.RowID, got.GroupKey, ref.GroupKey)
		}
		if got.Phase1LocalClusterKey != ref.LocalClusterKey {
			t.Fatalf("assignment local key mismatch for %s: got=%q want=%q", ref.RowID, got.Phase1LocalClusterKey, ref.LocalClusterKey)
		}
		if got.Confidence != ref.Confidence {
			t.Fatalf("assignment confidence mismatch for %s: got=%q want=%q", ref.RowID, got.Confidence, ref.Confidence)
		}
		if !reflect.DeepEqual(got.Reasons, ref.Reasons) {
			t.Fatalf("assignment reasons mismatch for %s: got=%v want=%v", ref.RowID, got.Reasons, ref.Reasons)
		}
	}

	goClusters, goReview, err := Compile(workset, goAssignments)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Behavioral parity checks: cluster cardinality/support profile should stay stable.
	refSupportByLocal := map[string]int{}
	for _, row := range refAssignments {
		refSupportByLocal[row.LocalClusterKey]++
	}
	refSupports := make([]int, 0, len(refSupportByLocal))
	for _, count := range refSupportByLocal {
		refSupports = append(refSupports, count)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(refSupports)))

	goSupports := make([]int, 0, len(goClusters))
	for _, cluster := range goClusters {
		goSupports = append(goSupports, cluster.SupportCount)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(goSupports)))
	if !reflect.DeepEqual(goSupports, refSupports) {
		t.Fatalf("cluster support distribution mismatch: got=%v want=%v", goSupports, refSupports)
	}

	// High-overlap membership by row_id within each group.
	refMembership := map[string][]map[string]struct{}{}
	for _, row := range refAssignments {
		groupSets := refMembership[row.GroupKey]
		found := false
		for _, set := range groupSets {
			if _, ok := set[row.RowID+"|"+row.LocalClusterKey]; ok {
				found = true
				break
			}
		}
		_ = found
	}

	refByGroupAndLocal := map[string]map[string]map[string]struct{}{}
	for _, row := range refAssignments {
		if _, ok := refByGroupAndLocal[row.GroupKey]; !ok {
			refByGroupAndLocal[row.GroupKey] = map[string]map[string]struct{}{}
		}
		if _, ok := refByGroupAndLocal[row.GroupKey][row.LocalClusterKey]; !ok {
			refByGroupAndLocal[row.GroupKey][row.LocalClusterKey] = map[string]struct{}{}
		}
		refByGroupAndLocal[row.GroupKey][row.LocalClusterKey][row.RowID] = struct{}{}
	}

	goByGroup := map[string][]map[string]struct{}{}
	for _, cluster := range goClusters {
		groupKey := buildGroupKey("", cluster.Lane, cluster.JobName, cluster.TestName)
		rowIDs := map[string]struct{}{}
		for _, ref := range cluster.References {
			rowIDs[buildRowID(ref.RunURL, ref.SignatureID, ref.OccurredAt)] = struct{}{}
		}
		goByGroup[groupKey] = append(goByGroup[groupKey], rowIDs)
	}

	for groupKey, refLocals := range refByGroupAndLocal {
		goSets, ok := goByGroup[groupKey]
		if !ok {
			t.Fatalf("missing compiled group_key=%q", groupKey)
		}
		for localKey, refRows := range refLocals {
			bestOverlap := 0.0
			for _, goRows := range goSets {
				overlap := jaccard(refRows, goRows)
				if overlap > bestOverlap {
					bestOverlap = overlap
				}
			}
			if bestOverlap < 0.99 {
				t.Fatalf("low membership overlap for group=%q local=%q: got=%.2f", groupKey, localKey, bestOverlap)
			}
		}
	}

	// Stable top review reasons and provider ambiguity surfacing.
	reasonCounts := map[string]int{}
	for _, row := range goReview {
		reasonCounts[row.Reason]++
	}
	if reasonCounts["ambiguous_provider_merge"] == 0 {
		t.Fatalf("expected ambiguous_provider_merge review reason in compiled outputs")
	}
	if reasonCounts["low_confidence_evidence"] == 0 {
		t.Fatalf("expected low_confidence_evidence review reason in compiled outputs")
	}
}

func jaccard(a map[string]struct{}, b map[string]struct{}) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 1
	}
	intersection := 0
	union := map[string]struct{}{}
	for key := range a {
		union[key] = struct{}{}
	}
	for key := range b {
		union[key] = struct{}{}
		if _, ok := a[key]; ok {
			intersection++
		}
	}
	return float64(intersection) / float64(len(union))
}

func readNDJSONFixture[T any](t *testing.T, path string) []T {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open fixture %s: %v", path, err)
	}
	defer file.Close()

	out := make([]T, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1024*1024), 20*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var row T
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatalf("decode fixture row in %s: %v", path, err)
		}
		out = append(out, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan fixture %s: %v", path, err)
	}
	return out
}
