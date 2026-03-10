package contracts

// SchemaVersionV1 is the current semantic contract version; it is used by
// readers/writers for compatibility checks and is emitted as a literal "v1".
const SchemaVersionV1 = "v1"

type Phase1WorksetRecord struct {
	// SchemaVersion declares the payload schema; readers use it to decode safely;
	// writers currently set it to SchemaVersionV1.
	SchemaVersion string `json:"schema_version"`
	// Environment is the source environment (dev/int/stg/prod); it is used for
	// partitioning and ID scoping; derived from normalized source env flags.
	Environment string `json:"environment"`
	// RowID uniquely identifies the failure row; it is used as the primary join
	// key across phase1 artifacts; derived from raw failure RowID or fallback hash.
	RowID string `json:"row_id"`
	// GroupKey is the coarse grouping key; it is used to bucket rows before local
	// clustering; derived deterministically from environment/lane/job/test.
	GroupKey string `json:"group_key"`
	// Lane is the failure lane family; it is used for segmentation and clustering;
	// derived by lane classification rules from test/run context.
	Lane string `json:"lane"`
	// JobName is the CI job identifier; it is used to scope grouping and reports;
	// derived from the associated run record.
	JobName string `json:"job_name"`
	// TestName is the failing test identifier; it is used for grouping and triage;
	// derived from raw/artifact-backed failure facts.
	TestName string `json:"test_name"`
	// TestSuite is the test suite label; it is used for lane rules and grouping;
	// derived from raw/artifact-backed failure facts.
	TestSuite string `json:"test_suite"`
	// SignatureID is the deterministic failure fingerprint; it is used for dedupe
	// and cross-layer linking; derived upstream from normalized failure text hash.
	SignatureID string `json:"signature_id"`
	// OccurredAt is the failure timestamp; it is used for ordering and windows;
	// derived from run/raw-failure occurrence time.
	OccurredAt string `json:"occurred_at"`
	// RunURL points to the backing CI run; it is used for drilldown and joins;
	// derived from run facts for the failure row.
	RunURL string `json:"run_url"`
	// PRNumber is the related PR number (when present); it is used for PR-aware
	// triage metrics; derived from enriched run metadata.
	PRNumber int `json:"pr_number"`
	// PostGoodCommit marks runs after the good-PR baseline; it is used for
	// systemic-signal semantics; derived in run enrichment from PR merge state.
	PostGoodCommit bool `json:"post_good_commit"`
	// RawText is the original failure message; it is used for phrase extraction and
	// auditability; derived directly from raw failure facts.
	RawText string `json:"raw_text"`
	// NormalizedText is normalized failure text; it is used for stable matching and
	// clustering; derived by text normalization on RawText.
	NormalizedText string `json:"normalized_text"`
}

type Phase1NormalizedRecord struct {
	// SchemaVersion declares the payload schema; used for compatibility checks;
	// emitted by normalization as SchemaVersionV1.
	SchemaVersion string `json:"schema_version"`
	// Environment is the normalized environment partition; used to isolate keys;
	// propagated from phase1 workset input.
	Environment string `json:"environment"`
	// RowID is the stable row identifier; used to join normalized rows back to
	// workset/assignments; propagated from phase1 workset.
	RowID string `json:"row_id"`
	// GroupKey is the coarse grouping key; used to constrain local clustering;
	// propagated from phase1 workset/buildGroupKey.
	GroupKey string `json:"group_key"`
	// Lane is the classified lane family; used in cluster boundaries and sorting;
	// propagated from workset classification.
	Lane string `json:"lane"`
	// JobName is the CI job label; used in grouping and report context; propagated
	// from workset.
	JobName string `json:"job_name"`
	// TestName is the test identifier; used in grouping and triage outputs;
	// propagated from workset.
	TestName string `json:"test_name"`
	// TestSuite is the suite label; used by lane/rule logic and grouping;
	// propagated from workset.
	TestSuite string `json:"test_suite"`
	// SignatureID is the failure fingerprint; used for dedupe and membership;
	// propagated from workset/raw facts.
	SignatureID string `json:"signature_id"`
	// OccurredAt is the event time; used for deterministic ordering and windows;
	// propagated from workset.
	OccurredAt string `json:"occurred_at"`
	// RunURL is the CI run link; used for traceability and joins; propagated from
	// workset.
	RunURL string `json:"run_url"`
	// PRNumber is the related PR number; used for PR-level semantics and reports;
	// propagated from enriched run facts.
	PRNumber int `json:"pr_number"`
	// PostGoodCommit marks post-good runs; used for systemic-signal tracking;
	// propagated from enriched run facts.
	PostGoodCommit bool `json:"post_good_commit"`
	// RawText stores original failure text; used by phrase extraction heuristics;
	// propagated from workset/raw failures.
	RawText string `json:"raw_text"`
	// NormalizedText stores normalized failure text; used by phrase/key extraction;
	// propagated from workset normalization.
	NormalizedText string `json:"normalized_text"`
	// CanonicalEvidencePhrase is the main evidence label; used as cluster identity
	// signal; derived by normalization/classification phrase heuristics.
	CanonicalEvidencePhrase string `json:"canonical_evidence_phrase"`
	// SearchQueryPhrase is the query-friendly evidence phrase; used for log search
	// seeds and review context; derived from normalization phrase extraction.
	SearchQueryPhrase string `json:"search_query_phrase"`
	// ProviderAnchor is an extracted provider token; used to split generic errors
	// by provider family; derived via regex over failure text.
	ProviderAnchor string `json:"provider_anchor"`
	// GenericPhrase flags low-specificity evidence text; used to trigger extra
	// review/provider-aware handling; derived from phrase-genericity heuristics.
	GenericPhrase bool `json:"generic_phrase"`
	// Phase1Key is the deterministic local-cluster key input; used by assignment
	// and compile steps; derived from normalized evidence dimensions.
	Phase1Key string `json:"phase1_key"`
}

type Phase1AssignmentRecord struct {
	// SchemaVersion declares assignment schema version; used for decode
	// compatibility; emitted as SchemaVersionV1.
	SchemaVersion string `json:"schema_version"`
	// Environment scopes assignment records per env; used to avoid cross-env
	// collisions; propagated from normalized rows.
	Environment string `json:"environment"`
	// RowID points to the assigned normalized row; used as assignment join key;
	// propagated from normalized/workset RowID.
	RowID string `json:"row_id"`
	// GroupKey is the pre-clustering bucket key; used to constrain assignment
	// candidates; propagated from normalized rows.
	GroupKey string `json:"group_key"`
	// Phase1LocalClusterKey is the local cluster identifier within GroupKey; used
	// to aggregate rows into test clusters; derived by classify/assignment logic.
	Phase1LocalClusterKey string `json:"phase1_local_cluster_key"`
	// CanonicalEvidencePhraseCandidate is the row-level canonical phrase proposal;
	// used by compile voting; derived from normalized phrase candidates.
	CanonicalEvidencePhraseCandidate string `json:"canonical_evidence_phrase_candidate"`
	// SearchQueryPhraseCandidate is the row-level search phrase proposal; used by
	// compile voting/source selection; derived from normalization heuristics.
	SearchQueryPhraseCandidate string `json:"search_query_phrase_candidate"`
	// Confidence captures assignment confidence class; used for review triggers;
	// derived from assignment rule quality signals.
	Confidence string `json:"confidence"`
	// Reasons lists structured rule outputs; used to explain and review decisions;
	// derived from assignment/classification heuristics.
	Reasons []string `json:"reasons"`
}

type ReferenceRecord struct {
	// RunURL points to one backing CI run; used for drilldown and joins to facts;
	// derived from workset/run records.
	RunURL string `json:"run_url"`
	// OccurredAt is the referenced event timestamp; used for ordering and windows;
	// derived from the source row timestamp.
	OccurredAt string `json:"occurred_at"`
	// SignatureID is the referenced failure fingerprint; used to join back to raw
	// failure signatures; derived from source row signature_id.
	SignatureID string `json:"signature_id"`
	// PRNumber is the PR context for the reference; used in PR-oriented triage;
	// derived from enriched run metadata.
	PRNumber int `json:"pr_number"`
	// PostGoodCommit flags post-good references; used for systemic-signal counts;
	// derived from enriched run metadata.
	PostGoodCommit bool `json:"post_good_commit"`
}

type TestClusterRecord struct {
	// SchemaVersion declares cluster schema version; used for compatibility in
	// downstream consumers; emitted as SchemaVersionV1.
	SchemaVersion string `json:"schema_version"`
	// Environment partitions clusters by env; used in IDs/report filtering;
	// propagated from assignment/workset environment.
	Environment string `json:"environment"`
	// Phase1ClusterID is the deterministic cluster identifier; used as primary key
	// for phase1 clusters and phase2 inputs; derived from cluster fingerprinting.
	Phase1ClusterID string `json:"phase1_cluster_id"`
	// Lane is the cluster lane family; used in ranking, filtering, and grouping;
	// derived from member row lane values.
	Lane string `json:"lane"`
	// JobName is the shared job scope for cluster rows; used for triage context;
	// derived from member rows.
	JobName string `json:"job_name"`
	// TestName is the shared test name for cluster rows; used for per-test views;
	// derived from member rows.
	TestName string `json:"test_name"`
	// TestSuite is the suite context for the cluster; used for lane semantics and
	// report readability; derived from member rows.
	TestSuite string `json:"test_suite"`
	// CanonicalEvidencePhrase is the selected cluster evidence label; used as the
	// primary human-readable signature; derived by candidate voting/refinement.
	CanonicalEvidencePhrase string `json:"canonical_evidence_phrase"`
	// SearchQueryPhrase is the selected cluster query seed; used for log search and
	// drilldown; derived by candidate voting plus fallback resolution.
	SearchQueryPhrase string `json:"search_query_phrase"`
	// SearchQuerySourceRunURL identifies the run backing SearchQueryPhrase; used to
	// trace the phrase to concrete evidence; derived by source selection logic.
	SearchQuerySourceRunURL string `json:"search_query_source_run_url"`
	// SearchQuerySourceSignatureID identifies the signature backing the phrase;
	// used to join to precise failures; derived by source selection logic.
	SearchQuerySourceSignatureID string `json:"search_query_source_signature_id"`
	// SupportCount is the number of member rows; used for prioritization and rates;
	// derived as len(cluster members).
	SupportCount int `json:"support_count"`
	// SeenPostGoodCommit indicates any post-good member exists; used as systemic
	// signal in triage; derived from member PostGoodCommit flags.
	SeenPostGoodCommit bool `json:"seen_post_good_commit"`
	// PostGoodCommitCount is the number of post-good members; used for impact
	// ranking; derived by counting member PostGoodCommit=true rows.
	PostGoodCommitCount int `json:"post_good_commit_count"`
	// MemberSignatureIDs lists distinct member signature IDs; used for joins,
	// dedupe, and review IDs; derived by set-union across cluster members.
	MemberSignatureIDs []string `json:"member_signature_ids"`
	// References are compact trace links to supporting failures; used for drilldown
	// and sampling; derived from sorted member rows.
	References []ReferenceRecord `json:"references"`
}

type ContributingTestRecord struct {
	// Lane is the contributing test lane; used in global cluster composition views;
	// derived from source phase1 test cluster.
	Lane string `json:"lane"`
	// JobName is the contributing job name; used to identify failure surface area;
	// derived from source phase1 test cluster.
	JobName string `json:"job_name"`
	// TestName is the contributing test identifier; used for cross-test summaries;
	// derived from source phase1 test cluster.
	TestName string `json:"test_name"`
	// SupportCount is that test's contribution volume; used for weighting within a
	// global cluster; derived by summing member phase1 supports.
	SupportCount int `json:"support_count"`
}

type GlobalClusterRecord struct {
	// SchemaVersion declares global-cluster schema version; used for compatibility;
	// emitted as SchemaVersionV1.
	SchemaVersion string `json:"schema_version"`
	// Environment partitions global clusters by env; used to avoid cross-env merge;
	// derived from member phase1 cluster environment.
	Environment string `json:"environment"`
	// Phase2ClusterID is the deterministic global cluster ID; used as the phase2
	// primary key; derived from sorted member phase1 cluster IDs fingerprint.
	Phase2ClusterID string `json:"phase2_cluster_id"`
	// CanonicalEvidencePhrase is the representative global evidence label; used for
	// triage communication; derived from representative member cluster.
	CanonicalEvidencePhrase string `json:"canonical_evidence_phrase"`
	// SearchQueryPhrase is the representative global query seed; used for
	// investigation pivots; derived from representative/fallback phrase logic.
	SearchQueryPhrase string `json:"search_query_phrase"`
	// SearchQuerySourceRunURL references the run backing SearchQueryPhrase; used to
	// validate provenance; derived from representative or fallback reference.
	SearchQuerySourceRunURL string `json:"search_query_source_run_url"`
	// SearchQuerySourceSignatureID references the signature backing the query
	// phrase; used for exact joins; derived from representative/fallback source.
	SearchQuerySourceSignatureID string `json:"search_query_source_signature_id"`
	// SupportCount is total failures represented by the global cluster; used for
	// prioritization; derived by summing member phase1 support counts.
	SupportCount int `json:"support_count"`
	// SeenPostGoodCommit indicates post-good presence in the cluster; used for
	// systemic-signal interpretation; derived from member post-good counts.
	SeenPostGoodCommit bool `json:"seen_post_good_commit"`
	// PostGoodCommitCount is post-good volume in the cluster; used in ranking and
	// impact summaries; derived by summing member post-good counts.
	PostGoodCommitCount int `json:"post_good_commit_count"`
	// ContributingTestsCount is the number of distinct contributing tests; used for
	// blast-radius estimation; derived from ContributingTests cardinality.
	ContributingTestsCount int `json:"contributing_tests_count"`
	// ContributingTests lists per-test contributors; used to explain global merges;
	// derived by aggregating member phase1 clusters per test key.
	ContributingTests []ContributingTestRecord `json:"contributing_tests"`
	// MemberPhase1ClusterIDs lists merged phase1 cluster IDs; used for traceability
	// and deterministic IDs; derived by sorted set of members.
	MemberPhase1ClusterIDs []string `json:"member_phase1_cluster_ids"`
	// MemberSignatureIDs lists distinct member signatures; used for review IDs and
	// deep links; derived by set-union across members.
	MemberSignatureIDs []string `json:"member_signature_ids"`
	// References are compact links to supporting failures; used for drilldown and
	// sampling; derived from merged member references.
	References []ReferenceRecord `json:"references"`
}

type ReviewItemRecord struct {
	// SchemaVersion declares review-item schema version; used for compatibility;
	// emitted as SchemaVersionV1.
	SchemaVersion string `json:"schema_version"`
	// Environment scopes the review item to one env; used for safe partitioning;
	// derived from source cluster(s) environment.
	Environment string `json:"environment"`
	// ReviewItemID is the deterministic review identifier; used for dedupe and
	// stable queue updates; derived from env+phase+reason+member/source IDs hash.
	ReviewItemID string `json:"review_item_id"`
	// Phase indicates where the ambiguity was detected (phase1/phase2); used for
	// workflow routing; derived from emitting engine.
	Phase string `json:"phase"`
	// Reason is a machine-readable review trigger code; used for triage actions and
	// reporting; derived from heuristic/rule outcomes.
	Reason string `json:"reason"`
	// ProposedCanonicalEvidencePhrase is the suggested canonical phrase; used by
	// reviewers to accept/tune grouping; derived from source cluster output.
	ProposedCanonicalEvidencePhrase string `json:"proposed_canonical_evidence_phrase"`
	// ProposedSearchQueryPhrase is the suggested search phrase; used for
	// investigation and reviewer edits; derived from source cluster output.
	ProposedSearchQueryPhrase string `json:"proposed_search_query_phrase"`
	// ProposedSearchQuerySourceRunURL points to phrase provenance; used to inspect
	// evidence context; derived from source cluster/reference selection.
	ProposedSearchQuerySourceRunURL string `json:"proposed_search_query_source_run_url"`
	// ProposedSearchQuerySourceSignatureID identifies phrase provenance at
	// signature level; used for exact traceability; derived from source selection.
	ProposedSearchQuerySourceSignatureID string `json:"proposed_search_query_source_signature_id"`
	// SourcePhase1ClusterIDs lists involved phase1 clusters; used for join-back and
	// reviewer context; derived from contributing source clusters.
	SourcePhase1ClusterIDs []string `json:"source_phase1_cluster_ids"`
	// MemberSignatureIDs lists involved signatures; used for targeted drilldown and
	// deterministic ReviewItemID generation; derived from source members.
	MemberSignatureIDs []string `json:"member_signature_ids"`
	// References are compact supporting links; used by reviewers to open concrete
	// examples quickly; derived from merged source references.
	References []ReferenceRecord `json:"references"`
}
