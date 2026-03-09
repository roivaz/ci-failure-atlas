package contracts

const SchemaVersionV1 = "v1"

type Phase1WorksetRecord struct {
	SchemaVersion  string `json:"schema_version"`
	Environment    string `json:"environment"`
	RowID          string `json:"row_id"`
	GroupKey       string `json:"group_key"`
	Lane           string `json:"lane"`
	JobName        string `json:"job_name"`
	TestName       string `json:"test_name"`
	TestSuite      string `json:"test_suite"`
	SignatureID    string `json:"signature_id"`
	OccurredAt     string `json:"occurred_at"`
	RunURL         string `json:"run_url"`
	PRNumber       int    `json:"pr_number"`
	PostGoodCommit bool   `json:"post_good_commit"`
	RawText        string `json:"raw_text"`
	NormalizedText string `json:"normalized_text"`
}

type Phase1NormalizedRecord struct {
	SchemaVersion           string `json:"schema_version"`
	Environment             string `json:"environment"`
	RowID                   string `json:"row_id"`
	GroupKey                string `json:"group_key"`
	Lane                    string `json:"lane"`
	JobName                 string `json:"job_name"`
	TestName                string `json:"test_name"`
	TestSuite               string `json:"test_suite"`
	SignatureID             string `json:"signature_id"`
	OccurredAt              string `json:"occurred_at"`
	RunURL                  string `json:"run_url"`
	PRNumber                int    `json:"pr_number"`
	PostGoodCommit          bool   `json:"post_good_commit"`
	RawText                 string `json:"raw_text"`
	NormalizedText          string `json:"normalized_text"`
	CanonicalEvidencePhrase string `json:"canonical_evidence_phrase"`
	SearchQueryPhrase       string `json:"search_query_phrase"`
	ProviderAnchor          string `json:"provider_anchor"`
	GenericPhrase           bool   `json:"generic_phrase"`
	Phase1Key               string `json:"phase1_key"`
}

type Phase1AssignmentRecord struct {
	SchemaVersion                    string   `json:"schema_version"`
	Environment                      string   `json:"environment"`
	RowID                            string   `json:"row_id"`
	GroupKey                         string   `json:"group_key"`
	Phase1LocalClusterKey            string   `json:"phase1_local_cluster_key"`
	CanonicalEvidencePhraseCandidate string   `json:"canonical_evidence_phrase_candidate"`
	SearchQueryPhraseCandidate       string   `json:"search_query_phrase_candidate"`
	Confidence                       string   `json:"confidence"`
	Reasons                          []string `json:"reasons"`
}

type ReferenceRecord struct {
	RunURL         string `json:"run_url"`
	OccurredAt     string `json:"occurred_at"`
	SignatureID    string `json:"signature_id"`
	PRNumber       int    `json:"pr_number"`
	PostGoodCommit bool   `json:"post_good_commit"`
	RawTextExcerpt string `json:"raw_text_excerpt"`
}

type TestClusterRecord struct {
	SchemaVersion                string            `json:"schema_version"`
	Environment                  string            `json:"environment"`
	Phase1ClusterID              string            `json:"phase1_cluster_id"`
	Lane                         string            `json:"lane"`
	JobName                      string            `json:"job_name"`
	TestName                     string            `json:"test_name"`
	TestSuite                    string            `json:"test_suite"`
	CanonicalEvidencePhrase      string            `json:"canonical_evidence_phrase"`
	SearchQueryPhrase            string            `json:"search_query_phrase"`
	SearchQuerySourceRunURL      string            `json:"search_query_source_run_url"`
	SearchQuerySourceSignatureID string            `json:"search_query_source_signature_id"`
	SupportCount                 int               `json:"support_count"`
	SeenPostGoodCommit           bool              `json:"seen_post_good_commit"`
	PostGoodCommitCount          int               `json:"post_good_commit_count"`
	MemberSignatureIDs           []string          `json:"member_signature_ids"`
	References                   []ReferenceRecord `json:"references"`
}

type ContributingTestRecord struct {
	Lane         string `json:"lane"`
	JobName      string `json:"job_name"`
	TestName     string `json:"test_name"`
	SupportCount int    `json:"support_count"`
}

type GlobalClusterRecord struct {
	SchemaVersion                string                   `json:"schema_version"`
	Environment                  string                   `json:"environment"`
	Phase2ClusterID              string                   `json:"phase2_cluster_id"`
	CanonicalEvidencePhrase      string                   `json:"canonical_evidence_phrase"`
	SearchQueryPhrase            string                   `json:"search_query_phrase"`
	SearchQuerySourceRunURL      string                   `json:"search_query_source_run_url"`
	SearchQuerySourceSignatureID string                   `json:"search_query_source_signature_id"`
	SupportCount                 int                      `json:"support_count"`
	SeenPostGoodCommit           bool                     `json:"seen_post_good_commit"`
	PostGoodCommitCount          int                      `json:"post_good_commit_count"`
	ContributingTestsCount       int                      `json:"contributing_tests_count"`
	ContributingTests            []ContributingTestRecord `json:"contributing_tests"`
	MemberPhase1ClusterIDs       []string                 `json:"member_phase1_cluster_ids"`
	MemberSignatureIDs           []string                 `json:"member_signature_ids"`
	References                   []ReferenceRecord        `json:"references"`
}

type ReviewItemRecord struct {
	SchemaVersion                        string            `json:"schema_version"`
	Environment                          string            `json:"environment"`
	ReviewItemID                         string            `json:"review_item_id"`
	Phase                                string            `json:"phase"`
	Reason                               string            `json:"reason"`
	ProposedCanonicalEvidencePhrase      string            `json:"proposed_canonical_evidence_phrase"`
	ProposedSearchQueryPhrase            string            `json:"proposed_search_query_phrase"`
	ProposedSearchQuerySourceRunURL      string            `json:"proposed_search_query_source_run_url"`
	ProposedSearchQuerySourceSignatureID string            `json:"proposed_search_query_source_signature_id"`
	SourcePhase1ClusterIDs               []string          `json:"source_phase1_cluster_ids"`
	MemberSignatureIDs                   []string          `json:"member_signature_ids"`
	References                           []ReferenceRecord `json:"references"`
}
