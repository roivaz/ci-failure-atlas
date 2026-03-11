package testsummary

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	"ci-failure-atlas/pkg/testrules"
	"k8s.io/utils/set"
)

type Options struct {
	OutputPath         string
	Format             string
	QualityExportPath  string
	TopTests           int
	RecentRuns         int
	MinRuns            int
	Environments       []string
	SplitByEnvironment bool
}

type reference struct {
	RunURL         string `json:"run_url"`
	OccurredAt     string `json:"occurred_at"`
	SignatureID    string `json:"signature_id"`
	PRNumber       int    `json:"pr_number"`
	PostGoodCommit bool   `json:"post_good_commit"`
	PostGoodKnown  bool   `json:"post_good_known"`
}

type testCluster struct {
	SchemaVersion           string      `json:"schema_version"`
	Environment             string      `json:"environment"`
	Phase1ClusterID         string      `json:"phase1_cluster_id"`
	Lane                    string      `json:"lane"`
	JobName                 string      `json:"job_name"`
	TestName                string      `json:"test_name"`
	TestSuite               string      `json:"test_suite"`
	CanonicalEvidencePhrase string      `json:"canonical_evidence_phrase"`
	SearchQueryPhrase       string      `json:"search_query_phrase"`
	SearchQuerySourceRunURL string      `json:"search_query_source_run_url"`
	SearchQuerySourceSigID  string      `json:"search_query_source_signature_id"`
	SupportCount            int         `json:"support_count"`
	SeenPostGoodCommit      bool        `json:"seen_post_good_commit"`
	PostGoodCommitCount     int         `json:"post_good_commit_count"`
	MemberSignatureIDs      []string    `json:"member_signature_ids"`
	References              []reference `json:"references"`
}

type rawFailureRecord struct {
	Environment               string   `json:"environment"`
	RunURL                    string   `json:"run_url"`
	SignatureID               string   `json:"signature_id"`
	RawText                   string   `json:"raw_text"`
	Lane                      string   `json:"lane"`
	JobName                   string   `json:"job_name"`
	TestName                  string   `json:"test_name"`
	TestSuite                 string   `json:"test_suite"`
	TestTags                  []string `json:"test_tags"`
	TestCurrentPassPercentage *float64 `json:"test_current_pass_percentage"`
	TestCurrentRuns           int      `json:"test_current_runs"`
}

type reviewItem struct {
	Environment            string   `json:"environment"`
	SchemaVersion          string   `json:"schema_version"`
	ReviewItemID           string   `json:"review_item_id"`
	Phase                  string   `json:"phase"`
	Reason                 string   `json:"reason"`
	SourcePhase1ClusterIDs []string `json:"source_phase1_cluster_ids"`
	MemberSignatureIDs     []string `json:"member_signature_ids"`
}

type testKey struct {
	Lane      string
	JobName   string
	TestName  string
	TestSuite string
}

type testKeyNoSuite struct {
	Lane     string
	JobName  string
	TestName string
}

type testMetadata struct {
	PassRate *float64
	Runs     int
	Tags     []string
}

type reviewSignalIndex struct {
	ByPhase1Cluster map[string]set.Set[string]
	BySignatureID   map[string]set.Set[string]
}

type referenceKey struct {
	RunURL      string
	SignatureID string
}

type testAggregate struct {
	Key                testKey
	Clusters           []testCluster
	TotalFailures      int
	DistinctSignatures int
	PostGoodFailures   int
	LatestFailure      time.Time
	PRCounts           map[int]int
	Metadata           testMetadata
}

const sparklineWindowDays = 7

const (
	reportFormatMarkdown = "markdown"
	reportFormatHTML     = "html"
)

func Run(ctx context.Context, args []string) error {
	_ = ctx
	_ = args
	return fmt.Errorf("report test-summary Run(args) is not wired; use Generate with an injected store")
}

func DefaultOptions() Options {
	return Options{
		OutputPath:         "data/reports/test-failure-summary.md",
		Format:             reportFormatMarkdown,
		QualityExportPath:  "",
		TopTests:           0,
		RecentRuns:         4,
		MinRuns:            0,
		SplitByEnvironment: false,
	}
}

func Generate(ctx context.Context, store storecontracts.Store, opts Options) error {
	validated, err := validateOptions(opts)
	if err != nil {
		return err
	}
	if store == nil {
		return fmt.Errorf("store is required")
	}

	logger := loggerFromContext(ctx).WithValues("component", "report.test-summary")

	storedClusters, err := store.ListTestClusters(ctx)
	if err != nil {
		return fmt.Errorf("list test clusters: %w", err)
	}
	reviewItems, err := loadReviewItemsFromStore(ctx, store)
	if err != nil {
		return fmt.Errorf("read review queue: %w", err)
	}
	allFlaggedExports := make([]qualityFlaggedSignatureExport, 0)
	if validated.SplitByEnvironment {
		targetEnvs := resolveTestSummaryTargetEnvironments(validated.Environments, storedClusters, reviewItems)
		if len(targetEnvs) == 0 {
			targetEnvs = []string{"unknown"}
		}
		for _, environment := range targetEnvs {
			filteredStoredClusters := filterStoredTestClustersByEnvironment(storedClusters, environment)
			testClusters := toReportTestClusters(filteredStoredClusters)
			metadataByFull, metadataByNoSuite, fullErrorsByReference, err := loadRawMetadataFromStore(ctx, store, testClusters)
			if err != nil {
				return fmt.Errorf("load raw failure metadata for environment %q: %w", environment, err)
			}
			filteredReviewItems := filterReviewItemsByEnvironment(reviewItems, environment)
			reviewIndex := buildReviewSignalIndex(filteredReviewItems)
			generatedAt := time.Now().UTC()
			var qualityRows []qualitySignatureRow
			if validated.Format == reportFormatHTML || strings.TrimSpace(validated.QualityExportPath) != "" {
				qualityRows = buildQualitySignatureRows(
					testClusters,
					metadataByFull,
					metadataByNoSuite,
					fullErrorsByReference,
					reviewIndex,
					generatedAt,
					validated.TopTests,
					validated.RecentRuns,
					validated.MinRuns,
				)
			}
			report := buildMarkdown(
				testClusters,
				metadataByFull,
				metadataByNoSuite,
				fullErrorsByReference,
				reviewIndex,
				"store:test_clusters",
				"store:raw_failures",
				generatedAt,
				validated.TopTests,
				validated.RecentRuns,
				validated.MinRuns,
			)
			if validated.Format == reportFormatHTML {
				report = buildHTML(
					qualityRows,
					"store:test_clusters",
					"store:raw_failures",
					generatedAt,
				)
			}
			allFlaggedExports = append(allFlaggedExports, toQualityFlaggedSignatureExports(qualityRows)...)
			outputPath := outputPathForEnvironment(validated.OutputPath, environment)
			if err := writeTestSummary(outputPath, report); err != nil {
				return err
			}
			logger.Info(
				"Wrote per-test summary report.",
				"output", outputPath,
				"format", validated.Format,
				"environment", environment,
				"testClusters", len(testClusters),
				"metadataByFull", len(metadataByFull),
				"metadataByNoSuite", len(metadataByNoSuite),
				"fullErrorReferences", len(fullErrorsByReference),
				"reviewItems", len(filteredReviewItems),
				"topTests", validated.TopTests,
				"recentRuns", validated.RecentRuns,
				"minRuns", validated.MinRuns,
			)
		}
		if strings.TrimSpace(validated.QualityExportPath) != "" {
			if err := writeQualityFlaggedSignatures(validated.QualityExportPath, allFlaggedExports); err != nil {
				return err
			}
			logger.Info(
				"Wrote quality export artifact.",
				"output", validated.QualityExportPath,
				"flaggedSignatures", len(allFlaggedExports),
			)
		}
		return nil
	}

	filteredStoredClusters := storedClusters
	filteredReviewItems := reviewItems
	if len(validated.Environments) > 0 {
		envSet := make(map[string]struct{}, len(validated.Environments))
		for _, environment := range validated.Environments {
			envSet[normalizeReportEnvironment(environment)] = struct{}{}
		}
		filteredStoredClusters = filterStoredTestClustersByEnvironmentSet(storedClusters, envSet)
		filteredReviewItems = filterReviewItemsByEnvironmentSet(reviewItems, envSet)
	}
	testClusters := toReportTestClusters(filteredStoredClusters)
	metadataByFull, metadataByNoSuite, fullErrorsByReference, err := loadRawMetadataFromStore(ctx, store, testClusters)
	if err != nil {
		return fmt.Errorf("load raw failure metadata: %w", err)
	}
	reviewIndex := buildReviewSignalIndex(filteredReviewItems)
	generatedAt := time.Now().UTC()
	var qualityRows []qualitySignatureRow
	if validated.Format == reportFormatHTML || strings.TrimSpace(validated.QualityExportPath) != "" {
		qualityRows = buildQualitySignatureRows(
			testClusters,
			metadataByFull,
			metadataByNoSuite,
			fullErrorsByReference,
			reviewIndex,
			generatedAt,
			validated.TopTests,
			validated.RecentRuns,
			validated.MinRuns,
		)
	}
	report := buildMarkdown(
		testClusters,
		metadataByFull,
		metadataByNoSuite,
		fullErrorsByReference,
		reviewIndex,
		"store:test_clusters",
		"store:raw_failures",
		generatedAt,
		validated.TopTests,
		validated.RecentRuns,
		validated.MinRuns,
	)
	if validated.Format == reportFormatHTML {
		report = buildHTML(
			qualityRows,
			"store:test_clusters",
			"store:raw_failures",
			generatedAt,
		)
	}
	if err := writeTestSummary(validated.OutputPath, report); err != nil {
		return err
	}
	if strings.TrimSpace(validated.QualityExportPath) != "" {
		flaggedExports := toQualityFlaggedSignatureExports(qualityRows)
		if err := writeQualityFlaggedSignatures(validated.QualityExportPath, flaggedExports); err != nil {
			return err
		}
		logger.Info(
			"Wrote quality export artifact.",
			"output", validated.QualityExportPath,
			"flaggedSignatures", len(flaggedExports),
		)
	}

	logger.Info(
		"Wrote per-test summary report.",
		"output", validated.OutputPath,
		"format", validated.Format,
		"testClusters", len(testClusters),
		"metadataByFull", len(metadataByFull),
		"metadataByNoSuite", len(metadataByNoSuite),
		"fullErrorReferences", len(fullErrorsByReference),
		"reviewItems", len(filteredReviewItems),
		"topTests", validated.TopTests,
		"recentRuns", validated.RecentRuns,
		"minRuns", validated.MinRuns,
	)
	return nil
}

func parse(args []string) (Options, error) {
	opts := DefaultOptions()
	sourceEnvs := strings.Join(opts.Environments, ",")

	fs := flag.NewFlagSet("test-summary", flag.ContinueOnError)
	fs.StringVar(&opts.OutputPath, "output", opts.OutputPath, "path to output report")
	fs.StringVar(&opts.Format, "format", opts.Format, "output format: markdown|html")
	fs.StringVar(&opts.QualityExportPath, "quality-export", opts.QualityExportPath, "optional path to write flagged semantic signatures as NDJSON")
	fs.IntVar(&opts.TopTests, "top", opts.TopTests, "max number of tests to render (0 renders all)")
	fs.IntVar(&opts.RecentRuns, "recent", opts.RecentRuns, "recent failing runs to render per signature")
	fs.IntVar(&opts.MinRuns, "min-runs", opts.MinRuns, "minimum current test runs required to include a test in report (from sippy daily metadata when available; 0 disables filter)")
	fs.StringVar(&sourceEnvs, "source.envs", sourceEnvs, "environments to include (comma-separated, e.g. dev,int,stg,prod)")
	fs.BoolVar(&opts.SplitByEnvironment, "split-by-env", opts.SplitByEnvironment, "write one output file per environment using <output>.<env>.<ext>")

	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}
	if strings.TrimSpace(sourceEnvs) != "" {
		opts.Environments = strings.Split(sourceEnvs, ",")
	}
	return validateOptions(opts)
}

func validateOptions(opts Options) (Options, error) {
	if strings.TrimSpace(opts.OutputPath) == "" {
		return Options{}, errors.New("missing --output path")
	}
	if opts.TopTests < 0 {
		return Options{}, errors.New("--top must be >= 0")
	}
	if opts.RecentRuns <= 0 {
		return Options{}, errors.New("--recent must be > 0")
	}
	if opts.MinRuns < 0 {
		return Options{}, errors.New("--min-runs must be >= 0")
	}
	switch strings.ToLower(strings.TrimSpace(opts.Format)) {
	case reportFormatMarkdown:
		opts.Format = reportFormatMarkdown
	case reportFormatHTML:
		opts.Format = reportFormatHTML
	default:
		return Options{}, fmt.Errorf("invalid --format %q (expected markdown or html)", strings.TrimSpace(opts.Format))
	}
	opts.QualityExportPath = strings.TrimSpace(opts.QualityExportPath)
	opts.Environments = normalizeReportEnvironments(opts.Environments)
	return opts, nil
}

func toReportTestClusters(rows []semanticcontracts.TestClusterRecord) []testCluster {
	out := make([]testCluster, 0, len(rows))
	for _, row := range rows {
		out = append(out, testCluster{
			SchemaVersion:           strings.TrimSpace(row.SchemaVersion),
			Environment:             normalizeReportEnvironment(row.Environment),
			Phase1ClusterID:         strings.TrimSpace(row.Phase1ClusterID),
			Lane:                    strings.TrimSpace(row.Lane),
			JobName:                 strings.TrimSpace(row.JobName),
			TestName:                strings.TrimSpace(row.TestName),
			TestSuite:               strings.TrimSpace(row.TestSuite),
			CanonicalEvidencePhrase: strings.TrimSpace(row.CanonicalEvidencePhrase),
			SearchQueryPhrase:       strings.TrimSpace(row.SearchQueryPhrase),
			SearchQuerySourceRunURL: strings.TrimSpace(row.SearchQuerySourceRunURL),
			SearchQuerySourceSigID:  strings.TrimSpace(row.SearchQuerySourceSignatureID),
			SupportCount:            row.SupportCount,
			SeenPostGoodCommit:      row.SeenPostGoodCommit,
			PostGoodCommitCount:     row.PostGoodCommitCount,
			MemberSignatureIDs:      append([]string(nil), row.MemberSignatureIDs...),
			References:              toReportReferences(row.References),
		})
	}
	return out
}

func toReportReferences(rows []semanticcontracts.ReferenceRecord) []reference {
	out := make([]reference, 0, len(rows))
	for _, row := range rows {
		out = append(out, reference{
			RunURL:         strings.TrimSpace(row.RunURL),
			OccurredAt:     strings.TrimSpace(row.OccurredAt),
			SignatureID:    strings.TrimSpace(row.SignatureID),
			PRNumber:       row.PRNumber,
			PostGoodCommit: row.PostGoodCommit,
			PostGoodKnown:  true,
		})
	}
	return out
}

func loadReviewItemsFromStore(ctx context.Context, store storecontracts.Store) ([]reviewItem, error) {
	rows, err := store.ListReviewQueue(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]reviewItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, reviewItem{
			Environment:            strings.TrimSpace(row.Environment),
			SchemaVersion:          strings.TrimSpace(row.SchemaVersion),
			ReviewItemID:           strings.TrimSpace(row.ReviewItemID),
			Phase:                  strings.TrimSpace(row.Phase),
			Reason:                 strings.TrimSpace(row.Reason),
			SourcePhase1ClusterIDs: append([]string(nil), row.SourcePhase1ClusterIDs...),
			MemberSignatureIDs:     append([]string(nil), row.MemberSignatureIDs...),
		})
	}
	return out, nil
}

func writeTestSummary(outputPath string, report string) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create test summary output directory: %w", err)
	}
	if err := os.WriteFile(outputPath, []byte(report), 0o644); err != nil {
		return fmt.Errorf("write test summary report: %w", err)
	}
	return nil
}

func normalizeReportEnvironments(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		normalized := normalizeReportEnvironment(value)
		if normalized == "" {
			continue
		}
		if _, exists := set[normalized]; exists {
			continue
		}
		set[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func normalizeReportEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func resolveTestSummaryTargetEnvironments(
	configured []string,
	testClusters []semanticcontracts.TestClusterRecord,
	reviewItems []reviewItem,
) []string {
	normalizedConfigured := normalizeReportEnvironments(configured)
	if len(normalizedConfigured) > 0 {
		return normalizedConfigured
	}
	set := map[string]struct{}{}
	for _, row := range testClusters {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		set[environment] = struct{}{}
	}
	for _, row := range reviewItems {
		environment := normalizeReportEnvironment(row.Environment)
		if environment == "" {
			continue
		}
		set[environment] = struct{}{}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for environment := range set {
		out = append(out, environment)
	}
	sort.Strings(out)
	return out
}

func outputPathForEnvironment(outputPath, environment string) string {
	base := strings.TrimSpace(outputPath)
	env := normalizeReportEnvironment(environment)
	if base == "" || env == "" {
		return base
	}
	ext := filepath.Ext(base)
	baseWithoutExt := strings.TrimSuffix(base, ext)
	if strings.HasSuffix(baseWithoutExt, "."+env) {
		return base
	}
	if ext == "" {
		return base + "." + env
	}
	return baseWithoutExt + "." + env + ext
}

func filterStoredTestClustersByEnvironment(rows []semanticcontracts.TestClusterRecord, environment string) []semanticcontracts.TestClusterRecord {
	envSet := map[string]struct{}{normalizeReportEnvironment(environment): {}}
	return filterStoredTestClustersByEnvironmentSet(rows, envSet)
}

func filterStoredTestClustersByEnvironmentSet(rows []semanticcontracts.TestClusterRecord, envSet map[string]struct{}) []semanticcontracts.TestClusterRecord {
	if len(envSet) == 0 {
		return append([]semanticcontracts.TestClusterRecord(nil), rows...)
	}
	out := make([]semanticcontracts.TestClusterRecord, 0, len(rows))
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		if _, ok := envSet[environment]; !ok {
			continue
		}
		out = append(out, row)
	}
	return out
}

func filterReviewItemsByEnvironment(rows []reviewItem, environment string) []reviewItem {
	envSet := map[string]struct{}{normalizeReportEnvironment(environment): {}}
	return filterReviewItemsByEnvironmentSet(rows, envSet)
}

func filterReviewItemsByEnvironmentSet(rows []reviewItem, envSet map[string]struct{}) []reviewItem {
	if len(envSet) == 0 {
		return append([]reviewItem(nil), rows...)
	}
	out := make([]reviewItem, 0, len(rows))
	for _, row := range rows {
		environment := normalizeReportEnvironment(row.Environment)
		if _, ok := envSet[environment]; !ok {
			continue
		}
		out = append(out, row)
	}
	return out
}

func loadRawMetadataFromStore(
	ctx context.Context,
	store storecontracts.Store,
	testClusters []testCluster,
) (map[testKey]testMetadata, map[testKeyNoSuite]testMetadata, map[referenceKey]string, error) {
	full := map[testKey]testMetadata{}
	noSuite := map[testKeyNoSuite]testMetadata{}
	fullErrorsByReference := map[referenceKey]string{}
	fullRunsByTest := map[testKey]map[string]struct{}{}
	noSuiteRunsByTest := map[testKeyNoSuite]map[string]struct{}{}

	referencedRunURLs := map[string]struct{}{}
	runURLEnvironments := map[string]map[string]struct{}{}
	for _, cluster := range testClusters {
		clusterEnvironment := normalizeReportEnvironment(cluster.Environment)
		testKeyWithSuite := toTestKey(cluster.Lane, cluster.JobName, cluster.TestName, cluster.TestSuite)
		if _, ok := fullRunsByTest[testKeyWithSuite]; !ok {
			fullRunsByTest[testKeyWithSuite] = map[string]struct{}{}
		}
		testKeyNoSuite := testKeyNoSuite{
			Lane:     normalizeKeyPart(cluster.Lane),
			JobName:  normalizeKeyPart(cluster.JobName),
			TestName: normalizeKeyPart(cluster.TestName),
		}
		if _, ok := noSuiteRunsByTest[testKeyNoSuite]; !ok {
			noSuiteRunsByTest[testKeyNoSuite] = map[string]struct{}{}
		}

		for _, ref := range cluster.References {
			runURL := strings.TrimSpace(ref.RunURL)
			if runURL == "" {
				continue
			}
			referencedRunURLs[runURL] = struct{}{}
			fullRunsByTest[testKeyWithSuite][runURL] = struct{}{}
			noSuiteRunsByTest[testKeyNoSuite][runURL] = struct{}{}
			if clusterEnvironment != "" {
				insertRunEnvironment(runURLEnvironments, runURL, clusterEnvironment)
			}
		}
	}
	if len(referencedRunURLs) == 0 {
		return full, noSuite, fullErrorsByReference, nil
	}

	for runURL, envSet := range runURLEnvironments {
		environments := sortedEnvironmentList(envSet)
		for _, environment := range environments {
			rows, err := store.ListRawFailuresByRun(ctx, environment, runURL)
			if err != nil {
				return nil, nil, nil, err
			}
			for _, row := range rows {
				refKey := referenceKey{
					RunURL:      strings.TrimSpace(row.RunURL),
					SignatureID: strings.TrimSpace(row.SignatureID),
				}
				if refKey.RunURL == "" {
					refKey.RunURL = runURL
				}
				if refKey.RunURL == "" || refKey.SignatureID == "" {
					continue
				}

				fullError := strings.TrimSpace(row.RawText)
				if fullError != "" {
					existing, ok := fullErrorsByReference[refKey]
					if !ok || len(fullError) > len(existing) {
						fullErrorsByReference[refKey] = fullError
					}
				}
			}
		}
	}

	for key, runs := range fullRunsByTest {
		full[key] = testMetadata{Runs: len(runs)}
	}
	for key, runs := range noSuiteRunsByTest {
		noSuite[key] = testMetadata{Runs: len(runs)}
	}
	if err := mergeLatestTestMetadataDaily(ctx, store, testClusters, full, noSuite); err != nil {
		return nil, nil, nil, err
	}

	return full, noSuite, fullErrorsByReference, nil
}

func mergeLatestTestMetadataDaily(
	ctx context.Context,
	store storecontracts.Store,
	testClusters []testCluster,
	metadataByFull map[testKey]testMetadata,
	metadataByNoSuite map[testKeyNoSuite]testMetadata,
) error {
	if len(testClusters) == 0 {
		return nil
	}

	fullByLaneAndSuite := map[string][]testKey{}
	fullBySuite := map[string][]testKey{}
	noSuiteByLane := map[string][]testKeyNoSuite{}
	noSuiteByName := map[string][]testKeyNoSuite{}
	candidateDatesByEnv := map[string]map[string]struct{}{}

	addCandidateDate := func(environment string, date string) {
		env := normalizeReportEnvironment(environment)
		normalizedDate := strings.TrimSpace(date)
		if env == "" || normalizedDate == "" {
			return
		}
		if _, ok := candidateDatesByEnv[env]; !ok {
			candidateDatesByEnv[env] = map[string]struct{}{}
		}
		candidateDatesByEnv[env][normalizedDate] = struct{}{}
	}

	today := time.Now().UTC().Format("2006-01-02")
	yesterday := time.Now().UTC().Add(-24 * time.Hour).Format("2006-01-02")

	for _, cluster := range testClusters {
		environment := normalizeReportEnvironment(cluster.Environment)
		if environment == "" {
			continue
		}
		lane := normalizeKeyPart(cluster.Lane)
		testName := normalizeKeyPart(cluster.TestName)
		testSuite := strings.TrimSpace(cluster.TestSuite)
		fullKey := toTestKey(cluster.Lane, cluster.JobName, cluster.TestName, cluster.TestSuite)
		noSuiteKey := testKeyNoSuite{
			Lane:     lane,
			JobName:  normalizeKeyPart(cluster.JobName),
			TestName: testName,
		}

		fullByLaneAndSuite[testMetadataLaneSuiteKey(environment, lane, testSuite, testName)] = append(fullByLaneAndSuite[testMetadataLaneSuiteKey(environment, lane, testSuite, testName)], fullKey)
		fullBySuite[testMetadataSuiteKey(environment, testSuite, testName)] = append(fullBySuite[testMetadataSuiteKey(environment, testSuite, testName)], fullKey)
		noSuiteByLane[testMetadataLaneNameKey(environment, lane, testName)] = append(noSuiteByLane[testMetadataLaneNameKey(environment, lane, testName)], noSuiteKey)
		noSuiteByName[testMetadataNameKey(environment, testName)] = append(noSuiteByName[testMetadataNameKey(environment, testName)], noSuiteKey)

		addCandidateDate(environment, today)
		addCandidateDate(environment, yesterday)
		for _, ref := range cluster.References {
			if ts, ok := parseTimestamp(ref.OccurredAt); ok {
				addCandidateDate(environment, ts.UTC().Format("2006-01-02"))
			}
		}
	}

	for environment, dateSet := range candidateDatesByEnv {
		dates := sortedDateList(dateSet)
		for _, date := range dates {
			rows, err := store.ListTestMetadataDailyByDate(ctx, environment, date)
			if err != nil {
				return fmt.Errorf("list test metadata daily rows for env=%q date=%q: %w", environment, date, err)
			}
			for _, row := range rows {
				testName := normalizeKeyPart(row.TestName)
				testSuite := strings.TrimSpace(row.TestSuite)
				if testName == "" || testSuite == "" {
					continue
				}
				lane := normalizeKeyPart(string(testrules.ClassifyLane(environment, testSuite, testName)))
				candidate := testMetadata{
					Runs: row.CurrentRuns,
				}
				candidate.PassRate = float64Ptr(row.CurrentPassPercentage)

				fullMatches := fullByLaneAndSuite[testMetadataLaneSuiteKey(environment, lane, testSuite, testName)]
				if len(fullMatches) == 0 {
					fullMatches = fullBySuite[testMetadataSuiteKey(environment, testSuite, testName)]
				}
				for _, key := range fullMatches {
					existing := metadataByFull[key]
					if preferMetadata(candidate, existing) {
						metadataByFull[key] = cloneMetadata(candidate)
					}
				}

				noSuiteMatches := noSuiteByLane[testMetadataLaneNameKey(environment, lane, testName)]
				if len(noSuiteMatches) == 0 {
					noSuiteMatches = noSuiteByName[testMetadataNameKey(environment, testName)]
				}
				for _, key := range noSuiteMatches {
					existing := metadataByNoSuite[key]
					if preferMetadata(candidate, existing) {
						metadataByNoSuite[key] = cloneMetadata(candidate)
					}
				}
			}
		}
	}
	return nil
}

func insertRunEnvironment(runURLEnvironments map[string]map[string]struct{}, runURL string, environment string) {
	normalizedRunURL := strings.TrimSpace(runURL)
	normalizedEnvironment := normalizeReportEnvironment(environment)
	if normalizedRunURL == "" || normalizedEnvironment == "" {
		return
	}
	if _, ok := runURLEnvironments[normalizedRunURL]; !ok {
		runURLEnvironments[normalizedRunURL] = map[string]struct{}{}
	}
	runURLEnvironments[normalizedRunURL][normalizedEnvironment] = struct{}{}
}

func sortedEnvironmentList(setByEnvironment map[string]struct{}) []string {
	if len(setByEnvironment) == 0 {
		return nil
	}
	out := make([]string, 0, len(setByEnvironment))
	for value := range setByEnvironment {
		normalized := normalizeReportEnvironment(value)
		if normalized == "" {
			continue
		}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func sortedDateList(dateSet map[string]struct{}) []string {
	if len(dateSet) == 0 {
		return nil
	}
	out := make([]string, 0, len(dateSet))
	for value := range dateSet {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}

func testMetadataLaneSuiteKey(environment, lane, suite, name string) string {
	return strings.Join([]string{
		normalizeReportEnvironment(environment),
		normalizeKeyPart(lane),
		strings.TrimSpace(suite),
		normalizeKeyPart(name),
	}, "|")
}

func testMetadataSuiteKey(environment, suite, name string) string {
	return strings.Join([]string{
		normalizeReportEnvironment(environment),
		strings.TrimSpace(suite),
		normalizeKeyPart(name),
	}, "|")
}

func testMetadataLaneNameKey(environment, lane, name string) string {
	return strings.Join([]string{
		normalizeReportEnvironment(environment),
		normalizeKeyPart(lane),
		normalizeKeyPart(name),
	}, "|")
}

func testMetadataNameKey(environment, name string) string {
	return strings.Join([]string{
		normalizeReportEnvironment(environment),
		normalizeKeyPart(name),
	}, "|")
}

func loggerFromContext(ctx context.Context) logr.Logger {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return logr.Discard()
	}
	return logger
}

func buildMarkdown(
	testClusters []testCluster,
	metadataByFull map[testKey]testMetadata,
	metadataByNoSuite map[testKeyNoSuite]testMetadata,
	fullErrorsByReference map[referenceKey]string,
	reviewIndex reviewSignalIndex,
	testPath string,
	rawPath string,
	generatedAt time.Time,
	topTests int,
	recentRuns int,
	minRuns int,
) string {
	aggregates := mergeUnknownJobAggregates(aggregateByTest(testClusters))
	for i := range aggregates {
		aggregates[i].Metadata = lookupMetadata(aggregates[i].Key, metadataByFull, metadataByNoSuite)
		sortClusters(aggregates[i].Clusters)
	}

	if minRuns > 0 {
		filtered := make([]testAggregate, 0, len(aggregates))
		for _, aggregate := range aggregates {
			if aggregate.Metadata.Runs < minRuns {
				continue
			}
			filtered = append(filtered, aggregate)
		}
		aggregates = filtered
	}

	sort.Slice(aggregates, func(i, j int) bool {
		pi := aggregates[i].Metadata.PassRate
		pj := aggregates[j].Metadata.PassRate
		switch {
		case pi != nil && pj != nil && *pi != *pj:
			// Lower success rate first to prioritize less stable tests.
			return *pi < *pj
		case pi != nil && pj == nil:
			return true
		case pi == nil && pj != nil:
			return false
		}
		if aggregates[i].Metadata.Runs != aggregates[j].Metadata.Runs {
			return aggregates[i].Metadata.Runs > aggregates[j].Metadata.Runs
		}
		if aggregates[i].TotalFailures != aggregates[j].TotalFailures {
			return aggregates[i].TotalFailures > aggregates[j].TotalFailures
		}
		if aggregates[i].DistinctSignatures != aggregates[j].DistinctSignatures {
			return aggregates[i].DistinctSignatures > aggregates[j].DistinctSignatures
		}
		if !aggregates[i].LatestFailure.Equal(aggregates[j].LatestFailure) {
			return aggregates[i].LatestFailure.After(aggregates[j].LatestFailure)
		}
		if aggregates[i].Key.Lane != aggregates[j].Key.Lane {
			return aggregates[i].Key.Lane < aggregates[j].Key.Lane
		}
		if aggregates[i].Key.JobName != aggregates[j].Key.JobName {
			return aggregates[i].Key.JobName < aggregates[j].Key.JobName
		}
		return aggregates[i].Key.TestName < aggregates[j].Key.TestName
	})

	limit := len(aggregates)
	if topTests > 0 && topTests < limit {
		limit = topTests
	}

	var b strings.Builder
	b.WriteString("# CI Test Failure Summary\n\n")
	b.WriteString(fmt.Sprintf("Generated from `%s` on `%s`.\n", strings.TrimSpace(testPath), generatedAt.Format("2006-01-02 15:04:05Z")))
	if strings.TrimSpace(rawPath) != "" {
		b.WriteString(fmt.Sprintf("\nRaw failure metadata source: `%s`.\n", strings.TrimSpace(rawPath)))
		if minRuns > 0 {
			b.WriteString(fmt.Sprintf("\nFiltered to tests with at least **%d** observed runs.\n", minRuns))
		}
	}
	b.WriteString("\n")

	if limit == 0 {
		if minRuns > 0 {
			b.WriteString(fmt.Sprintf("No test clusters found with at least %d observed runs.\n", minRuns))
		} else {
			b.WriteString("No test clusters found.\n")
		}
		return b.String()
	}

	for i := 0; i < limit; i++ {
		test := aggregates[i]
		b.WriteString(fmt.Sprintf("## %d. %s\n\n", i+1, cleanInline(test.Key.TestName, 240)))
		b.WriteString(fmt.Sprintf("- Lane: `%s`\n", cleanInline(test.Key.Lane, 64)))
		b.WriteString(fmt.Sprintf("- Job: `%s`\n", cleanInline(test.Key.JobName, 120)))
		b.WriteString(fmt.Sprintf("- Suite: `%s`\n", cleanInline(test.Key.TestSuite, 80)))
		b.WriteString(fmt.Sprintf("- Total failures: **%d**\n", test.TotalFailures))
		b.WriteString(fmt.Sprintf("- Distinct signatures: **%d**\n", test.DistinctSignatures))
		b.WriteString(fmt.Sprintf("- post-good-commit-runs: **%d**\n", test.PostGoodFailures))
		b.WriteString(fmt.Sprintf("- post-good signal coverage: **%d/%d** failures\n", postGoodKnownCountForAggregate(test), test.TotalFailures))
		b.WriteString(fmt.Sprintf("- Distinct failing PRs: **%d**\n", len(test.PRCounts)))
		b.WriteString(fmt.Sprintf("- Risk classification: **%s**\n", testRiskLabel(test)))
		if !test.LatestFailure.IsZero() {
			b.WriteString(fmt.Sprintf("- Latest failure: `%s`\n", test.LatestFailure.Format(time.RFC3339)))
		}
		if test.Metadata.PassRate != nil {
			b.WriteString(fmt.Sprintf("- Current success rate: **%.2f%%** over **%d** runs\n", *test.Metadata.PassRate, test.Metadata.Runs))
		} else {
			b.WriteString("- Current success rate: **n/a**\n")
		}
		if len(test.Metadata.Tags) > 0 {
			b.WriteString(fmt.Sprintf("- Tags: `%s`\n", cleanInline(strings.Join(test.Metadata.Tags, ","), 160)))
		}

		b.WriteString("\n### Failure Signatures\n\n")
		for signatureIndex, cluster := range test.Clusters {
			b.WriteString(fmt.Sprintf("#### Signature %d - %d failures (post-good-commit-runs: %d)\n\n", signatureIndex+1, cluster.SupportCount, cluster.PostGoodCommitCount))
			b.WriteString(fmt.Sprintf("- post-good signal coverage: **%d/%d** failures\n", postGoodKnownCountForCluster(cluster), cluster.SupportCount))
			b.WriteString(fmt.Sprintf("- Evidence phrase: `%s`\n", cleanInline(cluster.CanonicalEvidencePhrase, 280)))
			if strings.TrimSpace(cluster.SearchQueryPhrase) != "" {
				b.WriteString(fmt.Sprintf("- Search phrase: `%s`\n", cleanInline(cluster.SearchQueryPhrase, 280)))
			}
			if sourceRun := strings.TrimSpace(cluster.SearchQuerySourceRunURL); sourceRun != "" {
				sourceSig := strings.TrimSpace(cluster.SearchQuerySourceSigID)
				if sourceSig != "" {
					b.WriteString(fmt.Sprintf("- Query source: [%s](%s) (signature `%s`)\n", "run", sourceRun, cleanInline(sourceSig, 80)))
				} else {
					b.WriteString(fmt.Sprintf("- Query source: [%s](%s)\n", "run", sourceRun))
				}
			}
			reviewReasons := reviewReasonsForCluster(cluster, reviewIndex)
			if len(reviewReasons) == 0 {
				b.WriteString("- Confidence: **normal** (no review flags)\n")
			} else {
				b.WriteString(fmt.Sprintf("- Confidence: **needs review** (`%s`)\n", cleanInline(strings.Join(reviewReasons, "`, `"), 220)))
			}
			if cluster.PostGoodCommitCount > 0 {
				b.WriteString(fmt.Sprintf("- SIGNAL: signature has post-good-commit-runs evidence (**%d/%d** failures). Treat as likely systemic or pre-existing instability.\n", cluster.PostGoodCommitCount, cluster.SupportCount))
			}
			if unicodeSpark, counts, dateRange, ok := clusterDailyDensitySparkline(cluster, sparklineWindowDays, generatedAt); ok {
				b.WriteString(fmt.Sprintf("- Daily density (last %dd, oldest->newest, %s): `%s` (counts: %s)\n",
					sparklineWindowDays,
					dateRange,
					unicodeSpark,
					formatCounts(counts),
				))
			}
			runs := recentRunsForCluster(cluster, recentRuns)
			if warning, ok := signaturePRWarning(cluster, runs); ok {
				b.WriteString(fmt.Sprintf("- WARNING: %s\n", warning))
			}
			if len(runs) == 0 {
				b.WriteString("- Recent failing runs: (none)\n\n")
				continue
			}
			b.WriteString("- Recent failing runs:\n")
			for _, run := range runs {
				label := run.OccurredAt
				if label == "" {
					label = "unknown-time"
				}
				if run.PRNumber > 0 {
					label = label + fmt.Sprintf(" | PR #%d", run.PRNumber)
				} else {
					label = label + " | PR n/a"
				}
				b.WriteString(fmt.Sprintf("  - [%s](%s)\n", label, run.RunURL))
			}
			fullErrorSamples := fullErrorSamplesForCluster(cluster, runs, fullErrorsByReference, 2)
			if len(fullErrorSamples) > 0 {
				b.WriteString("- Full error samples (for eyeballing report accuracy):\n")
				for sampleIndex, sample := range fullErrorSamples {
					b.WriteString(fmt.Sprintf("  - Sample %d\n\n", sampleIndex+1))
					b.WriteString("```text\n")
					b.WriteString(sanitizeCodeFence(sample))
					if !strings.HasSuffix(sample, "\n") {
						b.WriteString("\n")
					}
					b.WriteString("```\n")
				}
			}
			b.WriteString("\n")
		}

		if i+1 < limit {
			b.WriteString("---\n\n")
		}
	}

	return b.String()
}

func buildReviewSignalIndex(items []reviewItem) reviewSignalIndex {
	index := reviewSignalIndex{
		ByPhase1Cluster: map[string]set.Set[string]{},
		BySignatureID:   map[string]set.Set[string]{},
	}

	for _, item := range items {
		reason := strings.TrimSpace(item.Reason)
		if reason == "" {
			reason = "(unspecified)"
		}

		for _, phase1ClusterID := range item.SourcePhase1ClusterIDs {
			key := strings.TrimSpace(phase1ClusterID)
			if key == "" {
				continue
			}
			current := index.ByPhase1Cluster[key]
			if current == nil {
				current = set.New[string]()
			}
			current.Insert(reason)
			index.ByPhase1Cluster[key] = current
		}

		for _, signatureID := range item.MemberSignatureIDs {
			key := strings.TrimSpace(signatureID)
			if key == "" {
				continue
			}
			current := index.BySignatureID[key]
			if current == nil {
				current = set.New[string]()
			}
			current.Insert(reason)
			index.BySignatureID[key] = current
		}
	}
	return index
}

func reviewReasonsForCluster(cluster testCluster, index reviewSignalIndex) []string {
	reasons := set.New[string]()

	if values, ok := index.ByPhase1Cluster[strings.TrimSpace(cluster.Phase1ClusterID)]; ok {
		for _, reason := range values.UnsortedList() {
			if isInformationalReviewReason(reason) {
				continue
			}
			reasons.Insert(reason)
		}
	}
	for _, signatureID := range cluster.MemberSignatureIDs {
		if values, ok := index.BySignatureID[strings.TrimSpace(signatureID)]; ok {
			for _, reason := range values.UnsortedList() {
				if isInformationalReviewReason(reason) {
					continue
				}
				reasons.Insert(reason)
			}
		}
	}

	list := reasons.UnsortedList()
	sort.Strings(list)
	return list
}

func isInformationalReviewReason(reason string) bool {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "phase1_cluster_id_collision":
		return true
	default:
		return false
	}
}

func testRiskLabel(aggregate testAggregate) string {
	if aggregate.PostGoodFailures > 0 {
		return "systemic signal (post-good-commit-runs > 0)"
	}
	switch len(aggregate.PRCounts) {
	case 0:
		return "unknown (no PR mapping)"
	case 1:
		return "likely PR-specific (single PR)"
	default:
		return "tentative PR-specific (multi-PR, no post-good-commit-runs)"
	}
}

func aggregateByTest(clusters []testCluster) []testAggregate {
	byTest := map[testKey]*testAggregate{}
	for _, cluster := range clusters {
		key := toTestKey(cluster.Lane, cluster.JobName, cluster.TestName, cluster.TestSuite)
		entry, ok := byTest[key]
		if !ok {
			entry = &testAggregate{
				Key:      key,
				PRCounts: map[int]int{},
			}
			byTest[key] = entry
		}

		entry.Clusters = append(entry.Clusters, cluster)
		entry.TotalFailures += cluster.SupportCount
		entry.DistinctSignatures++
		entry.PostGoodFailures += cluster.PostGoodCommitCount
		for _, ref := range cluster.References {
			if ref.PRNumber > 0 {
				entry.PRCounts[ref.PRNumber]++
			}
			occurredAt, ok := parseTimestamp(ref.OccurredAt)
			if !ok {
				continue
			}
			if entry.LatestFailure.IsZero() || occurredAt.After(entry.LatestFailure) {
				entry.LatestFailure = occurredAt
			}
		}
	}

	out := make([]testAggregate, 0, len(byTest))
	for _, value := range byTest {
		out = append(out, *value)
	}
	return out
}

func mergeUnknownJobAggregates(aggregates []testAggregate) []testAggregate {
	if len(aggregates) == 0 {
		return aggregates
	}
	type aggregateBaseKey struct {
		Lane      string
		TestName  string
		TestSuite string
	}

	knownByBase := map[aggregateBaseKey][]int{}
	for index := range aggregates {
		if isUnknownJobName(aggregates[index].Key.JobName) {
			continue
		}
		base := aggregateBaseKey{
			Lane:      aggregates[index].Key.Lane,
			TestName:  aggregates[index].Key.TestName,
			TestSuite: aggregates[index].Key.TestSuite,
		}
		knownByBase[base] = append(knownByBase[base], index)
	}

	absorbed := map[int]struct{}{}
	for index, aggregate := range aggregates {
		if !isUnknownJobName(aggregate.Key.JobName) {
			continue
		}
		base := aggregateBaseKey{
			Lane:      aggregate.Key.Lane,
			TestName:  aggregate.Key.TestName,
			TestSuite: aggregate.Key.TestSuite,
		}
		targets := knownByBase[base]
		if len(targets) != 1 {
			continue
		}
		targetIndex := targets[0]
		mergeTestAggregate(&aggregates[targetIndex], aggregate)
		absorbed[index] = struct{}{}
	}

	if len(absorbed) == 0 {
		return aggregates
	}
	out := make([]testAggregate, 0, len(aggregates)-len(absorbed))
	for index, aggregate := range aggregates {
		if _, shouldSkip := absorbed[index]; shouldSkip {
			continue
		}
		out = append(out, aggregate)
	}
	return out
}

func mergeTestAggregate(target *testAggregate, source testAggregate) {
	if target == nil {
		return
	}
	target.Clusters = append(target.Clusters, source.Clusters...)
	target.TotalFailures += source.TotalFailures
	target.DistinctSignatures += source.DistinctSignatures
	target.PostGoodFailures += source.PostGoodFailures
	if target.LatestFailure.IsZero() || source.LatestFailure.After(target.LatestFailure) {
		target.LatestFailure = source.LatestFailure
	}
	if target.PRCounts == nil {
		target.PRCounts = map[int]int{}
	}
	for prNumber, count := range source.PRCounts {
		if prNumber <= 0 || count <= 0 {
			continue
		}
		target.PRCounts[prNumber] += count
	}
}

func isUnknownJobName(jobName string) bool {
	normalized := strings.ToLower(strings.TrimSpace(jobName))
	return normalized == "" || normalized == "unknown"
}

func sortClusters(clusters []testCluster) {
	sort.Slice(clusters, func(i, j int) bool {
		if clusters[i].SupportCount != clusters[j].SupportCount {
			return clusters[i].SupportCount > clusters[j].SupportCount
		}
		if clusters[i].PostGoodCommitCount != clusters[j].PostGoodCommitCount {
			return clusters[i].PostGoodCommitCount > clusters[j].PostGoodCommitCount
		}
		return clusters[i].Phase1ClusterID < clusters[j].Phase1ClusterID
	})
}

func postGoodKnownCountForAggregate(aggregate testAggregate) int {
	count := 0
	for _, cluster := range aggregate.Clusters {
		count += postGoodKnownCountForCluster(cluster)
	}
	return count
}

func postGoodKnownCountForCluster(cluster testCluster) int {
	count := 0
	for _, reference := range cluster.References {
		if reference.PostGoodKnown {
			count++
		}
	}
	return count
}

func signaturePRWarning(cluster testCluster, recentRuns []reference) (string, bool) {
	if cluster.PostGoodCommitCount > 0 {
		return "", false
	}

	coverageSuffix := ""
	knownPostGood := postGoodKnownCountForCluster(cluster)
	if knownPostGood < cluster.SupportCount {
		unknownCount := cluster.SupportCount - knownPostGood
		coverageSuffix = fmt.Sprintf(" Missing run-level post-good metadata for %d/%d failures.", unknownCount, cluster.SupportCount)
	}

	allPRs := set.New[int]()
	prCounts := map[int]int{}
	for _, reference := range cluster.References {
		if reference.PRNumber <= 0 {
			continue
		}
		allPRs.Insert(reference.PRNumber)
		prCounts[reference.PRNumber]++
	}
	if allPRs.Len() == 1 {
		var prNumber int
		var count int
		for pr, c := range prCounts {
			prNumber = pr
			count = c
		}
		return fmt.Sprintf("signature post-good-commit-runs=0 and all observed failures map to PR #%d (%d/%d). Strong bad-PR signal.%s", prNumber, count, cluster.SupportCount, coverageSuffix), true
	}

	recentPRs := set.New[int]()
	recentPRCounts := map[int]int{}
	for _, item := range recentRuns {
		if item.PRNumber <= 0 {
			continue
		}
		recentPRs.Insert(item.PRNumber)
		recentPRCounts[item.PRNumber]++
	}
	if recentPRs.Len() == 1 && len(recentRuns) > 1 {
		var prNumber int
		var count int
		for pr, c := range recentPRCounts {
			prNumber = pr
			count = c
		}
		return fmt.Sprintf("signature post-good-commit-runs=0 and recent failures are concentrated on PR #%d (%d recent runs). Check PR-specific regressions first.%s", prNumber, count, coverageSuffix), true
	}

	// This report is built from failing runs only, so post-good-commit-runs=0
	// does not
	// prove a PR-local issue unless we also have post-merge signal.
	if recentPRs.Len() > 1 {
		return fmt.Sprintf("signature post-good-commit-runs=0 across %d recent PRs. Bad-PR attribution is tentative because there is no post-merge signal in failures.%s", recentPRs.Len(), coverageSuffix), true
	}
	if allPRs.Len() > 1 {
		return fmt.Sprintf("signature post-good-commit-runs=0 across %d PRs. Bad-PR attribution is tentative because there is no post-merge signal in failures.%s", allPRs.Len(), coverageSuffix), true
	}
	if allPRs.Len() == 1 {
		return "signature post-good-commit-runs=0. Bad-PR attribution is tentative because there is no post-merge signal in failures." + coverageSuffix, true
	}
	return "signature post-good-commit-runs=0 with unknown PR mapping. Bad-PR attribution is tentative because there is no post-merge signal in failures." + coverageSuffix, true
}

func lookupMetadata(
	key testKey,
	metadataByFull map[testKey]testMetadata,
	metadataByNoSuite map[testKeyNoSuite]testMetadata,
) testMetadata {
	if value, ok := metadataByFull[key]; ok {
		return cloneMetadata(value)
	}

	noSuiteKey := testKeyNoSuite{
		Lane:     key.Lane,
		JobName:  key.JobName,
		TestName: key.TestName,
	}
	if value, ok := metadataByNoSuite[noSuiteKey]; ok {
		return cloneMetadata(value)
	}
	return testMetadata{}
}

func cloneMetadata(value testMetadata) testMetadata {
	out := testMetadata{
		Runs: value.Runs,
		Tags: append([]string(nil), value.Tags...),
	}
	if value.PassRate != nil {
		out.PassRate = float64Ptr(*value.PassRate)
	}
	return out
}

func preferMetadata(candidate testMetadata, existing testMetadata) bool {
	if candidate.Runs != existing.Runs {
		return candidate.Runs > existing.Runs
	}
	switch {
	case candidate.PassRate != nil && existing.PassRate == nil:
		return true
	case candidate.PassRate == nil && existing.PassRate != nil:
		return false
	case candidate.PassRate != nil && existing.PassRate != nil:
		if *candidate.PassRate != *existing.PassRate {
			return *candidate.PassRate < *existing.PassRate
		}
	}
	return len(candidate.Tags) > len(existing.Tags)
}

func recentRunsForCluster(cluster testCluster, limit int) []reference {
	if len(cluster.References) == 0 || limit <= 0 {
		return nil
	}

	seen := map[string]struct{}{}
	references := append([]reference(nil), cluster.References...)
	sort.Slice(references, func(i, j int) bool {
		ti, okI := parseTimestamp(references[i].OccurredAt)
		tj, okJ := parseTimestamp(references[j].OccurredAt)
		switch {
		case okI && okJ && !ti.Equal(tj):
			return ti.After(tj)
		case okI != okJ:
			return okI
		}
		if references[i].RunURL != references[j].RunURL {
			return references[i].RunURL < references[j].RunURL
		}
		return references[i].SignatureID < references[j].SignatureID
	})

	out := make([]reference, 0, limit)
	for _, item := range references {
		runURL := strings.TrimSpace(item.RunURL)
		if runURL == "" {
			continue
		}
		if _, ok := seen[runURL]; ok {
			continue
		}
		seen[runURL] = struct{}{}
		out = append(out, item)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func fullErrorSamplesForCluster(cluster testCluster, recentRuns []reference, fullErrorsByReference map[referenceKey]string, limit int) []string {
	if limit <= 0 {
		return nil
	}

	samples := make([]string, 0, limit)
	seen := map[string]struct{}{}
	appendFromReferences := func(references []reference) {
		for _, ref := range references {
			if len(samples) >= limit {
				return
			}

			key := referenceKey{
				RunURL:      strings.TrimSpace(ref.RunURL),
				SignatureID: strings.TrimSpace(ref.SignatureID),
			}
			candidate := ""
			if key.RunURL != "" && key.SignatureID != "" {
				candidate = strings.TrimSpace(fullErrorsByReference[key])
			}
			if candidate == "" {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			samples = append(samples, candidate)
		}
	}

	appendFromReferences(recentRuns)
	if len(samples) < limit {
		appendFromReferences(cluster.References)
	}
	return samples
}

func parseTimestamp(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

func toTestKey(lane string, jobName string, testName string, testSuite string) testKey {
	return testKey{
		Lane:      normalizeKeyPart(lane),
		JobName:   normalizeKeyPart(jobName),
		TestName:  normalizeKeyPart(testName),
		TestSuite: normalizeKeyPart(testSuite),
	}
}

func normalizeKeyPart(value string) string {
	return strings.TrimSpace(value)
}

func cleanInline(input string, max int) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(input, "\n", " "), "\r", " "), "\t", " "))
	normalized = strings.Join(strings.Fields(normalized), " ")
	normalized = strings.ReplaceAll(normalized, "`", "'")
	if max <= 0 {
		return normalized
	}
	runes := []rune(normalized)
	if len(runes) <= max {
		return normalized
	}
	return string(runes[:max-1]) + "..."
}

func sanitizeCodeFence(input string) string {
	return strings.ReplaceAll(input, "```", "'''")
}

func escapePipe(input string) string {
	return strings.ReplaceAll(input, "|", "\\|")
}

func clusterDailyDensitySparkline(cluster testCluster, windowDays int, generatedAt time.Time) (string, []int, string, bool) {
	if windowDays <= 0 {
		return "", nil, "", false
	}

	endDay := generatedAt.UTC().Truncate(24 * time.Hour)
	if endDay.IsZero() {
		endDay = time.Now().UTC().Truncate(24 * time.Hour)
	}
	startDay := endDay.AddDate(0, 0, -(windowDays - 1))

	counts := make([]int, windowDays)
	seenTimestamp := false
	for _, reference := range cluster.References {
		ts, ok := parseTimestamp(reference.OccurredAt)
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

	dateRange := fmt.Sprintf("%s..%s", startDay.Format("2006-01-02"), endDay.Format("2006-01-02"))
	return unicodeBuilder.String(), counts, dateRange, true
}

func formatCounts(values []int) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return strings.Join(parts, ",")
}

func float64Ptr(value float64) *float64 {
	v := value
	return &v
}
