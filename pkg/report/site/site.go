package site

import (
	"context"
	"fmt"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	reportsummary "ci-failure-atlas/pkg/report/summary"
	"ci-failure-atlas/pkg/report/triagehtml"
	reportweekly "ci-failure-atlas/pkg/report/weekly"
	semhistory "ci-failure-atlas/pkg/semantic/history"
	semanticworkflow "ci-failure-atlas/pkg/semantic/workflow"
	storecontracts "ci-failure-atlas/pkg/store/contracts"
	postgresstore "ci-failure-atlas/pkg/store/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	weeklyReportFile        = "weekly-metrics.html"
	triageReportFile        = "triage-report.html"
	indexFileName           = "index.html"
	defaultSiteRoot         = "site"
	defaultHistoryWeeks     = 4
	defaultWeeklyTargetRate = 95.0
)

type BuildOptions struct {
	SiteRoot         string
	CurrentWeekStart string
	HistoryWeeks     int
	FromExisting     bool
	PostgresPool     *pgxpool.Pool
}

type WeekDirectory struct {
	Name string
	Path string
}

type BuildResult struct {
	SiteRoot   string
	Weeks      []WeekDirectory
	LatestWeek string
}

type PushOptions struct {
	SiteRoot       string
	StorageAccount string
	AuthMode       string
	ContainerName  string
	Uploader       BlobUploader
	PostgresPool   *pgxpool.Pool
}

type PushResult struct {
	WeeksUploaded int
	FilesUploaded int
	LatestWeek    string
}

type BlobUploadRequest struct {
	SourcePath  string
	TargetPath  string
	ContentType string
}

type BlobUploader interface {
	Upload(ctx context.Context, req BlobUploadRequest) error
}

func Build(ctx context.Context, opts BuildOptions) (BuildResult, error) {
	resolved, err := normalizeBuildOptions(opts)
	if err != nil {
		return BuildResult{}, err
	}
	if err := os.MkdirAll(resolved.SiteRoot, 0o755); err != nil {
		return BuildResult{}, fmt.Errorf("create site root: %w", err)
	}

	if !resolved.FromExisting {
		if err := generateSiteReportsForWeekStarts(ctx, resolved, weekStartsToGenerate(resolved.CurrentWeekStart, resolved.HistoryWeeks), true); err != nil {
			return BuildResult{}, err
		}
	} else {
		semanticWeekStarts, semanticErr := discoverSemanticWeekStarts(ctx, resolved)
		if semanticErr != nil {
			return BuildResult{}, semanticErr
		}
		if len(semanticWeekStarts) > 0 {
			if err := resetSiteRootForReportRebuild(resolved.SiteRoot); err != nil {
				return BuildResult{}, err
			}
			if err := generateSiteReportsForWeekStarts(ctx, resolved, semanticWeekStarts, false); err != nil {
				return BuildResult{}, err
			}
		}
	}

	weeks, err := discoverWeekDirectories(resolved.SiteRoot)
	if err != nil {
		return BuildResult{}, err
	}
	for _, week := range weeks {
		if err := writeWeekIndex(week); err != nil {
			return BuildResult{}, err
		}
	}

	latestWeek := ""
	if len(weeks) > 0 {
		latestWeek = weeks[0].Name
	}
	defaultRootWeek := latestCompleteWeekForRootRedirect(weeks, time.Now().UTC())
	if err := writeLatestDirectory(resolved.SiteRoot, weeks); err != nil {
		return BuildResult{}, err
	}
	if err := writeArchiveIndex(resolved.SiteRoot, weeks, latestWeek); err != nil {
		return BuildResult{}, err
	}
	if err := writeRootIndex(resolved.SiteRoot, defaultRootWeek); err != nil {
		return BuildResult{}, err
	}

	return BuildResult{
		SiteRoot:   resolved.SiteRoot,
		Weeks:      weeks,
		LatestWeek: latestWeek,
	}, nil
}

func generateSiteReportsForWeekStarts(
	ctx context.Context,
	resolved normalizedBuildOptions,
	weekStarts []time.Time,
	runSemanticWorkflow bool,
) error {
	weekSubdirectories := make([]string, 0, len(weekStarts))
	for _, weekStart := range weekStarts {
		weekSubdirectories = append(weekSubdirectories, weekStart.Format("2006-01-02"))
	}
	for index, weekStart := range weekStarts {
		weekSubdir := weekSubdirectories[index]
		previousWeekSubdirectory := ""
		if index > 0 {
			previousWeekSubdirectory = weekSubdirectories[index-1]
		}
		nextWeekSubdirectory := ""
		if index+1 < len(weekSubdirectories) {
			nextWeekSubdirectory = weekSubdirectories[index+1]
		}
		if runSemanticWorkflow {
			if err := runSemanticWorkflowForWeek(ctx, resolved, weekSubdir, weekStart); err != nil {
				return err
			}
		}
		if err := generateSiteReportsForWeek(
			ctx,
			resolved,
			weekSubdir,
			previousWeekSubdirectory,
			nextWeekSubdirectory,
			weekStart,
			resolved.HistoryWeeks,
		); err != nil {
			return err
		}
	}
	return nil
}

func Push(ctx context.Context, opts PushOptions) (PushResult, error) {
	storageAccount := strings.TrimSpace(opts.StorageAccount)
	if storageAccount == "" {
		return PushResult{}, fmt.Errorf("storage account must not be empty")
	}

	buildResult, err := Build(ctx, BuildOptions{
		SiteRoot:     opts.SiteRoot,
		FromExisting: true,
		PostgresPool: opts.PostgresPool,
	})
	if err != nil {
		return PushResult{}, err
	}
	if len(buildResult.Weeks) == 0 {
		return PushResult{}, fmt.Errorf("no report week directories found under %q", buildResult.SiteRoot)
	}

	uploader := opts.Uploader
	if uploader == nil {
		uploader = azBlobUploader{
			StorageAccount: storageAccount,
			AuthMode:       normalizedAuthMode(opts.AuthMode),
			ContainerName:  normalizedContainer(opts.ContainerName),
		}
	}

	requests, cleanup, err := buildUploadRequests(buildResult)
	if err != nil {
		return PushResult{}, err
	}
	defer cleanup()
	for _, req := range requests {
		if err := uploader.Upload(ctx, req); err != nil {
			return PushResult{}, err
		}
	}

	return PushResult{
		WeeksUploaded: len(buildResult.Weeks),
		FilesUploaded: len(requests),
		LatestWeek:    buildResult.LatestWeek,
	}, nil
}

type normalizedBuildOptions struct {
	SiteRoot         string
	CurrentWeekStart time.Time
	HistoryWeeks     int
	FromExisting     bool
	PostgresPool     *pgxpool.Pool
}

func normalizeBuildOptions(opts BuildOptions) (normalizedBuildOptions, error) {
	siteRoot := strings.TrimSpace(opts.SiteRoot)
	if siteRoot == "" {
		siteRoot = defaultSiteRoot
	}
	weeks := opts.HistoryWeeks
	if weeks <= 0 {
		weeks = defaultHistoryWeeks
	}
	currentWeekStart, err := resolveCurrentWeekStart(opts.CurrentWeekStart)
	if err != nil {
		return normalizedBuildOptions{}, err
	}
	pool := opts.PostgresPool
	if pool == nil {
		return normalizedBuildOptions{}, fmt.Errorf("postgres pool is required")
	}
	return normalizedBuildOptions{
		SiteRoot:         siteRoot,
		CurrentWeekStart: currentWeekStart,
		HistoryWeeks:     weeks,
		FromExisting:     opts.FromExisting,
		PostgresPool:     pool,
	}, nil
}

func resolveCurrentWeekStart(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return latestSundayUTC(time.Now().UTC()), nil
	}
	normalizedWeek, err := postgresstore.NormalizeWeek(trimmed)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid --start-date %q (expected YYYY-MM-DD Sunday start): %w", trimmed, err)
	}
	parsed, err := time.Parse("2006-01-02", normalizedWeek)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse week %q: %w", normalizedWeek, err)
	}
	return parsed.UTC(), nil
}

func latestSundayUTC(now time.Time) time.Time {
	normalized := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
	offset := int(normalized.Weekday())
	return normalized.AddDate(0, 0, -offset)
}

func weekStartsToGenerate(currentWeekStart time.Time, weeks int) []time.Time {
	if weeks <= 0 {
		return nil
	}
	out := make([]time.Time, 0, weeks)
	oldest := currentWeekStart.AddDate(0, 0, -7*(weeks-1))
	for i := 0; i < weeks; i++ {
		out = append(out, oldest.AddDate(0, 0, i*7).UTC())
	}
	return out
}

func discoverSemanticWeekStarts(ctx context.Context, resolved normalizedBuildOptions) ([]time.Time, error) {
	weeks, err := postgresstore.ListWeeks(ctx, resolved.PostgresPool)
	if err != nil {
		return nil, fmt.Errorf("list semantic weeks from postgres: %w", err)
	}
	weekNames := append([]string(nil), weeks...)

	weekStarts := make([]time.Time, 0, len(weekNames))
	seen := map[string]struct{}{}
	for _, weekName := range weekNames {
		parsed, ok := parseWeekDirectoryDate(weekName)
		if !ok {
			continue
		}
		key := parsed.Format("2006-01-02")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		weekStarts = append(weekStarts, parsed)
	}
	sort.Slice(weekStarts, func(i, j int) bool {
		return weekStarts[i].Before(weekStarts[j])
	})
	return weekStarts, nil
}

func resetSiteRootForReportRebuild(siteRoot string) error {
	entries, err := os.ReadDir(siteRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read site root %q for cleanup: %w", siteRoot, err)
	}
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name())
		if name == "" {
			continue
		}
		path := filepath.Join(siteRoot, name)
		if entry.IsDir() {
			if name == "latest" || name == "archive" {
				if err := os.RemoveAll(path); err != nil {
					return fmt.Errorf("remove existing site directory %q: %w", path, err)
				}
				continue
			}
			if _, isWeekDirectory := parseWeekDirectoryDate(name); isWeekDirectory {
				if err := os.RemoveAll(path); err != nil {
					return fmt.Errorf("remove existing week directory %q: %w", path, err)
				}
			}
			continue
		}
		if name == indexFileName {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove existing root index %q: %w", path, err)
			}
		}
	}
	return nil
}

func openSemanticStoreForWeek(resolved normalizedBuildOptions, semanticSubdirectory string) (storecontracts.Store, error) {
	week := strings.TrimSpace(semanticSubdirectory)
	if week == "" {
		return nil, fmt.Errorf("week is required")
	}
	store, err := postgresstore.New(resolved.PostgresPool, postgresstore.Options{
		Week: week,
	})
	if err != nil {
		return nil, fmt.Errorf("open postgres semantic store for week %q: %w", week, err)
	}
	return store, nil
}

func runSemanticWorkflowForWeek(
	ctx context.Context,
	resolved normalizedBuildOptions,
	semanticSubdirectory string,
	windowStart time.Time,
) error {
	store, err := openSemanticStoreForWeek(resolved, semanticSubdirectory)
	if err != nil {
		return err
	}
	defer func() {
		_ = store.Close()
	}()

	_, err = semanticworkflow.MaterializeWeek(ctx, store, windowStart.UTC())
	if err != nil {
		return fmt.Errorf("run semantic workflow for week %q: %w", semanticSubdirectory, err)
	}
	return nil
}

func generateSiteReportsForWeek(
	ctx context.Context,
	resolved normalizedBuildOptions,
	semanticSubdirectory string,
	previousWeekSubdirectory string,
	nextWeekSubdirectory string,
	windowStart time.Time,
	historyWeeks int,
) error {
	logger := loggerFromContext(ctx).WithValues("component", "report.site")
	totalStart := time.Now()
	currentStore, err := openSemanticStoreForWeek(resolved, semanticSubdirectory)
	if err != nil {
		return fmt.Errorf("open semantic store for week %q: %w", semanticSubdirectory, err)
	}
	defer func() {
		_ = currentStore.Close()
	}()

	var previousSemanticStore storecontracts.Store
	if strings.TrimSpace(previousWeekSubdirectory) != "" {
		previousStore, previousStoreErr := openSemanticStoreForWeek(resolved, previousWeekSubdirectory)
		if previousStoreErr != nil {
			return fmt.Errorf("open previous semantic store for week %q: %w", previousWeekSubdirectory, previousStoreErr)
		}
		previousSemanticStore = previousStore
		defer func() {
			_ = previousStore.Close()
		}()
	}

	siteWeekDirectory := filepath.Join(resolved.SiteRoot, semanticSubdirectory)
	if err := os.MkdirAll(siteWeekDirectory, 0o755); err != nil {
		return fmt.Errorf("create site week directory %q: %w", siteWeekDirectory, err)
	}

	historyStart := time.Now()
	historyResolverOptions := semhistory.BuildOptions{
		CurrentWeek:                  semanticSubdirectory,
		GlobalSignatureLookbackWeeks: historyWeeks,
	}
	historyResolverOptions.ListWeeks = func(ctx context.Context) ([]string, error) {
		return postgresstore.ListWeeks(ctx, resolved.PostgresPool)
	}
	historyResolverOptions.OpenStore = func(_ context.Context, week string) (storecontracts.Store, error) {
		return openSemanticStoreForWeek(resolved, week)
	}
	historyResolver, err := semhistory.BuildGlobalSignatureResolver(ctx, historyResolverOptions)
	if err != nil {
		return fmt.Errorf("build global signature history resolver for week %q: %w", semanticSubdirectory, err)
	}
	historyElapsed := time.Since(historyStart)

	weeklyStart := time.Now()
	weeklyOpts := reportweekly.DefaultOptions()
	weeklyOpts.OutputPath = filepath.Join(siteWeekDirectory, weeklyReportFile)
	weeklyOpts.StartDate = windowStart.UTC().Format("2006-01-02")
	weeklyOpts.TargetRate = defaultWeeklyTargetRate
	weeklyOpts.Week = semanticSubdirectory
	weeklyOpts.HistoryHorizonWeeks = historyWeeks
	weeklyOpts.HistoryResolver = historyResolver
	weeklyOpts.Chrome = buildReportChromeOptions(
		semanticSubdirectory,
		previousWeekSubdirectory,
		nextWeekSubdirectory,
		triagehtml.ReportViewWeekly,
	)
	if err := reportweekly.GenerateWithComparison(ctx, currentStore, previousSemanticStore, weeklyOpts); err != nil {
		return fmt.Errorf("generate weekly HTML for week %q: %w", semanticSubdirectory, err)
	}
	weeklyElapsed := time.Since(weeklyStart)

	summaryStart := time.Now()
	summaryOpts := reportsummary.DefaultOptions()
	summaryOpts.OutputPath = filepath.Join(siteWeekDirectory, triageReportFile)
	summaryOpts.Format = "html"
	summaryOpts.Top = 25
	summaryOpts.Week = semanticSubdirectory
	summaryOpts.HistoryHorizonWeeks = historyWeeks
	summaryOpts.HistoryResolver = historyResolver
	summaryOpts.Chrome = buildReportChromeOptions(
		semanticSubdirectory,
		previousWeekSubdirectory,
		nextWeekSubdirectory,
		triagehtml.ReportViewTriage,
	)
	if err := reportsummary.Generate(ctx, currentStore, summaryOpts); err != nil {
		return fmt.Errorf("generate triage HTML for week %q: %w", semanticSubdirectory, err)
	}
	summaryElapsed := time.Since(summaryStart)

	logger.Info(
		"Generated site reports for week.",
		"week", semanticSubdirectory,
		"history_resolver_ms", historyElapsed.Milliseconds(),
		"weekly_ms", weeklyElapsed.Milliseconds(),
		"summary_ms", summaryElapsed.Milliseconds(),
		"total_ms", time.Since(totalStart).Milliseconds(),
	)

	return nil
}

func buildReportChromeOptions(
	currentWeekSubdirectory string,
	previousWeekSubdirectory string,
	nextWeekSubdirectory string,
	currentView triagehtml.ReportView,
) triagehtml.ReportChromeOptions {
	reportFileName := weeklyReportFile
	if currentView == triagehtml.ReportViewTriage {
		reportFileName = triageReportFile
	}
	previousHref := ""
	trimmedPreviousWeekSubdirectory := strings.TrimSpace(previousWeekSubdirectory)
	if trimmedPreviousWeekSubdirectory != "" {
		previousHref = filepath.ToSlash(filepath.Join("..", trimmedPreviousWeekSubdirectory, reportFileName))
	}
	nextHref := ""
	trimmedNextWeekSubdirectory := strings.TrimSpace(nextWeekSubdirectory)
	if trimmedNextWeekSubdirectory != "" {
		nextHref = filepath.ToSlash(filepath.Join("..", trimmedNextWeekSubdirectory, reportFileName))
	}
	return triagehtml.ReportChromeOptions{
		CurrentWeek:  strings.TrimSpace(currentWeekSubdirectory),
		CurrentView:  currentView,
		PreviousWeek: trimmedPreviousWeekSubdirectory,
		PreviousHref: previousHref,
		NextWeek:     trimmedNextWeekSubdirectory,
		NextHref:     nextHref,
		WeeklyHref:   weeklyReportFile,
		TriageHref:   triageReportFile,
		ArchiveHref:  "../archive/",
	}
}

func discoverWeekDirectories(siteRoot string) ([]WeekDirectory, error) {
	entries, err := os.ReadDir(siteRoot)
	if err != nil {
		return nil, fmt.Errorf("read site root %q: %w", siteRoot, err)
	}

	weeks := make([]WeekDirectory, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" || name == "latest" || name == "archive" {
			continue
		}
		dirPath := filepath.Join(siteRoot, name)
		hasWeekly, weeklyErr := isRegularFile(filepath.Join(dirPath, weeklyReportFile))
		if weeklyErr != nil {
			return nil, weeklyErr
		}
		hasTriage, triageErr := isRegularFile(filepath.Join(dirPath, triageReportFile))
		if triageErr != nil {
			return nil, triageErr
		}
		if !hasWeekly || !hasTriage {
			continue
		}
		weeks = append(weeks, WeekDirectory{
			Name: name,
			Path: dirPath,
		})
	}

	sort.Slice(weeks, func(i, j int) bool {
		ti, okI := parseWeekDirectoryDate(weeks[i].Name)
		tj, okJ := parseWeekDirectoryDate(weeks[j].Name)
		switch {
		case okI && okJ && !ti.Equal(tj):
			return ti.After(tj)
		case okI != okJ:
			return okI
		}
		return weeks[i].Name > weeks[j].Name
	})
	return weeks, nil
}

func writeWeekIndex(week WeekDirectory) error {
	content := renderWeekIndexHTML(week.Name, time.Now().UTC())
	targetPath := filepath.Join(week.Path, indexFileName)
	if err := os.WriteFile(targetPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write week index for %q: %w", week.Name, err)
	}
	return nil
}

func writeRootIndex(siteRoot string, latestWeek string) error {
	content := renderRootIndexHTML(latestWeek, time.Now().UTC())
	targetPath := filepath.Join(siteRoot, indexFileName)
	if err := os.WriteFile(targetPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write root index: %w", err)
	}
	return nil
}

func writeArchiveIndex(siteRoot string, weeks []WeekDirectory, latestWeek string) error {
	content := renderArchiveIndexHTML(weeks, latestWeek, time.Now().UTC())
	targetPath := filepath.Join(siteRoot, "archive", indexFileName)
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("create archive index directory: %w", err)
	}
	if err := os.WriteFile(targetPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write archive index: %w", err)
	}
	return nil
}

func latestCompleteWeekForRootRedirect(weeks []WeekDirectory, now time.Time) string {
	if len(weeks) == 0 {
		return ""
	}
	currentWeekStart := latestSundayUTC(now.UTC())
	for _, week := range weeks {
		weekStart, ok := parseWeekDirectoryDate(strings.TrimSpace(week.Name))
		if !ok {
			continue
		}
		if weekStart.Before(currentWeekStart) {
			return strings.TrimSpace(week.Name)
		}
	}
	return strings.TrimSpace(weeks[0].Name)
}

func renderRootIndexHTML(latestWeek string, generatedAt time.Time) string {
	var b strings.Builder
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Reports</title>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString("  <h1>CI Reports</h1>\n")
	if strings.TrimSpace(latestWeek) != "" {
		redirectTarget := filepath.ToSlash(filepath.Join(strings.TrimSpace(latestWeek), weeklyReportFile))
		b.WriteString(fmt.Sprintf("  <meta http-equiv=\"refresh\" content=\"0; url=%s\" />\n", html.EscapeString(redirectTarget)))
		b.WriteString("  <script>\n")
		b.WriteString(fmt.Sprintf("    window.location.replace(%q);\n", redirectTarget))
		b.WriteString("  </script>\n")
		b.WriteString(fmt.Sprintf(
			"  <p>Redirecting to the default weekly report (<strong>%s</strong>). If this does not happen automatically, <a href=\"%s\">open the default weekly report</a>.</p>\n",
			html.EscapeString(latestWeek),
			html.EscapeString(redirectTarget),
		))
	} else {
		b.WriteString("  <p>No weekly reports are currently available.</p>\n")
	}
	b.WriteString("  <p><a href=\"archive/\">Browse weekly archive</a></p>\n")
	b.WriteString(fmt.Sprintf("  <p>Generated: %s</p>\n", html.EscapeString(generatedAt.Format(time.RFC3339))))
	b.WriteString("</body>\n")
	b.WriteString("</html>\n")
	return b.String()
}

func renderArchiveIndexHTML(weeks []WeekDirectory, latestWeek string, generatedAt time.Time) string {
	var b strings.Builder
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Reports Archive</title>\n")
	b.WriteString(triagehtml.ThemeInitScriptTag())
	b.WriteString("  <style>\n")
	b.WriteString("    body { font-family: Arial, sans-serif; margin: 20px; color: #1f2937; }\n")
	b.WriteString("    ul { line-height: 1.8; }\n")
	b.WriteString("    .meta { color: #4b5563; margin-bottom: 8px; }\n")
	b.WriteString(triagehtml.ThemeCSS())
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString(triagehtml.ThemeToggleHTML())
	b.WriteString("  <h1>CI Reports Archive</h1>\n")
	b.WriteString("  <ul>\n")
	if latestWeek != "" {
		b.WriteString("    <li><a href=\"../latest/weekly-metrics.html\">Latest weekly report</a>")
		b.WriteString(fmt.Sprintf(" (from %s)</li>\n", html.EscapeString(latestWeek)))
	} else {
		b.WriteString("    <li>Latest weekly report (n/a)</li>\n")
	}
	for _, week := range weeks {
		b.WriteString(fmt.Sprintf("    <li><a href=\"../%s/\">%s</a></li>\n", html.EscapeString(week.Name), html.EscapeString(week.Name)))
	}
	b.WriteString("  </ul>\n")
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Generated: %s</p>\n", html.EscapeString(generatedAt.Format(time.RFC3339))))
	b.WriteString(triagehtml.ThemeToggleScriptTag())
	b.WriteString("</body>\n")
	b.WriteString("</html>\n")
	return b.String()
}

func renderWeekIndexHTML(week string, generatedAt time.Time) string {
	var b strings.Builder
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString(fmt.Sprintf("  <title>CI Reports - %s</title>\n", html.EscapeString(week)))
	b.WriteString(triagehtml.ThemeInitScriptTag())
	b.WriteString("  <style>\n")
	b.WriteString("    body { font-family: Arial, sans-serif; margin: 20px; color: #1f2937; }\n")
	b.WriteString("    ul { line-height: 1.8; }\n")
	b.WriteString("    .meta { color: #4b5563; margin-bottom: 8px; }\n")
	b.WriteString(triagehtml.ThemeCSS())
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString(triagehtml.ThemeToggleHTML())
	b.WriteString(fmt.Sprintf("  <h1>CI Reports - %s</h1>\n", html.EscapeString(week)))
	b.WriteString("  <ul>\n")
	b.WriteString(fmt.Sprintf("    <li><a href=\"%s\">Weekly CI status</a></li>\n", weeklyReportFile))
	b.WriteString(fmt.Sprintf("    <li><a href=\"%s\">Triage report</a></li>\n", triageReportFile))
	b.WriteString("  </ul>\n")
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Generated: %s</p>\n", html.EscapeString(generatedAt.Format(time.RFC3339))))
	b.WriteString("  <p><a href=\"../archive/\">Back to archive</a></p>\n")
	b.WriteString(triagehtml.ThemeToggleScriptTag())
	b.WriteString("</body>\n")
	b.WriteString("</html>\n")
	return b.String()
}
func writeLatestDirectory(siteRoot string, weeks []WeekDirectory) error {
	latestDirectoryPath := filepath.Join(siteRoot, "latest")
	if err := os.RemoveAll(latestDirectoryPath); err != nil {
		return fmt.Errorf("remove existing latest directory: %w", err)
	}
	if len(weeks) == 0 {
		return nil
	}

	if err := os.MkdirAll(latestDirectoryPath, 0o755); err != nil {
		return fmt.Errorf("create latest directory: %w", err)
	}
	latestWeek := weeks[0]
	if err := writeLatestIndex(latestDirectoryPath, latestWeek.Name); err != nil {
		return err
	}
	if err := copyFile(filepath.Join(latestWeek.Path, weeklyReportFile), filepath.Join(latestDirectoryPath, weeklyReportFile)); err != nil {
		return fmt.Errorf("write latest weekly report: %w", err)
	}
	if err := copyFile(filepath.Join(latestWeek.Path, triageReportFile), filepath.Join(latestDirectoryPath, triageReportFile)); err != nil {
		return fmt.Errorf("write latest triage report: %w", err)
	}
	return nil
}

func writeLatestIndex(latestDirectoryPath string, latestWeek string) error {
	content := renderLatestIndexHTML(latestWeek)
	targetPath := filepath.Join(latestDirectoryPath, indexFileName)
	if err := os.WriteFile(targetPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write latest index: %w", err)
	}
	return nil
}

func copyFile(sourcePath string, targetPath string) error {
	content, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("read source file %q: %w", sourcePath, err)
	}
	if err := os.WriteFile(targetPath, content, 0o644); err != nil {
		return fmt.Errorf("write target file %q: %w", targetPath, err)
	}
	return nil
}

func buildUploadRequests(buildResult BuildResult) ([]BlobUploadRequest, func(), error) {
	requests := make([]BlobUploadRequest, 0, (len(buildResult.Weeks)*3)+5)
	for _, week := range buildResult.Weeks {
		requests = append(requests,
			BlobUploadRequest{
				SourcePath: filepath.Join(week.Path, weeklyReportFile),
				TargetPath: filepath.ToSlash(filepath.Join(week.Name, weeklyReportFile)),
			},
			BlobUploadRequest{
				SourcePath: filepath.Join(week.Path, triageReportFile),
				TargetPath: filepath.ToSlash(filepath.Join(week.Name, triageReportFile)),
			},
			BlobUploadRequest{
				SourcePath:  filepath.Join(week.Path, indexFileName),
				TargetPath:  filepath.ToSlash(filepath.Join(week.Name, indexFileName)),
				ContentType: "text/html",
			},
		)
	}

	latest := buildResult.Weeks[0]
	latestIndexPath, err := writeTempLatestIndex(latest.Name)
	if err != nil {
		return nil, nil, err
	}
	requests = append(requests,
		BlobUploadRequest{
			SourcePath: filepath.Join(latest.Path, weeklyReportFile),
			TargetPath: filepath.ToSlash(filepath.Join("latest", weeklyReportFile)),
		},
		BlobUploadRequest{
			SourcePath: filepath.Join(latest.Path, triageReportFile),
			TargetPath: filepath.ToSlash(filepath.Join("latest", triageReportFile)),
		},
		BlobUploadRequest{
			SourcePath:  latestIndexPath,
			TargetPath:  filepath.ToSlash(filepath.Join("latest", indexFileName)),
			ContentType: "text/html",
		},
		BlobUploadRequest{
			SourcePath:  filepath.Join(buildResult.SiteRoot, indexFileName),
			TargetPath:  indexFileName,
			ContentType: "text/html",
		},
		BlobUploadRequest{
			SourcePath:  filepath.Join(buildResult.SiteRoot, "archive", indexFileName),
			TargetPath:  filepath.ToSlash(filepath.Join("archive", indexFileName)),
			ContentType: "text/html",
		},
	)

	cleanup := func() {
		_ = os.Remove(latestIndexPath)
	}
	return requests, cleanup, nil
}

func writeTempLatestIndex(latestWeek string) (string, error) {
	file, err := os.CreateTemp("", "report-site-latest-*.html")
	if err != nil {
		return "", fmt.Errorf("create temporary latest index: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()
	content := renderLatestIndexHTML(latestWeek)
	if _, err := file.WriteString(content); err != nil {
		return "", fmt.Errorf("write temporary latest index: %w", err)
	}
	return file.Name(), nil
}

func renderLatestIndexHTML(latestWeek string) string {
	var b strings.Builder
	b.WriteString("<!doctype html>\n")
	b.WriteString("<html lang=\"en\">\n")
	b.WriteString("<head>\n")
	b.WriteString("  <meta charset=\"utf-8\" />\n")
	b.WriteString("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n")
	b.WriteString("  <title>CI Reports - latest</title>\n")
	b.WriteString(triagehtml.ThemeInitScriptTag())
	b.WriteString("  <style>\n")
	b.WriteString("    body { font-family: Arial, sans-serif; margin: 20px; color: #1f2937; }\n")
	b.WriteString("    ul { line-height: 1.8; }\n")
	b.WriteString("    .meta { color: #4b5563; margin-bottom: 8px; }\n")
	b.WriteString(triagehtml.ThemeCSS())
	b.WriteString("  </style>\n")
	b.WriteString("</head>\n")
	b.WriteString("<body>\n")
	b.WriteString(triagehtml.ThemeToggleHTML())
	b.WriteString("  <h1>CI Reports - latest</h1>\n")
	b.WriteString(fmt.Sprintf("  <p class=\"meta\">Latest week directory: <strong>%s</strong></p>\n", html.EscapeString(latestWeek)))
	b.WriteString("  <ul>\n")
	b.WriteString(fmt.Sprintf("    <li><a href=\"%s\">Weekly CI status</a></li>\n", weeklyReportFile))
	b.WriteString(fmt.Sprintf("    <li><a href=\"%s\">Triage report</a></li>\n", triageReportFile))
	b.WriteString("  </ul>\n")
	b.WriteString("  <p><a href=\"../archive/\">Back to archive</a></p>\n")
	b.WriteString(triagehtml.ThemeToggleScriptTag())
	b.WriteString("</body>\n")
	b.WriteString("</html>\n")
	return b.String()
}

func isRegularFile(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat %q: %w", path, err)
	}
	return info.Mode().IsRegular(), nil
}

func parseWeekDirectoryDate(value string) (time.Time, bool) {
	parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func normalizedAuthMode(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "login"
	}
	return trimmed
}

func normalizedContainer(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "$web"
	}
	return trimmed
}

func loggerFromContext(ctx context.Context) logr.Logger {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return logr.Discard()
	}
	return logger
}

type azBlobUploader struct {
	StorageAccount string
	AuthMode       string
	ContainerName  string
}

func (u azBlobUploader) Upload(ctx context.Context, req BlobUploadRequest) error {
	sourcePath := strings.TrimSpace(req.SourcePath)
	targetPath := strings.TrimSpace(req.TargetPath)
	if sourcePath == "" || targetPath == "" {
		return fmt.Errorf("upload source and target must not be empty")
	}
	args := []string{
		"storage", "blob", "upload",
		"--account-name", u.StorageAccount,
		"--auth-mode", u.AuthMode,
		"--container-name", u.ContainerName,
		"--name", targetPath,
		"--file", sourcePath,
		"--overwrite", "true",
	}
	if contentType := strings.TrimSpace(req.ContentType); contentType != "" {
		args = append(args, "--content-type", contentType)
	}
	cmd := exec.CommandContext(ctx, "az", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("upload blob %q: %w: %s", targetPath, err, strings.TrimSpace(string(output)))
	}
	return nil
}
