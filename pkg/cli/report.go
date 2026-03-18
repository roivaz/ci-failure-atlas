package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/ndjsonoptions"
	reportreview "ci-failure-atlas/pkg/report/review"
	reportsite "ci-failure-atlas/pkg/report/site"
	reporttestsummary "ci-failure-atlas/pkg/report/testsummary"
	"ci-failure-atlas/pkg/store/ndjson"
)

func NewReportCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "report",
		Short:         "Report generation commands.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	reportsSubdirectory := ""
	cmd.PersistentFlags().StringVar(
		&reportsSubdirectory,
		"reports.subdir",
		reportsSubdirectory,
		"Optional subdirectory under reports/ for generated report files. Defaults to --storage.ndjson.semantic-subdir when unset.",
	)

	qualityOpts := reporttestsummary.DefaultOptions()
	qualityOpts.OutputPath = "data/reports/semantic-quality.html"
	qualityOpts.Format = "html"
	qualityNDJSONOpts := ndjsonoptions.DefaultOptions()
	qualityCmd := &cobra.Command{
		Use:   "quality",
		Short: "Generate semantic quality HTML report.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			validated, err := qualityNDJSONOpts.Validate()
			if err != nil {
				return err
			}
			completed, err := validated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			store, err := ndjson.NewWithOptions(completed.DataDirectory, ndjson.Options{
				SemanticSubdirectory: completed.SemanticSubdirectory,
			})
			if err != nil {
				return fmt.Errorf("create NDJSON store: %w", err)
			}
			defer func() {
				_ = store.Close()
			}()

			resolvedOutputPath, err := resolveReportOutputPath(
				completed.DataDirectory,
				completed.SemanticSubdirectory,
				reportsSubdirectory,
				qualityOpts.OutputPath,
				cmd.Flags().Changed("output"),
			)
			if err != nil {
				return err
			}
			runOpts := qualityOpts
			runOpts.OutputPath = resolvedOutputPath
			runOpts.Format = "html"
			return reporttestsummary.Generate(cmd.Context(), store, runOpts)
		},
	}
	if err := ndjsonoptions.BindNDJSONOptions(qualityNDJSONOpts, qualityCmd); err != nil {
		return nil, fmt.Errorf("bind NDJSON options for report quality: %w", err)
	}
	qualityCmd.Flags().StringVar(&qualityOpts.OutputPath, "output", qualityOpts.OutputPath, "path to output quality HTML report")
	qualityCmd.Flags().StringVar(&qualityOpts.QualityExportPath, "quality-export", qualityOpts.QualityExportPath, "optional path to write flagged semantic signatures as NDJSON")
	qualityCmd.Flags().StringVar(&qualityOpts.WindowStart, "workflow.window.start", qualityOpts.WindowStart, "inclusive start of report window (RFC3339 or YYYY-MM-DD at 00:00:00Z)")
	qualityCmd.Flags().StringVar(&qualityOpts.WindowEnd, "workflow.window.end", qualityOpts.WindowEnd, "exclusive end of report window (RFC3339 or YYYY-MM-DD at 00:00:00Z)")
	qualityCmd.Flags().IntVar(&qualityOpts.TopTests, "top", qualityOpts.TopTests, "max number of tests to render (0 = all)")
	qualityCmd.Flags().IntVar(&qualityOpts.RecentRuns, "recent", qualityOpts.RecentRuns, "recent failing runs to render per signature")
	qualityCmd.Flags().IntVar(&qualityOpts.MinRuns, "min-runs", qualityOpts.MinRuns, "minimum runs threshold for including a test")
	qualityCmd.Flags().StringSliceVar(&qualityOpts.Environments, "source.envs", qualityOpts.Environments, "environments to include (e.g. dev,int,stg,prod)")
	qualityCmd.Flags().BoolVar(&qualityOpts.SplitByEnvironment, "split-by-env", qualityOpts.SplitByEnvironment, "write one output file per environment using <output>.<env>.<ext>")

	siteBuildDataDirectory := "data"
	siteBuildRoot := "site"
	siteBuildSourceEnvs := []string{"dev", "int", "stg", "prod"}
	siteBuildHistoryWeeks := 4
	siteBuildStartDate := ""
	siteBuildFromExisting := false
	siteBuildCmd := &cobra.Command{
		Use:   "build",
		Short: "Build management site (semantic + reports + indexes).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			result, err := reportsite.Build(cmd.Context(), reportsite.BuildOptions{
				DataDirectory:      siteBuildDataDirectory,
				SiteRoot:           siteBuildRoot,
				SourceEnvironments: siteBuildSourceEnvs,
				CurrentWeekStart:   siteBuildStartDate,
				HistoryWeeks:       siteBuildHistoryWeeks,
				FromExisting:       siteBuildFromExisting,
			})
			if err != nil {
				return err
			}
			cmd.Printf(
				"Built site under %s (weeks=%d, latest=%s)\n",
				result.SiteRoot,
				len(result.Weeks),
				result.LatestWeek,
			)
			return nil
		},
	}
	siteBuildCmd.Flags().StringVar(&siteBuildDataDirectory, "storage.ndjson.data-dir", siteBuildDataDirectory, "root directory for NDJSON facts/state/semantic data")
	siteBuildCmd.Flags().StringVar(&siteBuildRoot, "site.root", siteBuildRoot, "root directory for generated site HTML")
	siteBuildCmd.Flags().StringSliceVar(&siteBuildSourceEnvs, "source.envs", siteBuildSourceEnvs, "environments to include (e.g. dev,int,stg,prod)")
	siteBuildCmd.Flags().IntVar(&siteBuildHistoryWeeks, "history.weeks", siteBuildHistoryWeeks, "number of weekly 7-day windows to build and use as scoring/history horizon")
	siteBuildCmd.Flags().StringVar(&siteBuildStartDate, "start-date", siteBuildStartDate, "current week start date (YYYY-MM-DD). Defaults to latest Sunday.")
	siteBuildCmd.Flags().BoolVar(&siteBuildFromExisting, "from-existing", siteBuildFromExisting, "rebuild reports and indexes from existing semantic snapshots in data/semantic (skips data ingestion workflow)")

	sitePushRoot := "site"
	sitePushStorageAccount := ""
	sitePushAuthMode := "login"
	sitePushContainerName := "$web"
	sitePushCmd := &cobra.Command{
		Use:   "push",
		Short: "Push report site files to an Azure Storage static website container.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			result, err := reportsite.Push(cmd.Context(), reportsite.PushOptions{
				SiteRoot:       sitePushRoot,
				StorageAccount: sitePushStorageAccount,
				AuthMode:       sitePushAuthMode,
				ContainerName:  sitePushContainerName,
			})
			if err != nil {
				return err
			}
			cmd.Printf(
				"Pushed report site to storage account %s (weeks=%d, files=%d, latest=%s)\n",
				sitePushStorageAccount,
				result.WeeksUploaded,
				result.FilesUploaded,
				result.LatestWeek,
			)
			return nil
		},
	}
	sitePushCmd.Flags().StringVar(&sitePushRoot, "site.root", sitePushRoot, "root directory for generated site HTML")
	sitePushCmd.Flags().StringVar(&sitePushStorageAccount, "site.storage-account", sitePushStorageAccount, "Azure Storage account name for static website uploads")
	sitePushCmd.Flags().StringVar(&sitePushAuthMode, "site.auth-mode", sitePushAuthMode, "Azure Storage auth mode for uploads (for example: login|key)")
	sitePushCmd.Flags().StringVar(&sitePushContainerName, "site.container", sitePushContainerName, "target blob container name (default: $web)")
	if err := sitePushCmd.MarkFlagRequired("site.storage-account"); err != nil {
		return nil, fmt.Errorf("mark --site.storage-account required for report site push: %w", err)
	}

	siteRunRoot := "site"
	siteRunListen := "127.0.0.1:8080"
	siteRunCmd := &cobra.Command{
		Use:   "run",
		Short: "Run a local static server for the generated report site.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			root := strings.TrimSpace(siteRunRoot)
			if root == "" {
				root = "site"
			}
			rootInfo, err := os.Stat(root)
			if err != nil {
				if os.IsNotExist(err) {
					return fmt.Errorf("site root %q does not exist; run `cfa report site build` first", root)
				}
				return fmt.Errorf("stat site root %q: %w", root, err)
			}
			if !rootInfo.IsDir() {
				return fmt.Errorf("site root %q must be a directory", root)
			}
			absoluteRoot, err := filepath.Abs(root)
			if err != nil {
				return fmt.Errorf("resolve absolute site root %q: %w", root, err)
			}

			listenAddress := strings.TrimSpace(siteRunListen)
			if listenAddress == "" {
				listenAddress = "127.0.0.1:8080"
			}
			listener, err := net.Listen("tcp", listenAddress)
			if err != nil {
				return fmt.Errorf("listen on %q: %w", listenAddress, err)
			}
			defer func() {
				_ = listener.Close()
			}()

			server := &http.Server{
				Handler: http.FileServer(http.Dir(absoluteRoot)),
			}
			cmd.Printf(
				"Serving %s at %s (Ctrl+C to stop)\n",
				absoluteRoot,
				siteRunURLFromListenAddress(listenAddress),
			)

			go func() {
				<-cmd.Context().Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(shutdownCtx)
			}()
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("serve static site: %w", err)
			}
			return nil
		},
	}
	siteRunCmd.Flags().StringVar(&siteRunRoot, "site.root", siteRunRoot, "root directory for generated site HTML")
	siteRunCmd.Flags().StringVar(&siteRunListen, "site.listen", siteRunListen, "listen address for local static server (host:port)")

	siteCmd := &cobra.Command{
		Use:   "site",
		Short: "Report static website helper commands.",
	}
	siteCmd.AddCommand(siteBuildCmd, sitePushCmd, siteRunCmd)

	reviewDataDirectory := "data"
	reviewSemanticSubdirectory := ""
	reviewListen := "127.0.0.1:8081"
	reviewHistoryWeeks := 4
	reviewCmd := &cobra.Command{
		Use:   "review",
		Short: "Run a local dynamic semantic review app with manual Phase3 linking.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			handler, err := reportreview.NewHandler(reportreview.HandlerOptions{
				DataDirectory:        reviewDataDirectory,
				SemanticSubdirectory: reviewSemanticSubdirectory,
				HistoryHorizonWeeks:  reviewHistoryWeeks,
			})
			if err != nil {
				return err
			}
			listenAddress := strings.TrimSpace(reviewListen)
			if listenAddress == "" {
				listenAddress = "127.0.0.1:8081"
			}
			listener, err := net.Listen("tcp", listenAddress)
			if err != nil {
				return fmt.Errorf("listen on %q: %w", listenAddress, err)
			}
			defer func() {
				_ = listener.Close()
			}()

			server := &http.Server{Handler: handler}
			cmd.Printf(
				"Serving semantic review app at %s (Ctrl+C to stop)\n",
				siteRunURLFromListenAddress(listenAddress),
			)

			go func() {
				<-cmd.Context().Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(shutdownCtx)
			}()
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("serve semantic review app: %w", err)
			}
			return nil
		},
	}
	reviewCmd.Flags().StringVar(&reviewDataDirectory, "storage.ndjson.data-dir", reviewDataDirectory, "root directory for NDJSON facts/state/semantic data")
	reviewCmd.Flags().StringVar(&reviewSemanticSubdirectory, "storage.ndjson.semantic-subdir", reviewSemanticSubdirectory, "default semantic snapshot subdirectory (for example: 2026-03-15)")
	reviewCmd.Flags().StringVar(&reviewListen, "site.listen", reviewListen, "listen address for local semantic review app (host:port)")
	reviewCmd.Flags().IntVar(&reviewHistoryWeeks, "history.weeks", reviewHistoryWeeks, "number of most recent semantic weeks used for report history and cross-week phase3 propagation")

	cmd.AddCommand(
		qualityCmd,
		reviewCmd,
		siteCmd,
	)

	return cmd, nil
}

func resolveReportOutputPath(
	dataDirectory string,
	semanticSubdirectory string,
	reportSubdirectory string,
	outputPath string,
	outputFlagChanged bool,
) (string, error) {
	trimmedOutputPath := strings.TrimSpace(outputPath)
	if trimmedOutputPath == "" {
		return "", fmt.Errorf("report output path must not be empty")
	}
	if outputFlagChanged {
		return trimmedOutputPath, nil
	}

	effectiveSubdirectory := strings.TrimSpace(reportSubdirectory)
	if effectiveSubdirectory == "" {
		effectiveSubdirectory = strings.TrimSpace(semanticSubdirectory)
	}
	normalizedSubdirectory, err := normalizeReportSubdirectory(effectiveSubdirectory)
	if err != nil {
		return "", fmt.Errorf("invalid --reports.subdir: %w", err)
	}

	baseName := strings.TrimSpace(filepath.Base(trimmedOutputPath))
	if baseName == "" || baseName == "." {
		return "", fmt.Errorf("invalid report output file name %q", trimmedOutputPath)
	}
	if normalizedSubdirectory == "" {
		return filepath.Join(dataDirectory, "reports", baseName), nil
	}
	return filepath.Join(dataDirectory, "reports", normalizedSubdirectory, baseName), nil
}

func normalizeReportSubdirectory(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", nil
	}
	cleaned := filepath.Clean(trimmed)
	if cleaned == "." {
		return "", nil
	}
	if filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("must be a relative path")
	}
	parts := strings.Split(cleaned, string(filepath.Separator))
	for _, part := range parts {
		switch part {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("must not contain '..'")
		}
	}
	return cleaned, nil
}

func siteRunURLFromListenAddress(listenAddress string) string {
	trimmed := strings.TrimSpace(listenAddress)
	if trimmed == "" {
		return "http://127.0.0.1:8080"
	}
	host, port, err := net.SplitHostPort(trimmed)
	if err != nil {
		return "http://" + trimmed
	}
	normalizedHost := strings.Trim(strings.TrimSpace(host), "[]")
	if normalizedHost == "" || normalizedHost == "0.0.0.0" || normalizedHost == "::" {
		normalizedHost = "localhost"
	}
	return fmt.Sprintf("http://%s:%s", normalizedHost, port)
}
