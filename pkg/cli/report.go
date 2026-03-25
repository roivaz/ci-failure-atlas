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

	reportreview "ci-failure-atlas/pkg/report/review"
	reportsite "ci-failure-atlas/pkg/report/site"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

func NewReportCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:           "report",
		Short:         "Report generation commands.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	siteBuildDataDirectory := "data"
	siteBuildRoot := "site"
	siteBuildSourceEnvs := []string{"dev", "int", "stg", "prod"}
	siteBuildHistoryWeeks := 4
	siteBuildStartDate := ""
	siteBuildFromExisting := false
	siteBuildPostgres := postgresoptions.DefaultOptions()
	siteBuildCmd := &cobra.Command{
		Use:   "build",
		Short: "Build management site (semantic + reports + indexes).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			postgresValidated, err := siteBuildPostgres.Validate()
			if err != nil {
				return err
			}
			postgresCompleted, err := postgresValidated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			defer postgresCompleted.Cleanup()

			result, err := reportsite.Build(cmd.Context(), reportsite.BuildOptions{
				DataDirectory:      siteBuildDataDirectory,
				SiteRoot:           siteBuildRoot,
				SourceEnvironments: siteBuildSourceEnvs,
				CurrentWeekStart:   siteBuildStartDate,
				HistoryWeeks:       siteBuildHistoryWeeks,
				FromExisting:       siteBuildFromExisting,
				UsePostgres:        postgresCompleted.Enabled,
				PostgresPool:       postgresCompleted.Connection,
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
	if err := postgresoptions.BindOptions(siteBuildPostgres, siteBuildCmd); err != nil {
		return nil, err
	}

	sitePushRoot := "site"
	sitePushStorageAccount := ""
	sitePushAuthMode := "login"
	sitePushContainerName := "$web"
	sitePushDataDirectory := "data"
	sitePushPostgres := postgresoptions.DefaultOptions()
	sitePushCmd := &cobra.Command{
		Use:   "push",
		Short: "Push report site files to an Azure Storage static website container.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			postgresValidated, err := sitePushPostgres.Validate()
			if err != nil {
				return err
			}
			postgresCompleted, err := postgresValidated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			defer postgresCompleted.Cleanup()

			result, err := reportsite.Push(cmd.Context(), reportsite.PushOptions{
				SiteRoot:       sitePushRoot,
				DataDirectory:  sitePushDataDirectory,
				StorageAccount: sitePushStorageAccount,
				AuthMode:       sitePushAuthMode,
				ContainerName:  sitePushContainerName,
				UsePostgres:    postgresCompleted.Enabled,
				PostgresPool:   postgresCompleted.Connection,
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
	sitePushCmd.Flags().StringVar(&sitePushDataDirectory, "storage.ndjson.data-dir", sitePushDataDirectory, "root directory for NDJSON facts/state/semantic data")
	sitePushCmd.Flags().StringVar(&sitePushStorageAccount, "site.storage-account", sitePushStorageAccount, "Azure Storage account name for static website uploads")
	sitePushCmd.Flags().StringVar(&sitePushAuthMode, "site.auth-mode", sitePushAuthMode, "Azure Storage auth mode for uploads (for example: login|key)")
	sitePushCmd.Flags().StringVar(&sitePushContainerName, "site.container", sitePushContainerName, "target blob container name (default: $web)")
	if err := postgresoptions.BindOptions(sitePushPostgres, sitePushCmd); err != nil {
		return nil, err
	}
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
	reviewPostgres := postgresoptions.DefaultOptions()
	reviewCmd := &cobra.Command{
		Use:   "review",
		Short: "Run a local dynamic semantic review app with manual Phase3 linking.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			postgresValidated, err := reviewPostgres.Validate()
			if err != nil {
				return err
			}
			postgresCompleted, err := postgresValidated.Complete(cmd.Context())
			if err != nil {
				return err
			}
			defer postgresCompleted.Cleanup()

			handler, err := reportreview.NewHandler(reportreview.HandlerOptions{
				DataDirectory:        reviewDataDirectory,
				SemanticSubdirectory: reviewSemanticSubdirectory,
				HistoryHorizonWeeks:  reviewHistoryWeeks,
				UsePostgres:          postgresCompleted.Enabled,
				PostgresPool:         postgresCompleted.Connection,
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
	if err := postgresoptions.BindOptions(reviewPostgres, reviewCmd); err != nil {
		return nil, err
	}

	cmd.AddCommand(
		reviewCmd,
		siteCmd,
	)

	return cmd, nil
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
